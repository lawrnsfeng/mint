"""RedisBroker: Streams + consumer group wiring, all against a mocked client.

Regression coverage for bug #8 (BRPOP's at-most-once semantics): every assertion
here is about which Streams primitives (XADD/XREADGROUP/XACK/XDEL) our code calls
and with what arguments — that a crashed consumer's message is actually still
recoverable is a real-concurrency property, covered in test_redis_container.py.
"""

from dataclasses import replace
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ResponseError

from mint.worker.brokers.redis import RedisBroker, RedisStreamDelivery, StreamEntry
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

TOPIC = "sync.reference"


@pytest.fixture
def mock_client(mocker: "MockerFixture") -> AsyncMock:
    """Return a mocked async Redis client double.

    Deliberately NOT ``spec=Redis``, for the same verified reason as
    ``test_redis_mocked.py`` under ``stores/``: redis-py's command methods aren't
    detected as coroutine functions by ``inspect.iscoroutinefunction``.
    """
    return mocker.AsyncMock()


@pytest.fixture
def broker(mock_client: AsyncMock) -> RedisBroker:
    """Return a broker wired directly to the mock client, skipping real connection setup."""
    instance = RedisBroker("redis://fake", consumer_name="consumer-1")
    instance._client = mock_client
    return instance


class TestEnsureGroup:
    """_ensure_group must create the consumer group, tolerating BUSYGROUP but nothing else."""

    async def test_publish_creates_the_group_with_mkstream(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A fresh topic must get its stream and group created together."""
        await broker.publish(TOPIC, b"body")

        mock_client.xgroup_create.assert_awaited_once_with(
            TOPIC,
            RedisBroker.DEFAULT_GROUP,
            id="0",
            mkstream=True,
        )

    async def test_busygroup_error_is_swallowed(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """The group already existing (BUSYGROUP) must not be treated as a failure."""
        mock_client.xgroup_create.side_effect = ResponseError("BUSYGROUP Consumer Group exists")

        await broker.publish(TOPIC, b"body")  # must not raise

    async def test_other_response_errors_propagate(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Any other ResponseError must not be silently swallowed."""
        mock_client.xgroup_create.side_effect = ResponseError("WRONGTYPE not a stream")

        with pytest.raises(ResponseError, match="WRONGTYPE"):
            await broker.publish(TOPIC, b"body")


class TestPublish:
    """publish() must XADD body + attempt=1, merging in any headers."""

    async def test_publish_adds_body_and_attempt_one(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A fresh publish must always start at attempt 1."""
        await broker.publish(TOPIC, b"payload")

        mock_client.xadd.assert_awaited_once_with(
            TOPIC,
            {RedisBroker.BODY_FIELD: b"payload", RedisBroker.ATTEMPT_FIELD: 1},
        )

    async def test_publish_merges_headers_into_the_fields(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Headers must ride along as extra stream fields, not be dropped."""
        await broker.publish(TOPIC, b"payload", headers={"trace": "abc"})

        _, fields = mock_client.xadd.await_args.args
        assert fields["trace"] == "abc"
        assert fields[RedisBroker.BODY_FIELD] == b"payload"


class TestConsume:
    """consume() must decode XREADGROUP responses into deliveries, skipping empty polls."""

    async def test_consume_yields_a_decoded_delivery(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """body/attempt must be pulled from their fields; everything else becomes headers."""
        mock_client.xreadgroup.return_value = [
            (
                TOPIC.encode(),
                [(b"1-0", {RedisBroker.BODY_FIELD: b"payload", RedisBroker.ATTEMPT_FIELD: b"2"})],
            ),
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert isinstance(delivery, RedisStreamDelivery)
        assert delivery.body == b"payload"
        assert delivery.attempt == 2
        assert delivery.headers is None

    async def test_consume_extracts_extra_fields_as_headers(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Any field besides body/attempt must be surfaced as a header, not dropped."""
        mock_client.xreadgroup.return_value = [
            (
                TOPIC.encode(),
                [
                    (
                        b"1-0",
                        {
                            RedisBroker.BODY_FIELD: b"payload",
                            RedisBroker.ATTEMPT_FIELD: b"1",
                            b"trace": b"abc",
                        },
                    ),
                ],
            ),
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.headers == {b"trace": b"abc"}

    async def test_consume_uses_the_configured_group_and_consumer_name(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """XREADGROUP must be called with this broker's own group/consumer identity."""
        mock_client.xreadgroup.return_value = [(TOPIC.encode(), [(b"1-0", {})])]

        await anext(broker.consume(TOPIC))

        mock_client.xreadgroup.assert_awaited_once_with(
            groupname=RedisBroker.DEFAULT_GROUP,
            consumername="consumer-1",
            streams={TOPIC: ">"},
            count=1,
            block=RedisBroker.DEFAULT_BLOCK_MS,
        )

    async def test_consume_skips_empty_polls_without_yielding(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A timed-out (empty) poll must not yield a phantom delivery — it must poll again."""
        mock_client.xreadgroup.side_effect = [
            [],  # empty poll: block timed out with nothing new
            [(TOPIC.encode(), [(b"1-0", {RedisBroker.BODY_FIELD: b"payload"})])],
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.body == b"payload"
        assert mock_client.xreadgroup.await_count == 2

    async def test_consume_skips_a_stream_entry_with_no_messages(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A non-empty response whose stream carries zero messages must not yield or raise."""
        mock_client.xreadgroup.side_effect = [
            [(TOPIC.encode(), [])],  # stream present, but nothing claimed this poll
            [(TOPIC.encode(), [(b"1-0", {RedisBroker.BODY_FIELD: b"payload"})])],
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.body == b"payload"
        assert mock_client.xreadgroup.await_count == 2


class TestAckNack:
    """RedisStreamDelivery.ack()/nack() must map onto XACK/XDEL/XADD correctly."""

    def _entry(self, **overrides: object) -> StreamEntry:
        default = StreamEntry(topic=TOPIC, message_id=b"1-0", body=b"payload", attempt=1)
        return replace(default, **overrides)

    async def test_ack_acks_and_deletes(self, broker: RedisBroker, mock_client: AsyncMock) -> None:
        """ack() must XACK then XDEL the same entry, and nothing else."""
        delivery = RedisStreamDelivery(broker, self._entry())

        await delivery.ack()

        mock_client.xack.assert_awaited_once_with(TOPIC, RedisBroker.DEFAULT_GROUP, b"1-0")
        mock_client.xdel.assert_awaited_once_with(TOPIC, b"1-0")
        mock_client.xadd.assert_not_awaited()

    async def test_nack_requeue_true_bumps_attempt_and_re_adds(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A requeued nack must ack the old entry, then XADD with attempt incremented."""
        delivery = RedisStreamDelivery(broker, self._entry(attempt=1))

        await delivery.nack(requeue=True)

        mock_client.xack.assert_awaited_once_with(TOPIC, RedisBroker.DEFAULT_GROUP, b"1-0")
        mock_client.xdel.assert_awaited_once_with(TOPIC, b"1-0")
        _, fields = mock_client.xadd.await_args.args
        assert fields[RedisBroker.ATTEMPT_FIELD] == 2
        assert fields[RedisBroker.BODY_FIELD] == b"payload"

    async def test_nack_requeue_true_preserves_headers(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Headers on the original entry must survive a requeue, not be dropped."""
        delivery = RedisStreamDelivery(broker, self._entry(headers={b"trace": b"abc"}))

        await delivery.nack(requeue=True)

        _, fields = mock_client.xadd.await_args.args
        assert fields[b"trace"] == b"abc"

    async def test_nack_requeue_false_routes_to_the_dead_letter_stream(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Regression for bug #8's spirit: a dropped message must land somewhere inspectable."""
        delivery = RedisStreamDelivery(broker, self._entry())

        await delivery.nack(requeue=False)

        topic, fields = mock_client.xadd.await_args.args
        assert topic == f"{TOPIC}{RedisBroker.DLQ_SUFFIX}"
        assert fields[RedisBroker.BODY_FIELD] == b"payload"


class TestGuaranteeAndClose:
    """Declared delivery guarantee and resource cleanup."""

    def test_declares_at_least_once_guarantee(self) -> None:
        """RedisBroker is at-least-once, unlike the original BRPOP implementation."""
        assert RedisBroker.guarantee == DeliveryGuarantee.AT_LEAST_ONCE

    async def test_close_calls_client_close(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """An already-created client must be closed."""
        await broker.close()

        mock_client.aclose.assert_awaited_once()

    async def test_close_before_any_connection_is_a_no_op(self) -> None:
        """A broker that never touched Redis must not construct a client just to close it."""
        broker = RedisBroker("redis://fake")

        await broker.close()  # must not raise

    async def test_client_property_constructs_via_from_url(self, mocker: "MockerFixture") -> None:
        """A broker that never had its client injected must build one from its uri."""
        mock_from_url = mocker.patch(
            "mint.worker.brokers.redis.Redis.from_url",
            return_value=mocker.AsyncMock(),
        )
        broker = RedisBroker("redis://example:6379/0")

        client = broker.client

        mock_from_url.assert_called_once_with("redis://example:6379/0")
        assert client is broker.client  # cached, not reconstructed


class TestRedeliveryOrdering:
    """The replacement entry must be written before the original is retired."""

    @staticmethod
    def _call_order(mock_client: AsyncMock) -> list[str]:
        """Return the client method names in the order they were awaited."""
        return [name for name, *_ in mock_client.mock_calls if name in {"xadd", "xack", "xdel"}]

    async def test_requeue_adds_the_new_entry_before_acking_the_old(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """XACK/XDEL first left a window where a crash lost the message outright.

        That is precisely the at-most-once behaviour bug #8's Streams rewrite
        exists to eliminate; writing first means a crash duplicates instead, which
        the engine's idempotent fan-in already absorbs.
        """
        delivery = RedisStreamDelivery(broker, StreamEntry(TOPIC, b"1-1", b"body", 1))

        await delivery.nack(requeue=True)

        assert self._call_order(mock_client) == ["xadd", "xack", "xdel"]

    async def test_dead_letter_adds_to_the_dlq_before_acking_the_old(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Same ordering on the dead-letter path — evidence must be written before it's dropped."""
        delivery = RedisStreamDelivery(broker, StreamEntry(TOPIC, b"1-1", b"body", 1))

        await delivery.nack(requeue=False)

        assert self._call_order(mock_client) == ["xadd", "xack", "xdel"]
