"""RedisBroker: Streams + consumer group wiring, all against a mocked client.

Regression coverage for bug #8 (BRPOP's at-most-once semantics): every assertion
here is about which Streams primitives (XADD/XREADGROUP/XACK/XDEL) our code calls
and with what arguments — that a crashed consumer's message is actually still
recoverable is a real-concurrency property, covered in test_redis_container.py.
"""

from collections.abc import Mapping
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

    def _entry(
        self,
        *,
        topic: str = TOPIC,
        message_id: bytes = b"1-0",
        body: bytes = b"payload",
        attempt: int = 1,
        headers: Mapping[bytes, bytes] | None = None,
    ) -> StreamEntry:
        """Build a StreamEntry, overriding whichever fields a test cares about."""
        return StreamEntry(
            topic=topic,
            message_id=message_id,
            body=body,
            attempt=attempt,
            headers=headers,
        )

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


class TestPendingReclaim:
    """A message whose consumer died before acking must be redelivered to someone."""

    async def test_consume_reclaims_before_reading_new_work(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """XREADGROUP with `>` returns only never-delivered entries.

        A consumer's own pending list is reachable only via an explicit id or
        XAUTOCLAIM, so without this a message read by a consumer that then crashed
        sat in its PEL forever — at-most-once, in a broker declaring at-least-once.
        """
        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(b"5-1", {RedisBroker.BODY_FIELD: b"abandoned", RedisBroker.ATTEMPT_FIELD: b"2"})],
            [],
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.body == b"abandoned"
        assert delivery.attempt == 2
        mock_client.xreadgroup.assert_not_awaited()

    async def test_reclaim_asks_for_this_consumer_and_the_configured_idle_window(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Claiming transfers ownership, so it must name this consumer explicitly."""
        mock_client.xautoclaim.return_value = [b"0-0", [], []]
        mock_client.xreadgroup.return_value = [
            (TOPIC, [(b"9-1", {RedisBroker.BODY_FIELD: b"fresh"})]),
        ]

        await anext(broker.consume(TOPIC))

        args, kwargs = mock_client.xautoclaim.await_args
        assert args[0] == TOPIC
        assert args[1] == broker.group
        assert args[2] == broker.consumer_name
        assert args[3] == RedisBroker.DEFAULT_RECLAIM_IDLE_MS
        assert kwargs["count"] == 1

    async def test_nothing_to_reclaim_falls_through_to_a_normal_read(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """The common case must not cost a delivery — reclaim returning empty is normal."""
        mock_client.xautoclaim.return_value = [b"0-0", [], []]
        mock_client.xreadgroup.return_value = [
            (TOPIC, [(b"9-1", {RedisBroker.BODY_FIELD: b"fresh"})]),
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.body == b"fresh"
        mock_client.xreadgroup.assert_awaited_once()

    async def test_a_two_element_reply_from_redis_6_is_handled(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """XAUTOCLAIM only gained its third `deleted` element in Redis 7."""
        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(b"5-1", {RedisBroker.BODY_FIELD: b"old server"})],
        ]

        delivery = await anext(broker.consume(TOPIC))

        assert delivery.body == b"old server"


class TestReclaimSkipsOwnInFlightWork:
    """XAUTOCLAIM matches on idle time alone, with no regard for who owns the entry."""

    async def test_a_message_this_consumer_is_still_handling_is_not_re_yielded(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A handler slower than reclaim_idle_ms had its own message handed back.

        It would then be processed a second time, concurrently with the first —
        self-duplication rather than the cross-consumer recovery reclaim is for.
        """
        entry = (b"5-1", {RedisBroker.BODY_FIELD: b"slow"})
        mock_client.xautoclaim.return_value = [b"0-0", [entry], []]
        mock_client.xreadgroup.return_value = [
            (TOPIC, [(b"9-1", {RedisBroker.BODY_FIELD: b"fresh"})]),
        ]

        consumer = broker.consume(TOPIC)
        first = await anext(consumer)
        assert first.body == b"slow"

        # `5-1` is still unacked, so the next poll must not hand it back.
        second = await anext(consumer)

        assert second.body == b"fresh"

    async def test_a_settled_message_becomes_reclaimable_again(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """The guard is about *in-flight* work only — acking releases the id."""
        entry = (b"5-1", {RedisBroker.BODY_FIELD: b"slow"})
        mock_client.xautoclaim.return_value = [b"0-0", [entry], []]

        consumer = broker.consume(TOPIC)
        first = await anext(consumer)
        await first.ack()

        second = await anext(consumer)

        assert second.body == b"slow"


class TestReclaimGuardIsScopedToItsTopic:
    """A stream id is unique only within its own stream, and one broker serves many."""

    async def test_the_same_id_on_another_topic_is_still_reclaimable(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """Two topics can hand out the same id in the same millisecond.

        Keyed by id alone, one topic's in-flight entry masked the other's — and
        retiring the first stripped the guard from the second while it was still
        live, letting it be reclaimed and processed concurrently with itself.
        """
        shared_id = b"1700000000000-0"
        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(shared_id, {RedisBroker.BODY_FIELD: b"topic-a"})],
            [],
        ]
        first = await anext(broker.consume(TOPIC))
        assert first.body == b"topic-a"

        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(shared_id, {RedisBroker.BODY_FIELD: b"topic-b"})],
            [],
        ]
        second = await anext(broker.consume("other.topic"))

        assert second.body == b"topic-b"


class TestReclaimGuardIsReleasedEvenOnFailure:
    """A guard entry that outlives its delivery makes that message unreclaimable."""

    async def test_a_failing_nack_still_releases_the_guard(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """`Worker._safe_retry_or_drop` logs a failing nack rather than raising.

        Releasing only on success left the id in `_inflight_ids` forever, so
        `_reclaim` skipped that pending entry on every future pass — with a single
        consumer, the message is stuck until the process restarts.
        """
        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(b"5-1", {RedisBroker.BODY_FIELD: b"body"})],
            [],
        ]
        delivery = await anext(broker.consume(TOPIC))
        assert (TOPIC, b"5-1") in broker._inflight_ids
        mock_client.xadd.side_effect = ConnectionError("broker gone")

        with pytest.raises(ConnectionError):
            await delivery.nack(requeue=True)

        assert (TOPIC, b"5-1") not in broker._inflight_ids

    async def test_a_successful_ack_releases_the_guard(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """The ordinary path must keep working."""
        mock_client.xautoclaim.return_value = [
            b"0-0",
            [(b"5-1", {RedisBroker.BODY_FIELD: b"body"})],
            [],
        ]
        delivery = await anext(broker.consume(TOPIC))

        await delivery.ack()

        assert (TOPIC, b"5-1") not in broker._inflight_ids


class TestDeadLetterChainTerminates:
    """Every other broker in the package stops at one level; this one did not."""

    async def test_dead_lettering_from_a_dlq_stream_writes_nothing(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """A worker reprocessing `orders.dlq` would otherwise create `orders.dlq.dlq`."""
        entry = StreamEntry(f"{TOPIC}{RedisBroker.DLQ_SUFFIX}", b"1-1", b"body", 1)

        await broker.deadletter(entry)

        mock_client.xadd.assert_not_awaited()
        mock_client.xack.assert_awaited_once()

    async def test_dead_lettering_from_a_normal_stream_still_writes(
        self,
        broker: RedisBroker,
        mock_client: AsyncMock,
    ) -> None:
        """The guard must only catch the terminal case."""
        await broker.deadletter(StreamEntry(TOPIC, b"1-1", b"body", 1))

        mock_client.xadd.assert_awaited_once()
        assert mock_client.xadd.await_args.args[0] == f"{TOPIC}{RedisBroker.DLQ_SUFFIX}"
