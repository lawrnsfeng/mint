"""NatsBroker: per-topic durable consumer naming and ack/nack wiring, against a mocked client.

Regression coverage for bug #10 (one hardcoded durable name shared by every topic):
every assertion here is about what our code calls on the client — that two
different topics' consumers genuinely don't fight over the same JetStream cursor
is a real-broker property, covered separately in a container test.
"""

from itertools import product
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from mint.worker.brokers.nats import NatsBroker, NatsDelivery
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

TOPIC_A = "sync.reference"
TOPIC_B = "sync.folder"


@pytest.fixture
def mock_jetstream(mocker: "MockerFixture") -> AsyncMock:
    """Return a mocked JetStreamContext double."""
    return mocker.AsyncMock()


@pytest.fixture
def mock_client(mocker: "MockerFixture", mock_jetstream: AsyncMock) -> AsyncMock:
    """Return a mocked NATS client whose jetstream() returns mock_jetstream."""
    client = mocker.AsyncMock()
    client.jetstream = mocker.MagicMock(return_value=mock_jetstream)  # jetstream() is sync
    return client


@pytest.fixture
def broker(mock_client: AsyncMock, mock_jetstream: AsyncMock) -> NatsBroker:
    """Return a broker pre-wired to the mocked client/jetstream, skipping real connect()."""
    instance = NatsBroker("nats://fake")
    instance._client = mock_client
    instance._jetstream = mock_jetstream
    return instance


def fake_msg(
    mocker: "MockerFixture",
    *,
    subject: str,
    data: bytes,
    num_delivered: int = 1,
) -> AsyncMock:
    """Build a fake nats.aio.msg.Msg double with the fields NatsDelivery reads."""
    msg = mocker.AsyncMock()
    msg.subject = subject
    msg.data = data
    msg.headers = None
    msg.metadata = mocker.MagicMock(num_delivered=num_delivered)
    return msg


class TestDurableNaming:
    """Regression for bug #10: the durable consumer name must differ per topic."""

    def test_durable_name_differs_between_topics(self, broker: NatsBroker) -> None:
        """Two distinct topics must never resolve to the same durable name."""
        assert broker._durable_name(TOPIC_A) != broker._durable_name(TOPIC_B)

    def test_durable_name_is_stable_for_the_same_topic(self, broker: NatsBroker) -> None:
        """The same topic must always resolve to the same durable name."""
        assert broker._durable_name(TOPIC_A) == broker._durable_name(TOPIC_A)

    async def test_consume_subscribes_with_the_per_topic_durable_name(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """pull_subscribe must actually receive the per-topic durable name, not a constant."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload")
        mock_jetstream.pull_subscribe.return_value.fetch.return_value = [msg]

        await anext(broker.consume(TOPIC_A))

        mock_jetstream.pull_subscribe.assert_awaited_once_with(
            subject=TOPIC_A,
            durable=broker._durable_name(TOPIC_A),
        )


class TestPublish:
    """publish() must ensure the stream exists, then publish through JetStream."""

    async def test_publish_ensures_the_stream_then_publishes(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
    ) -> None:
        """add_stream must be called before publish, covering both the topic and its DLQ."""
        await broker.publish(TOPIC_A, b"payload")

        mock_jetstream.add_stream.assert_awaited_once()
        _, kwargs = mock_jetstream.add_stream.await_args
        assert kwargs["subjects"] == [TOPIC_A, f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}"]
        mock_jetstream.publish.assert_awaited_once_with(TOPIC_A, b"payload", headers=None)

    async def test_publish_passes_headers_through(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
    ) -> None:
        """Headers must reach JetStream's publish call, not be dropped."""
        await broker.publish(TOPIC_A, b"payload", headers={"trace": "abc"})

        mock_jetstream.publish.assert_awaited_once_with(
            TOPIC_A,
            b"payload",
            headers={"trace": "abc"},
        )


class TestConsume:
    """consume() must yield decoded deliveries and retry silently on a timed-out poll."""

    async def test_consume_yields_a_delivery(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A fetched message must come back as a NatsDelivery with body/attempt set."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload", num_delivered=3)
        mock_jetstream.pull_subscribe.return_value.fetch.return_value = [msg]

        delivery = await anext(broker.consume(TOPIC_A))

        assert isinstance(delivery, NatsDelivery)
        assert delivery.body == b"payload"
        assert delivery.attempt == 3

    async def test_consume_retries_silently_on_fetch_timeout(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A timed-out poll (no new messages) must not raise or yield — just poll again."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload")
        mock_jetstream.pull_subscribe.return_value.fetch.side_effect = [TimeoutError, [msg]]

        delivery = await anext(broker.consume(TOPIC_A))

        assert delivery.body == b"payload"
        assert mock_jetstream.pull_subscribe.return_value.fetch.call_count == 2

    async def test_consume_polls_again_on_an_empty_fetch(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A fetch that returns no messages at all (no timeout raised) must poll again."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload")
        mock_jetstream.pull_subscribe.return_value.fetch.side_effect = [[], [msg]]

        delivery = await anext(broker.consume(TOPIC_A))

        assert delivery.body == b"payload"
        assert mock_jetstream.pull_subscribe.return_value.fetch.call_count == 2


class TestAckNack:
    """NatsDelivery.ack()/nack() must map onto Msg.ack()/nak()/term() correctly."""

    async def test_ack_calls_msg_ack(self, broker: NatsBroker, mocker: "MockerFixture") -> None:
        """ack() must call the underlying message's ack()."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload")
        delivery = NatsDelivery(broker, msg)

        await delivery.ack()

        msg.ack.assert_awaited_once()

    async def test_nack_requeue_true_calls_nak(
        self,
        broker: NatsBroker,
        mocker: "MockerFixture",
    ) -> None:
        """A requeued nack must call nak(), not term()."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"payload")
        delivery = NatsDelivery(broker, msg)

        await delivery.nack(requeue=True)

        msg.nak.assert_awaited_once()
        msg.term.assert_not_awaited()

    async def test_nack_requeue_false_deadletters_then_terminates(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A dropped message must be published to its DLQ subject, then terminated."""
        msg = fake_msg(mocker, subject=TOPIC_A, data=b"doomed")
        delivery = NatsDelivery(broker, msg)

        await delivery.nack(requeue=False)

        mock_jetstream.publish.assert_awaited_once_with(
            f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}",
            b"doomed",
            headers=None,
        )
        msg.term.assert_awaited_once()
        msg.nak.assert_not_awaited()


class TestGuaranteeAndClose:
    """Declared delivery guarantee, lazy connect, and resource cleanup."""

    def test_declares_at_least_once_guarantee(self) -> None:
        """NatsBroker is at-least-once."""
        assert NatsBroker.guarantee == DeliveryGuarantee.AT_LEAST_ONCE

    async def test_connect_is_cached_across_calls(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A second _connect() must not reconnect — the jetstream context is reused."""
        mock_client = mocker.AsyncMock()
        mock_jetstream = mocker.AsyncMock()
        mock_client.jetstream = mocker.MagicMock(return_value=mock_jetstream)
        mock_connect = mocker.patch(
            "mint.worker.brokers.nats.connect",
            return_value=mock_client,
        )
        broker = NatsBroker("nats://fake")

        first = await broker._connect()
        second = await broker._connect()

        assert first is second is mock_jetstream
        mock_connect.assert_awaited_once_with("nats://fake")

    async def test_close_calls_client_close(
        self,
        broker: NatsBroker,
        mock_client: AsyncMock,
    ) -> None:
        """An already-connected client must be closed."""
        await broker.close()

        mock_client.close.assert_awaited_once()

    async def test_close_before_any_connection_is_a_no_op(self) -> None:
        """A broker that never connected must not construct a client just to close it."""
        broker = NatsBroker("nats://fake")

        await broker.close()  # must not raise


class TestStreamNamingIsInjective:
    """Two distinct topics must never collapse onto one stream or one durable name."""

    def test_a_dotted_and_a_dashed_topic_get_different_stream_names(
        self,
        broker: NatsBroker,
    ) -> None:
        """A plain `.` -> `-` replace mapped `a.b` and `a-b` onto the same stream.

        That is bug #10's cursor sharing reached a different way: one stream and
        one durable name shared by two logically unrelated topics.
        """
        assert broker._stream_name("a.b") != broker._stream_name("a-b")

    def test_topics_mixing_dots_and_dashes_do_not_collide(self, broker: NatsBroker) -> None:
        """Escaping `-` as `--` isn't injective either — dash runs become ambiguous.

        `a-.b` and `a.-b` both encoded to `a---b`, so the first attempt at fixing
        the collision above simply moved it to a less obvious pair of inputs.
        """
        assert broker._stream_name("a-.b") != broker._stream_name("a.-b")

    def test_the_encoding_is_injective_over_every_short_topic(
        self,
        broker: NatsBroker,
    ) -> None:
        """Exhaustive over the characters that interact: separators plus the tag chars."""
        encoded: dict[str, str] = {}
        for length in range(1, 6):
            for chars in product(".-hda", repeat=length):
                topic = "".join(chars)
                name = broker._stream_name(topic)
                assert encoded.setdefault(name, topic) == topic

    def test_a_dotted_and_a_dashed_topic_get_different_durable_names(
        self,
        broker: NatsBroker,
    ) -> None:
        """The durable name derives from the stream name, so it inherits the collision."""
        assert broker._durable_name("a.b") != broker._durable_name("a-b")

    def test_stream_names_never_contain_a_dot(self, broker: NatsBroker) -> None:
        """JetStream stream names can't contain `.`, which is why the mapping exists at all."""
        assert "." not in broker._stream_name("deeply.nested.topic-with-dashes")


class TestDeadLetterStreamOwnership:
    """A `.dlq` subject belongs to its parent's stream, not a stream of its own."""

    async def test_consuming_a_dlq_topic_reuses_its_parents_stream(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
    ) -> None:
        """Declaring `foo-dlq` over `foo.dlq` overlaps `foo`'s stream, which JetStream rejects.

        The same terminate-the-chain rule RabbitMQ needs for bug #21: a
        dead-letter destination ends the chain instead of extending it.
        """
        await broker.publish(f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}", b"body")

        _, kwargs = mock_jetstream.add_stream.await_args
        assert kwargs["name"] == broker._stream_name(TOPIC_A)
        assert kwargs["subjects"] == [TOPIC_A, f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}"]

    async def test_a_dlq_topic_never_claims_a_recursive_dlq_subject(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
    ) -> None:
        """`foo.dlq.dlq` must never be declared — the chain terminates at one level."""
        await broker.publish(f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}", b"body")

        _, kwargs = mock_jetstream.add_stream.await_args
        assert f"{TOPIC_A}.dlq.dlq" not in kwargs["subjects"]


class TestDeadLetterChainTerminates:
    """A message already on a `.dlq` subject has nowhere further to go."""

    async def test_dead_lettering_from_a_dlq_subject_publishes_nothing(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """`foo.dlq.dlq` belongs to no stream, so publishing there raises outright.

        `_ensure_stream` stops the chain at one level; this side had not learned
        the same rule, so dead-lettering a message consumed *from* a DLQ failed
        instead of terminating.
        """
        msg = mocker.MagicMock()
        msg.subject = f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}"
        msg.data = b"already dead"
        msg.headers = None

        await broker.deadletter(msg)

        mock_jetstream.publish.assert_not_awaited()

    async def test_dead_lettering_from_a_normal_subject_still_publishes(
        self,
        broker: NatsBroker,
        mock_jetstream: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """The guard must only catch the terminal case."""
        msg = mocker.MagicMock()
        msg.subject = TOPIC_A
        msg.data = b"doomed"
        msg.headers = None

        await broker.deadletter(msg)

        mock_jetstream.publish.assert_awaited_once()
        assert mock_jetstream.publish.await_args.args[0] == f"{TOPIC_A}{NatsBroker.DLQ_SUFFIX}"
