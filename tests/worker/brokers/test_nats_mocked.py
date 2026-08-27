"""NatsBroker: per-topic durable consumer naming and ack/nack wiring, against a mocked client.

Regression coverage for bug #10 (one hardcoded durable name shared by every topic):
every assertion here is about what our code calls on the client — that two
different topics' consumers genuinely don't fight over the same JetStream cursor
is a real-broker property, covered separately in a container test.
"""

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
