"""RabbitMQBroker: DLX declaration and ack/nack wiring, all against a mocked channel.

Regression coverage for bug #9 (rejected messages vanishing): every assertion here
is about what gets declared/called on the channel, not about real message routing —
that a rejected message genuinely reaches the DLQ is a real-broker property, covered
separately in ``test_rabbitmq_container.py``.
"""

from typing import TYPE_CHECKING, Self
from unittest.mock import AsyncMock

import pytest
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractQueue,
    AbstractRobustConnection,
)

from mint.worker.brokers.rabbitmq import ATTEMPT_HEADER, RabbitMQBroker, RabbitMQDelivery
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

TOPIC = "sync.reference"


class FakeQueueIterator:
    """A trivial async context manager + async iterator over a fixed list of messages."""

    def __init__(self, messages: list[object]) -> None:
        """Wrap ``messages``, yielded one at a time on iteration."""
        self._messages = list(messages)

    async def __aenter__(self) -> Self:
        """Return self as the iterator."""
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        """Do nothing; never suppress exceptions."""
        return False

    def __aiter__(self) -> Self:
        """Return self as the iterator."""
        return self

    async def __anext__(self) -> object:
        """Yield the next message, or stop."""
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)


class FakeAcquireContext[T]:
    """A trivial stand-in for what Pool.acquire() returns: yields a fixed value."""

    def __init__(self, value: T) -> None:
        """Wrap ``value``, returned as-is by ``__aenter__``."""
        self.value = value

    async def __aenter__(self) -> T:
        """Return the wrapped value."""
        return self.value

    async def __aexit__(self, *exc_info: object) -> bool:
        """Do nothing; never suppress exceptions."""
        return False


class FakePool[T]:
    """A trivial stand-in for aio_pika.pool.Pool: acquire() always yields the same mock."""

    def __init__(self, value: T) -> None:
        """Wrap ``value``, handed back by every acquire()."""
        self.value = value
        self.closed = False

    def acquire(self) -> FakeAcquireContext[T]:
        """Return an async context manager yielding the wrapped value."""
        return FakeAcquireContext(self.value)

    async def close(self) -> None:
        """Flag this pool as closed."""
        self.closed = True


@pytest.fixture
def mock_channel(mocker: "MockerFixture") -> AsyncMock:
    """Return an autospecced mock channel — aio_pika's methods are properly async-detected."""
    channel = mocker.create_autospec(AbstractChannel, instance=True)
    exchange = mocker.create_autospec(AbstractExchange, instance=True)
    queue = mocker.create_autospec(AbstractQueue, instance=True)
    queue.iterator.return_value = FakeQueueIterator([])
    channel.declare_exchange.return_value = exchange
    channel.declare_queue.return_value = queue
    # AbstractChannel.close isn't detected as a coroutine function by autospec
    # (same gap as AbstractRobustConnection.channel below), so override just it.
    channel.close = mocker.AsyncMock()
    return channel


@pytest.fixture
def mock_consumer_connection(mock_channel: AsyncMock, mocker: "MockerFixture") -> AsyncMock:
    """Return the dedicated connection a consumer opens its own channel on."""
    connection = mocker.create_autospec(AbstractRobustConnection, instance=True)
    # AbstractRobustConnection.channel isn't detected as a coroutine function by
    # autospec (verified — same gap as redis-py's command methods).
    connection.channel = mocker.AsyncMock(return_value=mock_channel)
    return connection


@pytest.fixture
def broker(mock_channel: AsyncMock, mock_consumer_connection: AsyncMock) -> RabbitMQBroker:
    """Return a broker wired directly to the mocks, skipping real connection setup.

    Both paths are wired: publishing goes through the channel *pool*, while
    consuming deliberately does not (see ``RabbitMQBroker.consume``) and opens its
    own channel on a dedicated connection instead.
    """
    instance = RabbitMQBroker("amqp://fake")
    instance._channel_pool = FakePool(mock_channel)
    instance._consumer_connection = mock_consumer_connection
    return instance


class TestDeclareTopic:
    """Every publish/consume must declare the topic's exchange/queue AND its DLX/DLQ."""

    async def test_publish_declares_dlx_before_the_main_exchange(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """The DLX and DLQ must exist before the topic's own exchange/queue reference them."""
        await broker.publish(TOPIC, b"body")

        exchange_names = [call.args[0] for call in mock_channel.declare_exchange.await_args_list]
        assert exchange_names == [f"{TOPIC}.dlx", TOPIC]

    async def test_publish_declares_the_dead_letter_queue_durable(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """The DLQ itself must be declared durable, same as the main queue."""
        await broker.publish(TOPIC, b"body")

        queue_calls = mock_channel.declare_queue.await_args_list
        dlq_call = next(call for call in queue_calls if call.args[0] == f"{TOPIC}.dlq")
        assert dlq_call.kwargs["durable"] is True

    async def test_consuming_from_a_dlq_topic_declares_it_without_its_own_dlx(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """A DLQ must not get a recursive DLX of its own — it terminates the chain.

        Regression: declaring a DLQ-suffixed topic the same way as a normal one
        re-declares an existing queue with different arguments, which real
        RabbitMQ rejects with PRECONDITION_FAILED (caught by the container test).
        """
        dlq_topic = f"{TOPIC}.dlq"

        await broker.publish(dlq_topic, b"body")

        exchange_names = [call.args[0] for call in mock_channel.declare_exchange.await_args_list]
        assert exchange_names == [dlq_topic]  # no "{dlq_topic}.dlx"
        queue_calls = mock_channel.declare_queue.await_args_list
        assert len(queue_calls) == 1
        assert queue_calls[0].args[0] == dlq_topic
        assert "arguments" not in queue_calls[0].kwargs

    async def test_publish_declares_the_main_queue_with_dead_letter_exchange_argument(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """The main queue's x-dead-letter-exchange argument must point at the DLX.

        This is the actual regression fix for bug #9 — without it, RabbitMQ has
        nowhere to route a rejected (requeue=False) message, and it's dropped.
        """
        await broker.publish(TOPIC, b"body")

        queue_calls = mock_channel.declare_queue.await_args_list
        main_call = next(call for call in queue_calls if call.args[0] == TOPIC)
        assert main_call.kwargs["arguments"]["x-dead-letter-exchange"] == f"{TOPIC}.dlx"

    async def test_consume_sets_qos_before_declaring(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """Prefetch must be configured before messages start flowing."""
        async for _ in broker.consume(TOPIC):
            pass

        mock_channel.set_qos.assert_awaited_once_with(
            prefetch_count=RabbitMQBroker.DEFAULT_PREFETCH_COUNT,
        )

    async def test_consume_yields_a_delivery_per_message(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """Each message off the queue iterator must come back as a RabbitMQDelivery."""
        message = mocker.AsyncMock()
        message.body = b"payload"
        message.headers = None
        mock_channel.declare_queue.return_value.iterator.return_value = FakeQueueIterator(
            [message],
        )

        deliveries = [delivery async for delivery in broker.consume(TOPIC)]

        assert len(deliveries) == 1
        assert isinstance(deliveries[0], RabbitMQDelivery)
        assert deliveries[0].body == b"payload"


class TestPublish:
    """publish() must send a persistent message through the topic's exchange."""

    async def test_publish_sends_through_the_declared_exchange(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """The message must be published on the exchange declare_exchange(topic) returned."""
        await broker.publish(TOPIC, b"body", headers={"trace": "abc"})

        exchange = mock_channel.declare_exchange.return_value
        exchange.publish.assert_awaited_once()
        message, kwargs = exchange.publish.await_args.args[0], exchange.publish.await_args.kwargs
        assert message.body == b"body"
        assert kwargs["routing_key"] == TOPIC


class TestDelivery:
    """RabbitMQDelivery: ack maps onto the message; requeue republishes with attempt bumped."""

    def _message(
        self,
        mocker: "MockerFixture",
        headers: dict[str, object] | None = None,
    ) -> AsyncMock:
        message = mocker.AsyncMock()
        message.body = b"body"
        message.headers = headers
        return message

    def _delivery(
        self,
        mocker: "MockerFixture",
        headers: dict[str, object] | None = None,
    ) -> tuple[RabbitMQDelivery, AsyncMock, AsyncMock]:
        broker = mocker.AsyncMock()
        message = self._message(mocker, headers)
        return RabbitMQDelivery(broker, TOPIC, message), broker, message

    async def test_ack_calls_message_ack(self, mocker: "MockerFixture") -> None:
        """ack() must call the underlying message's ack()."""
        delivery, _, message = self._delivery(mocker)

        await delivery.ack()

        message.ack.assert_awaited_once()

    async def test_nack_requeue_true_republishes_with_the_attempt_incremented(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """AMQP's native requeue can't touch headers, so attempt stayed 1 here forever.

        Every other broker increments it and ``IBroker``'s documented contract
        says it does, so a requeue republishes with the count bumped instead.
        """
        delivery, broker, message = self._delivery(mocker, headers={ATTEMPT_HEADER: 2})

        await delivery.nack(requeue=True)

        broker.redeliver.assert_awaited_once_with(TOPIC, message, 3)
        message.reject.assert_not_awaited()

    async def test_nack_requeue_true_acks_the_original_only_after_republishing(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """Publish first, ack second — a crash mid-sequence must duplicate, never drop."""
        delivery, broker, message = self._delivery(mocker)
        order: list[str] = []
        broker.redeliver.side_effect = lambda *_: order.append("redeliver")
        message.ack.side_effect = lambda: order.append("ack")

        await delivery.nack(requeue=True)

        assert order == ["redeliver", "ack"]

    async def test_nack_requeue_false_calls_reject_with_requeue_false(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """Regression for bug #9: requeue=False must be explicit, routed to the DLQ by the DLX.

        Still a native reject — that is what actually engages the queue's
        dead-letter exchange; only the requeue path changed.
        """
        delivery, broker, message = self._delivery(mocker)

        await delivery.nack(requeue=False)

        message.reject.assert_awaited_once_with(requeue=False)
        broker.redeliver.assert_not_awaited()

    async def test_attempt_defaults_to_one_with_no_headers(self, mocker: "MockerFixture") -> None:
        """A message with no headers at all must default to attempt 1."""
        delivery, _, _ = self._delivery(mocker, headers=None)

        assert delivery.attempt == 1

    async def test_attempt_reads_from_the_header_when_present(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A well-formed attempt header must be honored."""
        delivery, _, _ = self._delivery(mocker, headers={ATTEMPT_HEADER: 3})

        assert delivery.attempt == 3

    async def test_attempt_falls_back_to_one_on_a_non_int_header(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A malformed attempt header must not raise — fall back to 1."""
        delivery, _, _ = self._delivery(mocker, headers={ATTEMPT_HEADER: "not-an-int"})

        assert delivery.attempt == 1


class TestAttemptHeaderIsPublished:
    """The attempt header has to actually be written, or reading it back is meaningless."""

    async def test_publish_stamps_attempt_one(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """Nothing wrote ATTEMPT_HEADER before, so every delivery read the default of 1."""
        await broker.publish(TOPIC, b"body")

        (message,), _ = mock_channel.declare_exchange.return_value.publish.await_args
        assert message.headers[ATTEMPT_HEADER] == 1

    async def test_publish_keeps_caller_headers_alongside_the_attempt(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """Stamping the attempt must not clobber whatever the caller passed."""
        await broker.publish(TOPIC, b"body", headers={"trace": "abc"})

        (message,), _ = mock_channel.declare_exchange.return_value.publish.await_args
        assert message.headers["trace"] == "abc"
        assert message.headers[ATTEMPT_HEADER] == 1

    async def test_redeliver_replaces_rather_than_duplicates_the_attempt_header(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """The bumped count must overwrite the old one, with other headers preserved."""
        original = mocker.AsyncMock()
        original.body = b"body"
        original.headers = {ATTEMPT_HEADER: 1, "trace": "abc"}

        await broker.redeliver(TOPIC, original, 2)

        (message,), _ = mock_channel.declare_exchange.return_value.publish.await_args
        assert message.headers[ATTEMPT_HEADER] == 2
        assert message.headers["trace"] == "abc"


class TestGuaranteeAndClose:
    """Declared delivery guarantee and resource cleanup."""

    def test_declares_at_least_once_guarantee(self) -> None:
        """RabbitMQBroker is at-least-once."""
        assert RabbitMQBroker.guarantee == DeliveryGuarantee.AT_LEAST_ONCE

    def test_construction_outside_a_running_event_loop_does_not_raise(self) -> None:
        """Regression: Pool.__init__ used to be called eagerly and needed a running loop.

        This is a plain (non-async) test function on purpose — pytest-asyncio gives
        no running event loop here, exactly like ordinary synchronous DI/container
        setup before ``asyncio.run()`` is called. Constructing the broker must not
        require one; only actually using it (publish/consume/close) should.
        """
        RabbitMQBroker("amqp://fake")  # must not raise RuntimeError


class TestLazyPoolConstruction:
    """The connection/channel pools build on first use, via connect_robust, and are cached."""

    async def test_pools_are_built_lazily_and_reused(self, mocker: "MockerFixture") -> None:
        """First acquire() builds both pools through connect_robust; a second reuses them."""
        mock_connection = mocker.create_autospec(AbstractRobustConnection, instance=True)
        mock_channel = mocker.create_autospec(AbstractChannel, instance=True)
        # AbstractRobustConnection.channel isn't detected as a coroutine function by
        # autospec (verified — same class of gap as redis-py's command methods), so
        # it comes back as a sync MagicMock; override just this one attribute.
        mock_connection.channel = mocker.AsyncMock(return_value=mock_channel)
        mock_connect_robust = mocker.patch(
            "mint.worker.brokers.rabbitmq.connect_robust",
            return_value=mock_connection,
        )
        broker = RabbitMQBroker("amqp://fake")
        assert broker._connection_pool is None
        assert broker._channel_pool is None

        channel_pool = broker._ensure_channel_pool()
        assert broker._channel_pool is channel_pool

        async with channel_pool.acquire() as channel:
            assert channel is mock_channel
        assert broker._connection_pool is not None  # built lazily by the first acquire

        async with channel_pool.acquire() as channel:
            assert channel is mock_channel

        mock_connect_robust.assert_awaited_once_with("amqp://fake")

    async def test_close_closes_both_pools(self, mocker: "MockerFixture") -> None:
        """close() must release the channel pool and the connection pool."""
        broker = RabbitMQBroker("amqp://fake")
        fake_channel_pool = FakePool(mocker.create_autospec(AbstractChannel, instance=True))
        fake_connection_pool = FakePool(
            mocker.create_autospec(AbstractRobustConnection, instance=True),
        )
        broker._channel_pool = fake_channel_pool
        broker._connection_pool = fake_connection_pool

        await broker.close()

        assert fake_channel_pool.closed
        assert fake_connection_pool.closed

    async def test_close_before_any_pool_is_built_is_a_no_op(self) -> None:
        """A broker that never touched RabbitMQ must not construct pools just to close them."""
        broker = RabbitMQBroker("amqp://fake")

        await broker.close()  # must not raise

    async def test_ensure_connection_pool_is_idempotent(self) -> None:
        """A second call must reuse the already-built connection pool, not rebuild it.

        Async on purpose: ``aio_pika.pool.Pool.__init__`` needs a running event
        loop (see ``test_construction_outside_a_running_event_loop_does_not_raise``),
        so this exercises ``_ensure_connection_pool`` directly rather than through
        the broker's synchronous constructor.
        """
        broker = RabbitMQBroker("amqp://fake")

        first = broker._ensure_connection_pool()
        second = broker._ensure_connection_pool()

        assert first is second


class TestConsumersDoNotHoldPooledChannels:
    """A consumer holds its channel for life, so it must never take one from the pool."""

    async def test_consume_opens_its_own_channel_off_the_consumer_connection(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
        mock_consumer_connection: AsyncMock,
    ) -> None:
        """One broker is shared by every worker, so pooled channels would run out.

        With `channel_pool_size` workers consuming, every pooled channel is held
        permanently and the next `publish` blocks forever on `acquire()` — the app
        deadlocks with no error at all.
        """
        mock_channel.declare_queue.return_value.iterator.return_value = FakeQueueIterator([])

        async for _ in broker.consume(TOPIC):
            break

        mock_consumer_connection.channel.assert_awaited_once()

    async def test_the_consumer_channel_is_closed_when_consumption_ends(
        self,
        broker: RabbitMQBroker,
        mock_channel: AsyncMock,
    ) -> None:
        """A dedicated channel is only cheap if it's actually released afterwards."""
        mock_channel.declare_queue.return_value.iterator.return_value = FakeQueueIterator([])

        async for _ in broker.consume(TOPIC):
            break

        mock_channel.close.assert_awaited_once()

    async def test_close_releases_the_consumer_connection(
        self,
        broker: RabbitMQBroker,
        mock_consumer_connection: AsyncMock,
    ) -> None:
        """The dedicated connection is the broker's to own, so the broker must close it."""
        await broker.close()

        mock_consumer_connection.close.assert_awaited_once()
        assert broker._consumer_connection is None
