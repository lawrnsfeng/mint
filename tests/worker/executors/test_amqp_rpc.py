"""AMQPRPCExecutor: request/reply over RabbitMQ, against mocked aio_pika objects.

Regression coverage for bug #11: the original leaked a future (and its map entry)
forever on any lost reply, and its ``__del__`` called ``asyncio.run`` — which raises
immediately when called from a running loop, exactly when ``__del__`` actually
fires. Every assertion here is about our own bookkeeping (the pending-calls map, the
timeout, explicit ``aclose()``) — that a real reply genuinely round-trips over a
real broker is a container-test concern, mirroring the RabbitMQ broker's own split.
"""

import asyncio
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from pydantic import BaseModel, ValidationError

from mint.worker.exc import RemoteCallTimeoutError
from mint.worker.executors.amqp_rpc import AMQPRPCConfig, AMQPRPCExecutor

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

QUEUE = "rpc.echo"


class EchoInput(BaseModel):
    """Trivial RPC request payload."""

    value: str


class EchoOutput(BaseModel):
    """Trivial RPC reply payload."""

    value: str


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


def fake_incoming_message(
    mocker: "MockerFixture",
    *,
    correlation_id: str | None,
    body: bytes,
) -> AsyncMock:
    """Build an autospecced AbstractIncomingMessage double for a reply.

    ``.process`` is a *sync* method returning an async context manager (not
    itself a coroutine function — verified: autospec detects this correctly), so
    its return value is wired to a plain ``AsyncMock`` for ``async with`` support.
    """
    message = mocker.create_autospec(AbstractIncomingMessage, instance=True)
    message.correlation_id = correlation_id
    message.body = body
    message.process.return_value = mocker.AsyncMock()
    return message


@pytest.fixture
def mock_channel(mocker: "MockerFixture") -> AsyncMock:
    """Return an autospecced mock channel with a queue that captures its consume callback."""
    channel = mocker.create_autospec(AbstractChannel, instance=True)
    queue = mocker.create_autospec(AbstractQueue, instance=True)
    queue.name = "amq.gen-reply-queue"
    channel.declare_queue = mocker.AsyncMock(return_value=queue)
    channel.default_exchange = mocker.create_autospec(AbstractExchange, instance=True)
    channel.default_exchange.publish = mocker.AsyncMock()
    return channel


@pytest.fixture
def executor(mock_channel: AsyncMock) -> AMQPRPCExecutor[EchoInput, EchoOutput]:
    """Return an executor pre-wired to the mocked channel, skipping real pool construction."""
    instance: AMQPRPCExecutor[EchoInput, EchoOutput] = AMQPRPCExecutor(
        QUEUE,
        "amqp://fake",
        EchoOutput,
        config=AMQPRPCConfig(timeout=0.2),
    )
    instance._channel_pool = FakePool(mock_channel)
    return instance


async def deliver_reply(
    executor: AMQPRPCExecutor[EchoInput, EchoOutput],
    mock_channel: AsyncMock,
    mocker: "MockerFixture",
    *,
    correlation_id: str | None,
    output: EchoOutput,
) -> None:
    """Invoke the consumer callback the executor registered, as the real broker would."""
    queue = mock_channel.declare_queue.return_value
    callback = queue.consume.await_args.args[0]
    message = fake_incoming_message(
        mocker,
        correlation_id=correlation_id,
        body=output.model_dump_json().encode(),
    )
    await callback(message)


class TestExecute:
    """execute() must publish a correlated request and resolve on its matching reply."""

    async def test_execute_publishes_with_a_correlation_id_and_reply_to(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """The published message must carry a correlation_id and the reply queue's name."""
        task = asyncio.create_task(executor.execute(None, EchoInput(value="hi")))
        await asyncio.sleep(0)

        publish_call = mock_channel.default_exchange.publish.await_args
        message = publish_call.args[0]
        kwargs = publish_call.kwargs
        assert message.correlation_id is not None
        assert message.reply_to == "amq.gen-reply-queue"
        assert kwargs["routing_key"] == QUEUE

        await deliver_reply(
            executor,
            mock_channel,
            mocker,
            correlation_id=message.correlation_id,
            output=EchoOutput(value="hi"),
        )
        result = await task
        assert result == EchoOutput(value="hi")

    async def test_execute_returns_the_decoded_reply(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A reply matching the request's correlation_id must resolve execute()'s result."""
        task = asyncio.create_task(executor.execute(None, EchoInput(value="ping")))
        await asyncio.sleep(0)
        message = mock_channel.default_exchange.publish.await_args.args[0]

        await deliver_reply(
            executor,
            mock_channel,
            mocker,
            correlation_id=message.correlation_id,
            output=EchoOutput(value="pong"),
        )

        assert await task == EchoOutput(value="pong")


class TestUnknownCorrelationId:
    """Regression for bug #11: an unmatched reply must not disturb another call's future."""

    async def test_an_unknown_correlation_id_does_not_resolve_or_pop_any_pending_future(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """A reply for a correlation_id nobody is waiting on must be silently ignored."""
        task = asyncio.create_task(executor.execute(None, EchoInput(value="hi")))
        await asyncio.sleep(0)
        real_id = mock_channel.default_exchange.publish.await_args.args[0].correlation_id

        await deliver_reply(
            executor,
            mock_channel,
            mocker,
            correlation_id="not-a-real-correlation-id",
            output=EchoOutput(value="wrong"),
        )

        assert real_id in executor._pending
        assert not task.done()

        await deliver_reply(
            executor,
            mock_channel,
            mocker,
            correlation_id=real_id,
            output=EchoOutput(value="right"),
        )
        assert await task == EchoOutput(value="right")

    async def test_a_reply_with_no_correlation_id_is_logged_and_ignored(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mocker: "MockerFixture",
    ) -> None:
        """A reply that never carried a correlation_id at all must not raise."""
        message = fake_incoming_message(mocker, correlation_id=None, body=b"{}")

        await executor._on_reply(message)  # must not raise


class TestTimeout:
    """Regression for bug #11: a lost reply must not leak a future forever."""

    async def test_a_timed_out_call_raises_and_cleans_up_its_pending_entry(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
    ) -> None:
        """No reply ever arrives: the call must raise, and its map entry must be gone."""
        with pytest.raises(RemoteCallTimeoutError):
            await executor.execute(None, EchoInput(value="never answered"))

        assert executor._pending == {}


class TestLazyPoolConstruction:
    """The connection/channel pools build on first use, via connect_robust, and are cached."""

    async def test_pools_are_built_lazily_and_reused(self, mocker: "MockerFixture") -> None:
        """First acquire() builds both pools through connect_robust; a second reuses them."""
        mock_connection = mocker.create_autospec(AbstractRobustConnection, instance=True)
        mock_channel = mocker.create_autospec(AbstractChannel, instance=True)
        # AbstractRobustConnection.channel isn't detected as a coroutine function by
        # autospec (same gap as RabbitMQBroker's own lazy-pool test), so it comes
        # back as a sync MagicMock; override just this one attribute.
        mock_connection.channel = mocker.AsyncMock(return_value=mock_channel)
        mock_connect_robust = mocker.patch(
            "mint.worker.executors.amqp_rpc.connect_robust",
            return_value=mock_connection,
        )
        instance: AMQPRPCExecutor[EchoInput, EchoOutput] = AMQPRPCExecutor(
            QUEUE,
            "amqp://fake",
            EchoOutput,
        )
        assert instance._connection_pool is None
        assert instance._channel_pool is None

        channel_pool = instance._ensure_channel_pool()
        assert instance._channel_pool is channel_pool

        async with channel_pool.acquire() as channel:
            assert channel is mock_channel
        assert instance._connection_pool is not None  # built lazily by the first acquire

        async with channel_pool.acquire() as channel:
            assert channel is mock_channel

        mock_connect_robust.assert_awaited_once_with("amqp://fake")

    async def test_ensure_connection_pool_is_idempotent(self) -> None:
        """A second call must reuse the already-built connection pool, not rebuild it."""
        instance: AMQPRPCExecutor[EchoInput, EchoOutput] = AMQPRPCExecutor(
            QUEUE,
            "amqp://fake",
            EchoOutput,
        )

        first = instance._ensure_connection_pool()
        second = instance._ensure_connection_pool()

        assert first is second


class TestAclose:
    """aclose() must own cleanup explicitly — no __del__ side effects."""

    async def test_aclose_before_any_pool_is_built_is_a_no_op(self) -> None:
        """An executor that never touched RabbitMQ must not construct pools to close them."""
        instance = AMQPRPCExecutor(QUEUE, "amqp://fake", EchoOutput)

        await instance.aclose()  # must not raise

    async def test_aclose_closes_both_pools(self, mocker: "MockerFixture") -> None:
        """close() must release the channel pool and the connection pool."""
        instance = AMQPRPCExecutor(QUEUE, "amqp://fake", EchoOutput)
        fake_channel_pool = FakePool(mocker.create_autospec(AbstractChannel, instance=True))
        fake_connection_pool = FakePool(mocker.MagicMock())
        instance._channel_pool = fake_channel_pool
        instance._connection_pool = fake_connection_pool

        await instance.aclose()

        assert fake_channel_pool.closed
        assert fake_connection_pool.closed

    def test_construction_outside_a_running_event_loop_does_not_raise(self) -> None:
        """Pool construction is lazy, matching RabbitMQBroker — no running loop needed."""
        AMQPRPCExecutor(QUEUE, "amqp://fake", EchoOutput)  # must not raise


class TestReplyQueueLifecycle:
    """Every call must tear down the reply queue and consumer it created."""

    async def test_a_completed_call_cancels_its_consumer_and_deletes_its_queue(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """An exclusive queue outlives its channel, so a pooled channel would hoard it.

        The channel goes straight back into a pool of ~20 still carrying this
        consumer; without teardown a long-running worker accumulates one queue and
        one consumer per call until RabbitMQ's own limits stop it.
        """
        queue = mock_channel.declare_queue.return_value
        queue.consume.return_value = "ctag-1"

        call = asyncio.ensure_future(executor.execute(None, EchoInput(value="hi")))
        await asyncio.sleep(0)
        await deliver_reply(
            executor,
            mock_channel,
            mocker,
            correlation_id=next(iter(executor._pending)),
            output=EchoOutput(value="hi"),
        )
        await call

        queue.cancel.assert_awaited_once_with("ctag-1")
        queue.delete.assert_awaited_once_with(if_unused=False, if_empty=False)

    async def test_a_timed_out_call_still_tears_its_reply_queue_down(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
    ) -> None:
        """The leak is worst exactly when calls fail, so teardown runs on the error path too."""
        queue = mock_channel.declare_queue.return_value
        queue.consume.return_value = "ctag-2"

        with pytest.raises(RemoteCallTimeoutError):
            await executor.execute(None, EchoInput(value="hi"))

        queue.cancel.assert_awaited_once_with("ctag-2")
        queue.delete.assert_awaited_once()

    async def test_a_failing_teardown_never_masks_the_calls_own_result(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
    ) -> None:
        """A broker that already dropped the queue must not turn a timeout into its own error."""
        queue = mock_channel.declare_queue.return_value
        queue.cancel.side_effect = RuntimeError("channel closed")

        with pytest.raises(RemoteCallTimeoutError):
            await executor.execute(None, EchoInput(value="hi"))


class TestReplyValidation:
    """A reply that doesn't match output_type must surface as itself, not as a timeout."""

    async def test_an_unparseable_reply_raises_a_validation_error_not_a_timeout(
        self,
        executor: AMQPRPCExecutor[EchoInput, EchoOutput],
        mock_channel: AsyncMock,
        mocker: "MockerFixture",
    ) -> None:
        """The future is popped before validation, so a raise there stranded the caller.

        It blocked for the whole timeout and then reported "no reply on queue" —
        for a reply that did arrive and simply had the wrong shape.
        """
        queue = mock_channel.declare_queue.return_value
        queue.consume.return_value = "ctag-3"

        call = asyncio.ensure_future(executor.execute(None, EchoInput(value="hi")))
        await asyncio.sleep(0)
        correlation_id = next(iter(executor._pending))
        callback = queue.consume.await_args.args[0]
        await callback(
            fake_incoming_message(
                mocker,
                correlation_id=correlation_id,
                body=b'{"wrong_field": 1}',
            ),
        )

        with pytest.raises(ValidationError):
            await call
