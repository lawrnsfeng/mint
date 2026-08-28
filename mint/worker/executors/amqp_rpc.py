"""AMQP RPC executor — request/reply over RabbitMQ's default exchange.

Regression fixes (bug #11):

- The original's ``__del__`` called ``asyncio.run(self.shutdown())``.
  ``asyncio.run`` raises immediately when called from *inside* a running event
  loop — exactly the situation whenever ``__del__`` fires during normal operation
  (GC runs on the same loop that's using the executor). ``aclose()`` is explicit
  here instead; nothing runs from ``__del__``.
- A reply's future had no timeout at all: a lost reply left its future — and its
  entry in the pending-calls map — leaking forever. Every call here has an
  explicit timeout that cancels the future and removes its map entry.
- The connection/channel pools build lazily, for the same reason as
  ``RabbitMQBroker``: ``aio_pika.pool.Pool.__init__`` needs a running event loop,
  so building it eagerly would break ordinary synchronous DI/container setup.

Found by review: every call declared an exclusive reply queue and registered a
consumer on a *pooled* channel, and tore down neither. An exclusive queue only
disappears when its connection closes, so a long-running worker accumulated one
queue and one consumer per call until RabbitMQ's limits stopped it. Both are now
released in a ``finally``. A reply that fails ``output_type`` validation also
resolves its future with that error instead of escaping into aio-pika's callback
and leaving the caller to time out with a misleading "no reply".
"""

import asyncio
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Final, Protocol
from uuid import uuid4

from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
    ConsumerTag,
)
from aio_pika.pool import Pool
from pydantic import BaseModel, ValidationError

from mint.logger import get_logger
from mint.worker.exc import RemoteCallTimeoutError

logger = get_logger(__name__)

DEFAULT_CONNECTION_POOL_SIZE: Final[int] = 10
DEFAULT_CHANNEL_POOL_SIZE: Final[int] = 20
DEFAULT_TIMEOUT_SECONDS: Final[float] = 30.0


@dataclass(frozen=True)
class AMQPRPCConfig:
    """Tunable pool sizes and reply timeout for an ``AMQPRPCExecutor``."""

    connection_pool_size: int = DEFAULT_CONNECTION_POOL_SIZE
    channel_pool_size: int = DEFAULT_CHANNEL_POOL_SIZE
    timeout: float = DEFAULT_TIMEOUT_SECONDS


class _AcquirablePool[T](Protocol):
    """The subset of aio_pika.pool.Pool this executor needs — acquire and close.

    A Protocol (structural, matching ``RabbitMQBroker``'s), not the concrete
    ``Pool`` class: it's what lets a lightweight test double stand in for a real
    connection/channel pool.
    """

    def acquire(self) -> AbstractAsyncContextManager[T]: ...
    async def close(self) -> None: ...


type ConnectionPool = _AcquirablePool[AbstractRobustConnection]
type ChannelPool = _AcquirablePool[AbstractChannel]


class AMQPRPCExecutor[T: BaseModel, RT: BaseModel]:
    """Publishes ``input_`` to ``queue`` and awaits a correlated reply, with a timeout."""

    def __init__(
        self,
        queue: str,
        uri: str,
        output_type: type[RT],
        *,
        config: AMQPRPCConfig | None = None,
    ) -> None:
        """Configure a call target: publish to ``queue``, decode replies as ``output_type``."""
        cfg = config or AMQPRPCConfig()
        self.uri = uri
        self.queue = queue
        self.output_type = output_type
        self.timeout = cfg.timeout
        self._connection_pool_size = cfg.connection_pool_size
        self._channel_pool_size = cfg.channel_pool_size
        self._pending: dict[str, asyncio.Future[RT]] = {}
        self._connection_pool: ConnectionPool | None = None
        self._channel_pool: ChannelPool | None = None

    def _ensure_connection_pool(self) -> ConnectionPool:
        if self._connection_pool is None:
            self._connection_pool = Pool(
                self._get_connection,
                max_size=self._connection_pool_size,
            )
        return self._connection_pool

    def _ensure_channel_pool(self) -> ChannelPool:
        if self._channel_pool is None:
            self._channel_pool = Pool(self._get_channel, max_size=self._channel_pool_size)
        return self._channel_pool

    async def _get_connection(self) -> AbstractRobustConnection:
        return await connect_robust(self.uri)

    async def _get_channel(self) -> AbstractChannel:
        async with self._ensure_connection_pool().acquire() as connection:
            return await connection.channel()

    async def _on_reply(self, message: AbstractIncomingMessage) -> None:
        """Resolve the pending call this reply belongs to.

        Deliberately no ``async with message.process()``. The reply consumer is
        registered with ``no_ack=True``, and the two are mutually exclusive:
        aio-pika presets ``processed`` on a no-ack message, then
        ``ProcessContext.__aexit__`` calls ``ack()`` anyway on a clean exit, and
        ``ack()`` raises ``TypeError`` unconditionally under ``no_ack``. Since
        aiormq dispatches consumer callbacks with a bare
        ``create_task`` and never retrieves the result, that produced a "Task
        exception was never retrieved" traceback for *every* RPC reply — the call
        itself still returned, because the future is resolved before the exit.
        """
        correlation_id = message.correlation_id
        if correlation_id is None:
            logger.warning("AMQP RPC reply without a correlation_id")
            return
        future = self._pending.pop(correlation_id, None)
        if future is None or future.done():
            return
        try:
            future.set_result(self.output_type.model_validate_json(message.body))
        except ValidationError as exc:
            # The future is already popped, so letting this escape into aio-pika's
            # consumer callback would leave the caller blocked for the full timeout
            # and then raise RemoteCallTimeoutError — reporting "no reply" for a
            # reply that did arrive and simply didn't match output_type.
            future.set_exception(exc)

    async def _declare_reply_queue(
        self,
        channel: AbstractChannel,
    ) -> tuple[AbstractQueue, ConsumerTag]:
        queue = await channel.declare_queue(exclusive=True)
        consumer_tag = await queue.consume(self._on_reply, no_ack=True)
        return queue, consumer_tag

    async def execute(self, fn: object, input_: T) -> RT:
        """Publish ``input_`` and await its reply, or raise on timeout.

        ``fn`` is unused: an ``AMQPRPCExecutor`` replaces ``process`` entirely
        rather than wrapping it, and takes it only to satisfy the same call
        signature every executor shares — see ``GRPCExecutor`` for the same shape.
        """
        del fn
        async with self._ensure_channel_pool().acquire() as channel:
            reply_queue, consumer_tag = await self._declare_reply_queue(channel)
            try:
                return await self._call(channel, reply_queue, input_)
            finally:
                # An exclusive queue only disappears when its *connection* closes, and
                # the channel here goes straight back into a pool still carrying this
                # consumer. Without explicit teardown a long-running worker accumulates
                # one queue and one consumer per call, across ~20 pooled channels, until
                # it hits RabbitMQ's per-channel consumer or queue limits.
                await self._release_reply_queue(reply_queue, consumer_tag)

    async def _call(
        self,
        channel: AbstractChannel,
        reply_queue: AbstractQueue,
        input_: T,
    ) -> RT:
        """Publish one request on ``channel`` and await its correlated reply."""
        correlation_id = str(uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[RT] = loop.create_future()
        # Registered before publishing, because a reply can arrive the instant the
        # request lands — but a failed publish then has to undo it. Only
        # _await_reply's finally pops the map, and a raise here never reaches it,
        # so every call during a broker outage would leak an entry and a future
        # that nothing will ever resolve.
        self._pending[correlation_id] = future
        try:
            await channel.default_exchange.publish(
                Message(
                    input_.model_dump_json().encode(),
                    correlation_id=correlation_id,
                    reply_to=reply_queue.name,
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=self.queue,
            )
        except Exception:
            self._pending.pop(correlation_id, None)
            raise
        return await self._await_reply(correlation_id, future)

    @staticmethod
    async def _release_reply_queue(queue: AbstractQueue, consumer_tag: ConsumerTag) -> None:
        """Cancel this call's consumer and delete its reply queue, best-effort.

        Teardown must never mask the call's own result or error, so a broker that
        has already dropped the queue/consumer is logged rather than raised.
        """
        try:
            await queue.cancel(consumer_tag)
            await queue.delete(if_unused=False, if_empty=False)
        except Exception:
            logger.exception("Failed to release an AMQP RPC reply queue", queue=queue.name)

    async def _await_reply(self, correlation_id: str, future: asyncio.Future[RT]) -> RT:
        """Await one correlated reply, always clearing its pending-map entry.

        The cleanup has to be in a ``finally``, not just the timeout branch: a
        *cancelled* call — the ordinary shutdown path, via ``Worker._handle`` —
        otherwise left its correlation id and future in ``_pending`` forever, so a
        long-lived executor accumulated one entry per cancelled RPC. That is the
        same leak bug #11 fixed for lost replies, reached by a different route.
        """
        try:
            async with asyncio.timeout(self.timeout):
                return await future
        except TimeoutError as exc:
            future.cancel()
            raise RemoteCallTimeoutError(queue=self.queue, timeout=self.timeout) from exc
        finally:
            self._pending.pop(correlation_id, None)

    async def aclose(self) -> None:
        """Close both pools, if they were ever built. Never called from ``__del__``."""
        if self._channel_pool is not None:
            await self._channel_pool.close()
        if self._connection_pool is not None:
            await self._connection_pool.close()
