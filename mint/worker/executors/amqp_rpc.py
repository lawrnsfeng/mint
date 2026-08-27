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
)
from aio_pika.pool import Pool
from pydantic import BaseModel

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
        async with message.process():
            correlation_id = message.correlation_id
            if correlation_id is None:
                logger.warning("AMQP RPC reply without a correlation_id")
                return
            future = self._pending.pop(correlation_id, None)
            if future is None or future.done():
                return
            future.set_result(self.output_type.model_validate_json(message.body))

    async def _declare_reply_queue(self, channel: AbstractChannel) -> AbstractQueue:
        queue = await channel.declare_queue(exclusive=True)
        await queue.consume(self._on_reply, no_ack=True)
        return queue

    async def execute(self, fn: object, input_: T) -> RT:
        """Publish ``input_`` and await its reply, or raise on timeout.

        ``fn`` is unused: an ``AMQPRPCExecutor`` replaces ``process`` entirely
        rather than wrapping it, and takes it only to satisfy the same call
        signature every executor shares — see ``GRPCExecutor`` for the same shape.
        """
        del fn
        async with self._ensure_channel_pool().acquire() as channel:
            reply_queue = await self._declare_reply_queue(channel)
            correlation_id = str(uuid4())
            loop = asyncio.get_running_loop()
            future: asyncio.Future[RT] = loop.create_future()
            self._pending[correlation_id] = future
            await channel.default_exchange.publish(
                Message(
                    input_.model_dump_json().encode(),
                    correlation_id=correlation_id,
                    reply_to=reply_queue.name,
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=self.queue,
            )
            return await self._await_reply(correlation_id, future)

    async def _await_reply(self, correlation_id: str, future: asyncio.Future[RT]) -> RT:
        try:
            async with asyncio.timeout(self.timeout):
                return await future
        except TimeoutError as exc:
            self._pending.pop(correlation_id, None)
            future.cancel()
            raise RemoteCallTimeoutError(queue=self.queue, timeout=self.timeout) from exc

    async def aclose(self) -> None:
        """Close both pools, if they were ever built. Never called from ``__del__``."""
        if self._channel_pool is not None:
            await self._channel_pool.close()
        if self._connection_pool is not None:
            await self._connection_pool.close()
