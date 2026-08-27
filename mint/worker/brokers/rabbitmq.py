"""RabbitMQ broker: at-least-once, with a real dead-letter exchange per topic.

Regression fix for bug #9: the original implementation called ``message.reject()``
with no ``requeue`` argument (defaulting to ``False``) and never declared a
dead-letter exchange, so every rejected message vanished — RabbitMQ drops a
non-requeued message with nowhere configured to route it to. Here, every queue is
declared with ``x-dead-letter-exchange`` pointing at its own DLX, so
``nack(requeue=False)`` reliably lands in ``{topic}.dlq`` instead of disappearing.
"""

from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from typing import ClassVar, Final, Protocol

from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import (
    AbstractChannel,
    AbstractExchange,
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from aio_pika.pool import Pool

from mint.logger import get_logger
from mint.worker.enums import DeliveryGuarantee

logger = get_logger(__name__)


class _AcquirablePool[T](Protocol):
    """The subset of aio_pika.pool.Pool this broker actually needs — acquire and close.

    A Protocol (structural), not the concrete ``Pool`` class, deliberately: it's
    what lets a lightweight test double stand in for a real connection/channel
    pool without needing to fake aio_pika's full Pool implementation.
    """

    def acquire(self) -> AbstractAsyncContextManager[T]: ...
    async def close(self) -> None: ...


type ConnectionPool = _AcquirablePool[AbstractRobustConnection]
type ChannelPool = _AcquirablePool[AbstractChannel]

ATTEMPT_HEADER: Final[str] = "x-mint-attempt"


class RabbitMQDelivery:
    """One delivered message, wrapping aio_pika's incoming message with ack/nack."""

    def __init__(self, message: AbstractIncomingMessage) -> None:
        """Wrap ``message``, reading its attempt count from headers (default 1)."""
        self._message = message
        self.body = message.body
        headers = message.headers or {}
        raw_attempt = headers.get(ATTEMPT_HEADER, 1)
        self.attempt = raw_attempt if isinstance(raw_attempt, int) else 1

    async def ack(self) -> None:
        """Acknowledge this message."""
        await self._message.ack()

    async def nack(self, *, requeue: bool) -> None:
        """Reject this message. requeue=False routes it to the topic's dead-letter queue."""
        await self._message.reject(requeue=requeue)


class RabbitMQBroker:
    """At-least-once broker over RabbitMQ, with a dead-letter exchange per topic."""

    guarantee: ClassVar[DeliveryGuarantee] = DeliveryGuarantee.AT_LEAST_ONCE
    DEFAULT_CONNECTION_POOL_SIZE: Final[int] = 10
    DEFAULT_CHANNEL_POOL_SIZE: Final[int] = 20
    DEFAULT_PREFETCH_COUNT: Final[int] = 10
    DLX_SUFFIX: Final[str] = ".dlx"
    DLQ_SUFFIX: Final[str] = ".dlq"

    def __init__(
        self,
        uri: str,
        *,
        qos: int = DEFAULT_PREFETCH_COUNT,
        connection_pool_size: int = DEFAULT_CONNECTION_POOL_SIZE,
        channel_pool_size: int = DEFAULT_CHANNEL_POOL_SIZE,
    ) -> None:
        """Configure connection/channel pools over ``uri``; nothing connects until first use.

        The pools themselves are built lazily (see ``_ensure_pools``), not here:
        ``aio_pika.pool.Pool.__init__`` calls ``asyncio.get_event_loop()``
        synchronously at construction time, so building it eagerly would make
        ``RabbitMQBroker(uri)`` fail whenever it's constructed outside a running
        event loop — exactly what happens in ordinary synchronous DI/container
        setup, before ``asyncio.run()`` is even called.
        """
        self.uri = uri
        self.qos = qos
        self._connection_pool_size = connection_pool_size
        self._channel_pool_size = channel_pool_size
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

    async def _declare_topic(
        self,
        channel: AbstractChannel,
        topic: str,
    ) -> tuple[AbstractExchange, AbstractQueue]:
        """Declare topic's exchange/queue and its dead-letter exchange/queue; return both.

        A DLQ terminates the chain rather than extending it: consuming from
        ``{topic}.dlq`` (e.g. to inspect it) must declare that queue identically
        to how its parent topic first declared it as a plain, argument-less
        durable queue — RabbitMQ rejects re-declaring a queue with different
        arguments (``PRECONDITION_FAILED``), which is exactly what giving a DLQ
        its own recursive DLX would do.
        """
        if topic.endswith(self.DLQ_SUFFIX):
            exchange = await channel.declare_exchange(topic, durable=True)
            queue = await channel.declare_queue(topic, durable=True)
            await queue.bind(exchange, topic)
            return exchange, queue

        dlx_name = f"{topic}{self.DLX_SUFFIX}"
        dlq_name = f"{topic}{self.DLQ_SUFFIX}"
        dlx = await channel.declare_exchange(dlx_name, durable=True)
        dlq = await channel.declare_queue(dlq_name, durable=True)
        await dlq.bind(dlx, dlq_name)

        exchange = await channel.declare_exchange(topic, durable=True)
        queue = await channel.declare_queue(
            topic,
            durable=True,
            arguments={"x-dead-letter-exchange": dlx_name, "x-dead-letter-routing-key": dlq_name},
        )
        await queue.bind(exchange, topic)
        return exchange, queue

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``, declaring it (and its DLX) if needed."""
        async with self._ensure_channel_pool().acquire() as channel:
            exchange, _ = await self._declare_topic(channel, topic)
            await exchange.publish(
                Message(
                    body=message,
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers=dict(headers) if headers else None,
                ),
                routing_key=topic,
            )

    async def consume(self, topic: str) -> AsyncIterator[RabbitMQDelivery]:
        """Yield deliveries from ``topic`` until the channel is closed."""
        async with self._ensure_channel_pool().acquire() as channel:
            await channel.set_qos(prefetch_count=self.qos)
            _, queue = await self._declare_topic(channel, topic)
            async with queue.iterator() as iterator:
                async for message in iterator:
                    yield RabbitMQDelivery(message)

    async def close(self) -> None:
        """Close both the channel and connection pools, if they were ever built."""
        if self._channel_pool is not None:
            await self._channel_pool.close()
        if self._connection_pool is not None:
            await self._connection_pool.close()
