"""In-process broker — tests and single-process deployments."""

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import ClassVar, Final

from mint.worker.enums import DeliveryGuarantee


class MemoryDelivery:
    """A delivered message from ``MemoryBroker``, with ack/nack control."""

    def __init__(
        self,
        broker: "MemoryBroker",
        topic: str,
        body: bytes,
        attempt: int,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Wrap one message pulled from ``broker``'s ``topic`` queue."""
        self._broker = broker
        self._topic = topic
        self.body = body
        self.attempt = attempt
        self.headers = headers

    async def ack(self) -> None:
        """Acknowledge this message — a no-op: the queue already removed it on delivery."""
        return

    async def nack(self, *, requeue: bool) -> None:
        """Redeliver with an incremented attempt, or dead-letter."""
        if requeue:
            await self._broker.redeliver(self._topic, self.body, self.attempt + 1, self.headers)
        else:
            await self._broker.deadletter(self._topic, self.body, self.attempt, self.headers)


class MemoryBroker:
    """In-process, at-least-once broker backed by ``asyncio.Queue`` — tests and single-process use.

    Every topic gets its own queue; a rejected (``nack(requeue=False)``) message is
    routed to ``{topic}.dlq`` rather than dropped, mirroring what a real broker's
    dead-letter exchange does. ``redeliver``/``deadletter`` are the shared protocol
    between this class and ``MemoryDelivery`` (not underscore-prefixed: a delivery
    calls back into its own broker to act on a nack).
    """

    guarantee: ClassVar[DeliveryGuarantee] = DeliveryGuarantee.AT_LEAST_ONCE
    DLQ_SUFFIX: Final[str] = ".dlq"

    def __init__(self) -> None:
        """Start with no queues; each is created lazily on first publish/consume."""
        self._queues: dict[str, asyncio.Queue[MemoryDelivery | None]] = {}
        self._closed = False

    def _queue(self, topic: str) -> "asyncio.Queue[MemoryDelivery | None]":
        return self._queues.setdefault(topic, asyncio.Queue())

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``."""
        await self._queue(topic).put(
            MemoryDelivery(self, topic, message, attempt=1, headers=headers),
        )

    async def consume(self, topic: str) -> AsyncIterator[MemoryDelivery]:
        """Yield deliveries from ``topic`` until ``close()`` is called."""
        queue = self._queue(topic)
        while True:
            item = await queue.get()
            if item is None:
                return
            yield item

    async def close(self) -> None:
        """Unblock every open consumer by pushing a stop sentinel onto every known queue."""
        if self._closed:
            return
        self._closed = True
        for queue in self._queues.values():
            await queue.put(None)

    async def redeliver(
        self,
        topic: str,
        body: bytes,
        attempt: int,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Re-publish ``body`` to ``topic`` at the given attempt count."""
        await self._queue(topic).put(MemoryDelivery(self, topic, body, attempt, headers))

    async def deadletter(
        self,
        topic: str,
        body: bytes,
        attempt: int,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``body`` to ``topic``'s dead-letter queue instead of redelivering it."""
        dlq_topic = f"{topic}{self.DLQ_SUFFIX}"
        await self._queue(dlq_topic).put(MemoryDelivery(self, dlq_topic, body, attempt, headers))
