"""NATS JetStream broker — at-least-once, with a per-topic durable consumer.

Regression fix for bug #10: the original implementation used one hardcoded
durable consumer name (``DEFAULT_CONSUMER_NAME``) for every topic. JetStream scopes
a durable consumer's cursor to the (stream, consumer name) pair — reusing the same
name across different topics/subjects makes them fight over the same delivery
cursor, silently dropping or duplicating messages depending on subscribe order.
Here the durable name is derived from the topic, so each gets its own cursor.
"""

from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Final

from nats import connect
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from nats.aio.client import Client


class NatsDelivery:
    """One delivered JetStream message, with ack/nak/term-based ack/nack."""

    def __init__(self, broker: "NatsBroker", msg: Msg) -> None:
        """Wrap ``msg``, reading its delivery count as the attempt number."""
        self._broker = broker
        self._msg = msg
        self.body = msg.data
        metadata = msg.metadata
        self.attempt = metadata.num_delivered if metadata is not None else 1

    async def ack(self) -> None:
        """Acknowledge this message."""
        await self._msg.ack()

    async def nack(self, *, requeue: bool) -> None:
        """Negative-ack for redelivery, or dead-letter and terminate."""
        if requeue:
            await self._msg.nak()
        else:
            await self._broker.deadletter(self._msg)
            await self._msg.term()


class NatsBroker:
    """At-least-once broker over NATS JetStream, with a per-topic durable consumer."""

    guarantee = DeliveryGuarantee.AT_LEAST_ONCE
    DEFAULT_GROUP: Final[str] = "mint-worker"
    DLQ_SUFFIX: Final[str] = ".dlq"
    PULL_BATCH: Final[int] = 1
    PULL_TIMEOUT_SECONDS: Final[float] = 5.0

    def __init__(self, uri: str, *, group: str = DEFAULT_GROUP) -> None:
        """Configure a broker over ``uri``; nothing connects until first use."""
        self.uri = uri
        self.group = group
        self._client: Client | None = None
        self._jetstream: JetStreamContext | None = None

    async def _connect(self) -> JetStreamContext:
        if self._jetstream is not None:
            return self._jetstream
        self._client = await connect(self.uri)
        self._jetstream = self._client.jetstream()
        return self._jetstream

    def _stream_name(self, topic: str) -> str:
        return topic.replace(".", "-")

    def _durable_name(self, topic: str) -> str:
        """Return a per-topic durable consumer name — the actual fix for bug #10."""
        return f"{self.group}-{self._stream_name(topic)}"

    async def _ensure_stream(self, jetstream: JetStreamContext, topic: str) -> None:
        await jetstream.add_stream(
            name=self._stream_name(topic),
            subjects=[topic, f"{topic}{self.DLQ_SUFFIX}"],
        )

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``, creating its stream if needed."""
        jetstream = await self._connect()
        await self._ensure_stream(jetstream, topic)
        await jetstream.publish(topic, message, headers=dict(headers) if headers else None)

    async def consume(self, topic: str) -> AsyncIterator[NatsDelivery]:
        """Yield deliveries from ``topic`` via this broker's per-topic durable consumer."""
        jetstream = await self._connect()
        await self._ensure_stream(jetstream, topic)
        subscription = await jetstream.pull_subscribe(
            subject=topic,
            durable=self._durable_name(topic),
        )
        while True:
            try:
                messages = await subscription.fetch(
                    self.PULL_BATCH,
                    timeout=self.PULL_TIMEOUT_SECONDS,
                )
            except TimeoutError:
                continue
            for msg in messages:
                yield NatsDelivery(self, msg)

    async def deadletter(self, msg: Msg) -> None:
        """Publish ``msg`` to its subject's dead-letter subject."""
        jetstream = await self._connect()
        await jetstream.publish(f"{msg.subject}{self.DLQ_SUFFIX}", msg.data, headers=msg.headers)

    async def close(self) -> None:
        """Close the underlying NATS connection."""
        if self._client is not None:
            await self._client.close()
