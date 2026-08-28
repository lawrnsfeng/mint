"""NATS JetStream broker — at-least-once, with a per-topic durable consumer.

Regression fix for bug #10: the original implementation used one hardcoded
durable consumer name (``DEFAULT_CONSUMER_NAME``) for every topic. JetStream scopes
a durable consumer's cursor to the (stream, consumer name) pair — reusing the same
name across different topics/subjects makes them fight over the same delivery
cursor, silently dropping or duplicating messages depending on subscribe order.
Here the durable name is derived from the topic, so each gets its own cursor.

Two further issues found by review. ``_stream_name`` collapsed ``.`` to ``-``
with a plain replace, which is not injective: ``a.b`` and ``a-b`` mapped onto one
stream and one durable name — the same cursor sharing, reached a different way.
(The first attempt at a fix, escaping ``-`` as ``--``, was itself not injective:
it makes dash runs ambiguous, so ``a-.b`` and ``a.-b`` both encoded to ``a---b``.
Each character needs its own distinct escape.)
And ``_ensure_stream`` gave every topic its own stream claiming ``{topic}`` and
``{topic}.dlq``, so consuming ``foo.dlq`` tried to declare a stream over a subject
``foo``'s stream already owned, which JetStream rejects. A dead-letter subject now
resolves back to its parent's stream instead.
"""

import contextlib
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, Final

from nats import connect
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from mint.logger import get_logger
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from nats.aio.client import Client

logger = get_logger(__name__)


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

    DASH_ESCAPE: Final[str] = "-h"
    DOT_ESCAPE: Final[str] = "-d"

    def _stream_name(self, topic: str) -> str:
        """Return an injective stream name for ``topic``.

        JetStream stream names can't contain ``.``, so it has to be encoded away —
        and the encoding must be injective, or two unrelated topics share one
        stream *and* one durable consumer name, which is exactly bug #10's
        cursor-sharing failure.

        Escaping ``-`` as ``--`` before mapping ``.`` to ``-`` is *not* enough: it
        makes runs of dashes ambiguous, so ``a-.b`` and ``a.-b`` both encode to
        ``a---b``. Each source character needs its own distinct two-character
        escape, so every ``-`` in the output is unambiguously a marker followed by
        exactly one tag character.
        """
        return topic.replace("-", self.DASH_ESCAPE).replace(".", self.DOT_ESCAPE)

    def _durable_name(self, topic: str) -> str:
        """Return a per-topic durable consumer name — the actual fix for bug #10."""
        return f"{self.group}-{self._stream_name(topic)}"

    def _owning_topic(self, topic: str) -> str:
        """Return the topic whose stream owns ``topic``'s subject.

        A dead-letter subject lives inside its parent topic's stream, so a ``.dlq``
        topic resolves back to that parent. Declaring a stream of its own would
        claim ``{topic}.dlq``, a subject the parent stream already owns, and
        JetStream rejects overlapping subjects across streams — the same
        terminate-the-chain rule ``RabbitMQBroker._declare_topic`` needs for
        bug #21.
        """
        if topic.endswith(self.DLQ_SUFFIX):
            return topic[: -len(self.DLQ_SUFFIX)]
        return topic

    async def _ensure_stream(self, jetstream: JetStreamContext, topic: str) -> None:
        owner = self._owning_topic(topic)
        await jetstream.add_stream(
            name=self._stream_name(owner),
            subjects=[owner, f"{owner}{self.DLQ_SUFFIX}"],
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
        try:
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
        finally:
            # RabbitMQ and Kafka both release their consumer here; this one relied on
            # close() tearing down the whole client, which Worker.close_consumer()
            # does not do.
            with contextlib.suppress(Exception):
                await subscription.unsubscribe()

    async def deadletter(self, msg: Msg) -> None:
        """Publish ``msg`` to its subject's dead-letter subject, if it has one.

        A message already on a ``.dlq`` subject has nowhere further to go: its
        would-be target ``foo.dlq.dlq`` belongs to no stream, because
        ``_ensure_stream`` deliberately stops the chain at one level — so
        publishing there fails outright. ``_ensure_stream`` learned that rule; this
        side had not, so dead-lettering a message consumed *from* a DLQ raised
        instead of terminating.
        """
        if msg.subject.endswith(self.DLQ_SUFFIX):
            logger.warning(
                "Dropping a message already on a dead-letter subject",
                subject=msg.subject,
            )
            return
        jetstream = await self._connect()
        await jetstream.publish(f"{msg.subject}{self.DLQ_SUFFIX}", msg.data, headers=msg.headers)

    async def close(self) -> None:
        """Close the underlying NATS connection."""
        if self._client is not None:
            await self._client.close()
