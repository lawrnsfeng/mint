"""Redis broker: Streams + consumer groups — at-least-once, fixing bug #8.

The original implementation used ``BRPOP``: pop-and-gone, at-most-once — if a
consumer crashes after popping but before finishing the work, the message is
simply lost, with no record it ever existed. Redis Streams with a consumer group
keep every read message in that group's Pending Entries List until ``XACK``
removes it, so a crash before ack leaves it recoverable rather than gone.

Not yet implemented: reclaiming PEL entries abandoned by a crashed consumer
(``XAUTOCLAIM``) — today a message survives a crash but needs another consumer
to eventually re-read it via ``XREADGROUP``'s own-pending-first semantics or an
external sweep. Tracked as a known follow-up, not silently skipped.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, replace
from typing import Final
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from mint.worker.enums import DeliveryGuarantee

BUSYGROUP_MARKER: Final[str] = "BUSYGROUP"


@dataclass(frozen=True)
class StreamEntry:
    """One raw entry read off a Redis Stream — everything needed to ack/nack it."""

    topic: str
    message_id: bytes
    body: bytes
    attempt: int
    headers: Mapping[bytes, bytes] | None = None


class RedisStreamDelivery:
    """One delivered message from a Redis Stream consumer group, with ack/nack."""

    def __init__(self, broker: "RedisBroker", entry: StreamEntry) -> None:
        """Wrap ``entry``, delegating ack/nack back to ``broker``."""
        self._broker = broker
        self._entry = entry
        self.body = entry.body
        self.attempt = entry.attempt
        self.headers = entry.headers

    async def ack(self) -> None:
        """Acknowledge this message, removing it from the consumer group's pending list."""
        await self._broker.ack(self._entry)

    async def nack(self, *, requeue: bool) -> None:
        """Requeue with an incremented attempt (headers preserved), or dead-letter."""
        if requeue:
            await self._broker.redeliver(replace(self._entry, attempt=self._entry.attempt + 1))
        else:
            await self._broker.deadletter(self._entry)


class RedisBroker:
    """At-least-once broker over Redis Streams + consumer groups."""

    guarantee = DeliveryGuarantee.AT_LEAST_ONCE
    DEFAULT_GROUP: Final[str] = "mint-worker"
    DLQ_SUFFIX: Final[str] = ".dlq"
    BODY_FIELD: Final[bytes] = b"body"
    ATTEMPT_FIELD: Final[bytes] = b"attempt"
    DEFAULT_BLOCK_MS: Final[int] = 5_000

    def __init__(
        self,
        uri: str,
        *,
        group: str = DEFAULT_GROUP,
        consumer_name: str | None = None,
        block_ms: int = DEFAULT_BLOCK_MS,
    ) -> None:
        """Configure a broker over ``uri``; nothing connects until first use."""
        self.uri = uri
        self.group = group
        self.consumer_name = consumer_name or str(uuid4())
        self.block_ms = block_ms
        self._client: Redis | None = None

    @property
    def client(self) -> Redis:
        """Return the lazily-connected Redis client."""
        if self._client is None:
            self._client = Redis.from_url(self.uri)
        return self._client

    async def _ensure_group(self, topic: str) -> None:
        try:
            await self.client.xgroup_create(topic, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if BUSYGROUP_MARKER not in str(exc):
                raise

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``'s stream, creating the stream/group if needed."""
        await self._ensure_group(topic)
        fields: dict[bytes | str, bytes | float | str] = {
            self.BODY_FIELD: message,
            self.ATTEMPT_FIELD: 1,
            **(headers or {}),
        }
        await self.client.xadd(topic, fields)

    async def consume(self, topic: str) -> AsyncIterator[RedisStreamDelivery]:
        """Yield deliveries from ``topic``'s stream via this broker's consumer group."""
        await self._ensure_group(topic)
        while True:
            response = await self.client.xreadgroup(
                groupname=self.group,
                consumername=self.consumer_name,
                streams={topic: ">"},
                count=1,
                block=self.block_ms,
            )
            if not response:
                continue
            for _stream, messages in response:
                for message_id, fields in messages:
                    body = fields.get(self.BODY_FIELD, b"")
                    attempt = int(fields.get(self.ATTEMPT_FIELD, b"1"))
                    headers = {
                        key: value
                        for key, value in fields.items()
                        if key not in (self.BODY_FIELD, self.ATTEMPT_FIELD)
                    }
                    entry = StreamEntry(topic, message_id, body, attempt, headers or None)
                    yield RedisStreamDelivery(self, entry)

    async def ack(self, entry: StreamEntry) -> None:
        """Acknowledge and remove ``entry`` from its stream."""
        await self.client.xack(entry.topic, self.group, entry.message_id)
        await self.client.xdel(entry.topic, entry.message_id)

    async def redeliver(self, entry: StreamEntry) -> None:
        """Ack the old message and re-append ``entry`` (already bumped) in its place."""
        await self.client.xack(entry.topic, self.group, entry.message_id)
        await self.client.xdel(entry.topic, entry.message_id)
        fields: dict[bytes | str, bytes | float | str] = {
            self.BODY_FIELD: entry.body,
            self.ATTEMPT_FIELD: entry.attempt,
            **(entry.headers or {}),
        }
        await self.client.xadd(entry.topic, fields)

    async def deadletter(self, entry: StreamEntry) -> None:
        """Ack the old message and append it to its topic's dead-letter stream."""
        await self.client.xack(entry.topic, self.group, entry.message_id)
        await self.client.xdel(entry.topic, entry.message_id)
        fields: dict[bytes | str, bytes | float | str] = {
            self.BODY_FIELD: entry.body,
            **(entry.headers or {}),
        }
        await self.client.xadd(f"{entry.topic}{self.DLQ_SUFFIX}", fields)

    async def close(self) -> None:
        """Release the underlying Redis connection."""
        if self._client is not None:
            # redis-py's own .close() is deprecated in favor of .aclose() since
            # 5.0.1, but the installed stub package doesn't declare aclose() on
            # Redis (verified) even though it exists and works at runtime.
            await self._client.aclose()  # ty: ignore[unresolved-attribute]
