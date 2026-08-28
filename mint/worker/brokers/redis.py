"""Redis broker: Streams + consumer groups — at-least-once, fixing bug #8.

The original implementation used ``BRPOP``: pop-and-gone, at-most-once — if a
consumer crashes after popping but before finishing the work, the message is
simply lost, with no record it ever existed. Redis Streams with a consumer group
keep every read message in that group's Pending Entries List until ``XACK``
removes it, so a crash before ack leaves it recoverable rather than gone.

``redeliver``/``deadletter`` write the replacement entry *before* retiring the
original, so a crash mid-sequence duplicates a message rather than losing one —
the reverse order reintroduced exactly the at-most-once hole this rewrite exists
to close.

Crashed-consumer recovery is what actually makes the at-least-once claim true, so
``consume`` reclaims before it reads. ``XREADGROUP`` with ``>`` returns only
never-delivered entries — a consumer's own pending list is reachable only via an
explicit id or ``XAUTOCLAIM`` — so a message read by a consumer that dies before
acking sat in that consumer's PEL forever and was redelivered to nobody. That is
at-most-once for exactly the crash window, in a broker declaring
``AT_LEAST_ONCE``. (An earlier version of this docstring claimed
``XREADGROUP``'s "own-pending-first semantics" covered it; ``>`` has no such
behaviour, so the stated mitigation did not exist.) Each poll now first runs
``XAUTOCLAIM`` for entries idle beyond ``reclaim_idle_ms``, which transfers them
to this consumer and redelivers them.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, replace
from typing import Final
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from mint.logger import get_logger
from mint.worker.enums import DeliveryGuarantee

logger = get_logger(__name__)

BUSYGROUP_MARKER: Final[str] = "BUSYGROUP"

type StreamFields = dict[bytes | str, bytes | float | str]


def _stream_fields(
    body: bytes,
    headers: Mapping[bytes, bytes] | Mapping[str, str] | None,
    attempt: int | None = None,
) -> StreamFields:
    """Build one stream entry's field map, with headers copied in one key at a time.

    Built explicitly rather than with ``**headers``: a ``Mapping[str, str]`` is not
    a ``SupportsKeysAndGetItem[bytes | str, bytes | float | str]``, because ``dict``
    is invariant in its key and value types. Copying key by key widens each entry
    where it's actually added.
    """
    fields: StreamFields = {RedisBroker.BODY_FIELD: body}
    if attempt is not None:
        fields[RedisBroker.ATTEMPT_FIELD] = attempt
    for key, value in (headers or {}).items():
        fields[key] = value
    return fields


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
        try:
            await self._broker.ack(self._entry)
        finally:
            self._broker.release(self._entry)

    async def nack(self, *, requeue: bool) -> None:
        """Requeue with an incremented attempt (headers preserved), or dead-letter.

        The reclaim guard is released whether or not the settle succeeded. Releasing
        it only on success meant a swallowed nack — ``Worker._safe_retry_or_drop``
        logs rather than raises — left the id in ``_inflight_ids`` forever, and
        ``_reclaim`` then skipped that pending entry on every future pass. With a
        single consumer on the topic, that message is stuck unacked and undelivered
        until the process restarts, in a broker declaring at-least-once.
        """
        try:
            if requeue:
                await self._broker.redeliver(replace(self._entry, attempt=self._entry.attempt + 1))
            else:
                await self._broker.deadletter(self._entry)
        finally:
            self._broker.release(self._entry)


class RedisBroker:
    """At-least-once broker over Redis Streams + consumer groups."""

    guarantee = DeliveryGuarantee.AT_LEAST_ONCE
    DEFAULT_GROUP: Final[str] = "mint-worker"
    DLQ_SUFFIX: Final[str] = ".dlq"
    BODY_FIELD: Final[bytes] = b"body"
    ATTEMPT_FIELD: Final[bytes] = b"attempt"
    DEFAULT_BLOCK_MS: Final[int] = 5_000
    DEFAULT_RECLAIM_IDLE_MS: Final[int] = 60_000
    # How many self-owned entries to skip past per poll before giving up until the
    # next one. Bounded so a large pending list can't stall the consume loop.
    RECLAIM_SCAN_LIMIT: Final[int] = 8

    def __init__(
        self,
        uri: str,
        *,
        group: str = DEFAULT_GROUP,
        consumer_name: str | None = None,
        block_ms: int = DEFAULT_BLOCK_MS,
        reclaim_idle_ms: int = DEFAULT_RECLAIM_IDLE_MS,
    ) -> None:
        """Configure a broker over ``uri``; nothing connects until first use.

        ``reclaim_idle_ms`` is how long a delivered-but-unacked message may sit in
        another consumer's pending list before this one takes it over. It bounds
        how long a crashed consumer's in-flight work stays stuck, so it should
        comfortably exceed the slowest expected handler.
        """
        self.uri = uri
        self.group = group
        self.consumer_name = consumer_name or str(uuid4())
        self.block_ms = block_ms
        self.reclaim_idle_ms = reclaim_idle_ms
        self._client: Redis[bytes] | None = None
        # Entries this consumer is currently handling. XAUTOCLAIM matches purely on
        # idle time, with no regard for who owns the entry — including entries this
        # very consumer is still working on. Any handler slower than reclaim_idle_ms
        # would otherwise have its own message handed back and processed a second
        # time, concurrently with the first.
        #
        # Keyed by (topic, id), never id alone: a stream id is unique only within
        # its own stream, and one broker is shared across every worker in a
        # WorkerApp. Two topics can hand out the same id in the same millisecond,
        # which would make one topic's entry mask the other's — and retiring the
        # first would strip the protection from the second while it was still live.
        self._inflight_ids: set[tuple[str, bytes]] = set()

    @property
    def client(self) -> "Redis[bytes]":
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
        await self.client.xadd(topic, _stream_fields(message, headers, attempt=1))

    async def consume(self, topic: str) -> AsyncIterator[RedisStreamDelivery]:
        """Yield deliveries from ``topic``'s stream via this broker's consumer group.

        Each pass reclaims abandoned work before reading new work, so a message
        whose consumer died before acking is redelivered here rather than sitting
        in that consumer's pending list forever.
        """
        await self._ensure_group(topic)
        while True:
            reclaimed = await self._reclaim(topic)
            if reclaimed is not None:
                yield RedisStreamDelivery(self, reclaimed)
                continue
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
                    yield RedisStreamDelivery(self, self._entry(topic, message_id, fields))

    async def _reclaim(self, topic: str) -> StreamEntry | None:
        """Take over one message abandoned by a consumer that never acked it.

        Returns None when there is nothing idle enough to claim, which is the
        normal case — the cost of this is one ``XAUTOCLAIM`` per poll.
        """
        cursor: bytes | str = "0-0"
        for _ in range(self.RECLAIM_SCAN_LIMIT):
            response = await self.client.xautoclaim(
                topic,
                self.group,
                self.consumer_name,
                self.reclaim_idle_ms,
                start_id=cursor,
                count=1,
            )
            # XAUTOCLAIM replies (next_cursor, entries[, deleted]) — the third element
            # only exists on Redis >= 7, so unpack positionally rather than by arity.
            entries = response[1] if len(response) > 1 else []
            if not entries:
                return None
            message_id, fields = entries[0]
            if (topic, message_id) not in self._inflight_ids:
                return self._entry(topic, message_id, fields)
            # One of ours, still running. XAUTOCLAIM scans in id order, so returning
            # here would let a single slow handler hide every abandoned entry behind
            # it — advance past it and keep looking instead.
            cursor = response[0]
        return None

    def _entry(
        self,
        topic: str,
        message_id: bytes,
        fields: Mapping[bytes, bytes],
    ) -> StreamEntry:
        """Build a StreamEntry from one raw stream reply."""
        body = fields.get(self.BODY_FIELD, b"")
        attempt = int(fields.get(self.ATTEMPT_FIELD, b"1"))
        headers = {
            key: value
            for key, value in fields.items()
            if key not in (self.BODY_FIELD, self.ATTEMPT_FIELD)
        }
        self._inflight_ids.add((topic, message_id))
        return StreamEntry(topic, message_id, body, attempt, headers or None)

    async def ack(self, entry: StreamEntry) -> None:
        """Acknowledge and remove ``entry`` from its stream."""
        await self._retire(entry)

    async def redeliver(self, entry: StreamEntry) -> None:
        """Re-append ``entry`` (already bumped), then retire the original.

        The new entry is written *before* the old one is acked and deleted. The
        reverse order left a window where a crash between the delete and the add
        lost the message outright — exactly the at-most-once behaviour bug #8's
        Streams rewrite existed to eliminate. This way a crash mid-sequence
        duplicates instead, which the canvas engine's idempotent fan-in already
        handles.
        """
        fields = _stream_fields(entry.body, entry.headers, attempt=entry.attempt)
        await self.client.xadd(entry.topic, fields)
        await self._retire(entry)

    async def deadletter(self, entry: StreamEntry) -> None:
        """Append ``entry`` to its topic's dead-letter stream, then retire the original.

        Same ordering as ``redeliver``, for the same reason: write first, retire
        second, so a crash duplicates rather than drops.
        """
        if entry.topic.endswith(self.DLQ_SUFFIX):
            # Terminate rather than extend, as every other broker here does. A worker
            # pointed at `orders.dlq` to reprocess failures would otherwise create and
            # write an `orders.dlq.dlq` stream once a message exhausted max_attempts.
            logger.warning(
                "Dropping a message already on a dead-letter stream",
                topic=entry.topic,
            )
            await self._retire(entry)
            return
        fields = _stream_fields(entry.body, entry.headers)
        await self.client.xadd(f"{entry.topic}{self.DLQ_SUFFIX}", fields)
        await self._retire(entry)

    def release(self, entry: StreamEntry) -> None:
        """Drop ``entry`` from the reclaim guard, settled or not."""
        self._inflight_ids.discard((entry.topic, entry.message_id))

    async def _retire(self, entry: StreamEntry) -> None:
        """Ack ``entry`` out of the pending list and delete it from its stream."""
        # types-redis declares xack() without annotations, so mypy sees an untyped
        # call in a typed context. Nothing on our side can make it typed.
        await self.client.xack(  # type: ignore[no-untyped-call]
            entry.topic,
            self.group,
            entry.message_id,
        )
        await self.client.xdel(entry.topic, entry.message_id)

    async def close(self) -> None:
        """Release the underlying Redis connection."""
        if self._client is not None:
            # redis-py's own .close() is deprecated in favor of .aclose() since
            # 5.0.1, but the installed stub package doesn't declare aclose() on
            # Redis (verified) even though it exists and works at runtime.
            await self._client.aclose()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
