"""Kafka broker — at-least-once, with the original's startup bugs fixed (bug #7).

Three separate bugs in the original made the consumer/admin path completely
non-functional:

- ``started`` was checked with ``is None``, but it's a bool property that's never
  actually ``None`` (``self._fetcher is not None`` always returns True/False) — so
  the guard was always false and the consumer was never started.
- ``admin_client.create_topics(...)`` was called without ``await``, silently
  discarding the coroutine and doing nothing.
- ``AIOKafkaProducer(group_id=...)`` isn't a valid producer constructor argument
  at all — ``group_id`` is a consumer-only concept.

Here every client (producer/consumer/admin) is started explicitly, lazily, and
awaited — no property-based "is it started" heuristics.

A fourth issue was found live during this session's container testing:
``AIOKafkaConsumer`` defaults ``auto_offset_reset`` to ``"latest"``, so a brand-new
consumer group (e.g. a worker restarting with a fresh ``group_id``, or a topic that
already has a backlog when a group first attaches) silently starts *after* whatever
is already sitting in the topic — an at-least-once violation. Every consumer here is
built with ``auto_offset_reset="earliest"`` instead: with manual commit already off
for auto-commit, this only affects a group's *first-ever* attach to a topic, at
which point it must see the backlog, not skip it.

A fifth issue, also only reachable against a real broker: a real ``ConsumerRecord``'s
``.headers`` comes back as a ``tuple`` of pairs, but aiokafka's producer requires a
``list`` (its Cython record-batch builder indexes into it and rejects a tuple with
``TypeError: Expected list, got tuple``). ``deadletter`` passed the tuple straight
through; a mocked producer never enforces the type difference, so this only ever
surfaced against the real broker. Fixed by copying into a ``list`` before publishing.

A sixth, found by review: the broker kept a single ``self._consumer`` slot even
though ``WorkerApp`` deliberately shares one broker across every registered
worker. A second ``consume()`` overwrote the first, so worker A's ack committed
worker B's consumer — A replayed everything on restart while B's offsets
advanced past records still in flight. Consumers are now keyed by topic, each
``KafkaDelivery`` carries the consumer it came from, and a commit names that
record's own partition offset instead of the consumer's whole fetch position
(which ``Worker.run()``'s concurrent handling would otherwise commit past).
"""

from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from mint.logger import get_logger
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord

logger = get_logger(__name__)

ATTEMPT_HEADER: Final[str] = "x-mint-attempt"


class KafkaDelivery:
    """One delivered Kafka record, with ack/nack via manual commit + republish."""

    def __init__(
        self,
        broker: "KafkaBroker",
        record: "ConsumerRecord",
        consumer: AIOKafkaConsumer,
    ) -> None:
        """Wrap ``record``, reading its attempt count from headers (default 1).

        ``consumer`` is the one this record was actually fetched from, carried
        explicitly rather than looked up on the broker: a single ``KafkaBroker``
        is shared across every worker in a ``WorkerApp``, so there is no single
        "current" consumer to commit against.
        """
        self._broker = broker
        self._record = record
        self._consumer = consumer
        # Constructing a delivery *is* what "this offset is in flight" means, so the
        # broker learns about it here rather than in the consume loop — nothing can
        # hand out a delivery without its offset being tracked.
        broker.track(record)
        self.body: bytes = record.value if record.value is not None else b""
        headers = dict(record.headers or ())
        raw_attempt = headers.get(ATTEMPT_HEADER)
        self.attempt = int(raw_attempt) if raw_attempt else 1

    async def ack(self) -> None:
        """Commit past this record's offset, on this record's own consumer."""
        await self._commit()

    async def nack(self, *, requeue: bool) -> None:
        """Republish with an incremented attempt, or dead-letter — then commit either way.

        Kafka has no native per-message requeue: redelivery here means
        re-publishing a fresh record and committing past the original, since a
        consumer group's offset can only move forward.
        """
        if requeue:
            await self._broker.redeliver(self._record, self.attempt + 1)
        else:
            await self._broker.deadletter(self._record)
        await self._commit()

    async def _commit(self) -> None:
        """Commit only the contiguous run of settled offsets behind this record.

        A bare ``consumer.commit()`` commits every partition's current fetch
        position, and committing this record's own ``offset + 1`` is no better:
        ``Worker.run()`` settles up to ``max_concurrency`` records concurrently, so
        they finish out of order. Settling offset 7 while 5 and 6 are still running
        would move the group past all three, and a restart would never redeliver
        them — silent loss, from a broker declaring at-least-once.

        The broker therefore tracks what is in flight per partition and hands back
        the highest offset with nothing unsettled before it, or None when this
        record was not the blocker.
        """
        commit_offset = self._broker.settle(self._record)
        if commit_offset is None:
            return
        partition = TopicPartition(self._record.topic, self._record.partition)
        await self._consumer.commit({partition: commit_offset})


class KafkaBroker:
    """At-least-once broker over Kafka: manual commit, explicit lazy client startup."""

    guarantee = DeliveryGuarantee.AT_LEAST_ONCE
    DEFAULT_GROUP_ID: Final[str] = "mint-worker"
    DLQ_SUFFIX: Final[str] = ".dlq"
    NUM_PARTITIONS: Final[int] = 1
    REPLICATION_FACTOR: Final[int] = 1

    def __init__(self, bootstrap_servers: str, *, group_id: str = DEFAULT_GROUP_ID) -> None:
        """Configure a broker over ``bootstrap_servers``; nothing connects until first use."""
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._producer: AIOKafkaProducer | None = None
        # One consumer per topic, not one slot: a WorkerApp shares a single broker
        # across every registered worker, and each worker consumes its own topic.
        self._consumers: dict[str, AIOKafkaConsumer] = {}
        self._admin: AIOKafkaAdminClient | None = None
        # Per partition: offsets delivered but not yet settled, in delivery order,
        # plus the settled ones still waiting on an earlier offset. See settle().
        self._inflight: dict[tuple[str, int], list[int]] = {}
        self._settled: dict[tuple[str, int], set[int]] = {}

    async def _ensure_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await producer.start()
            self._producer = producer
        return self._producer

    async def _ensure_admin(self) -> AIOKafkaAdminClient:
        if self._admin is None:
            admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            await admin.start()
            self._admin = admin
        return self._admin

    async def _ensure_topic(self, topic: str) -> None:
        admin = await self._ensure_admin()
        new_topic = NewTopic(
            name=topic,
            num_partitions=self.NUM_PARTITIONS,
            replication_factor=self.REPLICATION_FACTOR,
        )
        with suppress(TopicAlreadyExistsError):
            await admin.create_topics([new_topic])

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``, creating it if needed, and wait for the ack."""
        await self._ensure_topic(topic)
        producer = await self._ensure_producer()
        # The attempt header is this broker's to own: a caller-supplied one is
        # dropped rather than duplicated, matching what redeliver() already does.
        # Kafka headers are a list of pairs, so a duplicate key really is carried on
        # the wire — only dict()-based readers happen not to notice.
        kafka_headers = [
            (key, value.encode())
            for key, value in (headers or {}).items()
            if key != ATTEMPT_HEADER
        ]
        kafka_headers.append((ATTEMPT_HEADER, b"1"))
        await producer.send_and_wait(topic, value=message, headers=kafka_headers)

    async def consume(self, topic: str) -> AsyncIterator[KafkaDelivery]:
        """Yield deliveries from ``topic`` via this broker's consumer group; commit manually."""
        await self._ensure_topic(topic)
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumers[topic] = consumer
        # No `finally` stopping the consumer: cancelling a task suspended in
        # `async for` unwinds the generator and runs it, which would stop the
        # consumer that in-flight handlers still need to commit on. `close()`
        # already stops every consumer and forgets its offsets.
        async for record in consumer:
            yield KafkaDelivery(self, record, consumer)

    def _forget_offsets(self, topic: str) -> None:
        """Drop offset bookkeeping for a topic whose consumer has stopped.

        Nothing can settle those offsets any more, so keeping them would block the
        partition for the lifetime of the broker if the topic is consumed again.
        """
        for key in [key for key in self._inflight if key[0] == topic]:
            unsettled = [
                offset for offset in self._inflight[key] if offset not in self._settled[key]
            ]
            if unsettled:
                # Their acks can no longer commit anything, so those records will be
                # reprocessed on restart. At-least-once, but say so rather than
                # letting it happen silently.
                logger.warning(
                    "Dropping offset bookkeeping with work still in flight",
                    topic=topic,
                    partition=key[1],
                    unsettled=len(unsettled),
                )
            del self._inflight[key]
            self._settled.pop(key, None)

    def track(self, record: "ConsumerRecord") -> None:
        """Record that ``record``'s offset is delivered and not yet settled.

        Idempotent per offset. A consumer-group rebalance redelivers offsets that
        were fetched but never committed, and appending a duplicate would wedge the
        partition permanently: ``settle`` pops one instance and discards the offset
        from the settled set, leaving the twin at the head of the queue with nothing
        that can ever clear it — no offset for that partition is committed again.
        """
        key = (record.topic, record.partition)
        inflight = self._inflight.setdefault(key, [])
        self._settled.setdefault(key, set())
        if record.offset not in inflight:
            inflight.append(record.offset)

    def settle(self, record: "ConsumerRecord") -> int | None:
        """Mark ``record`` settled and return the offset that is now safe to commit.

        Safe means every offset before it is settled too. Returns None while an
        earlier offset is still in flight — committing then would skip past live
        work. The settled offset is remembered and released later, by whichever
        record finally unblocks the run.
        """
        key = (record.topic, record.partition)
        inflight = self._inflight.get(key)
        if inflight is None or record.offset not in inflight:
            # Not queued: either the consumer already stopped, or this is the second
            # KafkaDelivery for an offset track() deliberately deduplicated. Recording
            # it anyway would leave a settled marker with nothing to pop, and if that
            # offset is ever tracked again it would count as settled the moment it
            # reached the head — committing past a handler still running.
            return None
        self._settled[key].add(record.offset)

        commit_offset: int | None = None
        while inflight and inflight[0] in self._settled[key]:
            offset = inflight.pop(0)
            self._settled[key].discard(offset)
            commit_offset = offset + 1
        return commit_offset

    async def redeliver(self, record: "ConsumerRecord", attempt: int) -> None:
        """Re-publish ``record`` at the given attempt count."""
        producer = await self._ensure_producer()
        headers = [(key, value) for key, value in (record.headers or ()) if key != ATTEMPT_HEADER]
        headers.append((ATTEMPT_HEADER, str(attempt).encode()))
        await producer.send_and_wait(record.topic, value=record.value, headers=headers)

    async def deadletter(self, record: "ConsumerRecord") -> None:
        """Publish ``record`` to its topic's dead-letter topic.

        ``record.headers`` comes back from a real ``ConsumerRecord`` as a tuple of
        pairs; aiokafka's producer requires a ``list`` (its record-batch builder
        indexes into it), so it must be copied into one rather than passed through —
        a real, container-test-only-catchable bug (bug #17), since a mocked
        producer never enforces the type at all.
        """
        if record.topic.endswith(self.DLQ_SUFFIX):
            # Terminate rather than extend, as RabbitMQ and NATS already do. Without
            # this, dead-lettering a message consumed from foo.dlq creates and
            # publishes to foo.dlq.dlq, one new topic per pass.
            logger.warning(
                "Dropping a message already on a dead-letter topic",
                topic=record.topic,
            )
            return
        dlq_topic = f"{record.topic}{self.DLQ_SUFFIX}"
        await self._ensure_topic(dlq_topic)
        producer = await self._ensure_producer()
        await producer.send_and_wait(
            dlq_topic,
            value=record.value,
            headers=list(record.headers or ()),
        )

    async def close(self) -> None:
        """Stop the producer and every consumer, and close the admin client, if started."""
        if self._producer is not None:
            await self._producer.stop()
        for topic in list(self._consumers):
            self._forget_offsets(topic)
        for consumer in list(self._consumers.values()):
            await consumer.stop()
        self._consumers.clear()
        if self._admin is not None:
            await self._admin.close()
