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
"""

from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from typing import TYPE_CHECKING, Final

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord

ATTEMPT_HEADER: Final[str] = "x-mint-attempt"


class KafkaDelivery:
    """One delivered Kafka record, with ack/nack via manual commit + republish."""

    def __init__(self, broker: "KafkaBroker", record: "ConsumerRecord") -> None:
        """Wrap ``record``, reading its attempt count from headers (default 1)."""
        self._broker = broker
        self._record = record
        self.body: bytes = record.value if record.value is not None else b""
        headers = dict(record.headers or ())
        raw_attempt = headers.get(ATTEMPT_HEADER)
        self.attempt = int(raw_attempt) if raw_attempt else 1

    async def ack(self) -> None:
        """Commit this record's offset."""
        await self._broker.commit()

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
        await self._broker.commit()


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
        self._consumer: AIOKafkaConsumer | None = None
        self._admin: AIOKafkaAdminClient | None = None

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
        kafka_headers = [(key, value.encode()) for key, value in (headers or {}).items()]
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
        self._consumer = consumer
        try:
            async for record in consumer:
                yield KafkaDelivery(self, record)
        finally:
            await consumer.stop()

    async def commit(self) -> None:
        """Commit the current consumer's offsets."""
        if self._consumer is not None:
            await self._consumer.commit()

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
        producer = await self._ensure_producer()
        await producer.send_and_wait(
            f"{record.topic}{self.DLQ_SUFFIX}",
            value=record.value,
            headers=list(record.headers or ()),
        )

    async def close(self) -> None:
        """Stop the producer/consumer and close the admin client, whichever were started."""
        if self._producer is not None:
            await self._producer.stop()
        if self._consumer is not None:
            await self._consumer.stop()
        if self._admin is not None:
            await self._admin.close()
