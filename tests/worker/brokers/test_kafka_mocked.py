"""KafkaBroker: producer/consumer/admin startup wiring, against mocked aiokafka classes.

Regression coverage for bug #7 (the ``started`` check that was always False, the
un-awaited ``create_topics``, and the invalid ``group_id`` producer kwarg) and for
the offset-reset bug found live during this session's container testing (aiokafka's
``auto_offset_reset`` default of ``"latest"`` silently skips a topic's backlog on a
consumer group's first attach): every assertion here is about whether our code
actually starts/awaits/configures the right client, not about real broker behavior —
that's a container-test concern.
"""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiokafka.errors import TopicAlreadyExistsError

from aiokafka import TopicPartition

from mint.worker.brokers.kafka import ATTEMPT_HEADER, KafkaBroker, KafkaDelivery
from mint.worker.enums import DeliveryGuarantee

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

TOPIC = "sync.reference"


async def async_iter(items: list[object]) -> AsyncIterator[object]:
    """Turn a plain list into a proper async iterator, for mocking consumer.__aiter__()."""
    for item in items:
        yield item


@pytest.fixture
def mock_producer_cls(mocker: "MockerFixture") -> AsyncMock:
    """Patch AIOKafkaProducer to return a fresh mock instance on construction."""
    cls = mocker.patch("mint.worker.brokers.kafka.AIOKafkaProducer")
    cls.return_value = mocker.AsyncMock()
    return cls


@pytest.fixture
def mock_consumer_cls(mocker: "MockerFixture") -> AsyncMock:
    """Patch AIOKafkaConsumer to return a fresh mock instance on construction."""
    cls = mocker.patch("mint.worker.brokers.kafka.AIOKafkaConsumer")
    instance = mocker.AsyncMock()
    instance.__aiter__ = mocker.Mock(return_value=async_iter([]))
    cls.return_value = instance
    return cls


@pytest.fixture
def mock_admin_cls(mocker: "MockerFixture") -> AsyncMock:
    """Patch AIOKafkaAdminClient to return a fresh mock instance on construction."""
    cls = mocker.patch("mint.worker.brokers.kafka.AIOKafkaAdminClient")
    cls.return_value = mocker.AsyncMock()
    return cls


@pytest.mark.usefixtures("mock_admin_cls")
class TestProducerStartup:
    """Regression for bug #7: the producer must actually be constructed and started.

    Every test here also needs ``mock_admin_cls`` active (``publish`` always
    ensures the topic first), but only for its patching side-effect — no test in
    this class asserts anything about the admin client itself, so it's applied via
    ``usefixtures`` rather than a parameter every method would leave unused.
    """

    async def test_publish_starts_the_producer_exactly_once(
        self,
        mock_producer_cls: AsyncMock,
    ) -> None:
        """A second publish must reuse the already-started producer, not restart it."""
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"payload")
        await broker.publish(TOPIC, b"payload again")

        mock_producer_cls.return_value.start.assert_awaited_once()

    async def test_publish_never_passes_group_id_to_the_producer(
        self,
        mock_producer_cls: AsyncMock,
    ) -> None:
        """Regression: AIOKafkaProducer(group_id=...) isn't a valid constructor argument."""
        broker = KafkaBroker("localhost:9092", group_id="my-group")

        await broker.publish(TOPIC, b"payload")

        _, kwargs = mock_producer_cls.call_args
        assert "group_id" not in kwargs

    async def test_publish_sends_and_waits_with_an_attempt_one_header(
        self,
        mock_producer_cls: AsyncMock,
    ) -> None:
        """A fresh publish must send with attempt=1 recorded in the headers."""
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"payload")

        mock_producer_cls.return_value.send_and_wait.assert_awaited_once()
        _, kwargs = mock_producer_cls.return_value.send_and_wait.await_args
        assert kwargs["value"] == b"payload"
        assert (ATTEMPT_HEADER, b"1") in kwargs["headers"]


@pytest.mark.usefixtures("mock_producer_cls")
class TestAdminStartupAndTopicCreation:
    """Regression for bug #7: create_topics must be awaited, and the admin client started.

    Every test here also constructs a producer (``publish`` always ensures one),
    but ``mock_producer_cls`` is only needed for its patching side-effect — nothing
    in this class asserts on the producer, so it's applied via ``usefixtures``.
    """

    async def test_ensure_topic_starts_the_admin_client(
        self,
        mock_admin_cls: AsyncMock,
    ) -> None:
        """The admin client must be started before create_topics is ever called."""
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"payload")

        mock_admin_cls.return_value.start.assert_awaited_once()

    async def test_ensure_topic_awaits_create_topics(
        self,
        mock_admin_cls: AsyncMock,
    ) -> None:
        """create_topics must actually be awaited — the original silently discarded it."""
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"payload")

        mock_admin_cls.return_value.create_topics.assert_awaited_once()
        (topics,) = mock_admin_cls.return_value.create_topics.await_args.args
        assert topics[0].name == TOPIC

    async def test_topic_already_exists_is_swallowed(
        self,
        mock_admin_cls: AsyncMock,
    ) -> None:
        """A topic that already exists must not fail the publish."""
        mock_admin_cls.return_value.create_topics.side_effect = TopicAlreadyExistsError()
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"payload")  # must not raise

    async def test_admin_client_is_started_only_once_across_calls(
        self,
        mock_admin_cls: AsyncMock,
    ) -> None:
        """Two publishes to different topics must not restart the admin client."""
        broker = KafkaBroker("localhost:9092")

        await broker.publish(TOPIC, b"a")
        await broker.publish("another.topic", b"b")

        mock_admin_cls.return_value.start.assert_awaited_once()
        assert mock_admin_cls.return_value.create_topics.await_count == 2


@pytest.mark.usefixtures("mock_admin_cls")
class TestConsume:
    """consume() must actually start the consumer and stop it on exit.

    Every test here also ensures the topic (``consume`` calls ``_ensure_topic``
    first), but ``mock_admin_cls`` is only needed for its patching side-effect —
    nothing in this class asserts on the admin client, so it's applied via
    ``usefixtures``.
    """

    async def test_consume_starts_and_stops_the_consumer(
        self,
        mock_consumer_cls: AsyncMock,
    ) -> None:
        """Regression: the original's `started is None` check never actually started it."""
        broker = KafkaBroker("localhost:9092")

        async for _ in broker.consume(TOPIC):
            pass

        mock_consumer_cls.return_value.start.assert_awaited_once()
        mock_consumer_cls.return_value.stop.assert_awaited_once()

    async def test_consumer_is_constructed_with_manual_commit(
        self,
        mock_consumer_cls: AsyncMock,
    ) -> None:
        """Auto-commit must be off — acking is this broker's own responsibility."""
        broker = KafkaBroker("localhost:9092", group_id="my-group")

        async for _ in broker.consume(TOPIC):
            pass

        _, kwargs = mock_consumer_cls.call_args
        assert kwargs["enable_auto_commit"] is False
        assert kwargs["group_id"] == "my-group"

    async def test_consumer_resets_to_earliest_not_latest(
        self,
        mock_consumer_cls: AsyncMock,
    ) -> None:
        """A brand-new consumer group must see a topic's backlog, not skip past it.

        aiokafka defaults to ``auto_offset_reset="latest"``, which would silently
        drop whatever was already published before this group's first attach.
        """
        broker = KafkaBroker("localhost:9092")

        async for _ in broker.consume(TOPIC):
            pass

        _, kwargs = mock_consumer_cls.call_args
        assert kwargs["auto_offset_reset"] == "earliest"

    async def test_consume_yields_a_delivery_per_record(
        self,
        mocker: "MockerFixture",
        mock_consumer_cls: AsyncMock,
    ) -> None:
        """Each ConsumerRecord off the async iterator must come back as a KafkaDelivery."""
        record = mocker.MagicMock()
        record.value = b"payload"
        record.headers = ((ATTEMPT_HEADER, b"2"),)
        mock_consumer_cls.return_value.__aiter__ = mocker.Mock(return_value=async_iter([record]))
        broker = KafkaBroker("localhost:9092")

        delivery = await anext(broker.consume(TOPIC))

        assert isinstance(delivery, KafkaDelivery)
        assert delivery.body == b"payload"
        assert delivery.attempt == 2


class TestAckNack:
    """KafkaDelivery.ack()/nack() must commit its own consumer, and republish/dead-letter."""

    def _record(self, mocker: "MockerFixture", **overrides: object) -> MagicMock:
        record = mocker.MagicMock()
        record.value = overrides.get("value", b"payload")
        record.topic = overrides.get("topic", TOPIC)
        record.headers = overrides.get("headers", ())
        record.partition = overrides.get("partition", 0)
        record.offset = overrides.get("offset", 41)
        return record

    async def test_ack_commits_this_records_own_partition_offset(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """ack() must commit `offset + 1` for this record's partition, nothing wider.

        A bare ``commit()`` commits every partition's current fetch position,
        which ``Worker.run()``'s concurrent handling would push past records
        still in flight — losing them if the process then dies.
        """
        broker = KafkaBroker("localhost:9092")
        consumer = mocker.AsyncMock()
        record = self._record(mocker, partition=3, offset=41)
        delivery = KafkaDelivery(broker, record, consumer)

        await delivery.ack()

        consumer.commit.assert_awaited_once_with({TopicPartition(TOPIC, 3): 42})

    async def test_each_delivery_commits_the_consumer_it_came_from(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """One broker serves many workers, so a delivery must never commit a sibling's consumer.

        With a single shared ``_consumer`` slot, worker A's ack committed worker
        B's consumer: A replayed everything on restart while B's offsets advanced
        past records it was still processing.
        """
        broker = KafkaBroker("localhost:9092")
        consumer_a, consumer_b = mocker.AsyncMock(), mocker.AsyncMock()
        delivery_a = KafkaDelivery(broker, self._record(mocker, topic="a"), consumer_a)
        delivery_b = KafkaDelivery(broker, self._record(mocker, topic="b"), consumer_b)

        await delivery_a.ack()

        consumer_a.commit.assert_awaited_once()
        consumer_b.commit.assert_not_awaited()

        await delivery_b.ack()

        consumer_a.commit.assert_awaited_once()
        consumer_b.commit.assert_awaited_once()

    async def test_nack_requeue_true_republishes_with_incremented_attempt(
        self,
        mocker: "MockerFixture",
        mock_producer_cls: AsyncMock,
    ) -> None:
        """A requeued nack must republish with attempt bumped, then commit."""
        broker = KafkaBroker("localhost:9092")
        consumer = mocker.AsyncMock()
        record = self._record(mocker, headers=((ATTEMPT_HEADER, b"1"),))
        delivery = KafkaDelivery(broker, record, consumer)

        await delivery.nack(requeue=True)

        mock_producer_cls.return_value.send_and_wait.assert_awaited_once()
        _, kwargs = mock_producer_cls.return_value.send_and_wait.await_args
        assert (ATTEMPT_HEADER, b"2") in kwargs["headers"]
        consumer.commit.assert_awaited_once()

    @pytest.mark.usefixtures("mock_admin_cls")
    async def test_nack_requeue_false_publishes_to_the_dead_letter_topic(
        self,
        mocker: "MockerFixture",
        mock_producer_cls: AsyncMock,
    ) -> None:
        """A dropped message must land on `{topic}.dlq`, then still commit past it."""
        broker = KafkaBroker("localhost:9092")
        consumer = mocker.AsyncMock()
        record = self._record(mocker, topic=TOPIC, value=b"doomed")
        delivery = KafkaDelivery(broker, record, consumer)

        await delivery.nack(requeue=False)

        mock_producer_cls.return_value.send_and_wait.assert_awaited_once_with(
            f"{TOPIC}{KafkaBroker.DLQ_SUFFIX}",
            value=b"doomed",
            headers=[],
        )

    async def test_nack_requeue_false_creates_the_dead_letter_topic_first(
        self,
        mocker: "MockerFixture",
        mock_admin_cls: AsyncMock,
        mock_producer_cls: AsyncMock,
    ) -> None:
        """The DLQ topic is created like any other — a broker with auto-create off still works."""
        broker = KafkaBroker("localhost:9092")
        delivery = KafkaDelivery(broker, self._record(mocker), mocker.AsyncMock())

        await delivery.nack(requeue=False)

        mock_admin_cls.return_value.create_topics.assert_awaited_once()
        (new_topics,), _ = mock_admin_cls.return_value.create_topics.await_args
        assert new_topics[0].name == f"{TOPIC}{KafkaBroker.DLQ_SUFFIX}"

    @pytest.mark.usefixtures("mock_admin_cls")
    async def test_nack_requeue_false_converts_the_records_header_tuple_to_a_list(
        self,
        mocker: "MockerFixture",
        mock_producer_cls: AsyncMock,
    ) -> None:
        """Regression for bug #17: aiokafka's producer requires a list, not a tuple.

        A real ``ConsumerRecord.headers`` is a tuple; a mocked producer never
        enforces the type difference, so this asserts on the type explicitly
        rather than relying on equality alone catching a silent tuple pass-through.
        """
        broker = KafkaBroker("localhost:9092")
        consumer = mocker.AsyncMock()
        record = self._record(mocker, headers=((ATTEMPT_HEADER, b"1"), ("trace", b"abc")))
        delivery = KafkaDelivery(broker, record, consumer)

        await delivery.nack(requeue=False)

        _, kwargs = mock_producer_cls.return_value.send_and_wait.await_args
        assert isinstance(kwargs["headers"], list)
        assert kwargs["headers"] == [(ATTEMPT_HEADER, b"1"), ("trace", b"abc")]
        consumer.commit.assert_awaited_once()


class TestGuaranteeAndClose:
    """Declared delivery guarantee and resource cleanup."""

    def test_declares_at_least_once_guarantee(self) -> None:
        """KafkaBroker is at-least-once."""
        assert KafkaBroker.guarantee == DeliveryGuarantee.AT_LEAST_ONCE

    async def test_close_stops_only_what_was_started(self, mocker: "MockerFixture") -> None:
        """A broker that never touched Kafka must not construct clients just to close them."""
        broker = KafkaBroker("localhost:9092")

        await broker.close()  # must not raise

        broker._producer = mocker.AsyncMock()
        broker._consumers["t"] = mocker.AsyncMock()
        broker._admin = mocker.AsyncMock()
        consumer = broker._consumers["t"]
        await broker.close()

        broker._producer.stop.assert_awaited_once()
        consumer.stop.assert_awaited_once()
        broker._admin.close.assert_awaited_once()

    async def test_close_stops_every_consumer_not_just_the_last(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A shared broker holds one consumer per topic; close() must stop all of them."""
        broker = KafkaBroker("localhost:9092")
        consumers = {topic: mocker.AsyncMock() for topic in ("a", "b", "c")}
        broker._consumers.update(consumers)

        await broker.close()

        for consumer in consumers.values():
            consumer.stop.assert_awaited_once()
        assert broker._consumers == {}
