"""KafkaBroker against a real broker: proving the bug #7/#16 fixes actually hold.

The one thing mocking cannot prove: that the consumer really starts and consumes
from a real broker (JVM `KafkaConsumer` protocol handshake, group coordination,
auto topic creation, offset semantics), and that a rejected message really lands
on `{topic}.dlq`.

Every test here keeps its `consume()` async generator alive in a named local
variable for the whole test and pulls every delivery it needs from that *same*
generator, mirroring how `Worker.run()` actually drives it
(`async for delivery in broker.consume(topic):`, held for the loop's lifetime) —
never the fire-and-forget `anext(broker.consume(topic))` pattern the RabbitMQ/Redis
container tests use. Kafka's ack/nack route through the broker's own shared
`self._consumer` (unlike RabbitMQ/Redis, where ack/nack are self-contained on the
delivery), so letting a generator go out of scope mid-test schedules its
`finally: await consumer.stop()` as a background task that races the next
`ack()`/`nack()` call against that same consumer — a real hang, caught live while
first writing this file with the fire-and-forget pattern.

This is the heaviest single test file in this whole suite — Kafka's JVM broker
image. Run standalone, memory-capped, per the project's standing rule:
``make test-worker-capped TARGET=tests/worker/brokers/test_kafka_container.py
MEM=2G``. Never bundled with another broker's container test.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
from testcontainers.community.kafka import KafkaContainer

from mint.worker.brokers.kafka import KafkaBroker

FETCH_TIMEOUT_SECONDS = 30


@pytest.fixture(scope="module")
def kafka_bootstrap_server() -> Iterator[str]:
    """Start one Kafka container for this module's tests; stop it when they're done."""
    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


@pytest.fixture
async def broker(kafka_bootstrap_server: str) -> AsyncIterator[KafkaBroker]:
    """Return a fresh broker over the shared container, with a unique consumer group.

    A unique ``group_id`` per test keeps consumer-group state (offsets, membership)
    from leaking between tests sharing one broker/topic-prefix-free container.
    """
    instance = KafkaBroker(kafka_bootstrap_server, group_id=f"test-{uuid4().hex}")
    yield instance
    await instance.close()


def unique_topic() -> str:
    """Return a fresh topic name so each test gets its own auto-created topic."""
    return f"test.{uuid4().hex}"


class TestPublishConsume:
    """Regression for bug #7: the consumer must actually start and receive records."""

    async def test_publish_then_consume_round_trips(self, broker: KafkaBroker) -> None:
        """A published message must arrive at the consumer with the same body."""
        topic = unique_topic()
        await broker.publish(topic, b"payload")
        consumer = broker.consume(topic)

        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        assert delivery.body == b"payload"
        await delivery.ack()


class TestOffsetResetRegression:
    """Regression for bug #16: a brand-new consumer group must see the backlog."""

    async def test_a_message_published_before_the_first_ever_consume_is_still_delivered(
        self,
        broker: KafkaBroker,
    ) -> None:
        """The exact scenario aiokafka's default `auto_offset_reset="latest"` broke."""
        topic = unique_topic()
        await broker.publish(topic, b"already waiting")
        await broker.publish(topic, b"already waiting too")
        consumer = broker.consume(topic)

        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        assert delivery.body == b"already waiting"
        await delivery.ack()


class TestDeadLetterRegression:
    """A rejected message must land on the topic's dead-letter topic, not vanish."""

    async def test_nack_requeue_false_lands_in_the_dead_letter_topic(
        self,
        broker: KafkaBroker,
    ) -> None:
        """The exact scenario the DLQ routing exists for."""
        topic = unique_topic()
        await broker.publish(topic, b"doomed payload")
        consumer = broker.consume(topic)
        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        await delivery.nack(requeue=False)

        dlq_consumer = broker.consume(f"{topic}{KafkaBroker.DLQ_SUFFIX}")
        dead = await asyncio.wait_for(anext(dlq_consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert dead.body == b"doomed payload"
        await dead.ack()

    async def test_nack_requeue_true_redelivers_with_incremented_attempt(
        self,
        broker: KafkaBroker,
    ) -> None:
        """A requeued nack must republish with the attempt bumped, on the same topic."""
        topic = unique_topic()
        await broker.publish(topic, b"retry me")
        consumer = broker.consume(topic)
        first = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert first.attempt == 1

        await first.nack(requeue=True)

        second = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert second.body == b"retry me"
        assert second.attempt == 2
        await second.ack()
