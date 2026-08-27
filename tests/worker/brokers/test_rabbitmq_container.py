"""RabbitMQBroker against a real broker: proving bug #9's fix actually holds.

The one thing mocking cannot prove: that a rejected message genuinely, physically
lands in the dead-letter queue instead of vanishing. Run standalone, memory-capped,
per the project's standing rule: ``make test-worker-capped
TARGET=tests/worker/brokers/test_rabbitmq_container.py``. Never bundled with
another broker's container test.

Every test here keeps its ``consume()`` async generator alive in a named local for
the whole test and pulls every delivery from that *same* generator — mirroring how
``Worker.run()`` drives it (``async for delivery in broker.consume(topic):``, held
for the loop's lifetime), and never the fire-and-forget ``anext(broker.consume(t))``
shape. A consumer owns its channel and closes it when the generator is finalized,
so a dropped generator closes the channel out from under any delivery still
waiting to be acked — a real ``ChannelInvalidStateError``, caught live here. The
Kafka container tests carry the same rule for the same underlying reason.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
from testcontainers.community.rabbitmq import RabbitMqContainer

from mint.worker.brokers.rabbitmq import RabbitMQBroker


@pytest.fixture(scope="module")
def rabbitmq_uri() -> Iterator[str]:
    """Start one RabbitMQ container for this module's tests; stop it when they're done."""
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(container.port)
        yield f"amqp://{container.username}:{container.password}@{host}:{port}/"


@pytest.fixture
async def broker(rabbitmq_uri: str) -> AsyncIterator[RabbitMQBroker]:
    """Return a fresh broker over the shared container."""
    instance = RabbitMQBroker(rabbitmq_uri)
    yield instance
    await instance.close()


def unique_topic() -> str:
    """Return a fresh topic name so each test gets its own exchange/queue/DLX."""
    return f"test.{uuid4().hex}"


class TestPublishConsume:
    """Basic publish/consume/ack round-tripping against a real broker."""

    async def test_publish_then_consume_round_trips(self, broker: RabbitMQBroker) -> None:
        """A published message must arrive at the consumer with the same body."""
        topic = unique_topic()
        await broker.publish(topic, b"payload")

        delivery = await anext(broker.consume(topic))

        assert delivery.body == b"payload"
        await delivery.ack()


class TestDeadLetterRegression:
    """Regression for bug #9: a rejected message must land in the DLQ, not vanish."""

    async def test_nack_requeue_false_lands_in_the_dead_letter_queue(
        self,
        broker: RabbitMQBroker,
    ) -> None:
        """The exact scenario that used to silently drop messages."""
        topic = unique_topic()
        await broker.publish(topic, b"doomed payload")
        consumer = broker.consume(topic)
        delivery = await anext(consumer)

        await delivery.nack(requeue=False)

        dlq_topic = f"{topic}{RabbitMQBroker.DLQ_SUFFIX}"
        dlq_consumer = broker.consume(dlq_topic)
        dead = await asyncio.wait_for(anext(dlq_consumer), timeout=10)
        assert dead.body == b"doomed payload"
        await dead.ack()

    async def test_nack_requeue_true_redelivers_with_an_incremented_attempt(
        self,
        broker: RabbitMQBroker,
    ) -> None:
        """A requeued nack must come back on its own topic with the attempt bumped.

        Only a real broker proves this: AMQP's native ``reject(requeue=True)``
        redelivers the original frame, so its headers can't be rewritten and
        ``attempt`` stayed 1 forever — while Redis, Kafka and MemoryBroker all
        increment it and ``IBroker``'s contract says so. A mock accepts either
        shape without complaint.
        """
        topic = unique_topic()
        await broker.publish(topic, b"retry me")
        consumer = broker.consume(topic)
        first = await anext(consumer)
        assert first.attempt == 1

        await first.nack(requeue=True)

        second = await asyncio.wait_for(anext(consumer), timeout=10)
        assert second.body == b"retry me"
        assert second.attempt == 2
        await second.ack()
