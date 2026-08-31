"""RedisBroker against a real Redis: proving the at-least-once claim actually holds.

The one thing mocking cannot prove: that a message read by a consumer which then
dies without acking is genuinely recoverable. `XREADGROUP` with `>` returns only
never-delivered entries, so that message sits in the dead consumer's pending
entries list and reaches nobody unless something reclaims it — a mock will happily
return whatever the test tells it to and prove nothing about either.

Run standalone, memory-capped, per the project's standing rule: ``make
test-worker-capped TARGET=tests/worker/brokers/test_redis_container.py``. Never
bundled with another broker's container test.

Every test keeps its ``consume()`` generator in a named local for the whole test,
mirroring how ``Worker.run()`` drives it, rather than the fire-and-forget
``anext(broker.consume(topic))`` shape.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator
from uuid import uuid4

import pytest
from testcontainers.community.redis import RedisContainer

from mint.worker.brokers.redis import RedisBroker

RECLAIM_IDLE_MS = 50
FETCH_TIMEOUT_SECONDS = 10.0


@pytest.fixture(scope="module")
def redis_uri() -> Iterator[str]:
    """Start one Redis container for this module's tests; stop it when they're done."""
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def broker(redis_uri: str) -> "AsyncIterator[RedisBroker]":
    """Return a broker over the shared container, flushed before each test."""
    instance = RedisBroker(redis_uri, reclaim_idle_ms=RECLAIM_IDLE_MS)
    await instance.client.flushdb()
    yield instance
    await instance.close()


def unique_topic() -> str:
    """Return a fresh stream name so each test gets its own consumer group."""
    return f"test.{uuid4().hex}"


class TestPublishConsume:
    """The basic round trip, against the real Streams protocol."""

    async def test_publish_then_consume_round_trips(self, broker: RedisBroker) -> None:
        """A published message must arrive with the same body and attempt 1."""
        topic = unique_topic()
        await broker.publish(topic, b"payload")
        consumer = broker.consume(topic)

        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        assert delivery.body == b"payload"
        assert delivery.attempt == 1
        await delivery.ack()


class TestCrashRecovery:
    """Regression: a consumer that dies before acking must not strand its message."""

    async def test_a_message_left_unacked_by_a_dead_consumer_is_reclaimed(
        self,
        redis_uri: str,
        broker: RedisBroker,
    ) -> None:
        """The whole point of Streams over BRPOP (bug #8) — and it did not hold.

        `XREADGROUP` with `>` never returns another consumer's pending entries, and
        nothing here called `XAUTOCLAIM`, so this message was delivered exactly
        once and then lost forever: at-most-once, from a broker declaring
        at-least-once.
        """
        topic = unique_topic()
        await broker.publish(topic, b"orphaned")

        # A first consumer reads the message and then "crashes" — never acks.
        dead = RedisBroker(redis_uri, consumer_name="dead-consumer")
        dead_consumer = dead.consume(topic)
        first = await asyncio.wait_for(anext(dead_consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert first.body == b"orphaned"
        await dead.close()

        await asyncio.sleep(RECLAIM_IDLE_MS / 1000 * 2)

        survivor = broker.consume(topic)
        recovered = await asyncio.wait_for(anext(survivor), timeout=FETCH_TIMEOUT_SECONDS)

        assert recovered.body == b"orphaned"
        await recovered.ack()

    async def test_an_acked_message_is_never_reclaimed(self, broker: RedisBroker) -> None:
        """Reclaim must only ever pick up genuinely abandoned work."""
        topic = unique_topic()
        await broker.publish(topic, b"handled")
        consumer = broker.consume(topic)
        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)
        await delivery.ack()

        await asyncio.sleep(RECLAIM_IDLE_MS / 1000 * 2)

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(consumer), timeout=1.0)


class TestRedeliveryAndDeadLetter:
    """nack() must genuinely requeue or dead-letter against the real server."""

    async def test_nack_requeue_true_redelivers_with_an_incremented_attempt(
        self,
        broker: RedisBroker,
    ) -> None:
        """The redelivered entry must be a real new stream entry, not the original."""
        topic = unique_topic()
        await broker.publish(topic, b"retry me")
        consumer = broker.consume(topic)
        first = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        await first.nack(requeue=True)

        second = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert second.body == b"retry me"
        assert second.attempt == 2
        await second.ack()

    async def test_nack_requeue_false_lands_in_the_dead_letter_stream(
        self,
        broker: RedisBroker,
    ) -> None:
        """A dropped message must be inspectable on `{topic}.dlq`, not gone."""
        topic = unique_topic()
        await broker.publish(topic, b"doomed")
        consumer = broker.consume(topic)
        delivery = await asyncio.wait_for(anext(consumer), timeout=FETCH_TIMEOUT_SECONDS)

        await delivery.nack(requeue=False)

        dlq_consumer = broker.consume(f"{topic}{RedisBroker.DLQ_SUFFIX}")
        dead = await asyncio.wait_for(anext(dlq_consumer), timeout=FETCH_TIMEOUT_SECONDS)
        assert dead.body == b"doomed"
        await dead.ack()
