"""MemoryBroker: the in-process broker used by Worker/App tests and single-process deployments."""

import asyncio

import pytest

from mint.worker.brokers.memory import MemoryBroker
from mint.worker.enums import DeliveryGuarantee


class TestPublishConsume:
    """Basic publish/consume round-tripping."""

    async def test_publish_then_consume_round_trips_bytes(self) -> None:
        """A published message must arrive at the consumer byte-for-byte."""
        broker = MemoryBroker()
        await broker.publish("t1", b'{"x":1}')

        delivery = await anext(broker.consume("t1"))

        assert delivery.body == b'{"x":1}'
        assert delivery.attempt == 1

    async def test_headers_propagate_to_the_delivery(self) -> None:
        """Headers passed to publish() must be visible on the resulting delivery."""
        broker = MemoryBroker()
        await broker.publish("t1", b"{}", headers={"trace": "abc"})

        delivery = await anext(broker.consume("t1"))

        assert delivery.headers == {"trace": "abc"}

    async def test_declares_at_least_once_guarantee(self) -> None:
        """MemoryBroker is at-least-once: nack(requeue=True) redelivers."""
        assert MemoryBroker.guarantee == DeliveryGuarantee.AT_LEAST_ONCE


class TestAckNack:
    """ack()/nack() control over redelivery."""

    async def test_ack_is_a_no_op(self) -> None:
        """Acking must not raise and must not redeliver the message."""
        broker = MemoryBroker()
        await broker.publish("t1", b"body")
        delivery = await anext(broker.consume("t1"))

        await delivery.ack()

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(anext(broker.consume("t1")), timeout=0.05)

    async def test_nack_requeue_true_redelivers_with_incremented_attempt(self) -> None:
        """A requeued nack must come back on the same topic with attempt + 1."""
        broker = MemoryBroker()
        await broker.publish("t1", b"body")
        first = await anext(broker.consume("t1"))

        await first.nack(requeue=True)
        second = await anext(broker.consume("t1"))

        assert second.body == b"body"
        assert second.attempt == 2

    async def test_nack_requeue_false_routes_to_the_dead_letter_topic(self) -> None:
        """A rejected, non-requeued nack must land on `{topic}.dlq`, not vanish."""
        broker = MemoryBroker()
        await broker.publish("t1", b"body")
        delivery = await anext(broker.consume("t1"))

        await delivery.nack(requeue=False)

        dead = await anext(broker.consume(f"t1{MemoryBroker.DLQ_SUFFIX}"))
        assert dead.body == b"body"
        assert dead.attempt == 1

    async def test_nack_preserves_headers_across_redelivery_and_deadletter(self) -> None:
        """Headers must survive both requeue and dead-lettering, not just the first delivery."""
        broker = MemoryBroker()
        await broker.publish("t1", b"body", headers={"trace": "abc"})
        delivery = await anext(broker.consume("t1"))

        await delivery.nack(requeue=True)
        redelivered = await anext(broker.consume("t1"))
        await redelivered.nack(requeue=False)
        dead = await anext(broker.consume(f"t1{MemoryBroker.DLQ_SUFFIX}"))

        assert redelivered.headers == {"trace": "abc"}
        assert dead.headers == {"trace": "abc"}


class TestClose:
    """close() must unblock any consumer waiting on a queue, cleanly."""

    async def test_close_unblocks_a_pending_consume(self) -> None:
        """A consumer blocked on an empty queue must return (not hang) once close() runs."""
        broker = MemoryBroker()
        consumer = broker.consume("empty-topic")

        async def close_soon() -> None:
            await asyncio.sleep(0)
            await broker.close()

        results = await asyncio.gather(anext(consumer, "sentinel"), close_soon())

        assert (
            results[0] == "sentinel"
        )  # the generator returned (StopAsyncIteration), not a delivery

    async def test_close_is_idempotent(self) -> None:
        """Calling close() twice must not raise."""
        broker = MemoryBroker()
        await broker.close()

        await broker.close()


class TestCloseWakesEveryConsumer:
    """close() pushes one sentinel per queue, so consumers must pass it along."""

    async def test_a_queue_stays_drainable_after_close(self) -> None:
        """Anything already requeued must still be readable once the broker closes.

        That is what makes "was this nacked rather than dropped?" checkable.
        """
        broker = MemoryBroker()
        await broker.publish("t1", b"still here")

        await broker.close()

        async with asyncio.timeout(1.0):
            assert (await anext(broker.consume("t1"))).body == b"still here"

    async def test_two_consumers_already_waiting_when_close_lands_both_stop(self) -> None:
        """The real shape: both consumers parked on get() before close() is called.

        Only the first used to wake; the rest blocked on get() forever. A
        Coordinator alongside a worker on the same topic is exactly that shape.
        """
        broker = MemoryBroker()
        first, second = broker.consume("t1"), broker.consume("t1")
        waiting = [asyncio.ensure_future(anext(first)), asyncio.ensure_future(anext(second))]
        await asyncio.sleep(0)

        await broker.close()

        async with asyncio.timeout(1.0):
            results = await asyncio.gather(*waiting, return_exceptions=True)

        assert all(isinstance(result, StopAsyncIteration) for result in results)
