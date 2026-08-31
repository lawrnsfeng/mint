"""Worker[T, RT]: message decoding, hooks, canvas advancement, and ack/nack ordering.

Note: test 37 ("a ThreadExecutor runs a blocking process without stalling the
loop") is deferred to Phase 5. ``ITaskExecutor.execute`` takes an async callable
by design (``Worker.process`` is always a coroutine function) — a real thread-pool
executor for *blocking* work needs its own reconciled interface, which is exactly
what Phase 5 (``executors/thread_pool.py`` wired into ``Worker``) delivers. Writing
that test now against the current interface would test nothing meaningful.
"""

import asyncio
from collections.abc import Mapping
from typing import Any, cast

import pytest
from pydantic import BaseModel

from mint.worker.brokers.memory import MemoryBroker, MemoryDelivery
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ChainNode, GroupNode, NodeOutcome, TaskNode
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import NodeNotFoundError, WorkerNotBoundError
from mint.worker.executors.inline import InlineExecutor
from mint.worker.stores.memory import MemoryCanvasStore
from mint.worker.worker import Worker, WorkerBinding
from tests.worker.conftest import OrderSpy

CANVAS = "c1"
TOPIC = "double"
NEXT_MESSAGE_TIMEOUT = 1.0


async def next_delivery(broker: MemoryBroker, topic: str) -> MemoryDelivery:
    """Return the next delivery published to ``topic``, failing fast if none arrives.

    Deliberately timeout-guarded rather than a bare ``anext``: the bugs these
    tests cover manifest as a message that never arrives at all, and an unguarded
    ``anext`` on ``MemoryBroker`` would hang the whole suite instead of failing
    the one test (the same class of mistake as the ``envelope_delivery`` hang
    documented as bug #22).
    """
    async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
        return await anext(broker.consume(topic))


async def next_envelope(broker: MemoryBroker, topic: str) -> Envelope:
    """Return the next envelope published to ``topic``, failing fast if none arrives."""
    return Envelope.from_bytes((await next_delivery(broker, topic)).body)


class DoublingIn(BaseModel):
    """Test input: one integer."""

    value: int


class DoublingOut(BaseModel):
    """Test output: the doubled integer."""

    doubled: int


class DoublingWorker(Worker[DoublingIn, DoublingOut]):
    """Doubles its input; records every hook call for assertions."""

    topic = TOPIC
    Input = DoublingIn
    Output = DoublingOut

    def __init__(self) -> None:
        """Start with no recorded hook calls and no forced failure."""
        super().__init__()
        self.before_start_calls: list[DoublingIn] = []
        self.on_success_calls: list[tuple[DoublingIn, DoublingOut]] = []
        self.on_failure_calls: list[tuple[DoublingIn, Exception]] = []
        self.should_fail = False

    async def process(self, input_obj: DoublingIn) -> DoublingOut:
        """Double the input, or raise if ``should_fail`` is set."""
        if self.should_fail:
            raise RuntimeError("boom")
        return DoublingOut(doubled=input_obj.value * 2)

    async def before_start(self, input_obj: DoublingIn) -> None:
        """Record the call."""
        self.before_start_calls.append(input_obj)

    async def on_success(self, input_obj: DoublingIn, result: DoublingOut) -> None:
        """Record the call."""
        self.on_success_calls.append((input_obj, result))

    async def on_failure(self, input_obj: DoublingIn, exc: Exception) -> None:
        """Record the call."""
        self.on_failure_calls.append((input_obj, exc))


class BadOutputWorker(DoublingWorker):
    """A worker whose process() returns something DoublingOut can never validate."""

    async def process(self, input_obj: DoublingIn) -> DoublingOut:
        """Return a shape DoublingOut rejects, to prove bad output is treated as failure."""
        self.received_input = input_obj
        # Deliberately the wrong shape: proving that a bad return is treated as a
        # failure requires returning something DoublingOut rejects, which no honest
        # annotation can express.
        return cast("DoublingOut", {"totally": "wrong"})


class ExplodingBeforeStartWorker(DoublingWorker):
    """A DoublingWorker whose before_start() always raises."""

    async def before_start(self, input_obj: DoublingIn) -> None:
        """Record then raise."""
        await super().before_start(input_obj)
        raise RuntimeError("before_start failed")


class RaisingHooksWorker(DoublingWorker):
    """A DoublingWorker whose success/failure hooks always raise, after recording the call."""

    async def on_success(self, input_obj: DoublingIn, result: DoublingOut) -> None:
        """Record then raise."""
        await super().on_success(input_obj, result)
        raise RuntimeError("on_success exploded")

    async def on_failure(self, input_obj: DoublingIn, exc: Exception) -> None:
        """Record then raise."""
        await super().on_failure(input_obj, exc)
        raise RuntimeError("on_failure exploded")


class OrderTrackingStore(MemoryCanvasStore):
    """A MemoryCanvasStore that reports set_result to an OrderSpy."""

    def __init__(self, order_spy: OrderSpy) -> None:
        """Wrap a fresh store, reporting set_result to order_spy."""
        super().__init__()
        self._order_spy = order_spy

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Record the event, then persist as usual."""
        self._order_spy.record(f"store_write:{node_id}")
        await super().set_result(canvas_id, node_id, outcome)


class OrderTrackingBroker(MemoryBroker):
    """A MemoryBroker that reports publish() to an OrderSpy."""

    def __init__(self, order_spy: OrderSpy) -> None:
        """Wrap a fresh broker, reporting publish to order_spy."""
        super().__init__()
        self._order_spy = order_spy

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Record the event, then publish as usual."""
        self._order_spy.record(f"publish:{topic}")
        await super().publish(topic, message, headers=headers)


class OrderTrackingDelivery:
    """Wraps a MemoryDelivery, reporting ack()/nack() to an OrderSpy."""

    def __init__(self, inner: MemoryDelivery, order_spy: OrderSpy) -> None:
        """Wrap ``inner``, reporting ack/nack to order_spy."""
        self._inner = inner
        self._order_spy = order_spy
        self.body = inner.body
        self.attempt = inner.attempt

    async def ack(self) -> None:
        """Record the event, then ack as usual."""
        self._order_spy.record("ack")
        await self._inner.ack()

    async def nack(self, *, requeue: bool) -> None:
        """Record the event, then nack as usual."""
        self._order_spy.record(f"nack:requeue={requeue}")
        await self._inner.nack(requeue=requeue)


class RaisingOnPublishBroker(MemoryBroker):
    """A MemoryBroker whose publish() always raises — simulates a dead downstream broker."""

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Raise unconditionally, after recording the attempted call for inspection."""
        self.last_attempt = (topic, message, headers)
        detail = f"broker unreachable: {topic}"
        raise ConnectionError(detail)


def envelope_delivery(
    broker: MemoryBroker,
    node_id: str,
    canvas_id: str,
    body: str,
) -> MemoryDelivery:
    """Build a MemoryDelivery whose body is a well-formed Envelope, ready to hand to a worker.

    Must be built against the *same* broker the test later inspects — a nack routes
    back through this delivery's own broker, not whichever one the caller has in scope.
    """
    envelope = Envelope(node_id=node_id, canvas_id=canvas_id, body=body)
    return MemoryDelivery(broker, TOPIC, envelope.to_bytes(), attempt=1)


def bind_worker(
    worker: Worker[Any, Any],
    broker: MemoryBroker,
    store: MemoryCanvasStore,
    *,
    results_topic: str | None = None,
) -> WorkerBinding:
    """Bind worker to broker/store with a fresh engine and InlineExecutor; return the binding.

    ``results_topic`` set switches the binding into centralized mode.
    """
    binding = WorkerBinding(
        broker=broker,
        store=store,
        engine=CanvasEngine(store),
        executor=InlineExecutor(),
        results_topic=results_topic,
    )
    worker.bind(binding)
    return binding


class TestMalformedMessages:
    """Malformed envelopes/inputs must be dead-lettered, never silently acked."""

    async def test_body_that_is_not_json_is_dead_lettered_not_acked(self) -> None:
        """A delivery whose body isn't a valid Envelope must be nacked, not dropped silently."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        delivery = MemoryDelivery(broker, TOPIC, b"not json at all", attempt=1)

        await worker._process_delivery(delivery, binding)

        dead = await next_delivery(broker, f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}")
        assert dead.body == b"not json at all"

    async def test_body_that_fails_input_validation_is_dead_lettered_not_acked(self) -> None:
        """A well-formed Envelope whose body doesn't satisfy DoublingIn must also be nacked."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"not_value": "wrong shape"}')

        await worker._process_delivery(delivery, binding)

        dead = await next_delivery(broker, f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}")
        assert dead.attempt == 1


class TestUndeliverableInputFailsItsNode:
    """A body this worker's DoublingIn can never validate must fail the node, not strand it."""

    async def test_an_undecodable_input_records_an_error_outcome(self) -> None:
        """Retrying can't help, so the node must be failed rather than left PENDING forever.

        Dead-lettering alone left the node PENDING and its canvas RUNNING with
        nothing left to advance it — and embedded mode has no sweeper to notice.
        """
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"not_value": "wrong shape"}')

        await worker._process_delivery(delivery, binding)

        outcome = await store.get_result(CANVAS, "t1")
        assert outcome is not None
        assert outcome.status == NodeStatus.ERROR
        assert outcome.error is not None
        assert outcome.error.type == "ValidationError"

    async def test_an_undecodable_input_still_reaches_a_terminal_canvas_status(self) -> None:
        """A single bad message must not leave the whole canvas RUNNING forever."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._process_delivery(
            envelope_delivery(broker, "t1", CANVAS, "{}"),
            binding,
        )

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_an_undecodable_input_is_still_dead_lettered(self) -> None:
        """Recording the failure must not replace the dead-letter — the body is still evidence."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._process_delivery(
            envelope_delivery(broker, "t1", CANVAS, "{}"),
            binding,
        )

        dead = await next_delivery(broker, f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}")
        assert b"not_value" not in dead.body

    async def test_in_centralized_mode_the_failure_is_reported_not_stored(self) -> None:
        """Centralized mode never touches the engine — the ERROR must go to results_topic."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store, results_topic="results")
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._process_delivery(
            envelope_delivery(broker, "t1", CANVAS, "{}"),
            binding,
        )

        reported = await next_envelope(broker, "results")
        assert NodeOutcome.model_validate_json(reported.body).status == NodeStatus.ERROR


class TestConcurrencyBound:
    """run() must stop pulling once max_concurrency deliveries are in flight."""

    @staticmethod
    async def _seed(broker: MemoryBroker, store: MemoryCanvasStore, count: int) -> None:
        """Publish ``count`` well-formed deliveries, each against its own canvas."""
        for index in range(count):
            canvas = f"c{index}"
            node = f"t{index}"
            await store.create_canvas(
                canvas,
                {node: TaskNode(id=node, canvas_id=canvas, topic=TOPIC)},
            )
            envelope = Envelope(node_id=node, canvas_id=canvas, body='{"value": 1}')
            await broker.publish(TOPIC, envelope.to_bytes())

    async def test_run_never_exceeds_max_concurrency_in_flight(self) -> None:
        """An unbounded loop spawns one handler per backlogged message; this must not.

        MemoryBroker (like Kafka) yields as fast as the topic supplies, with no
        prefetch of its own to provide backpressure. Every handler parks on
        ``gate``, so with 8 messages queued an unbounded loop would show all 8
        in flight at once.
        """
        gate = asyncio.Event()
        at_bound = asyncio.Event()
        peak = 0

        class SlowWorker(DoublingWorker):
            max_concurrency = 2

            async def process(self, input_obj: DoublingIn) -> DoublingOut:
                nonlocal peak
                peak = max(peak, len(self._inflight))
                if peak >= SlowWorker.max_concurrency:
                    at_bound.set()
                await gate.wait()
                return DoublingOut(doubled=input_obj.value * 2)

        worker = SlowWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        bind_worker(worker, broker, store)
        await self._seed(broker, store, 8)

        run_task = asyncio.create_task(worker.run())
        try:
            async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
                await at_bound.wait()
            # Give an unbounded loop every chance to over-spawn before asserting.
            for _ in range(20):
                await asyncio.sleep(0)

            assert len(worker._inflight) == SlowWorker.max_concurrency
            assert peak == SlowWorker.max_concurrency
        finally:
            gate.set()
            worker.stop_consuming()
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)
            await worker.drain()

    async def test_a_finished_handler_frees_its_slot(self) -> None:
        """The bound is a live count, not a lifetime cap — every message still gets handled."""
        all_done = asyncio.Event()
        expected = 3

        class SingleSlotWorker(DoublingWorker):
            max_concurrency = 1

            async def on_success(self, input_obj: DoublingIn, result: DoublingOut) -> None:
                await super().on_success(input_obj, result)
                if len(self.on_success_calls) == expected:
                    all_done.set()

        worker = SingleSlotWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        bind_worker(worker, broker, store)
        await self._seed(broker, store, expected)

        run_task = asyncio.create_task(worker.run())
        try:
            async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
                await all_done.wait()

            assert len(worker.on_success_calls) == expected
        finally:
            run_task.cancel()
            await asyncio.gather(run_task, return_exceptions=True)


class TestTaskExecution:
    """process() success/failure and the hooks around it."""

    async def test_process_raising_calls_on_failure_records_error_and_acks(self) -> None:
        """A failing process() must become an ERROR outcome, call on_failure, and still ack."""
        worker = DoublingWorker()
        worker.should_fail = True
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        assert len(worker.on_failure_calls) == 1
        assert worker.on_failure_calls[0][0] == DoublingIn(value=5)
        assert isinstance(worker.on_failure_calls[0][1], RuntimeError)
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.ERROR

    async def test_output_failing_validation_is_treated_as_a_failure(self) -> None:
        """A process() result that Output can't validate must be a failure, not a success."""
        worker = BadOutputWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        assert len(worker.on_failure_calls) == 1
        assert len(worker.on_success_calls) == 0
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.ERROR

    async def test_before_start_raising_is_treated_as_a_failure(self) -> None:
        """before_start() raising must short-circuit process() and still call on_failure."""
        worker = ExplodingBeforeStartWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        assert len(worker.before_start_calls) == 1
        assert len(worker.on_failure_calls) == 1
        assert len(worker.on_success_calls) == 0


class TestHookFailuresAreContained:
    """A raising hook must not crash message handling — it's logged and swallowed."""

    async def test_on_success_raising_does_not_prevent_ack_or_rerun_process(self) -> None:
        """on_success() raising after the canvas advanced must not un-advance or re-run process."""
        worker = RaisingHooksWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        # _handle, not _process_delivery: the success hook runs there now, after the
        # delivery's settlement is recorded, so a slow hook can't un-settle an ack.
        await worker._handle(delivery, binding)

        assert len(worker.on_success_calls) == 1  # called exactly once, not retried
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.FINISHED
        assert stored.result == '{"doubled":10}'

    async def test_on_failure_raising_does_not_prevent_the_error_outcome(self) -> None:
        """on_failure() raising must not stop the ERROR outcome from being recorded."""
        worker = RaisingHooksWorker()
        worker.should_fail = True
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        assert len(worker.on_failure_calls) == 1
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.ERROR


class TestAckOrdering:
    """The crash-safety contract: ack only after the store write and any dispatch publish."""

    async def test_ack_happens_after_store_write_and_dispatch_publish(self) -> None:
        """Order must be exactly: store write, dispatch publish, ack."""
        order_spy = OrderSpy()
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["t1", "t2"])
        store = OrderTrackingStore(order_spy)
        broker = OrderTrackingBroker(order_spy)
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic=TOPIC),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="next"),
                "chain": chain,
            },
        )
        worker = DoublingWorker()
        binding = bind_worker(worker, broker, store)
        inner_delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')
        tracked_delivery = OrderTrackingDelivery(inner_delivery, order_spy)

        # _handle, not _process_delivery: the ack moved there so settlement can be
        # recorded before it is awaited (a cancellation mid-ack can't tell you
        # whether it landed, so nacking afterwards would double-settle).
        await worker._handle(tracked_delivery, binding)

        assert order_spy.events == ["store_write:t1", "publish:next", "ack"]


class TestCentralizedMode:
    """A binding with results_topic set must report, not advance the canvas itself."""

    async def test_reports_the_outcome_to_the_results_topic_instead_of_advancing(self) -> None:
        """The worker must never touch engine/dispatch when results_topic is set."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic=TOPIC)},
        )
        broker = MemoryBroker()
        worker = DoublingWorker()
        binding = bind_worker(worker, broker, store, results_topic="results")
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._handle(delivery, binding)

        # No local advancement: the engine never wrote a result for t1.
        assert await store.get_result(CANVAS, "t1") is None
        # The outcome landed on the results topic instead.
        envelope = await next_envelope(broker, "results")
        assert envelope.node_id == "t1"
        assert envelope.canvas_id == CANVAS
        outcome = NodeOutcome.model_validate_json(envelope.body)
        assert outcome.status == NodeStatus.FINISHED
        assert outcome.result == '{"doubled":10}'
        assert worker.on_success_calls  # on_success still fires locally

    async def test_a_failed_task_still_reports_an_error_outcome(self) -> None:
        """A process() failure must report status=ERROR, not just drop silently."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic=TOPIC)},
        )
        broker = MemoryBroker()
        worker = DoublingWorker()
        worker.should_fail = True
        binding = bind_worker(worker, broker, store, results_topic="results")
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        reported = await anext(broker.consume("results"))
        envelope = Envelope.from_bytes(reported.body)
        outcome = NodeOutcome.model_validate_json(envelope.body)
        assert outcome.status == NodeStatus.ERROR

    async def test_a_failure_reporting_to_the_results_topic_nacks_for_redelivery(self) -> None:
        """If publishing the report itself fails, the delivery must be nacked, not lost."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic=TOPIC)},
        )
        failing_broker = RaisingOnPublishBroker()
        worker = DoublingWorker()
        binding = bind_worker(worker, failing_broker, store, results_topic="results")
        delivery = envelope_delivery(failing_broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        redelivered = await anext(failing_broker.consume(TOPIC))
        assert redelivered.attempt == 2


class TestRedeliverySafety:
    """A dispatch-publish failure must nack for redelivery, and the retry must be safe."""

    async def test_publish_failure_nacks_for_redelivery_without_losing_the_store_write(
        self,
    ) -> None:
        """The store write already happened; only the publish failed — the retry must stay safe."""
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["t1", "t2"])
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic=TOPIC),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="next"),
                "chain": chain,
            },
        )
        worker = DoublingWorker()
        failing_broker = RaisingOnPublishBroker()
        failing_binding = bind_worker(worker, failing_broker, store)
        delivery = envelope_delivery(failing_broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, failing_binding)

        # The store write already succeeded even though the publish failed.
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.FINISHED

        # Retry against a healthy broker: must succeed and not double-process.
        healthy_broker = MemoryBroker()
        healthy_binding = bind_worker(worker, healthy_broker, store)
        retry_delivery = envelope_delivery(healthy_broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(retry_delivery, healthy_binding)

        assert (await next_envelope(healthy_broker, "next")).node_id == "t2"
        stored_again = await store.get_result(CANVAS, "t1")
        assert stored_again is not None
        assert stored_again.status == NodeStatus.FINISHED

    async def test_a_failed_callback_publish_leaves_the_chord_callback_dispatchable(
        self,
    ) -> None:
        """The lost-callback bug, end to end through the worker's own publish path.

        The worker burns the group's fan-in guard inside engine.complete(), then
        fails to publish the callback. Without the rollback in _advance_canvas the
        redelivery dispatches nothing and the worker acks — the chord's callback is
        gone and the canvas hangs with no terminal status.
        """
        group = GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1"],
            callback="cb",
        )
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "leg1": TaskNode(id="leg1", canvas_id=CANVAS, parent_id="g", topic=TOPIC),
                "cb": TaskNode(id="cb", canvas_id=CANVAS, parent_id="g", topic="callback"),
                "g": group,
            },
        )
        worker = DoublingWorker()
        failing_broker = RaisingOnPublishBroker()
        failing_binding = bind_worker(worker, failing_broker, store)
        delivery = envelope_delivery(failing_broker, "leg1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, failing_binding)

        healthy_broker = MemoryBroker()
        healthy_binding = bind_worker(worker, healthy_broker, store)
        retry = envelope_delivery(healthy_broker, "leg1", CANVAS, '{"value": 5}')

        await worker._process_delivery(retry, healthy_binding)

        assert (await next_envelope(healthy_broker, "callback")).node_id == "cb"

    async def test_a_failed_chain_publish_rolls_back_nothing_and_still_retries(
        self,
    ) -> None:
        """A chain dispatch carries no group, so rollback is a no-op and the retry works."""
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["t1", "t2"])
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic=TOPIC),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="next"),
                "chain": chain,
            },
        )
        worker = DoublingWorker()
        failing_broker = RaisingOnPublishBroker()
        await worker._process_delivery(
            envelope_delivery(failing_broker, "t1", CANVAS, '{"value": 5}'),
            bind_worker(worker, failing_broker, store),
        )

        healthy_broker = MemoryBroker()
        await worker._process_delivery(
            envelope_delivery(healthy_broker, "t1", CANVAS, '{"value": 5}'),
            bind_worker(worker, healthy_broker, store),
        )

        assert (await next_envelope(healthy_broker, "next")).node_id == "t2"


class RaisingOnCompleteStore(MemoryCanvasStore):
    """A store whose set_result always fails — stands in for a persistently broken store."""

    def __init__(self, delegate: MemoryCanvasStore) -> None:
        """Share ``delegate``'s graph so nodes resolve, but never record an outcome."""
        super().__init__()
        self._nodes = delegate._nodes
        self._canvas_status = delegate._canvas_status

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Fail the way an unreachable store would — as a WorkerError the engine expects."""
        del outcome
        raise NodeNotFoundError(node_id=node_id, canvas_id=canvas_id)


class UnexpectedlyRaisingStore(MemoryCanvasStore):
    """A store that fails with something outside the WorkerError hierarchy.

    `_advance_canvas` only catches `WorkerError`, so anything else escapes
    `_process_delivery` entirely — the case `_handle` has to cope with.
    """

    def __init__(self, delegate: MemoryCanvasStore) -> None:
        """Share ``delegate``'s graph so nodes resolve, but never record an outcome."""
        super().__init__()
        self._nodes = delegate._nodes
        self._canvas_status = delegate._canvas_status

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Fail the way a driver-level error would — not a WorkerError at all."""
        del canvas_id, node_id, outcome
        detail = "connection reset by peer"
        raise ConnectionResetError(detail)


class TestDeliveryIsAlwaysSettled:
    """Every exit path must leave the delivery acked or nacked, exactly once."""

    async def test_an_unexpected_exception_nacks_instead_of_stranding_the_delivery(
        self,
    ) -> None:
        """`_handle` is fire-and-forget, so an escaping exception is never even logged.

        The delivery would be left unacked and unnacked — invisible to the broker,
        to the canvas, and to the operator.
        """
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, UnexpectedlyRaisingStore(store))
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._handle(delivery, binding)

        redelivered = await next_delivery(broker, TOPIC)
        assert redelivered.attempt == 2

    async def test_cancellation_after_the_ack_does_not_double_settle(self) -> None:
        """Drain-timeout cancellation lands in on_success, after the ack has happened.

        Nacking there settles the delivery twice: on RabbitMQ that republishes the
        message *and* acks a second time, so a node that already finished runs again
        after a clean shutdown.
        """
        acked_then_cancelled = asyncio.Event()

        class CancelInHookWorker(DoublingWorker):
            async def on_success(self, input_obj: DoublingIn, result: DoublingOut) -> None:
                del input_obj, result
                acked_then_cancelled.set()
                await asyncio.sleep(3600)

        worker = CancelInHookWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        task = asyncio.create_task(worker._handle(delivery, binding))
        async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
            await acked_then_cancelled.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        # Nothing was requeued: the delivery was already settled by the ack.
        assert TOPIC not in broker._queues or broker._queues[TOPIC].empty()


class TestRetryCap:
    """A persistently failing store or broker must not retry forever."""

    async def _failing_delivery(self, attempt: int) -> tuple[DoublingWorker, MemoryBroker]:
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, RaisingOnCompleteStore(store))
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        envelope = Envelope(node_id="t1", canvas_id=CANVAS, body='{"value": 5}')
        delivery = MemoryDelivery(broker, TOPIC, envelope.to_bytes(), attempt=attempt)
        await worker._handle(delivery, binding)
        return worker, broker

    async def test_a_failure_under_the_cap_is_requeued(self) -> None:
        """Ordinary transient failures must still retry, with the attempt bumped."""
        _, broker = await self._failing_delivery(attempt=1)

        assert (await next_delivery(broker, TOPIC)).attempt == 2

    async def test_a_failure_at_the_cap_is_dead_lettered_instead(self) -> None:
        """Nothing read Delivery.attempt before, so a broken store retried forever.

        On MemoryBroker that is a tight CPU-burning loop, since requeue redelivers
        synchronously onto the queue the same consumer is polling.
        """
        _, broker = await self._failing_delivery(attempt=DoublingWorker.max_attempts)

        dead = await next_delivery(broker, f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}")
        assert dead.attempt == DoublingWorker.max_attempts

    async def test_a_node_whose_failure_cannot_be_recorded_is_retried_not_dropped(
        self,
    ) -> None:
        """Dead-lettering an unrecordable failure leaves the canvas RUNNING forever."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, RaisingOnCompleteStore(store))
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._handle(envelope_delivery(broker, "t1", CANVAS, "{}"), binding)

        assert (await next_delivery(broker, TOPIC)).attempt == 2


class TestBinding:
    """A worker must not be usable before it's bound."""

    async def test_run_before_bind_raises(self) -> None:
        """run() on an unbound worker must raise a clear error, not AttributeError."""
        worker = DoublingWorker()

        with pytest.raises(WorkerNotBoundError):
            await worker.run()


class TestBaseProcessNotImplemented:
    """A subclass that never overrides process() gets a clear error, not silent no-op."""

    async def test_base_process_raises_not_implemented(self) -> None:
        """The base Worker.process() must raise, proving subclasses must override it."""

        class Incomplete(Worker[DoublingIn, DoublingOut]):
            topic = TOPIC
            Input = DoublingIn
            Output = DoublingOut

        worker = Incomplete()

        with pytest.raises(NotImplementedError):
            await worker.process(DoublingIn(value=1))


class TestRunStoppedBranch:
    """run() must nack-and-stop, never dispatch, once stop_consuming() was already called."""

    async def test_run_nacks_and_returns_without_dispatching_when_already_stopped(self) -> None:
        """A delivery arriving after stop_consuming() must be requeued, not handled."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        bind_worker(worker, broker, store)
        worker.stop_consuming()
        await broker.publish(TOPIC, b"irrelevant, never decoded")

        await worker.run()

        assert worker.before_start_calls == []  # never dispatched to _handle/process
        redelivered = await anext(broker.consume(TOPIC))
        assert redelivered.attempt == 2


class TestEngineErrorDuringComplete:
    """A canvas-engine error while advancing the graph must nack for redelivery, not crash."""

    async def test_engine_error_nacks_for_redelivery(self) -> None:
        """engine.complete() raising (e.g. unknown node) must nack(requeue=True), not propagate."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()  # empty: "t1" was never created, so complete() will raise
        binding = bind_worker(worker, broker, store)
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"value": 5}')

        await worker._process_delivery(delivery, binding)

        redelivered = await anext(broker.consume(TOPIC))
        assert redelivered.attempt == 2


class TestNodeIsObservablyRunning:
    """A worker must record that it picked a node up, not just that it finished."""

    async def test_processing_marks_the_node_running(self) -> None:
        """Bug #13 promised every transition writes its status; RUNNING never did.

        A canvas stuck mid-flight was indistinguishable from one never dispatched.
        """
        seen: list[NodeStatus] = []

        class ObservingWorker(DoublingWorker):
            async def process(self, input_obj: DoublingIn) -> DoublingOut:
                node = await self._require_binding().store.get_node(CANVAS, "t1")
                assert node is not None
                seen.append(node.status)
                return await super().process(input_obj)

        worker = ObservingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._handle(
            envelope_delivery(broker, "t1", CANVAS, '{"value": 5}'),
            binding,
        )

        assert seen == [NodeStatus.RUNNING]

    async def test_a_store_failure_marking_running_does_not_fail_the_message(self) -> None:
        """Observability must never cost a message that is about to run fine."""

        class RefusingStore(MemoryCanvasStore):
            async def mark_node_running(self, canvas_id: str, node_id: str) -> None:
                del canvas_id, node_id
                detail = "status write refused"
                raise ConnectionResetError(detail)

        worker = DoublingWorker()
        broker = MemoryBroker()
        store = RefusingStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._handle(
            envelope_delivery(broker, "t1", CANVAS, '{"value": 5}'),
            binding,
        )

        recorded = await store.get_result(CANVAS, "t1")
        assert recorded is not None
        assert recorded.status == NodeStatus.FINISHED


class TestConcurrencySlotIsAlwaysReleased:
    """A slot must come back even if the handler task never starts."""

    async def test_a_task_cancelled_before_it_starts_still_frees_its_slot(self) -> None:
        """`_handle`'s finally never runs for a task cancelled before its first step.

        The drain timeout does exactly that, so releasing there cost the worker a
        slot permanently. A done-callback fires whatever became of the task.
        """

        class SingleSlotWorker(DoublingWorker):
            max_concurrency = 1

        worker = SingleSlotWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})

        await worker._slots.acquire()
        task = asyncio.create_task(
            worker._handle(
                envelope_delivery(broker, "t1", CANVAS, '{"value": 5}'),
                binding,
            ),
        )
        worker._inflight.add(task)
        task.add_done_callback(worker._settle_slot)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        assert worker._slots.locked() is False
        assert worker._inflight == set()


class TestSettlementIsRecordedBeforeTheAck:
    """A cancellation mid-ack can't tell you whether the ack landed."""

    async def test_cancellation_during_the_ack_does_not_nack(self) -> None:
        """`settled` was only assigned after `_process_delivery` returned.

        The ack happened inside it, so a cancellation in flight there propagated
        with `settled` still False and nacked a delivery that may already have been
        acked — the double-settle the design says it prevents. The flag protected
        the on_success window but not the ack itself.
        """
        acking = asyncio.Event()
        nacked: list[bool] = []

        class SlowAckDelivery(MemoryDelivery):
            async def ack(self) -> None:
                acking.set()
                await asyncio.sleep(3600)

            async def nack(self, *, requeue: bool) -> None:
                nacked.append(requeue)
                await super().nack(requeue=requeue)

        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        envelope = Envelope(node_id="t1", canvas_id=CANVAS, body='{"value": 5}')
        delivery = SlowAckDelivery(broker, TOPIC, envelope.to_bytes(), attempt=1)

        task = asyncio.create_task(worker._handle(delivery, binding))
        async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
            await acking.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert nacked == []


class TestRollbackSurvivesCancellation:
    """`except Exception` does not catch CancelledError, and the drain raises exactly that."""

    async def test_a_cancelled_dispatch_publish_still_rolls_back(self) -> None:
        """A chord's callback would otherwise be lost on an ordinary graceful shutdown.

        The guard is burned inside `complete()`, the publish is cancelled by the
        drain timeout, and without a rollback the redelivery finds `fired=False`,
        dispatches nothing, and acks.
        """
        publishing = asyncio.Event()

        class HangingPublishBroker(MemoryBroker):
            async def publish(
                self,
                topic: str,
                message: bytes,
                *,
                headers: Mapping[str, str] | None = None,
            ) -> None:
                if topic == "callback":
                    publishing.set()
                    await asyncio.sleep(3600)
                await super().publish(topic, message, headers=headers)

        worker = DoublingWorker()
        broker = HangingPublishBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(
            CANVAS,
            {
                "leg1": TaskNode(id="leg1", canvas_id=CANVAS, parent_id="g", topic=TOPIC),
                "cb": TaskNode(id="cb", canvas_id=CANVAS, parent_id="g", topic="callback"),
                "g": GroupNode(
                    id="g",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["leg1"],
                    callback="cb",
                ),
            },
        )

        task = asyncio.create_task(
            worker._handle(
                envelope_delivery(broker, "leg1", CANVAS, '{"value": 5}'),
                binding,
            ),
        )
        async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
            await publishing.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        # The guard must be released, so a healthy retry can dispatch the callback.
        healthy = MemoryBroker()
        retry_binding = bind_worker(worker, healthy, store)
        await worker._handle(
            envelope_delivery(healthy, "leg1", CANVAS, '{"value": 5}'),
            retry_binding,
        )

        assert (await next_envelope(healthy, "callback")).node_id == "cb"
