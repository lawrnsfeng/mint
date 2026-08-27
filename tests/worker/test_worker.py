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

import pytest
from pydantic import BaseModel

from mint.worker.brokers.memory import MemoryBroker, MemoryDelivery
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ChainNode, GroupNode, NodeOutcome, TaskNode
from mint.worker.enums import NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import WorkerNotBoundError
from mint.worker.executors.inline import InlineExecutor
from mint.worker.stores.memory import MemoryCanvasStore
from mint.worker.worker import Worker, WorkerBinding
from tests.worker.conftest import OrderSpy

CANVAS = "c1"
TOPIC = "double"
NEXT_MESSAGE_TIMEOUT = 1.0


async def next_envelope(broker: MemoryBroker, topic: str) -> Envelope:
    """Return the next envelope published to ``topic``, failing fast if none arrives.

    Deliberately timeout-guarded rather than a bare ``anext``: the bugs these
    tests cover manifest as a message that never arrives at all, and an unguarded
    ``anext`` on ``MemoryBroker`` would hang the whole suite instead of failing
    the one test (the same class of mistake as the ``envelope_delivery`` hang
    documented as bug #22).
    """
    async with asyncio.timeout(NEXT_MESSAGE_TIMEOUT):
        delivery = await anext(broker.consume(topic))
    return Envelope.from_bytes(delivery.body)


class Input(BaseModel):
    """Test input: one integer."""

    value: int


class Output(BaseModel):
    """Test output: the doubled integer."""

    doubled: int


class DoublingWorker(Worker[Input, Output]):
    """Doubles its input; records every hook call for assertions."""

    topic = TOPIC
    Input = Input
    Output = Output

    def __init__(self) -> None:
        """Start with no recorded hook calls and no forced failure."""
        super().__init__()
        self.before_start_calls: list[Input] = []
        self.on_success_calls: list[tuple[Input, Output]] = []
        self.on_failure_calls: list[tuple[Input, Exception]] = []
        self.should_fail = False

    async def process(self, input_obj: Input) -> Output:
        """Double the input, or raise if ``should_fail`` is set."""
        if self.should_fail:
            raise RuntimeError("boom")
        return Output(doubled=input_obj.value * 2)

    async def before_start(self, input_obj: Input) -> None:
        """Record the call."""
        self.before_start_calls.append(input_obj)

    async def on_success(self, input_obj: Input, result: Output) -> None:
        """Record the call."""
        self.on_success_calls.append((input_obj, result))

    async def on_failure(self, input_obj: Input, exc: Exception) -> None:
        """Record the call."""
        self.on_failure_calls.append((input_obj, exc))


class BadOutputWorker(DoublingWorker):
    """A worker whose process() returns something Output can never validate."""

    async def process(self, input_obj: Input) -> Output:
        """Return a shape Output rejects, to prove bad output is treated as failure."""
        self.received_input = input_obj
        return {"totally": "wrong"}  # ty: ignore[invalid-return-type]


class ExplodingBeforeStartWorker(DoublingWorker):
    """A DoublingWorker whose before_start() always raises."""

    async def before_start(self, input_obj: Input) -> None:
        """Record then raise."""
        await super().before_start(input_obj)
        raise RuntimeError("before_start failed")


class RaisingHooksWorker(DoublingWorker):
    """A DoublingWorker whose success/failure hooks always raise, after recording the call."""

    async def on_success(self, input_obj: Input, result: Output) -> None:
        """Record then raise."""
        await super().on_success(input_obj, result)
        raise RuntimeError("on_success exploded")

    async def on_failure(self, input_obj: Input, exc: Exception) -> None:
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
    worker: Worker,
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

        dead = await anext(broker.consume(f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}"))
        assert dead.body == b"not json at all"

    async def test_body_that_fails_input_validation_is_dead_lettered_not_acked(self) -> None:
        """A well-formed Envelope whose body doesn't satisfy Input must also be nacked."""
        worker = DoublingWorker()
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        binding = bind_worker(worker, broker, store)
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=TOPIC)})
        delivery = envelope_delivery(broker, "t1", CANVAS, '{"not_value": "wrong shape"}')

        await worker._process_delivery(delivery, binding)

        dead = await anext(broker.consume(f"{TOPIC}{MemoryBroker.DLQ_SUFFIX}"))
        assert dead.attempt == 1


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
        assert worker.on_failure_calls[0][0] == Input(value=5)
        assert isinstance(worker.on_failure_calls[0][1], RuntimeError)
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.ERROR

    async def test_output_failing_validation_is_treated_as_a_failure(self) -> None:
        """process() returning something Output can't validate must be a failure, not a success."""
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

        await worker._process_delivery(delivery, binding)

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

        await worker._process_delivery(tracked_delivery, binding)

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

        await worker._process_delivery(delivery, binding)

        # No local advancement: the engine never wrote a result for t1.
        assert await store.get_result(CANVAS, "t1") is None
        # The outcome landed on the results topic instead.
        reported = await anext(broker.consume("results"))
        envelope = Envelope.from_bytes(reported.body)
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

        class Incomplete(Worker[Input, Output]):
            topic = TOPIC
            Input = Input
            Output = Output

        worker = Incomplete()

        with pytest.raises(NotImplementedError):
            await worker.process(Input(value=1))


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
