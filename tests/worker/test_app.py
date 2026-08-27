"""WorkerApp: registration validation, run/stop lifecycle, and graceful shutdown."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from pydantic import BaseModel

from mint.worker.app import WorkerApp
from mint.worker.brokers.memory import MemoryBroker
from mint.worker.canvas.models import TaskNode
from mint.worker.envelope import Envelope
from mint.worker.exc import AppAlreadyRunningError, DuplicateTopicError, MissingWorkerConfigError
from mint.worker.executors.inline import InlineExecutor
from mint.worker.stores.memory import MemoryCanvasStore
from mint.worker.worker import Worker

CANVAS = "c1"


class SignalingIn(BaseModel):
    """Test input: one integer."""

    value: int


class SignalingOut(BaseModel):
    """Test output: the doubled integer."""

    doubled: int


class SignalingWorker(Worker[SignalingIn, SignalingOut]):
    """Doubles its input; sets ``processed`` once on_success fires, for deterministic waits.

    ``unblock`` lets a test hold ``process`` open indefinitely to exercise drain/cancel
    behavior without any wall-clock guessing.
    """

    topic = "signaling"
    Input = SignalingIn
    Output = SignalingOut

    def __init__(self) -> None:
        """Start unblocked (process() returns immediately) and unprocessed."""
        super().__init__()
        self.unblock = asyncio.Event()
        self.unblock.set()
        self.processed = asyncio.Event()
        self.was_cancelled = False

    async def process(self, input_obj: SignalingIn) -> SignalingOut:
        """Wait for ``unblock``, then double the input — cancellable while waiting."""
        try:
            await self.unblock.wait()
        except asyncio.CancelledError:
            self.was_cancelled = True
            raise
        return SignalingOut(doubled=input_obj.value * 2)

    async def on_success(self, input_obj: SignalingIn, result: SignalingOut) -> None:
        """Signal that this message was fully processed."""
        self.last_call = (input_obj, result)
        self.processed.set()


class OtherWorker(SignalingWorker):
    """A second worker class with its own topic, for duplicate-topic tests."""

    topic = "other"


class SpyCloseExecutor:
    """A closable ITaskExecutor test double that counts its own aclose() calls."""

    def __init__(self) -> None:
        """Start with no calls recorded."""
        self.close_count = 0

    async def execute(
        self,
        fn: Callable[[SignalingIn], Awaitable[SignalingOut]],
        input_: SignalingIn,
    ) -> SignalingOut:
        """Delegate straight through, like InlineExecutor."""
        return await fn(input_)

    async def aclose(self) -> None:
        """Record that this executor was asked to close."""
        self.close_count += 1


class WorkerWithOwnExecutor(SignalingWorker):
    """A worker declaring its own executor, to prove it overrides the app's default."""

    topic = "own-executor"

    def __init__(self, executor: SpyCloseExecutor) -> None:
        """Bind ``executor`` as this worker's own, distinct from the app's default."""
        super().__init__()
        self.executor = executor


class SpyCloseBroker(MemoryBroker):
    """A MemoryBroker that records whether close() was called."""

    def __init__(self) -> None:
        """Start not closed."""
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        """Record the close, then close as usual."""
        self.closed = True
        await super().close()


class SpyCloseStore(MemoryCanvasStore):
    """A MemoryCanvasStore that records whether close() was called."""

    def __init__(self) -> None:
        """Start not closed."""
        super().__init__()
        self.closed = False

    async def close(self) -> None:
        """Record the close, then close as usual."""
        self.closed = True
        await super().close()


async def publish_one(
    broker: MemoryBroker,
    worker: Worker[Any, Any],
    node_id: str,
    value: int,
) -> None:
    """Publish one well-formed message for ``worker`` to consume."""
    envelope = Envelope(node_id=node_id, canvas_id=CANVAS, body=f'{{"value": {value}}}')
    await broker.publish(worker.topic, envelope.to_bytes())


class TestRegister:
    """register() validates workers before they can ever run."""

    def test_two_workers_on_the_same_topic_is_rejected(self) -> None:
        """Two distinct worker instances claiming the same topic must not silently overwrite."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app = WorkerApp(broker, store)
        app.register(SignalingWorker())

        with pytest.raises(DuplicateTopicError):
            app.register(SignalingWorker())

    def test_worker_missing_a_required_class_attribute_is_rejected(self) -> None:
        """A worker missing Input/Output/topic must fail at register(), not on first message."""

        class Incomplete(Worker[Any, Any]):
            pass

        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app = WorkerApp(broker, store)

        with pytest.raises(MissingWorkerConfigError):
            app.register(Incomplete())

    def test_distinct_topics_both_register_successfully(self) -> None:
        """Two different topics must coexist without conflict."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app = WorkerApp(broker, store)

        app.register(SignalingWorker())
        app.register(OtherWorker())

        assert set(app._workers) == {"signaling", "other"}

    def test_a_workers_own_executor_overrides_the_apps_default(self) -> None:
        """A worker declaring its own executor must be bound to it, not the app's default."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app_default: InlineExecutor[Any, Any] = InlineExecutor()
        own_executor = SpyCloseExecutor()
        app = WorkerApp(broker, store, executor=app_default)
        worker = WorkerWithOwnExecutor(own_executor)

        app.register(worker)

        assert worker._binding is not None
        assert worker._binding.executor is own_executor
        assert worker._binding.executor is not app_default

    def test_a_worker_without_its_own_executor_uses_the_apps_default(self) -> None:
        """A worker that never sets its own executor must fall back to the app's."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app_default: InlineExecutor[Any, Any] = InlineExecutor()
        app = WorkerApp(broker, store, executor=app_default)
        worker = SignalingWorker()

        app.register(worker)

        assert worker._binding is not None
        assert worker._binding.executor is app_default


class TestRunStopLifecycle:
    """run()/stop(): consumes registered workers until told to stop, then shuts down cleanly."""

    async def test_stop_drains_in_flight_work_acks_it_and_closes_broker_and_store(self) -> None:
        """A graceful stop must let in-flight work finish, then close broker and store."""
        broker = SpyCloseBroker()
        store = SpyCloseStore()
        worker = SignalingWorker()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=worker.topic)},
        )
        app = WorkerApp(broker, store)
        app.register(worker)
        await publish_one(broker, worker, "t1", 5)

        run_task = asyncio.create_task(app.run())
        await asyncio.wait_for(worker.processed.wait(), timeout=2)
        await app.stop()
        await run_task

        assert broker.closed
        assert store.closed
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.result == '{"doubled":10}'

    async def test_second_run_while_already_running_raises(self) -> None:
        """run() must not be re-entrant."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app = WorkerApp(broker, store)
        app.register(SignalingWorker())

        run_task = asyncio.create_task(app.run())
        await asyncio.sleep(0)  # let run() actually start and flip _running

        with pytest.raises(AppAlreadyRunningError):
            await app.run()

        await app.stop()
        await run_task

    async def test_in_flight_work_exceeding_the_drain_timeout_is_nacked_not_dropped(self) -> None:
        """Work still running when the drain timeout expires must be nacked for redelivery."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        worker = SignalingWorker()
        worker.unblock.clear()  # process() will hang until cancelled by the drain timeout
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic=worker.topic)},
        )
        app = WorkerApp(broker, store, drain_timeout=0.05)
        app.register(worker)
        await publish_one(broker, worker, "t1", 5)

        run_task = asyncio.create_task(app.run())
        await asyncio.sleep(0)  # let the message actually reach process() and start blocking
        await app.stop()
        await run_task

        assert worker.was_cancelled
        # Nacked with requeue=True: the message must be back on the same topic, not gone.
        redelivered = await asyncio.wait_for(anext(broker.consume(worker.topic)), timeout=1)
        assert redelivered.attempt == 2

    async def test_stop_closes_every_distinct_closable_executor_exactly_once(self) -> None:
        """Each distinct closable executor in use must be closed once — never zero, never twice."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app_default = SpyCloseExecutor()
        shared_own_executor = SpyCloseExecutor()
        app = WorkerApp(broker, store, executor=app_default)
        default_user = SignalingWorker()
        default_user.topic = "default-user"
        first_owner = WorkerWithOwnExecutor(shared_own_executor)
        first_owner.topic = "first-owner"
        second_owner = WorkerWithOwnExecutor(shared_own_executor)  # same instance, two workers
        second_owner.topic = "second-owner"
        app.register(default_user)
        app.register(first_owner)
        app.register(second_owner)

        run_task = asyncio.create_task(app.run())
        await asyncio.sleep(0)
        await app.stop()
        await run_task

        assert app_default.close_count == 1
        assert shared_own_executor.close_count == 1

    async def test_stop_with_a_non_closable_executor_does_not_raise(self) -> None:
        """The default InlineExecutor has no aclose() — shutdown must not require one."""
        broker = MemoryBroker()
        store = MemoryCanvasStore()
        app = WorkerApp(broker, store)  # defaults to InlineExecutor
        app.register(SignalingWorker())

        run_task = asyncio.create_task(app.run())
        await asyncio.sleep(0)
        await app.stop()
        await run_task  # must not raise
