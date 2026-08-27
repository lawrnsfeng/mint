"""Coordinator: centralized mode — advancing the canvas from a shared results topic.

Every assertion here is against the in-memory bookkeeping (``_in_flight``/
``_by_canvas``) and the engine/dispatch calls Coordinator makes — the same engine
already has its own exhaustive suite in ``canvas/test_engine.py``; this file is
about the coordinator's own responsibilities: decoding, tracking, cancelling, and
sweeping for timeouts.
"""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from mint.worker.brokers.interface import Delivery
from mint.worker.brokers.memory import MemoryBroker, MemoryDelivery
from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.models import ChainNode, ErrorInfo, NodeOutcome, TaskNode
from mint.worker.coordinator import Coordinator, CoordinatorConfig, InFlightNode
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import CoordinatorAlreadyRunningError
from mint.worker.stores.memory import MemoryCanvasStore

CANVAS = "c1"
RESULTS_TOPIC = "results"


def ok_outcome(node_id: str, result: str = "{}") -> NodeOutcome:
    """Build a FINISHED outcome for ``node_id``."""
    return NodeOutcome(node_id=node_id, status=NodeStatus.FINISHED, result=result)


async def publish_result(broker: MemoryBroker, node_id: str, outcome: NodeOutcome) -> None:
    """Publish an outcome to the results topic, exactly like a centralized-mode worker would."""
    envelope = Envelope(node_id=node_id, canvas_id=CANVAS, body=outcome.model_dump_json())
    await broker.publish(RESULTS_TOPIC, envelope.to_bytes())


class RaisingOnTopicBroker(MemoryBroker):
    """A MemoryBroker whose publish() raises for one specific topic only."""

    def __init__(self, *, raises_for: str) -> None:
        """Start raising only when ``raises_for`` is published to."""
        super().__init__()
        self._raises_for = raises_for

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish normally, unless ``topic`` is the one configured to fail."""
        if topic == self._raises_for:
            detail = f"broker unreachable: {topic}"
            raise ConnectionError(detail)
        await super().publish(topic, message, headers=headers)


class TestHandleResult:
    """_handle_result: decode, advance the engine, dispatch, ack/nack."""

    async def test_a_valid_result_advances_the_chain_and_dispatches_the_next_step(self) -> None:
        """A middle node's result must dispatch to the chain's next step."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic="topic-t1"),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="topic-t2"),
                "chain": ChainNode(
                    id="chain",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["t1", "t2"],
                ),
            },
        )
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        await publish_result(broker, "t1", ok_outcome("t1", '{"v":1}'))
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        dispatched = await anext(broker.consume("topic-t2"))
        envelope = Envelope.from_bytes(dispatched.body)
        assert envelope.node_id == "t2"
        assert envelope.body == '{"v":1}'

    async def test_a_malformed_envelope_is_dead_lettered_not_acked(self) -> None:
        """Garbage bytes on the results topic must not be silently dropped."""
        store = MemoryCanvasStore()
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        await broker.publish(RESULTS_TOPIC, b"not json at all")
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        dead = await anext(broker.consume(f"{RESULTS_TOPIC}.dlq"))
        assert dead.body == b"not json at all"

    async def test_a_malformed_outcome_body_is_dead_lettered_not_acked(self) -> None:
        """A well-formed envelope whose body isn't a valid NodeOutcome must not be dropped."""
        store = MemoryCanvasStore()
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        envelope = Envelope(node_id="t1", canvas_id=CANVAS, body="not a node outcome")
        await broker.publish(RESULTS_TOPIC, envelope.to_bytes())
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        dead = await anext(broker.consume(f"{RESULTS_TOPIC}.dlq"))
        assert dead.body == envelope.to_bytes()

    async def test_an_engine_error_nacks_for_redelivery(self) -> None:
        """An unknown node must nack for redelivery, not silently ack-and-lose it."""
        store = MemoryCanvasStore()  # canvas never created: t1 is unknown
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        await publish_result(broker, "t1", ok_outcome("t1"))
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        redelivered = await anext(broker.consume(RESULTS_TOPIC))
        assert redelivered.attempt == 2

    async def test_a_dispatch_publish_failure_nacks_for_redelivery(self) -> None:
        """The store write already advanced; only the dispatch publish failed."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic="topic-t1"),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="topic-t2"),
                "chain": ChainNode(
                    id="chain",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["t1", "t2"],
                ),
            },
        )
        broker = RaisingOnTopicBroker(raises_for="topic-t2")
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        await publish_result(broker, "t1", ok_outcome("t1"))
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        redelivered = await anext(broker.consume(RESULTS_TOPIC))
        assert redelivered.attempt == 2


class TestTracking:
    """track_and_publish/dispatch must register every publish for the sweeper."""

    async def test_track_and_publish_records_the_dispatch_and_publishes_it(self) -> None:
        """A tracked publish must both reach the broker and be recorded in memory."""
        store = MemoryCanvasStore()
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        envelope = Envelope(node_id="t1", canvas_id=CANVAS, body="{}")

        await coordinator.track_and_publish("topic-t1", envelope.to_bytes())

        assert (CANVAS, "t1") in coordinator._in_flight
        assert coordinator._by_canvas[CANVAS] == {"t1"}
        delivered = await anext(broker.consume("topic-t1"))
        assert delivered.body == envelope.to_bytes()

    async def test_dispatch_tracks_every_dispatch_in_the_batch(self) -> None:
        """Multiple dispatches from one engine.complete() call must all be tracked."""
        store = MemoryCanvasStore()
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        dispatches = [
            Dispatch(topic="a", node_id="na", canvas_id=CANVAS, body="{}"),
            Dispatch(topic="b", node_id="nb", canvas_id=CANVAS, body="{}"),
        ]

        await coordinator.dispatch(dispatches)

        assert coordinator._by_canvas[CANVAS] == {"na", "nb"}

    async def test_handling_a_result_untracks_that_node(self) -> None:
        """A node that just reported must no longer be considered in flight."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        coordinator._track(CANVAS, "t1")
        await publish_result(broker, "t1", ok_outcome("t1"))
        delivery = await anext(broker.consume(RESULTS_TOPIC))

        await coordinator._handle_result(delivery)

        assert (CANVAS, "t1") not in coordinator._in_flight
        assert CANVAS not in coordinator._by_canvas


class TestCancel:
    """cancel() must mark every in-flight node CANCELLED and error the canvas."""

    async def test_cancel_marks_in_flight_nodes_cancelled_and_errors_the_canvas(self) -> None:
        """Every node this coordinator dispatched for the canvas must be cancelled."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1"),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id=None, topic="topic-t2"),
            },
        )
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        coordinator._track(CANVAS, "t1")
        coordinator._track(CANVAS, "t2")

        await coordinator.cancel(CANVAS)

        t1 = await store.get_node(CANVAS, "t1")
        t2 = await store.get_node(CANVAS, "t2")
        assert t1 is not None
        assert t1.status == NodeStatus.CANCELLED
        assert t2 is not None
        assert t2.status == NodeStatus.CANCELLED
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR
        assert CANVAS not in coordinator._by_canvas

    async def test_cancel_with_nothing_tracked_still_errors_the_canvas(self) -> None:
        """A canvas cancelled after everything already finished must still be marked ERROR."""
        store = MemoryCanvasStore()
        await store.create_canvas(CANVAS, {})
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)

        await coordinator.cancel(CANVAS)  # nothing tracked: must not raise

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestSweep:
    """The timeout sweeper must error out any node that outlived max_age."""

    async def test_a_stale_in_flight_node_is_timed_out(self) -> None:
        """A node dispatched long ago with no reply must be reported as a timeout error."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = Coordinator(
            broker,
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(max_age=60.0),
        )
        stale_time = datetime.now(UTC) - timedelta(seconds=120)
        coordinator._in_flight[CANVAS, "t1"] = InFlightNode(CANVAS, "t1", stale_time)
        coordinator._by_canvas[CANVAS] = {"t1"}

        await coordinator._sweep_once()

        assert (CANVAS, "t1") not in coordinator._in_flight
        stored = await store.get_result(CANVAS, "t1")
        assert stored is not None
        assert stored.status == NodeStatus.ERROR
        assert stored.error == ErrorInfo(type="TimeoutError", message="No result within 60.0s")

    async def test_a_fresh_in_flight_node_is_left_alone(self) -> None:
        """A node dispatched moments ago must not be swept, even past a short max_age check."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = Coordinator(
            broker,
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(max_age=3600.0),
        )
        coordinator._track(CANVAS, "t1")

        await coordinator._sweep_once()

        assert (CANVAS, "t1") in coordinator._in_flight
        assert await store.get_result(CANVAS, "t1") is None


class BlockingStore(MemoryCanvasStore):
    """A MemoryCanvasStore whose get_canvas_status() blocks until unblocked.

    ``engine.complete()`` calls this first, on every invocation — blocking here
    is what lets a test deterministically catch ``_handle_result`` mid-flight.
    """

    def __init__(self) -> None:
        """Start blocked."""
        super().__init__()
        self.unblock = asyncio.Event()
        self.entered = asyncio.Event()

    async def get_canvas_status(self, canvas_id: str) -> CanvasStatus:
        """Signal entry, then wait for the test to release it before delegating."""
        self.entered.set()
        await self.unblock.wait()
        return await super().get_canvas_status(canvas_id)


class TestRunStopLifecycle:
    """run()/stop(): consumes results and sweeps until told to stop."""

    async def test_cancelling_mid_handle_result_nacks_for_redelivery(self) -> None:
        """A shutdown that cancels _consume_results mid-flight must not lose the delivery."""
        store = BlockingStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = Coordinator(
            broker,
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(sweep_interval=100.0),
        )
        await publish_result(broker, "t1", ok_outcome("t1"))

        run_task = asyncio.create_task(coordinator.run())
        await asyncio.wait_for(store.entered.wait(), timeout=2)
        await coordinator.stop()
        store.unblock.set()  # let the blocked call resume, into the cancellation
        await run_task

        redelivered = await asyncio.wait_for(anext(broker.consume(RESULTS_TOPIC)), timeout=2)
        assert redelivered.attempt == 2

    async def test_run_consumes_a_result_and_stop_shuts_down_cleanly(self) -> None:
        """A result published while running must be handled before a clean stop."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic="topic-t1"),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="topic-t2"),
                "chain": ChainNode(
                    id="chain",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["t1", "t2"],
                ),
            },
        )
        broker = MemoryBroker()
        coordinator = Coordinator(
            broker,
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(sweep_interval=100.0),
        )

        run_task = asyncio.create_task(coordinator.run())
        await publish_result(broker, "t1", ok_outcome("t1", '{"v":1}'))
        dispatched = await asyncio.wait_for(anext(broker.consume("topic-t2")), timeout=2)
        await coordinator.stop()
        await run_task

        envelope = Envelope.from_bytes(dispatched.body)
        assert envelope.node_id == "t2"

    async def test_second_run_while_already_running_raises(self) -> None:
        """run() must not be re-entrant."""
        store = MemoryCanvasStore()
        broker = MemoryBroker()
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)

        run_task = asyncio.create_task(coordinator.run())
        await asyncio.sleep(0)  # let run() actually start and flip _running

        with pytest.raises(CoordinatorAlreadyRunningError):
            await coordinator.run()

        await coordinator.stop()
        await run_task

    async def test_the_sweep_loop_runs_periodically_while_stopped_cleanly(self) -> None:
        """A tiny sweep_interval must fire at least once before stop() tears it down."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id=None, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = Coordinator(
            broker,
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(sweep_interval=0.01, max_age=0.0),
        )
        coordinator._track(CANVAS, "t1")

        run_task = asyncio.create_task(coordinator.run())
        await asyncio.sleep(0.05)
        await coordinator.stop()
        await run_task

        assert (CANVAS, "t1") not in coordinator._in_flight


class ClosingSpyBroker(MemoryBroker):
    """A MemoryBroker that records when it is closed, into a shared ordered log."""

    def __init__(self, log: list[str]) -> None:
        """Record close calls into ``log``."""
        super().__init__()
        self._log = log

    async def close(self) -> None:
        """Record the call, then close as usual."""
        self._log.append("broker")
        await super().close()


class ClosingSpyStore(MemoryCanvasStore):
    """A MemoryCanvasStore that records when it is closed, into a shared ordered log."""

    def __init__(self, log: list[str]) -> None:
        """Record close calls into ``log``."""
        super().__init__()
        self._log = log

    async def close(self) -> None:
        """Record the call, then close as usual."""
        self._log.append("store")
        await super().close()


class TestCrossCanvasTracking:
    """Two canvases sharing a node id must be tracked, swept, and cancelled independently.

    ``Node(topic, input, id=...)`` and ``apply(canvas_id=...)`` both exist so a
    caller can pin ids for idempotent retries, which makes the same node id
    running in two canvases at once ordinary usage rather than a corner case.
    """

    OTHER_CANVAS = "c2"

    async def test_two_canvases_sharing_a_node_id_are_tracked_separately(self) -> None:
        """A bare node_id key let the second canvas evict the first from the sweeper."""
        coordinator = Coordinator(MemoryBroker(), MemoryCanvasStore(), RESULTS_TOPIC)

        coordinator._track(CANVAS, "shared")
        coordinator._track(self.OTHER_CANVAS, "shared")

        assert (CANVAS, "shared") in coordinator._in_flight
        assert (self.OTHER_CANVAS, "shared") in coordinator._in_flight

    async def test_untracking_one_canvas_leaves_the_others_entry_alone(self) -> None:
        """Untracking canvas B must not pop canvas A's identically-named node."""
        coordinator = Coordinator(MemoryBroker(), MemoryCanvasStore(), RESULTS_TOPIC)
        coordinator._track(CANVAS, "shared")
        coordinator._track(self.OTHER_CANVAS, "shared")

        coordinator._untrack(self.OTHER_CANVAS, "shared")

        assert (CANVAS, "shared") in coordinator._in_flight
        assert (self.OTHER_CANVAS, "shared") not in coordinator._in_flight
        assert coordinator._by_canvas[CANVAS] == {"shared"}
        assert self.OTHER_CANVAS not in coordinator._by_canvas

    async def test_a_stale_node_in_one_canvas_does_not_time_out_the_other(self) -> None:
        """The sweeper must fail only the canvas whose node actually went stale."""
        store = MemoryCanvasStore()
        coordinator = Coordinator(
            MemoryBroker(),
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(max_age=1.0),
        )
        for canvas in (CANVAS, self.OTHER_CANVAS):
            await store.create_canvas(
                canvas,
                {"shared": TaskNode(id="shared", canvas_id=canvas, topic="t")},
            )
        stale = datetime.now(UTC) - timedelta(seconds=99)
        coordinator._in_flight[CANVAS, "shared"] = InFlightNode(CANVAS, "shared", stale)
        coordinator._track(self.OTHER_CANVAS, "shared")

        await coordinator._sweep_once()

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR
        assert await store.get_canvas_status(self.OTHER_CANVAS) == CanvasStatus.RUNNING


class TestShutdownReleasesResources:
    """_shutdown must release everything it owns, matching WorkerApp._shutdown."""

    async def test_shutdown_closes_the_store_as_well_as_the_broker(self) -> None:
        """A RedisCanvasStore's connection pool leaked on every coordinator shutdown.

        ``WorkerApp._shutdown`` closes both; the coordinator only closed the broker.
        """
        closed: list[str] = []
        broker = ClosingSpyBroker(closed)
        store = ClosingSpyStore(closed)
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)

        run_task = asyncio.create_task(coordinator.run())
        async with asyncio.timeout(1.0):
            await coordinator.stop()
            await run_task

        assert closed == ["broker", "store"]

    async def test_a_stop_that_lands_before_the_loop_starts_still_stops_it(self) -> None:
        """`run()` used to replace `_stop_event`, discarding a stop from that window.

        `create_task(run())` doesn't run the coroutine immediately, so a `stop()`
        issued on the next line set an event `run()` was about to throw away —
        leaving a coordinator running with nothing left able to stop it.
        """
        coordinator = Coordinator(MemoryBroker(), MemoryCanvasStore(), RESULTS_TOPIC)

        run_task = asyncio.create_task(coordinator.run())
        await coordinator.stop()

        async with asyncio.timeout(1.0):
            await run_task

        assert coordinator._running is False

    async def test_an_instance_can_run_again_after_a_clean_shutdown(self) -> None:
        """The stop event is cleared once serviced, so a stopped instance is reusable."""
        coordinator = Coordinator(MemoryBroker(), MemoryCanvasStore(), RESULTS_TOPIC)
        first = asyncio.create_task(coordinator.run())
        await coordinator.stop()
        async with asyncio.timeout(1.0):
            await first

        second = asyncio.create_task(coordinator.run())
        await asyncio.sleep(0)
        assert coordinator._running is True

        await coordinator.stop()
        async with asyncio.timeout(1.0):
            await second


class FailFirstResultCoordinator(Coordinator):
    """A Coordinator whose first result handling raises something outside WorkerError."""

    def __init__(
        self,
        broker: MemoryBroker,
        store: MemoryCanvasStore,
        results_topic: str,
        *,
        sweep_interval: float = CoordinatorConfig().sweep_interval,
    ) -> None:
        """Start with an empty call log."""
        super().__init__(
            broker,
            store,
            results_topic,
            config=CoordinatorConfig(sweep_interval=sweep_interval),
        )
        self.handled: list[str] = []
        self.second_call = asyncio.Event()

    async def _handle_result(self, delivery: Delivery) -> None:
        """Raise on the first call, behave normally afterwards."""
        self.handled.append("call")
        if len(self.handled) == 1:
            detail = "ack failed"
            raise ConnectionResetError(detail)
        self.second_call.set()
        await super()._handle_result(delivery)


class TestResultLoopSurvivesOneBadDelivery:
    """One unexpected exception must not kill the coordinator's only result loop."""

    async def test_a_failing_ack_does_not_stop_the_loop(self) -> None:
        """Guarding only CancelledError stalled every canvas in the deployment silently.

        The process stayed up and the sweeper kept running, so nothing looked
        wrong — results simply stopped being processed, forever.
        """
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="topic-t1")},
        )
        broker = MemoryBroker()
        coordinator = FailFirstResultCoordinator(
            broker,
            store,
            RESULTS_TOPIC,
            sweep_interval=100.0,
        )

        task = asyncio.create_task(coordinator._consume_results())
        await publish_result(broker, "t1", ok_outcome("t1"))
        await publish_result(broker, "t1", ok_outcome("t1"))
        async with asyncio.timeout(1.0):
            await coordinator.second_call.wait()

        assert len(coordinator.handled) >= 2
        assert not task.done()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class TestSweepDoesNotRaceARealResult:
    """A result arriving mid-sweep must win over the synthetic timeout outcome."""

    async def test_a_node_untracked_during_the_sweep_is_not_timed_out(self) -> None:
        """_sweep_once snapshots stale entries then awaits, leaving a window.

        Completing the node twice dispatches a chain's next step twice, or
        double-counts a group leg.
        """
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="topic-t1")},
        )
        coordinator = Coordinator(
            MemoryBroker(),
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(max_age=1.0),
        )
        stale = datetime.now(UTC) - timedelta(seconds=99)
        entry = InFlightNode(CANVAS, "t1", stale)
        coordinator._in_flight[CANVAS, "t1"] = entry
        # the real result landed first, in the window the sweep awaits through
        coordinator._untrack(CANVAS, "t1")

        await coordinator._timeout_node(entry)

        assert await store.get_result(CANVAS, "t1") is None
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.RUNNING

    async def test_a_genuinely_stale_node_is_still_timed_out(self) -> None:
        """The re-check must not stop the sweeper doing its actual job."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="topic-t1")},
        )
        coordinator = Coordinator(
            MemoryBroker(),
            store,
            RESULTS_TOPIC,
            config=CoordinatorConfig(max_age=1.0),
        )
        stale = datetime.now(UTC) - timedelta(seconds=99)
        coordinator._in_flight[CANVAS, "t1"] = InFlightNode(CANVAS, "t1", stale)

        await coordinator._sweep_once()

        recorded = await store.get_result(CANVAS, "t1")
        assert recorded is not None
        assert recorded.status == NodeStatus.ERROR


class TestResultRetryCap:
    """The coordinator needs the same poison-message escape Worker has."""

    async def _failing_delivery(self, attempt: int) -> MemoryBroker:
        """Complete t1 of a two-step chain against a broker that can't publish t2."""
        store = MemoryCanvasStore()
        await store.create_canvas(
            CANVAS,
            {
                "t1": TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain", topic="topic-t1"),
                "t2": TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain", topic="topic-t2"),
                "chain": ChainNode(
                    id="chain",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["t1", "t2"],
                ),
            },
        )
        broker = RaisingOnTopicBroker(raises_for="topic-t2")
        coordinator = Coordinator(broker, store, RESULTS_TOPIC)
        envelope = Envelope(
            node_id="t1",
            canvas_id=CANVAS,
            body=ok_outcome("t1").model_dump_json(),
        )
        delivery = MemoryDelivery(broker, RESULTS_TOPIC, envelope.to_bytes(), attempt=attempt)
        await coordinator._handle_result(delivery)
        return broker

    async def test_a_dispatch_failure_under_the_cap_is_requeued(self) -> None:
        """A transient publish failure must still retry."""
        broker = await self._failing_delivery(attempt=1)

        async with asyncio.timeout(1.0):
            assert (await anext(broker.consume(RESULTS_TOPIC))).attempt == 2

    async def test_a_dispatch_failure_at_the_cap_is_dead_lettered(self) -> None:
        """An unconditional requeue is an unbounded retry storm with no escape.

        A WorkerError self-heals (the engine marks the canvas ERROR first, so the
        replay short-circuits), but a dispatch-publish failure does not.
        """
        broker = await self._failing_delivery(attempt=CoordinatorConfig().max_attempts)

        async with asyncio.timeout(1.0):
            dead = await anext(broker.consume(f"{RESULTS_TOPIC}{MemoryBroker.DLQ_SUFFIX}"))
        assert dead.attempt == CoordinatorConfig().max_attempts
