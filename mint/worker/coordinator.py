"""Coordinator: the opt-in centralized deployment mode, off the same CanvasEngine.

Embedded mode (the default — see ``worker.py``) has each worker advance the canvas
itself, right after finishing its own task. Centralized mode instead has every
worker report its ``NodeOutcome`` to one shared results topic
(``WorkerBinding.results_topic`` set), and a single ``Coordinator`` process is the
only thing that ever calls ``engine.complete()``. Both modes share the exact same
``CanvasEngine`` and produce identical dispatch sequences for the same input — the
only difference is *where* that call happens, which is what makes centralized mode
a deployment choice rather than a second implementation to keep in sync.

Centralizing dispatch is also what makes ``cancel()`` and the timeout sweeper
possible without touching the store's schema at all: since every non-entry dispatch
in the whole canvas passes through this one process, it can track "what am I still
waiting a result for" purely in memory.
"""

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from pydantic import ValidationError

from mint.logger import get_logger
from mint.worker.brokers.interface import Delivery, IBroker
from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ErrorInfo, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import (
    CoordinatorAlreadyRunningError,
    CoordinatorAlreadyShutDownError,
    WorkerError,
)
from mint.worker.stores.interface import ICanvasStore

logger = get_logger(__name__)


@dataclass(frozen=True)
class CoordinatorConfig:
    """Tunables for a ``Coordinator``: sweep cadence, timeout window, retry cap.

    Bundled rather than passed individually, matching ``AMQPRPCConfig`` — the
    three of them are one policy decision about how patient the coordinator is.
    """

    sweep_interval: float = 30.0
    max_age: float = 300.0
    max_attempts: int = 5


@dataclass(frozen=True)
class InFlightNode:
    """One dispatch the coordinator is waiting on a result for."""

    canvas_id: str
    node_id: str
    dispatched_at: datetime


class Coordinator:
    """Drives one ``CanvasEngine`` from a shared results topic.

    ``track_and_publish`` is a ``PublishFn`` (see ``canvas/builder.py``): pass it
    as ``Chain.apply()``'s or ``Chord.apply()``'s ``publish`` argument — instead of
    a bare ``broker.publish`` — to bring a canvas's very first dispatch(es) under
    the timeout sweeper too. Without that, the coordinator only ever sees the
    dispatches it derives itself from ``engine.complete()``, and never learns a
    canvas's entry node(s) were dispatched at all.
    """

    def __init__(
        self,
        broker: IBroker,
        store: ICanvasStore,
        results_topic: str,
        *,
        config: CoordinatorConfig | None = None,
    ) -> None:
        """Build a coordinator over ``broker``/``store``, consuming ``results_topic``."""
        cfg = config or CoordinatorConfig()
        self.broker = broker
        self.store = store
        self.engine = CanvasEngine(store)
        self.results_topic = results_topic
        self.sweep_interval = cfg.sweep_interval
        self.max_age = cfg.max_age
        self.max_attempts = cfg.max_attempts
        # Keyed by (canvas_id, node_id), never node_id alone: Node(..., id=...) and
        # apply(canvas_id=...) both let a caller fix ids, so the same node id running
        # in two canvases at once is supported usage — and a bare node_id key means
        # one canvas silently evicts the other from the sweeper.
        self._in_flight: dict[tuple[str, str], InFlightNode] = {}
        # Nodes the sweeper has already failed. A late real result for one of these
        # must stand down, or the node completes twice. This can't be inferred from
        # a failed _claim: an entry node dispatched with a bare broker.publish was
        # never tracked either, and its result must still advance the canvas.
        self._timed_out: set[tuple[str, str]] = set()
        self._by_canvas: dict[str, set[str]] = {}
        self._running = False
        self._shut_down = False
        self._stop_event = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()

    async def track_and_publish(self, topic: str, body: bytes) -> None:
        """Publish ``body`` to ``topic``, tracking it for the timeout sweeper first.

        ``body`` is always an ``Envelope`` (every publisher in this package wraps
        one) — decoding it here is what lets a bare ``PublishFn`` carry the
        canvas/node identity the sweeper needs, with no signature change.
        """
        envelope = Envelope.from_bytes(body)
        self._track(envelope.canvas_id, envelope.node_id)
        await self.broker.publish(topic, body)

    async def dispatch(self, dispatches: list[Dispatch], trace_id: str | None = None) -> None:
        """Publish every dispatch, tracked, carrying ``trace_id`` forward."""
        for one in dispatches:
            await self.track_and_publish(one.topic, one.to_envelope(trace_id).to_bytes())

    async def cancel(self, canvas_id: str) -> None:
        """Cancel a running canvas: mark every in-flight node CANCELLED, then error it."""
        node_ids = list(self._by_canvas.get(canvas_id, ()))
        if node_ids:
            await self.store.cancel_nodes(canvas_id, node_ids)
            for node_id in node_ids:
                self._untrack(canvas_id, node_id)
        # This canvas is over; nothing will arrive late for it any more.
        self._timed_out -= {key for key in self._timed_out if key[0] == canvas_id}
        await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)

    async def run(self) -> None:
        """Consume results and sweep for timeouts until stopped (SIGTERM/SIGINT or stop())."""
        if self._running:
            raise CoordinatorAlreadyRunningError
        if self._shut_down:
            # Same reasoning as WorkerApp: shutdown closed the broker and store, and
            # nothing reopens them.
            raise CoordinatorAlreadyShutDownError
        self._running = True
        self._tasks = {
            asyncio.create_task(self._consume_results()),
            asyncio.create_task(self._sweep_loop()),
        }
        for task in self._tasks:
            task.add_done_callback(self._on_task_exit)
        self._install_signal_handlers()
        try:
            await self._stop_event.wait()
        finally:
            await self._shutdown()

    async def stop(self) -> None:
        """Trigger a graceful shutdown programmatically — also what SIGTERM/SIGINT call.

        Safe to call before ``run()`` has actually begun: the event is created once,
        in ``__init__``, and cleared only once a shutdown has fully run. ``run()``
        used to replace it on entry, which silently discarded a ``stop()`` that
        landed in the window between ``create_task(run())`` and the loop starting —
        leaving it running with nothing left to stop it.
        """
        self._stop_event.set()

    def _on_task_exit(self, task: asyncio.Task[None]) -> None:
        """Shut down if the result loop or the sweeper dies on its own.

        ``run()`` only awaits ``_stop_event``, so either task dying left the
        process alive and idle-looking while doing none of its actual work.
        """
        if task.cancelled() or self._stop_event.is_set():
            return
        exc = task.exception()
        logger.error(
            "Coordinator task exited unexpectedly",
            topic=self.results_topic,
            error=repr(exc) if exc is not None else None,
        )
        self._stop_event.set()

    def _track(self, canvas_id: str, node_id: str) -> None:
        self._in_flight[canvas_id, node_id] = InFlightNode(canvas_id, node_id, datetime.now(UTC))
        self._by_canvas.setdefault(canvas_id, set()).add(node_id)

    def _claim(self, canvas_id: str, node_id: str) -> InFlightNode | None:
        """Take exclusive ownership of a tracked node, returning what was claimed.

        Whoever removes the entry first is the one that gets to complete the node;
        the loser sees None and stands down. Returns None when the node was never
        tracked, in which case there is nothing to contend over and nothing to
        restore.
        """
        entry = self._in_flight.get((canvas_id, node_id))
        if entry is None:
            return None
        self._untrack(canvas_id, node_id)
        return entry

    def _restore(self, entry: InFlightNode | None) -> None:
        """Put a claimed node back, keeping its original dispatch time.

        Preserving ``dispatched_at`` matters: re-tracking with a fresh timestamp
        would silently grant the node another full ``max_age`` before the sweeper
        would look at it again.
        """
        if entry is None:
            return
        self._in_flight[entry.canvas_id, entry.node_id] = entry
        self._by_canvas.setdefault(entry.canvas_id, set()).add(entry.node_id)

    def _untrack(self, canvas_id: str, node_id: str) -> None:
        self._in_flight.pop((canvas_id, node_id), None)
        nodes = self._by_canvas.get(canvas_id)
        if nodes is None:
            return
        nodes.discard(node_id)
        if not nodes:
            del self._by_canvas[canvas_id]

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stop_event.set)

    async def _shutdown(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        try:
            await self._safe_close(self.broker.close, "broker")
            await self._safe_close(self.store.close, "store")
        finally:
            self._remove_signal_handlers()
            self._running = False
            self._shut_down = True
            self._stop_event.clear()

    @staticmethod
    async def _safe_close(close: Callable[[], Awaitable[None]], what: str) -> None:
        """Run one teardown step, logging rather than raising."""
        try:
            await close()
        except Exception:
            logger.exception("Failed to close cleanly during shutdown", component=what)

    def _remove_signal_handlers(self) -> None:
        """Hand SIGTERM/SIGINT back; ``add_signal_handler`` is loop-global."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.remove_signal_handler(sig)

    async def _consume_results(self) -> None:
        """Consume results until cancelled, surviving anything one delivery can throw.

        Guarding only ``CancelledError`` meant a single unexpected exception — most
        plausibly a failing ``ack()`` — killed the coordinator's result loop for
        good. With ``run()`` only awaiting ``_stop_event``, the process stayed up,
        the sweeper kept timing nodes out, and every canvas in the deployment
        stalled with no error anywhere.
        """
        async for delivery in self.broker.consume(self.results_topic):
            settled = False
            try:
                settled = await self._handle_result(delivery)
            except asyncio.CancelledError:
                # Only nack what was never settled. Cancellation can land after the
                # ack, and nacking a settled delivery double-settles it — on RabbitMQ
                # that republishes *and* acks again, re-running a node whose canvas
                # already advanced. Worker._handle carries the same flag for the same
                # reason; this loop was missing it.
                if not settled:
                    await self._safe_nack(delivery)
                raise
            except Exception:
                logger.exception("Unhandled error handling a result", topic=self.results_topic)
                if not settled:
                    await self._safe_nack(delivery)

    async def _safe_nack(self, delivery: Delivery) -> None:
        """Retry-or-drop this delivery, logging rather than raising.

        Used where nothing above would catch an exception — the consume loop's own
        guards. Capped like every other retry path here, so a delivery that keeps
        blowing up the loop eventually dead-letters instead of cycling forever.
        """
        try:
            await self._retry_or_drop(delivery, node_id=None)
        except Exception:
            logger.exception("Failed to nack a result", topic=self.results_topic)

    async def _handle_result(self, delivery: Delivery) -> bool:
        """Handle one result. Returns whether the delivery was settled (acked/nacked)."""
        envelope = self._decode_envelope(delivery.body)
        if envelope is None:
            await delivery.nack(requeue=False)
            return True
        outcome = self._decode_outcome(envelope)
        if outcome is None:
            await delivery.nack(requeue=False)
            return True

        # Claimed before advancing, and restored if that fails. Untracking is the
        # mutual exclusion against the sweeper (see _timeout_node), so it has to
        # happen before the store I/O, not after — otherwise the sweeper sees the
        # entry still present mid-advance and completes the same node a second time
        # with a synthetic timeout. Restoring on failure is what stops a result that
        # then dead-letters from escaping the sweeper entirely.
        key = (envelope.canvas_id, envelope.node_id)
        if key in self._timed_out:
            # The sweeper already completed this node with a synthetic timeout.
            # Advancing again dispatches a chain's next step a second time.
            self._timed_out.discard(key)
            logger.warning(
                "Discarding a result for a node already timed out",
                node_id=envelope.node_id,
                canvas_id=envelope.canvas_id,
            )
            await delivery.ack()
            return True

        claim = self._claim(envelope.canvas_id, envelope.node_id)
        if not await self._advance(
            envelope.canvas_id,
            envelope.node_id,
            outcome,
            trace_id=envelope.trace_id,
        ):
            self._restore(claim)
            await self._retry_or_drop(delivery, envelope.node_id)
            return True
        await delivery.ack()
        return True

    async def _retry_or_drop(self, delivery: Delivery, node_id: str | None) -> None:
        """Requeue this result, or dead-letter it once ``max_attempts`` is spent.

        The same cap ``Worker`` has, for the same reason: an unconditional
        ``nack(requeue=True)`` on every failure is an unbounded retry storm with
        no poison-message escape, and a tight CPU-burning loop on a broker that
        redelivers synchronously. A ``WorkerError`` self-heals (``complete()``
        marks the canvas ERROR first, so the replay short-circuits), but a
        dispatch-publish failure or a failing ``ack()`` does not.
        """
        if delivery.attempt >= self.max_attempts:
            logger.error(
                "Giving up on a result after repeated failures",
                topic=self.results_topic,
                node_id=node_id,
                attempt=delivery.attempt,
            )
            await delivery.nack(requeue=False)
            return
        await delivery.nack(requeue=True)

    async def _advance(
        self,
        canvas_id: str,
        node_id: str,
        outcome: NodeOutcome,
        *,
        trace_id: str | None = None,
    ) -> bool:
        try:
            dispatches = await self.engine.complete(canvas_id, node_id, outcome)
        except WorkerError:
            logger.exception("Canvas engine error advancing node", node_id=node_id)
            return False
        try:
            await self.dispatch(dispatches, trace_id)
        except Exception:
            logger.exception("Failed to publish dispatch", node_id=node_id)
            # Same reasoning as Worker._advance_canvas: a burned fan-in guard must be
            # released or the redelivery this False triggers dispatches nothing.
            await self.engine.rollback(dispatches)
            return False
        return True

    def _decode_envelope(self, body: bytes) -> Envelope | None:
        try:
            return Envelope.from_bytes(body)
        except ValidationError:
            logger.exception("Malformed result envelope", topic=self.results_topic)
            return None

    def _decode_outcome(self, envelope: Envelope) -> NodeOutcome | None:
        try:
            return NodeOutcome.model_validate_json(envelope.body)
        except ValidationError:
            logger.exception("Malformed result outcome", node_id=envelope.node_id)
            return None

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(self.sweep_interval)
            await self._sweep_once()

    async def _sweep_once(self) -> None:
        now = datetime.now(UTC)
        stale = [
            entry
            for entry in list(self._in_flight.values())
            if (now - entry.dispatched_at).total_seconds() > self.max_age
        ]
        for entry in stale:
            await self._timeout_node(entry)

    async def _timeout_node(self, entry: InFlightNode) -> None:
        """Fail one node that never reported, unless its real result just arrived.

        ``_sweep_once`` snapshots the stale entries and then awaits per entry, so a
        genuine result can be handled concurrently in that window. Without this
        re-check the node completes twice — once with its real outcome and once
        with the synthetic timeout — which dispatches a chain's next step twice, or
        double-counts a group leg. Untracking *is* the check: whichever of the two
        removes the entry first is the one that gets to complete the node.
        """
        if self._claim(entry.canvas_id, entry.node_id) is None:
            return
        outcome = NodeOutcome(
            node_id=entry.node_id,
            status=NodeStatus.ERROR,
            error=ErrorInfo(
                type="TimeoutError",
                message=f"No result within {self.max_age}s",
            ),
        )
        if not await self._advance(entry.canvas_id, entry.node_id, outcome):
            # There is no delivery behind a synthetic timeout, so nothing else will
            # ever retry this. Put it back and let the next sweep try again, rather
            # than leaving the canvas RUNNING with no error recorded at all.
            self._restore(entry)
            return
        self._timed_out.add((entry.canvas_id, entry.node_id))
