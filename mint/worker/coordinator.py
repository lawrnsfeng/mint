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
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Final

from pydantic import ValidationError

from mint.logger import get_logger
from mint.worker.brokers.interface import Delivery, IBroker
from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ErrorInfo, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import CoordinatorAlreadyRunningError, WorkerError
from mint.worker.stores.interface import ICanvasStore

logger = get_logger(__name__)


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

    DEFAULT_SWEEP_INTERVAL_SECONDS: Final[float] = 30.0
    DEFAULT_MAX_AGE_SECONDS: Final[float] = 300.0

    def __init__(
        self,
        broker: IBroker,
        store: ICanvasStore,
        results_topic: str,
        *,
        sweep_interval: float = DEFAULT_SWEEP_INTERVAL_SECONDS,
        max_age: float = DEFAULT_MAX_AGE_SECONDS,
    ) -> None:
        """Build a coordinator over ``broker``/``store``, consuming ``results_topic``."""
        self.broker = broker
        self.store = store
        self.engine = CanvasEngine(store)
        self.results_topic = results_topic
        self.sweep_interval = sweep_interval
        self.max_age = max_age
        # Keyed by (canvas_id, node_id), never node_id alone: Node(..., id=...) and
        # apply(canvas_id=...) both let a caller fix ids, so the same node id running
        # in two canvases at once is supported usage — and a bare node_id key means
        # one canvas silently evicts the other from the sweeper.
        self._in_flight: dict[tuple[str, str], InFlightNode] = {}
        self._by_canvas: dict[str, set[str]] = {}
        self._running = False
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

    async def dispatch(self, dispatches: list[Dispatch]) -> None:
        """Publish every dispatch, tracked, exactly like ``track_and_publish``."""
        for one in dispatches:
            await self.track_and_publish(one.topic, one.to_envelope().to_bytes())

    async def cancel(self, canvas_id: str) -> None:
        """Cancel a running canvas: mark every in-flight node CANCELLED, then error it."""
        node_ids = list(self._by_canvas.get(canvas_id, ()))
        if node_ids:
            await self.store.cancel_nodes(canvas_id, node_ids)
            for node_id in node_ids:
                self._untrack(canvas_id, node_id)
        await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)

    async def run(self) -> None:
        """Consume results and sweep for timeouts until stopped (SIGTERM/SIGINT or stop())."""
        if self._running:
            raise CoordinatorAlreadyRunningError
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
        await self.broker.close()
        await self.store.close()
        self._running = False
        self._stop_event.clear()

    async def _consume_results(self) -> None:
        """Consume results until cancelled, surviving anything one delivery can throw.

        Guarding only ``CancelledError`` meant a single unexpected exception — most
        plausibly a failing ``ack()`` — killed the coordinator's result loop for
        good. With ``run()`` only awaiting ``_stop_event``, the process stayed up,
        the sweeper kept timing nodes out, and every canvas in the deployment
        stalled with no error anywhere.
        """
        async for delivery in self.broker.consume(self.results_topic):
            try:
                await self._handle_result(delivery)
            except asyncio.CancelledError:
                await self._safe_nack(delivery)
                raise
            except Exception:
                logger.exception("Unhandled error handling a result", topic=self.results_topic)
                await self._safe_nack(delivery)

    async def _safe_nack(self, delivery: Delivery) -> None:
        """Requeue a delivery, logging rather than raising — nothing above would catch it."""
        try:
            await delivery.nack(requeue=True)
        except Exception:
            logger.exception("Failed to nack a result", topic=self.results_topic)

    async def _handle_result(self, delivery: Delivery) -> None:
        envelope = self._decode_envelope(delivery.body)
        if envelope is None:
            await delivery.nack(requeue=False)
            return
        outcome = self._decode_outcome(envelope)
        if outcome is None:
            await delivery.nack(requeue=False)
            return

        self._untrack(envelope.canvas_id, envelope.node_id)
        if not await self._advance(envelope.canvas_id, envelope.node_id, outcome):
            await delivery.nack(requeue=True)
            return
        await delivery.ack()

    async def _advance(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> bool:
        try:
            dispatches = await self.engine.complete(canvas_id, node_id, outcome)
        except WorkerError:
            logger.exception("Canvas engine error advancing node", node_id=node_id)
            return False
        try:
            await self.dispatch(dispatches)
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
        if (entry.canvas_id, entry.node_id) not in self._in_flight:
            return
        self._untrack(entry.canvas_id, entry.node_id)
        outcome = NodeOutcome(
            node_id=entry.node_id,
            status=NodeStatus.ERROR,
            error=ErrorInfo(
                type="TimeoutError",
                message=f"No result within {self.max_age}s",
            ),
        )
        await self._advance(entry.canvas_id, entry.node_id, outcome)
