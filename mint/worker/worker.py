"""Worker[T, RT]: consumes one topic, runs process(), and advances the canvas.

Acks only after both the canvas engine's store write and every resulting dispatch
publish have succeeded — that ordering is what makes at-least-once redelivery safe
to rely on instead of something to work around (see ``CanvasEngine``'s idempotent
fan-in). A handler cancelled mid-flight during shutdown nacks for redelivery rather
than leaving its delivery stranded unacked.
"""

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from dataclasses import dataclass
from typing import Any, Final

from pydantic import BaseModel, ValidationError

from mint.logger import get_logger
from mint.worker.brokers.interface import Delivery, IBroker
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ErrorInfo, NodeOutcome
from mint.worker.enums import NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import WorkerError, WorkerNotBoundError
from mint.worker.executors.interface import ITaskExecutor
from mint.worker.stores.interface import ICanvasStore

logger = get_logger(__name__)

DEFAULT_MAX_CONCURRENCY: Final[int] = 32
# Runtime state a Worker must not carry into a child process; see __getstate__.
_UNPICKLABLE_RUNTIME_STATE: Final[frozenset[str]] = frozenset(
    {"_binding", "_inflight", "_stopped", "_slots", "_consumer"},
)
DEFAULT_MAX_ATTEMPTS: Final[int] = 5

MALFORMED_INPUT_ERROR: Final[ErrorInfo] = ErrorInfo(
    type="ValidationError",
    message="message body does not match this worker's Input model",
)


@dataclass(frozen=True)
class DeliveryOutcome[T: BaseModel, RT: BaseModel]:
    """What handling one delivery settled, and any success hook still owed.

    ``settled`` has to be readable by ``_handle`` *before* the hook runs: the ack
    happens inside ``_process_delivery``, but ``on_success`` runs after it, and a
    cancellation in that window must not nack an already-acked delivery.
    """

    settled: bool
    input_obj: T | None = None
    result: RT | None = None


@dataclass(frozen=True)
class WorkerBinding:
    """Runtime dependencies ``WorkerApp.register()`` injects before a worker runs.

    ``results_topic`` selects the deployment mode: ``None`` (the default) is
    embedded mode — this worker advances the canvas itself, right here, via
    ``engine``. Set, it's centralized mode — the worker only reports its outcome
    to ``results_topic`` and never touches ``engine``/dispatch at all; a
    ``Coordinator`` elsewhere consumes that topic and does the advancing. Both
    modes share the exact same engine and produce identical dispatch sequences —
    only who calls ``engine.complete()`` differs.
    """

    broker: IBroker
    store: ICanvasStore
    engine: CanvasEngine
    executor: ITaskExecutor[Any, Any]
    results_topic: str | None = None


class Worker[T: BaseModel, RT: BaseModel]:
    """Consumes ``topic``, runs ``process`` on each message, and advances the canvas.

    Subclasses declare ``topic``, ``Input``, ``Output`` as class attributes and
    implement ``process``; ``WorkerApp.register()`` calls ``bind()`` with the
    shared broker/store/engine before ``run()`` is ever called.
    """

    topic: str
    Input: type[T]
    Output: type[RT]
    # Override the app-wide default executor for this worker specifically. Left
    # None, WorkerApp.register() falls back to its own shared executor — set this
    # when one worker needs its own execution strategy (a CPU-bound process on a
    # ProcessPoolExecutor, or a GRPCExecutor/AMQPRPCExecutor that replaces process
    # with a remote call).
    executor: ITaskExecutor[T, RT] | None = None
    # How many deliveries this worker will handle at once. run() stops pulling from
    # the broker while this many are in flight, which is the only backpressure some
    # brokers get: RabbitMQ has prefetch_count and Redis reads one entry at a time,
    # but Kafka and MemoryBroker yield as fast as the topic supplies, so an
    # unbounded loop spawns one handler task per backlogged message.
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY
    # How many delivery attempts a message gets before it is dead-lettered instead
    # of requeued. Every broker maintains Delivery.attempt; this is what reads it.
    max_attempts: int = DEFAULT_MAX_ATTEMPTS

    def __init__(self) -> None:
        """Start unbound; ``WorkerApp.register()`` supplies runtime dependencies."""
        self._binding: WorkerBinding | None = None
        self._inflight: set[asyncio.Task[None]] = set()
        self._stopped = asyncio.Event()
        # A plain Semaphore rather than mint.utils.ConcurrencyLimiter: this acquires
        # in run() and releases in the handler task, and the limiter's ContextVar
        # reentrancy assumes both happen in the same task.
        self._slots = asyncio.Semaphore(self.max_concurrency)
        # Held so the app can close it *after* draining. Cancelling run() unwinds the
        # `async for`, and letting the generator be finalised then would run its
        # cleanup — closing the RabbitMQ channel / stopping the Kafka consumer that
        # in-flight handlers still need in order to ack.
        self._consumer: AsyncIterator[Delivery] | None = None

    def __getstate__(self) -> dict[str, object]:
        """Drop this worker's runtime state so ``process`` can cross a process boundary.

        ``ProcessPoolExecutor`` pickles ``(self.process, input_)``, and a bound
        method drags its whole instance along — including ``_inflight``, which
        *always* holds the currently-running ``asyncio.Task``, plus the semaphore,
        the stop event, and ``_binding``'s live broker/store handles. None of those
        are picklable, so a worker configured with a process pool failed every
        message with ``UnpicklableTaskError`` before doing any work.

        The child process only ever calls ``process``; it has no use for any of it.
        A worker's own domain dependencies still have to be picklable, which is the
        constraint ``UnpicklableTaskError`` exists to report.
        """
        return {
            key: value
            for key, value in self.__dict__.items()
            if key not in _UNPICKLABLE_RUNTIME_STATE
        }

    def __setstate__(self, state: dict[str, object]) -> None:
        """Restore a worker in the child process, unbound and with fresh primitives."""
        self.__dict__.update(state)
        self._binding = None
        self._inflight = set()
        self._stopped = asyncio.Event()
        self._slots = asyncio.Semaphore(self.max_concurrency)
        self._consumer = None

    async def process(self, input_obj: T) -> RT:
        """Do the actual work for one message. Must be implemented by subclasses."""
        raise NotImplementedError

    async def before_start(self, input_obj: T) -> None:
        """Run before ``process()``. Override for side effects; default is a no-op."""

    async def on_success(self, input_obj: T, result: RT) -> None:
        """Run after a successful ``process()``. Override for side effects; default is a no-op."""

    async def on_failure(self, input_obj: T, exc: Exception) -> None:
        """Run after a failed ``process()``/``before_start()``. Default is a no-op."""

    def bind(self, binding: WorkerBinding) -> None:
        """Wire this worker to its runtime dependencies. Called by ``WorkerApp.register()``."""
        self._binding = binding

    def stop_consuming(self) -> None:
        """Stop pulling new messages; in-flight handling continues until ``drain()``."""
        self._stopped.set()

    def resume_consuming(self) -> None:
        """Clear the stop flag so this worker can run again.

        ``WorkerApp._shutdown`` resets its own state for a restart, and ``run()``'s
        ``AppAlreadyRunningError`` guard implies restarting is allowed — but a
        worker whose ``_stopped`` was never cleared nacks its first delivery and
        exits immediately on the second run, taking the app down with it.
        """
        self._stopped = asyncio.Event()

    async def close_consumer(self) -> None:
        """Release the broker resources this worker's consume loop holds.

        Called by the app only after draining, so a handler finishing late still
        has a live channel to ack on.
        """
        if self._consumer is None:
            return
        consumer, self._consumer = self._consumer, None
        if isinstance(consumer, AsyncGenerator):
            await consumer.aclose()

    async def drain(self) -> None:
        """Wait for every in-flight handler to finish.

        The caller controls the deadline by wrapping this call in
        ``asyncio.timeout()``; a handler cancelled that way nacks its delivery
        for redelivery (see ``_handle``) rather than leaving it stranded unacked.
        """
        if not self._inflight:
            return
        await asyncio.gather(*self._inflight, return_exceptions=True)

    async def run(self) -> None:
        """Consume ``topic`` until ``stop_consuming()`` is called.

        At most ``max_concurrency`` deliveries are handled at once; the loop stops
        pulling from the broker while that many are in flight.

        A delivery that arrives in the race window after ``stop_consuming()`` but
        before this loop is cancelled is nacked and the loop stops immediately —
        it does NOT loop back for more. On a broker whose ``requeue=True`` redelivers
        synchronously onto the same queue (like ``MemoryBroker``), looping back here
        would immediately re-receive that same requeued message, see ``_stopped``
        still set, nack it again, and spin forever.
        """
        binding = self._require_binding()
        self._consumer = binding.broker.consume(self.topic)
        async for delivery in self._consumer:
            if self._stopped.is_set():
                await delivery.nack(requeue=True)
                return
            # Acquired here, released by _handle: holding it across the yield point
            # is what stops the loop pulling more while max_concurrency are in flight.
            await self._slots.acquire()
            task = asyncio.create_task(self._handle(delivery, binding))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)

    def _require_binding(self) -> WorkerBinding:
        if self._binding is None:
            raise WorkerNotBoundError(worker=type(self).__name__)
        return self._binding

    async def _handle(self, delivery: Delivery, binding: WorkerBinding) -> None:
        """Run one delivery to completion, settling it exactly once, whatever happens.

        Every exit path — success, cancellation during shutdown, or an unexpected
        exception — must leave the delivery either acked or nacked. This task is
        fire-and-forget (``run()`` only keeps it alive in ``_inflight``), so an
        exception escaping here would never be retrieved or logged, and the
        delivery would simply be stranded: unacked, unnacked, invisible.
        """
        settled = False
        try:
            handled = await self._process_delivery(delivery, binding)
            settled = handled.settled
            if handled.input_obj is not None and handled.result is not None:
                await self._safe_on_success(handled.input_obj, handled.result)
        except asyncio.CancelledError:
            # Only nack what we never settled. Cancellation can land *after* the ack
            # (in the on_success hook), and nacking an acked delivery double-settles
            # it — on RabbitMQ that republishes the message and acks a second time,
            # which the broker rejects and which reruns a node that already finished.
            if not settled:
                await self._safe_retry_or_drop(delivery)
            raise
        except Exception:
            logger.exception("Unhandled error handling a delivery", topic=self.topic)
            if not settled:
                # Capped, not an unconditional requeue: a persistently failing ack()
                # would otherwise re-run the work forever, which is the exact storm
                # _retry_or_drop exists to stop.
                await self._safe_retry_or_drop(delivery)
        finally:
            self._slots.release()

    async def _process_delivery(
        self,
        delivery: Delivery,
        binding: WorkerBinding,
    ) -> DeliveryOutcome[T, RT]:
        """Handle one delivery, settling it, and report any success hook still owed.

        The hook is deliberately *not* run here. It runs in ``_handle``, after the
        settlement flag has been read, so that a cancellation inside a slow hook
        can't nack a delivery this method already acked.
        """
        envelope = self._decode_envelope(delivery.body)
        if envelope is None:
            await delivery.nack(requeue=False)
            return DeliveryOutcome(settled=True)

        input_obj = self._decode_input(envelope)
        if input_obj is None:
            # The body doesn't match this worker's Input, so retrying can never help —
            # but unlike a malformed envelope, canvas_id/node_id are both known here.
            # Failing the node explicitly is what stops one schema-mismatched message
            # from leaving the node PENDING and its canvas RUNNING forever (embedded
            # mode, the default, has no sweeper that would ever notice).
            if not await self._fail_node(envelope, MALFORMED_INPUT_ERROR, binding):
                # The failure couldn't even be recorded. Dead-lettering now would
                # leave the canvas RUNNING with nothing able to advance it, so retry
                # instead — bounded by max_attempts, which dead-letters in the end.
                await self._retry_or_drop(delivery, envelope.node_id)
                return DeliveryOutcome(settled=True)
            await delivery.nack(requeue=False)
            return DeliveryOutcome(settled=True)

        # Marked before the work starts, so a canvas stuck mid-flight is diagnosable:
        # without this a node reads PENDING right up until it terminates, and
        # "dispatched and running" is indistinguishable from "never dispatched".
        # Bug #13 claimed every transition writes its status; this is the one that
        # was still missing.
        await self._safe_mark_running(envelope, binding)

        outcome, result = await self._run_task(input_obj, envelope.node_id, binding.executor)

        if not await self._advance(envelope, outcome, binding):
            await self._retry_or_drop(delivery, envelope.node_id)
            return DeliveryOutcome(settled=True)

        await delivery.ack()
        return DeliveryOutcome(settled=True, input_obj=input_obj, result=result)

    async def _retry_or_drop(self, delivery: Delivery, node_id: str | None) -> None:
        """Requeue this delivery, or dead-letter it once ``max_attempts`` is spent.

        Without a cap, a persistently failing store or broker means every failure
        nacks for redelivery forever. On a broker that redelivers synchronously
        (``MemoryBroker``) that is a tight CPU-burning loop; on the rest it is an
        unbounded retry storm with no poison-message escape. Every broker already
        maintains ``Delivery.attempt`` — nothing read it until now.
        """
        if delivery.attempt >= self.max_attempts:
            logger.error(
                "Giving up on a delivery after repeated failures",
                topic=self.topic,
                node_id=node_id,
                attempt=delivery.attempt,
            )
            await delivery.nack(requeue=False)
            return
        await delivery.nack(requeue=True)

    async def _safe_mark_running(self, envelope: Envelope, binding: WorkerBinding) -> None:
        """Record that this node is running, logging rather than raising.

        Observability must never cost a message: a store hiccup here would
        otherwise fail work that is about to run perfectly well.
        """
        try:
            await binding.store.mark_node_running(envelope.canvas_id, envelope.node_id)
        except Exception:
            logger.exception(
                "Could not mark a node running",
                node_id=envelope.node_id,
                canvas_id=envelope.canvas_id,
            )

    async def _safe_retry_or_drop(self, delivery: Delivery) -> None:
        """Retry-or-drop, logging rather than raising — nothing above would catch it."""
        try:
            await self._retry_or_drop(delivery, node_id=None)
        except Exception:
            logger.exception("Failed to nack a delivery", topic=self.topic)

    async def _advance(
        self,
        envelope: Envelope,
        outcome: NodeOutcome,
        binding: WorkerBinding,
    ) -> bool:
        """Route an outcome by deployment mode. False means the delivery must be retried."""
        if binding.results_topic is not None:
            return await self._report_result(envelope, outcome, binding, binding.results_topic)
        return await self._advance_canvas(envelope, outcome, binding)

    async def _fail_node(
        self,
        envelope: Envelope,
        error: ErrorInfo,
        binding: WorkerBinding,
    ) -> bool:
        """Record an ERROR outcome for a node whose message can't be processed.

        Returns whether the failure was actually recorded. A caller that gets
        ``False`` must not dead-letter: the node would be left with no outcome and
        its canvas RUNNING forever, which is the exact stall this method exists to
        prevent.
        """
        outcome = NodeOutcome(node_id=envelope.node_id, status=NodeStatus.ERROR, error=error)
        if await self._advance(envelope, outcome, binding):
            return True
        logger.error(
            "Could not record the failure of an undeliverable message",
            node_id=envelope.node_id,
            canvas_id=envelope.canvas_id,
        )
        return False

    async def _advance_canvas(
        self,
        envelope: Envelope,
        outcome: NodeOutcome,
        binding: WorkerBinding,
    ) -> bool:
        """Embedded mode: advance the canvas and dispatch right here. False means retry."""
        try:
            dispatches = await binding.engine.complete(
                envelope.canvas_id,
                envelope.node_id,
                outcome,
            )
        except WorkerError:
            logger.exception("Canvas engine error advancing node", node_id=envelope.node_id)
            return False

        try:
            for dispatch in dispatches:
                envelope_out = dispatch.to_envelope(trace_id=envelope.trace_id)
                await binding.broker.publish(dispatch.topic, envelope_out.to_bytes())
        except Exception:
            logger.exception("Failed to publish dispatch", node_id=envelope.node_id)
            # Release any fan-in guard complete() burned to authorise these dispatches,
            # so the redelivery this False triggers can actually produce them again.
            await binding.engine.rollback(dispatches)
            return False
        return True

    async def _report_result(
        self,
        envelope: Envelope,
        outcome: NodeOutcome,
        binding: WorkerBinding,
        results_topic: str,
    ) -> bool:
        """Centralized mode: report the outcome and let a Coordinator advance it."""
        report = Envelope(
            node_id=envelope.node_id,
            canvas_id=envelope.canvas_id,
            trace_id=envelope.trace_id,
            body=outcome.model_dump_json(),
        )
        try:
            await binding.broker.publish(results_topic, report.to_bytes())
        except Exception:
            logger.exception("Failed to report result", node_id=envelope.node_id)
            return False
        return True

    def _decode_envelope(self, body: bytes) -> Envelope | None:
        try:
            return Envelope.from_bytes(body)
        except ValidationError:
            logger.exception("Malformed envelope", topic=self.topic)
            return None

    def _decode_input(self, envelope: Envelope) -> T | None:
        try:
            return self.Input.model_validate_json(envelope.body)
        except ValidationError:
            logger.exception("Malformed input", topic=self.topic, node_id=envelope.node_id)
            return None

    async def _run_task(
        self,
        input_obj: T,
        node_id: str,
        executor: ITaskExecutor[Any, Any],
    ) -> tuple[NodeOutcome, RT | None]:
        try:
            await self.before_start(input_obj)
            raw = await executor.execute(self.process, input_obj)
            result = self.Output.model_validate(raw, from_attributes=True)
        except Exception as exc:
            await self._safe_on_failure(input_obj, exc)
            outcome = NodeOutcome(
                node_id=node_id,
                status=NodeStatus.ERROR,
                error=ErrorInfo(type=type(exc).__name__, message=str(exc)),
            )
            return outcome, None
        outcome = NodeOutcome(
            node_id=node_id,
            status=NodeStatus.FINISHED,
            result=result.model_dump_json(),
        )
        return outcome, result

    async def _safe_on_success(self, input_obj: T, result: RT) -> None:
        try:
            await self.on_success(input_obj, result)
        except Exception:
            logger.exception("on_success hook raised", worker=type(self).__name__)

    async def _safe_on_failure(self, input_obj: T, exc: Exception) -> None:
        try:
            await self.on_failure(input_obj, exc)
        except Exception:
            logger.exception("on_failure hook raised", worker=type(self).__name__)
