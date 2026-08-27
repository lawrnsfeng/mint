"""Worker[T, RT]: consumes one topic, runs process(), and advances the canvas.

Acks only after both the canvas engine's store write and every resulting dispatch
publish have succeeded — that ordering is what makes at-least-once redelivery safe
to rely on instead of something to work around (see ``CanvasEngine``'s idempotent
fan-in). A handler cancelled mid-flight during shutdown nacks for redelivery rather
than leaving its delivery stranded unacked.
"""

import asyncio
from dataclasses import dataclass
from typing import Final

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

MALFORMED_INPUT_ERROR: Final[ErrorInfo] = ErrorInfo(
    type="ValidationError",
    message="message body does not match this worker's Input model",
)


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
    executor: ITaskExecutor
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

    def __init__(self) -> None:
        """Start unbound; ``WorkerApp.register()`` supplies runtime dependencies."""
        self._binding: WorkerBinding | None = None
        self._inflight: set[asyncio.Task[None]] = set()
        self._stopped = asyncio.Event()
        # A plain Semaphore rather than mint.utils.ConcurrencyLimiter: this acquires
        # in run() and releases in the handler task, and the limiter's ContextVar
        # reentrancy assumes both happen in the same task.
        self._slots = asyncio.Semaphore(self.max_concurrency)

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
        async for delivery in binding.broker.consume(self.topic):
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
        try:
            await self._process_delivery(delivery, binding)
        except asyncio.CancelledError:
            await delivery.nack(requeue=True)
            raise
        finally:
            self._slots.release()

    async def _process_delivery(self, delivery: Delivery, binding: WorkerBinding) -> None:
        envelope = self._decode_envelope(delivery.body)
        if envelope is None:
            await delivery.nack(requeue=False)
            return

        input_obj = self._decode_input(envelope)
        if input_obj is None:
            # The body doesn't match this worker's Input, so retrying can never help —
            # but unlike a malformed envelope, canvas_id/node_id are both known here.
            # Failing the node explicitly is what stops one schema-mismatched message
            # from leaving the node PENDING and its canvas RUNNING forever (embedded
            # mode, the default, has no sweeper that would ever notice).
            await self._fail_node(envelope, MALFORMED_INPUT_ERROR, binding)
            await delivery.nack(requeue=False)
            return

        outcome, result = await self._run_task(input_obj, envelope.node_id, binding.executor)

        if not await self._advance(envelope, outcome, binding):
            await delivery.nack(requeue=True)
            return

        await delivery.ack()

        if result is not None:
            await self._safe_on_success(input_obj, result)

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
    ) -> None:
        """Record an ERROR outcome for a node whose message will be dead-lettered.

        Best-effort by design: the delivery is being dropped either way, so a
        store or broker failure here must not turn an unretryable message into a
        redelivery loop. It is logged and the dead-letter still happens.
        """
        outcome = NodeOutcome(node_id=envelope.node_id, status=NodeStatus.ERROR, error=error)
        if not await self._advance(envelope, outcome, binding):
            logger.error(
                "Could not record the failure of an undeliverable message",
                node_id=envelope.node_id,
                canvas_id=envelope.canvas_id,
            )

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
                await binding.broker.publish(dispatch.topic, dispatch.to_envelope().to_bytes())
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
        executor: ITaskExecutor,
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
