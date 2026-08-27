"""WorkerApp: owns broker/store wiring, the topic registry, run(), and graceful shutdown.

Registering a worker here is what removes the container boilerplate every easyrag
service currently hand-rolls: workers are constructed with only their own domain
dependencies (settings, db, connectors); the app injects the shared broker, store,
canvas engine, and executor at ``register()`` time.
"""

import asyncio
import contextlib
import signal
from functools import partial
from typing import Final

from mint.logger import get_logger
from mint.worker.brokers.interface import IBroker
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.exc import AppAlreadyRunningError, DuplicateTopicError, MissingWorkerConfigError
from mint.worker.executors.inline import InlineExecutor
from mint.worker.executors.interface import IClosableExecutor, ITaskExecutor
from mint.worker.stores.interface import ICanvasStore
from mint.worker.worker import Worker, WorkerBinding

logger = get_logger(__name__)

REQUIRED_WORKER_ATTRS: Final[tuple[str, ...]] = ("topic", "Input", "Output")


class WorkerApp:
    """Owns the broker/store, the topic registry, and the run/shutdown lifecycle."""

    DEFAULT_DRAIN_TIMEOUT: Final[float] = 5.0

    def __init__(
        self,
        broker: IBroker,
        store: ICanvasStore,
        *,
        executor: ITaskExecutor | None = None,
        drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
        results_topic: str | None = None,
    ) -> None:
        """Build an app over a shared broker/store; every registered worker uses them.

        ``results_topic`` set switches every worker registered on this app into
        centralized mode (see ``WorkerBinding``) — pair it with a ``Coordinator``
        consuming that same topic. Left ``None`` (the default), workers advance
        the canvas themselves.
        """
        self.broker = broker
        self.store = store
        self.engine = CanvasEngine(store)
        self.executor: ITaskExecutor = executor or InlineExecutor()
        self.drain_timeout = drain_timeout
        self.results_topic = results_topic
        self._workers: dict[str, Worker] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._running = False
        self._stop_event = asyncio.Event()

    def register(self, worker: Worker) -> None:
        """Validate and wire a worker into this app's topic registry."""
        cls = type(worker)
        for attr in REQUIRED_WORKER_ATTRS:
            if not hasattr(cls, attr):
                raise MissingWorkerConfigError(worker=cls.__name__, attribute=attr)
        if worker.topic in self._workers:
            raise DuplicateTopicError(topic=worker.topic)
        worker.bind(
            WorkerBinding(
                broker=self.broker,
                store=self.store,
                engine=self.engine,
                executor=worker.executor or self.executor,
                results_topic=self.results_topic,
            ),
        )
        self._workers[worker.topic] = worker

    async def run(self) -> None:
        """Run every registered worker until stopped (SIGTERM/SIGINT or ``stop()``)."""
        if self._running:
            raise AppAlreadyRunningError
        self._running = True
        self._tasks = {topic: asyncio.create_task(w.run()) for topic, w in self._workers.items()}
        for topic, task in self._tasks.items():
            task.add_done_callback(partial(self._on_worker_exit, topic))
        self._install_signal_handlers()
        try:
            await self._stop_event.wait()
        finally:
            await self._shutdown()

    def _on_worker_exit(self, topic: str, task: asyncio.Task[None]) -> None:
        """Shut the app down if a consume loop dies on its own.

        ``run()`` only awaits ``_stop_event``, so a worker task that died — a
        dropped broker connection propagating out of ``consume()``, say — used to
        go completely unobserved: the process stayed alive and healthy-looking
        while consuming nothing from that topic, forever. A dead loop is not
        recoverable in place, so it takes the app down and lets the supervisor
        restart it, rather than degrading silently.
        """
        if task.cancelled() or self._stop_event.is_set():
            return
        exc = task.exception()
        if exc is None:
            logger.error("Worker consume loop exited unexpectedly", topic=topic)
        else:
            logger.error("Worker consume loop failed", topic=topic, error=repr(exc))
        self._stop_event.set()

    async def stop(self) -> None:
        """Trigger a graceful shutdown programmatically — also what SIGTERM/SIGINT call.

        Safe to call before ``run()`` has actually begun: the event is created once,
        in ``__init__``, and cleared only once a shutdown has fully run. ``run()``
        used to replace it on entry, which silently discarded a ``stop()`` that
        landed in the window between ``create_task(run())`` and the loop starting —
        leaving it running with nothing left to stop it.
        """
        self._stop_event.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stop_event.set)

    async def _shutdown(self) -> None:
        # Stop pulling new messages first: cancel every consume loop before draining
        # in-flight work. A consume loop left alive during drain() can pick up a
        # message drain's own timeout just requeued and immediately re-nack it —
        # not incorrect, but an avoidable extra round-trip; cancelling the loops up
        # front removes that race entirely.
        for worker in self._workers.values():
            worker.stop_consuming()
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        await self._drain_workers()
        await self.broker.close()
        await self.store.close()
        await self._close_executors()
        self._running = False
        self._stop_event.clear()

    async def _drain_workers(self) -> None:
        """Let in-flight handlers finish, then make sure the timed-out ones are done too.

        On timeout, cancelling the outer gather cancels each handler task — but
        cancellation is only *requested* there, not completed. Closing the broker
        immediately afterwards tore the connection down while those handlers were
        still inside ``await delivery.nack(requeue=True)``, so their nack raised and
        the delivery was stranded — exactly what ``Worker.drain``'s contract says
        this design prevents. The second, un-timed drain waits for the cancellation
        to actually land before anything is closed.
        """
        drains = [worker.drain() for worker in self._workers.values()]
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(self.drain_timeout):
                await asyncio.gather(*drains)
                return
        await asyncio.gather(*(w.drain() for w in self._workers.values()), return_exceptions=True)

    async def _close_executors(self) -> None:
        """Close every distinct closable executor in use, each exactly once.

        Per-worker executor overrides (see ``Worker.executor``) may repeat the
        same instance across workers, or reuse the app's shared default — a plain
        ``set`` keyed by identity keeps a shared instance from being closed twice.
        """
        executors = {id(self.executor): self.executor}
        for worker in self._workers.values():
            if worker.executor is not None:
                executors[id(worker.executor)] = worker.executor
        closable = [e for e in executors.values() if isinstance(e, IClosableExecutor)]
        await asyncio.gather(*(e.aclose() for e in closable))
