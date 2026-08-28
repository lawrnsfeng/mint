"""Runs ``process`` in a worker process — for CPU-bound work.

Same ``asyncio.run``-inside-the-worker bridging as ``ThreadPoolExecutor``, plus one
concern threads never have: crossing a process boundary needs ``fn``/``input_`` to
be picklable. A bound method whose ``self`` holds an unpicklable dependency (a DB
client, an open socket — exactly what a ``Worker`` subclass typically carries) would
otherwise fail deep inside the pool, or hang waiting on a submission that silently
never completed. Every call checks picklability up front instead, raising a clear
``UnpicklableTaskError`` before ever touching the pool.
"""

import asyncio
import pickle
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor as _ProcessPoolExecutor
from typing import Final

from mint.worker.exc import UnpicklableTaskError


class ProcessPoolExecutor[T, RT]:
    """Offloads ``fn(input_)`` onto a process pool so CPU-bound work never blocks the loop."""

    DEFAULT_MAX_WORKERS: Final[int] = 4

    def __init__(
        self,
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
        pool: _ProcessPoolExecutor | None = None,
    ) -> None:
        """Build a pool of ``max_workers`` processes, or adopt an existing ``pool``.

        An adopted pool is assumed to be owned elsewhere and is never shut down by
        ``aclose()`` — only a pool this executor built itself is.
        """
        self._owns_pool = pool is None
        self._pool = pool or _ProcessPoolExecutor(max_workers=max_workers)

    async def execute(self, fn: Callable[[T], Awaitable[RT]], input_: T) -> RT:
        """Run ``fn(input_)`` to completion on a worker process and return its result."""
        self._ensure_picklable(fn, input_)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._run_to_completion, fn, input_)

    async def aclose(self) -> None:
        """Shut down the pool, if this executor built it. Never called from ``__del__``.

        Off the event loop, and cancelling what has not started. ``shutdown()``
        defaults to ``wait=True`` and blocks its caller — called directly from
        ``WorkerApp._shutdown`` it blocks the *loop*, so a single hung process
        turns a graceful shutdown into an unkillable one. Cancelling a
        ``run_in_executor`` future does not cancel the underlying work, so the
        drain timeout leaves exactly that behind.
        """
        if not self._owns_pool:
            return
        await asyncio.to_thread(self._pool.shutdown, cancel_futures=True)

    @staticmethod
    def _ensure_picklable(fn: Callable[[T], Awaitable[RT]], input_: T) -> None:
        try:
            pickle.dumps((fn, input_))
        except (pickle.PicklingError, TypeError, AttributeError) as exc:
            raise UnpicklableTaskError(fn=repr(fn), detail=str(exc)) from exc

    @staticmethod
    def _run_to_completion(  # pragma: no cover
        fn: Callable[[T], Awaitable[RT]],
        input_: T,
    ) -> RT:
        """Drive ``fn(input_)`` to completion on whatever process this runs on.

        ``asyncio.run`` needs an actual coroutine, not just anything awaitable
        (``fn`` is typed as the latter to match ``ITaskExecutor``); wrapping the
        call in a small ``async def`` gives it one.

        Excluded from coverage: this body runs inside a *child* process spawned by
        the pool, which pytest-cov's default (non-multiprocessing) tracer cannot
        observe — the surrounding executor tests already prove it runs and returns
        the right result end to end; the exclusion is a measurement gap, not an
        untested path.
        """

        async def runner() -> RT:
            return await fn(input_)

        return asyncio.run(runner())
