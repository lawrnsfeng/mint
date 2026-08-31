"""Runs ``process`` on a worker thread — for blocking (I/O-bound) work.

``Worker.process`` is always a coroutine function; a thread has no event loop of
its own to run it on, so each call drives it to completion with ``asyncio.run``
*inside the worker thread*. That is safe there specifically because a pool thread
never has a running loop of its own to conflict with — unlike calling
``asyncio.run`` from ``__del__`` (bug #11, see ``AMQPRPCExecutor``), which fires
while the *main* loop is already running.
"""

import asyncio
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor as _ThreadPoolExecutor
from typing import Final


class ThreadPoolExecutor[T, RT]:
    """Offloads ``fn(input_)`` onto a thread pool so it never blocks the event loop."""

    DEFAULT_MAX_WORKERS: Final[int] = 4

    def __init__(
        self,
        *,
        max_workers: int = DEFAULT_MAX_WORKERS,
        pool: _ThreadPoolExecutor | None = None,
    ) -> None:
        """Build a pool of ``max_workers`` threads, or adopt an existing ``pool``.

        An adopted pool is assumed to be owned elsewhere and is never shut down by
        ``aclose()`` — only a pool this executor built itself is.
        """
        self._owns_pool = pool is None
        self._pool = pool or _ThreadPoolExecutor(max_workers=max_workers)

    async def execute(self, fn: Callable[[T], Awaitable[RT]], input_: T) -> RT:
        """Run ``fn(input_)`` to completion on a worker thread and return its result."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._pool, self._run_to_completion, fn, input_)

    async def aclose(self) -> None:
        """Shut down the pool, if this executor built it. Never called from ``__del__``.

        Off the event loop, and cancelling what has not started. ``shutdown()``
        defaults to ``wait=True`` and blocks its caller — called directly from
        ``WorkerApp._shutdown`` it blocks the *loop*, so a single hung thread
        turns a graceful shutdown into an unkillable one. Cancelling a
        ``run_in_executor`` future does not cancel the underlying work, so the
        drain timeout leaves exactly that behind.
        """
        if not self._owns_pool:
            return
        await asyncio.to_thread(self._pool.shutdown, cancel_futures=True)

    @staticmethod
    def _run_to_completion(fn: Callable[[T], Awaitable[RT]], input_: T) -> RT:
        """Drive ``fn(input_)`` to completion on whatever thread this runs on.

        ``asyncio.run`` needs an actual coroutine, not just anything awaitable
        (``fn`` is typed as the latter to match ``ITaskExecutor``); wrapping the
        call in a small ``async def`` gives it one.
        """

        async def runner() -> RT:
            return await fn(input_)

        return asyncio.run(runner())
