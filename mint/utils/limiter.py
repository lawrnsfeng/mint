"""Concurrency limiting utilities for async operations."""

import asyncio
import weakref
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from functools import wraps
from types import TracebackType
from typing import Any, Concatenate, Final, Self

from mint.utils.exc import InvalidConcurrencyLimitError


class ConcurrencyLimiter:
    """Reusable concurrency limiter using asyncio.Semaphore.

    This class provides a mechanism to limit the number of concurrent
    operations, useful for rate limiting API calls, database connections,
    or any resource that needs controlled access.

    The limiter uses ContextVar to provide per-coroutine isolation,
    allowing nested calls within the same async context to share
    the semaphore acquisition.

    Example:
        limiter = ConcurrencyLimiter(max_concurrent=10)

        @limiter.limit
        async def fetch_data(url: str) -> bytes:
            async with aiohttp.get(url) as resp:
                return await resp.read()

        # Or use as context manager:
        async with limiter:
            await some_operation()

    """

    DEFAULT_MAX_CONCURRENT: Final[int] = 10

    def __init__(self, max_concurrent: int | None = None) -> None:
        """Initialize the concurrency limiter.

        Args:
            max_concurrent: Maximum number of concurrent operations.
                Defaults to DEFAULT_MAX_CONCURRENT (10).

        Raises:
            InvalidConcurrencyLimitError: If max_concurrent is not None
                and is <= 0.

        """
        if max_concurrent is not None and max_concurrent <= 0:
            raise InvalidConcurrencyLimitError(max_concurrent)
        self._max_concurrent = (
            max_concurrent if max_concurrent is not None else self.DEFAULT_MAX_CONCURRENT
        )
        self._semaphores: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            asyncio.Semaphore,
        ] = weakref.WeakKeyDictionary()
        self._acquired_ctx: ContextVar[int] = ContextVar(
            f"_limiter_depth_{id(self)}",
            default=0,
        )

    @property
    def max_concurrent(self) -> int:
        """Get the maximum concurrent operations allowed."""
        return self._max_concurrent

    @property
    def _semaphore(self) -> asyncio.Semaphore:
        """Return the semaphore for the running loop, creating it on demand.

        `asyncio.Semaphore` binds to the first event loop that contends on it,
        so a single eagerly-built semaphore makes the whole limiter -- and any
        object holding one -- unusable on a second loop. That matters for a
        limiter held at module scope: the storage it belongs to recovers from
        one loop ending, and the limiter has to recover with it.

        Returns:
            The semaphore belonging to the running loop.

        """
        loop = asyncio.get_running_loop()
        # A bound semaphore keeps a reference to its own loop, so the weak key
        # never fires on its own; prune closed loops explicitly or the map
        # grows for the life of a process that cycles event loops.
        for dead in [known for known in self._semaphores if known.is_closed()]:
            self._semaphores.pop(dead, None)
        semaphore = self._semaphores.get(loop)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._max_concurrent)
            self._semaphores[loop] = semaphore
        return semaphore

    async def __aenter__(self) -> Self:
        """Acquire the semaphore on context entry."""
        depth = self._acquired_ctx.get()
        if depth == 0:
            await self._semaphore.acquire()
        self._acquired_ctx.set(depth + 1)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the semaphore on context exit."""
        depth = self._acquired_ctx.get()
        self._acquired_ctx.set(depth - 1)
        if depth - 1 == 0:
            self._semaphore.release()

    @property
    def bound_loop_count(self) -> int:
        """How many event loops this limiter currently holds a semaphore for."""
        return len(self._semaphores)

    def limit[S, **P, R](
        self,
        func: Callable[Concatenate[S, P], Coroutine[Any, Any, R]],
    ) -> Callable[Concatenate[S, P], Coroutine[Any, Any, R]]:
        """Decorate an async method to limit its concurrency.

        The decorator uses ContextVar to track whether the semaphore
        is already acquired in the current async context, allowing
        nested calls to share the same acquisition.

        Args:
            func: The async function to wrap.

        Returns:
            Wrapped function with concurrency limiting.

        """

        @wraps(func)
        async def wrapper(
            self_inner: S,
            /,
            *args: P.args,
            **kwargs: P.kwargs,
        ) -> R:
            if self._acquired_ctx.get() > 0:
                return await func(self_inner, *args, **kwargs)

            semaphore = self._semaphore
            await semaphore.acquire()
            token = self._acquired_ctx.set(1)
            try:
                return await func(self_inner, *args, **kwargs)
            finally:
                self._acquired_ctx.reset(token)
                semaphore.release()

        return wrapper
