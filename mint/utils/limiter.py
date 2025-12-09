"""Concurrency limiting utilities for async operations."""

import asyncio
from collections.abc import Callable, Coroutine
from contextvars import ContextVar
from functools import wraps
from types import TracebackType
from typing import Any, Concatenate, Final


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

        """
        self._max_concurrent = max_concurrent or self.DEFAULT_MAX_CONCURRENT
        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._acquired_ctx: ContextVar[bool] = ContextVar(
            f"_limiter_acquired_{id(self)}",
            default=False,
        )

    @property
    def max_concurrent(self) -> int:
        """Get the maximum concurrent operations allowed."""
        return self._max_concurrent

    async def __aenter__(self) -> "ConcurrencyLimiter":
        """Acquire the semaphore on context entry."""
        if not self._acquired_ctx.get():
            await self._semaphore.acquire()
            self._acquired_ctx.set(True)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Release the semaphore on context exit."""
        if self._acquired_ctx.get():
            self._semaphore.release()
            self._acquired_ctx.set(False)

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
            if self._acquired_ctx.get():
                return await func(self_inner, *args, **kwargs)

            await self._semaphore.acquire()
            token = self._acquired_ctx.set(True)
            try:
                return await func(self_inner, *args, **kwargs)
            finally:
                self._acquired_ctx.reset(token)
                self._semaphore.release()

        return wrapper
