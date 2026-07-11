"""Concurrency control for async tree traversal.

Implements concurrency limiting using:
1. asyncio.Semaphore for capping concurrent in-flight operations
2. Leaky token bucket for rate limiting (operations per second)

Pattern inspired by aiometer/_impl/run_on_each.py
(https://github.com/florimondmanca/aiometer, MIT License).
"""

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager


class ConcurrencyGate:
    """Controls concurrency via semaphore and rate limiting.

    Provides an async context manager that enforces both:
    - Maximum concurrent operations (via asyncio.Semaphore)
    - Maximum operations per second (via leaky token bucket)

    """

    def __init__(
        self,
        max_at_once: int,
        max_per_second: float = 0.0,
    ) -> None:
        """Initialize ConcurrencyGate.

        Args:
            max_at_once: Maximum concurrent operations (must be > 0).
            max_per_second: Maximum operations per second (0 = unlimited).

        Raises:
            ValueError: If max_at_once <= 0 or max_per_second < 0.

        """
        if max_at_once <= 0:
            msg = "max_at_once must be > 0"
            raise ValueError(msg)
        if max_per_second < 0:
            msg = "max_per_second must be >= 0"
            raise ValueError(msg)

        self._semaphore = asyncio.Semaphore(max_at_once)
        self._max_per_second = max_per_second
        self._rate_limit_lock = asyncio.Lock()
        self._last_operation_time: float | None = None

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[None]:
        """Acquire a slot respecting both semaphore and rate limit.

        Yields:
            None — caller performs work inside the context.

        """
        async with self._semaphore:
            await self._wait_for_rate_limit()
            yield

    async def _wait_for_rate_limit(self) -> None:
        """Sleep if necessary to honour the per-second rate limit."""
        if self._max_per_second <= 0:
            return

        async with self._rate_limit_lock:
            now = time.monotonic()

            if self._last_operation_time is not None:
                min_interval = 1.0 / self._max_per_second
                elapsed = now - self._last_operation_time
                if elapsed < min_interval:
                    sleep_duration = min_interval - elapsed
                    await asyncio.sleep(sleep_duration)
                    now = time.monotonic()

            self._last_operation_time = now
