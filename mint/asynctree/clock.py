"""Grand timeout clocks for async tree traversal."""

import asyncio
import time
from abc import ABC, abstractmethod


class GrandClock(ABC):
    """Abstract base for grand timeout clocks.

    A grand clock manages the overall timeout budget for a tree traversal.
    Different implementations provide static or dynamic timeout strategies.
    """

    def __init__(self) -> None:
        """Initialize the grand clock."""
        self._cancelled = False
        self._start_time: float | None = None

    def start(self) -> None:
        """Start the clock."""
        self._start_time = time.monotonic()

    def cancel(self) -> None:
        """Cancel the clock, marking traversal as timed out."""
        self._cancelled = True

    @property
    def is_cancelled(self) -> bool:
        """Whether the clock has been cancelled."""
        return self._cancelled

    @property
    def elapsed_ms(self) -> int:
        """Elapsed time in milliseconds since start."""
        if self._start_time is None:
            return 0
        return int((time.monotonic() - self._start_time) * 1000)

    @abstractmethod
    def remaining_seconds(self) -> float:
        """Remaining time budget in seconds (0 if expired)."""

    @abstractmethod
    async def wait_for_timeout(self) -> None:
        """Block until the timeout fires, then mark as cancelled."""


class StaticClock(GrandClock):
    """Fixed-budget grand clock used for depth-bounded traversals.

    Timeout = depth * per_level_seconds (computed by caller).
    """

    def __init__(self, total_seconds: float) -> None:
        """Initialize StaticClock.

        Args:
            total_seconds: Total timeout budget (must be > 0).

        Raises:
            ValueError: If total_seconds <= 0.

        """
        if total_seconds <= 0:
            msg = "total_seconds must be > 0"
            raise ValueError(msg)
        super().__init__()
        self._total_seconds = total_seconds

    def remaining_seconds(self) -> float:
        """Remaining seconds (0 if expired)."""
        if self._start_time is None:
            return self._total_seconds
        elapsed = time.monotonic() - self._start_time
        return max(0.0, self._total_seconds - elapsed)

    async def wait_for_timeout(self) -> None:
        """Sleep for the total budget then cancel."""
        await asyncio.sleep(self._total_seconds)
        self.cancel()


class DynamicClock(GrandClock):
    """Growing-budget grand clock for unbounded traversals.

    Starts with a base budget, then extends by per_level_seconds each time
    a new maximum depth is discovered. Stops growing once expansion stalls.
    """

    def __init__(
        self,
        base_seconds: float = 30.0,
        per_level_seconds: float = 30.0,
    ) -> None:
        """Initialize DynamicClock.

        Args:
            base_seconds: Initial timeout budget (must be > 0).
            per_level_seconds: Budget added per new depth level (>= 0).

        Raises:
            ValueError: If base_seconds <= 0 or per_level_seconds < 0.

        """
        if base_seconds <= 0:
            msg = "base_seconds must be > 0"
            raise ValueError(msg)
        if per_level_seconds < 0:
            msg = "per_level_seconds must be >= 0"
            raise ValueError(msg)
        super().__init__()
        self._base_seconds = base_seconds
        self._per_level_seconds = per_level_seconds
        self._max_depth_seen = 0
        self._current_budget = base_seconds
        self._lock = asyncio.Lock()

    def remaining_seconds(self) -> float:
        """Remaining seconds (0 if expired)."""
        if self._start_time is None:
            return self._current_budget
        elapsed = time.monotonic() - self._start_time
        return max(0.0, self._current_budget - elapsed)

    async def notify_depth(self, depth: int) -> None:
        """Extend budget if a new max depth is discovered.

        Args:
            depth: The depth level just reached.

        """
        async with self._lock:
            if depth > self._max_depth_seen:
                self._max_depth_seen = depth
                self._current_budget += self._per_level_seconds

    async def wait_for_timeout(self) -> None:
        """Poll remaining budget in 1-second slices, cancel when expired."""
        while not self._cancelled:
            remaining = self.remaining_seconds()
            if remaining <= 0:
                self.cancel()
                break
            await asyncio.sleep(min(remaining, 1.0))
