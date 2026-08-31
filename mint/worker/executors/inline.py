"""The default executor: just await the coroutine directly."""

from collections.abc import Awaitable, Callable


class InlineExecutor[T, RT]:
    """Runs ``process`` in the worker's own event loop — correct for any non-blocking task."""

    async def execute(self, fn: Callable[[T], Awaitable[RT]], input_: T) -> RT:
        """Await ``fn(input_)`` directly."""
        return await fn(input_)
