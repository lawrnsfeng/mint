"""InlineExecutor: the default — just await the coroutine directly."""

import pytest

from mint.worker.executors.inline import InlineExecutor


async def double(x: int) -> int:
    """Double the input, asynchronously."""
    return x * 2


async def boom(x: int) -> int:
    """Raise unconditionally, to prove exceptions propagate through the executor unchanged."""
    detail = f"boom: {x}"
    raise ValueError(detail)


class TestInlineExecutor:
    """Runs a coroutine function in the caller's own event loop."""

    async def test_execute_returns_the_coroutines_result(self) -> None:
        """The executor must return exactly what the function returns."""
        executor: InlineExecutor[int, int] = InlineExecutor()

        result = await executor.execute(double, 21)

        assert result == 42

    async def test_execute_propagates_exceptions(self) -> None:
        """A failing function's exception must propagate, not be swallowed."""
        executor: InlineExecutor[int, int] = InlineExecutor()

        with pytest.raises(ValueError, match="boom: 5"):
            await executor.execute(boom, 5)
