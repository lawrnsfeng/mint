"""ProcessPoolExecutor: offloads a coroutine function onto a worker process.

Every ``fn`` passed to a real process pool test must be a module-level function —
pickle cannot cross a process boundary with a closure or a local function, which is
exactly what test 58 (a non-picklable fn) deliberately exploits.
"""

import asyncio
import time
from concurrent.futures import ProcessPoolExecutor as StdlibProcessPoolExecutor

import pytest

from mint.worker.exc import UnpicklableTaskError
from mint.worker.executors.process_pool import ProcessPoolExecutor

BLOCK_SECONDS = 0.2


async def double(x: int) -> int:
    """Double the input, asynchronously."""
    return x * 2


async def block_the_process(x: int) -> int:
    """Block synchronously (not `await asyncio.sleep`) for a bit, then return."""
    time.sleep(BLOCK_SECONDS)
    return x


async def boom(x: int) -> int:
    """Raise unconditionally, to prove exceptions propagate through the executor."""
    detail = f"boom: {x}"
    raise ValueError(detail)


class TestExecute:
    """execute() must run the function on a worker process and return its result."""

    async def test_execute_returns_the_coroutines_result(self) -> None:
        """The executor must return exactly what the function returns."""
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor()

        result = await executor.execute(double, 21)

        assert result == 42
        await executor.aclose()

    async def test_execute_propagates_exceptions(self) -> None:
        """A failing function's exception must propagate, not be swallowed."""
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor()

        with pytest.raises(ValueError, match="boom: 5"):
            await executor.execute(boom, 5)
        await executor.aclose()

    async def test_a_blocking_function_does_not_block_the_event_loop(self) -> None:
        """A synchronously-blocking fn must run without stalling other coroutines.

        Same heartbeat proof as ``ThreadPoolExecutor`` — never a sleep.
        """
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor()
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0)

        heartbeat_task = asyncio.create_task(heartbeat())
        await executor.execute(block_the_process, 1)
        stop.set()
        await heartbeat_task

        assert ticks > 1
        await executor.aclose()


class TestPicklabilityGuard:
    """Regression: a non-picklable fn must fail clearly up front, not hang the pool."""

    async def test_a_local_function_raises_immediately_instead_of_hanging(self) -> None:
        """A closure/local function can never cross a process boundary via pickle."""
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor()

        async def local_fn(x: int) -> int:  # not picklable: defined inside a test
            return x

        with pytest.raises(UnpicklableTaskError):
            await executor.execute(local_fn, 1)
        await executor.aclose()


class TestAclose:
    """aclose() must own its cleanup responsibility explicitly — no __del__ side effects."""

    async def test_aclose_shuts_down_a_pool_it_built_itself(self) -> None:
        """An executor-built pool must actually be shut down."""
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor(max_workers=1)

        await executor.aclose()

        assert executor._pool._shutdown_thread

    async def test_aclose_does_not_shut_down_an_adopted_pool(self) -> None:
        """A pool passed in by the caller is owned by the caller, not this executor."""
        adopted = StdlibProcessPoolExecutor(max_workers=1)
        executor: ProcessPoolExecutor[int, int] = ProcessPoolExecutor(pool=adopted)

        await executor.aclose()

        assert not adopted._shutdown_thread
        adopted.shutdown()
