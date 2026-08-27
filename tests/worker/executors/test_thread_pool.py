"""ThreadPoolExecutor: offloads a coroutine function onto a worker thread.

Test 57 (a blocking fn must not block the event loop) is proven with a concurrent
heartbeat task, never a sleep — the heartbeat only gets a chance to tick if the
event loop is genuinely free while the blocking work runs elsewhere.
"""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor as StdlibThreadPoolExecutor

import pytest

from mint.worker.executors.thread_pool import ThreadPoolExecutor

BLOCK_SECONDS = 0.2


async def double(x: int) -> int:
    """Double the input, asynchronously."""
    return x * 2


async def block_the_thread(x: int) -> int:
    """Block synchronously (not `await asyncio.sleep`) for a bit, then return."""
    time.sleep(BLOCK_SECONDS)
    return x


async def boom(x: int) -> int:
    """Raise unconditionally, to prove exceptions propagate through the executor."""
    detail = f"boom: {x}"
    raise ValueError(detail)


class TestExecute:
    """execute() must run the function on a worker thread and return its result."""

    async def test_execute_returns_the_coroutines_result(self) -> None:
        """The executor must return exactly what the function returns."""
        executor = ThreadPoolExecutor()

        result = await executor.execute(double, 21)

        assert result == 42
        await executor.aclose()

    async def test_execute_propagates_exceptions(self) -> None:
        """A failing function's exception must propagate, not be swallowed."""
        executor = ThreadPoolExecutor()

        with pytest.raises(ValueError, match="boom: 5"):
            await executor.execute(boom, 5)
        await executor.aclose()

    async def test_a_blocking_function_does_not_block_the_event_loop(self) -> None:
        """A synchronously-blocking fn must run without stalling other coroutines.

        A heartbeat task increments a counter on every loop turn; if the executor
        actually ran the blocking call inline (on the event loop thread), the
        heartbeat would be starved for the whole ``BLOCK_SECONDS`` and this count
        would come back at (or near) zero.
        """
        executor = ThreadPoolExecutor()
        ticks = 0
        stop = asyncio.Event()

        async def heartbeat() -> None:
            nonlocal ticks
            while not stop.is_set():
                ticks += 1
                await asyncio.sleep(0)

        heartbeat_task = asyncio.create_task(heartbeat())
        await executor.execute(block_the_thread, 1)
        stop.set()
        await heartbeat_task

        assert ticks > 1
        await executor.aclose()


class TestAclose:
    """aclose() must own its cleanup responsibility explicitly — no __del__ side effects."""

    async def test_aclose_shuts_down_a_pool_it_built_itself(self) -> None:
        """An executor-built pool must actually be shut down."""
        executor = ThreadPoolExecutor(max_workers=1)

        await executor.aclose()

        assert executor._pool._shutdown

    async def test_aclose_does_not_shut_down_an_adopted_pool(self) -> None:
        """A pool passed in by the caller is owned by the caller, not this executor."""
        adopted = StdlibThreadPoolExecutor(max_workers=1)
        executor = ThreadPoolExecutor(pool=adopted)

        await executor.aclose()

        assert not adopted._shutdown
        adopted.shutdown()
