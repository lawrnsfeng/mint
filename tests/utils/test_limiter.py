"""Tests for the ConcurrencyLimiter utility."""

import asyncio

import pytest

from mint.utils.exc import InvalidConcurrencyLimitError
from mint.utils.limiter import ConcurrencyLimiter


@pytest.mark.asyncio
async def test_limiter_limits_concurrent_operations() -> None:
    """Test that limiter properly limits concurrent operations."""
    limiter = ConcurrencyLimiter(max_concurrent=3)
    active_count = 0
    max_active = 0
    lock = asyncio.Lock()

    async def tracked_operation(idx: int) -> int:
        nonlocal active_count, max_active
        async with limiter:
            async with lock:
                active_count += 1
                max_active = max(max_active, active_count)
            await asyncio.sleep(0.01)
            async with lock:
                active_count -= 1
        return idx

    results = await asyncio.gather(*[tracked_operation(i) for i in range(20)])

    assert len(results) == 20
    assert max_active <= 3


@pytest.mark.asyncio
async def test_limiter_decorator_limits_method_calls() -> None:
    """Test that the limit decorator properly limits method calls."""
    limiter = ConcurrencyLimiter(max_concurrent=2)
    active_count = 0
    max_active = 0
    lock = asyncio.Lock()

    class Service:
        @limiter.limit
        async def operation(self, idx: int) -> int:
            nonlocal active_count, max_active
            async with lock:
                active_count += 1
                max_active = max(max_active, active_count)
            await asyncio.sleep(0.01)
            async with lock:
                active_count -= 1
            return idx

    service = Service()
    results = await asyncio.gather(*[service.operation(i) for i in range(15)])

    assert len(results) == 15
    assert max_active <= 2


@pytest.mark.asyncio
async def test_limiter_nested_context_shares_acquisition() -> None:
    """Test that nested contexts share the same acquisition."""
    limiter = ConcurrencyLimiter(max_concurrent=1)
    call_count = 0

    async def inner_operation() -> str:
        nonlocal call_count
        async with limiter:
            call_count += 1
            return "inner"

    async def outer_operation() -> str:
        async with limiter:
            result = await inner_operation()
            return f"outer-{result}"

    result = await outer_operation()

    assert result == "outer-inner"
    assert call_count == 1


@pytest.mark.asyncio
async def test_limiter_decorator_nested_calls_share_acquisition() -> None:
    """Test that decorated nested method calls share the same acquisition."""
    limiter = ConcurrencyLimiter(max_concurrent=1)
    acquisition_count = 0

    class Service:
        @limiter.limit
        async def outer(self) -> str:
            nonlocal acquisition_count
            acquisition_count += 1
            return await self.inner()

        @limiter.limit
        async def inner(self) -> str:
            nonlocal acquisition_count
            acquisition_count += 1
            return "done"

    service = Service()
    result = await service.outer()

    assert result == "done"
    assert acquisition_count == 2


@pytest.mark.asyncio
async def test_limiter_default_max_concurrent() -> None:
    """Test that default max_concurrent is DEFAULT_MAX_CONCURRENT."""
    limiter = ConcurrencyLimiter()

    assert limiter.max_concurrent == ConcurrencyLimiter.DEFAULT_MAX_CONCURRENT


@pytest.mark.asyncio
async def test_limiter_custom_max_concurrent() -> None:
    """Test that custom max_concurrent is used."""
    limiter = ConcurrencyLimiter(max_concurrent=42)

    assert limiter.max_concurrent == 42


@pytest.mark.parametrize("max_concurrent", [0, -1, -10])
def test_limiter_rejects_non_positive_max_concurrent(
    max_concurrent: int,
) -> None:
    """Test that max_concurrent<=0 raises InvalidConcurrencyLimitError.

    Args:
        max_concurrent: A non-positive value that must be rejected.

    """
    with pytest.raises(InvalidConcurrencyLimitError, match="max_concurrent"):
        ConcurrencyLimiter(max_concurrent=max_concurrent)


@pytest.mark.asyncio
async def test_limiter_releases_on_exception() -> None:
    """Test that limiter releases semaphore even when exception occurs."""
    limiter = ConcurrencyLimiter(max_concurrent=1)

    async def failing_operation() -> None:
        async with limiter:
            raise ValueError("test error")

    with pytest.raises(ValueError, match="test error"):
        await failing_operation()

    async with limiter:
        pass


@pytest.mark.asyncio
async def test_limiter_decorator_releases_on_exception() -> None:
    """Test that decorated method releases semaphore on exception."""
    limiter = ConcurrencyLimiter(max_concurrent=1)

    class Service:
        @limiter.limit
        async def failing_method(self) -> None:
            raise ValueError("test error")

        @limiter.limit
        async def success_method(self) -> str:
            return "success"

    service = Service()

    with pytest.raises(ValueError, match="test error"):
        await service.failing_method()

    result = await service.success_method()
    assert result == "success"


@pytest.mark.asyncio
async def test_limiter_reentrancy_correctness() -> None:
    """Test that the semaphore stays held until the outermost context exits."""
    limiter = ConcurrencyLimiter(max_concurrent=1)
    critical_section_active = False

    async def inner() -> None:
        async with limiter:
            pass

    async def outer() -> None:
        nonlocal critical_section_active
        async with limiter:
            critical_section_active = True
            await inner()
            await asyncio.sleep(0.05)
            critical_section_active = False

    async def intruder() -> None:
        await asyncio.sleep(0.01)
        async with limiter:
            assert not critical_section_active, (
                "Intruder acquired lock while outer was still active"
            )

    await asyncio.gather(outer(), intruder())


@pytest.mark.asyncio
async def test_limiter_deep_nesting_releases_only_at_outermost() -> None:
    """Test that 3 levels of nesting release only when the outermost exits."""
    limiter = ConcurrencyLimiter(max_concurrent=1)
    outermost_active = False

    async def level_three() -> None:
        async with limiter:
            pass

    async def level_two() -> None:
        async with limiter:
            await level_three()

    async def level_one() -> None:
        nonlocal outermost_active
        async with limiter:
            outermost_active = True
            await level_two()
            await asyncio.sleep(0.05)
            outermost_active = False

    async def prober() -> None:
        await asyncio.sleep(0.01)
        async with limiter:
            assert not outermost_active, "Prober acquired lock before outermost level released it"

    await asyncio.gather(level_one(), prober())


@pytest.mark.asyncio
async def test_limiter_nested_context_releases_on_inner_exception() -> None:
    """A failing nested block still frees the limiter for later use."""
    limiter = ConcurrencyLimiter(max_concurrent=1)

    async def inner_failing() -> None:
        async with limiter:
            raise ValueError("inner failure")

    async def outer() -> None:
        async with limiter:
            await inner_failing()

    with pytest.raises(ValueError, match="inner failure"):
        await outer()

    async with limiter:
        pass


@pytest.mark.asyncio
async def test_limiter_decorator_and_context_manager_share_depth() -> None:
    """The decorator and bare context manager share one depth counter."""
    limiter = ConcurrencyLimiter(max_concurrent=1)
    critical_section_active = False

    class Service:
        @limiter.limit
        async def outer(self) -> None:
            nonlocal critical_section_active
            critical_section_active = True
            async with limiter:
                pass
            await asyncio.sleep(0.05)
            critical_section_active = False

    service = Service()

    async def intruder() -> None:
        await asyncio.sleep(0.01)
        async with limiter:
            assert not critical_section_active, (
                "Intruder acquired lock while decorated outer call was still active"
            )

    await asyncio.gather(service.outer(), intruder())


class TestEventLoopAffinity:
    """`asyncio.Semaphore` binds to the loop that first contends on it.

    A limiter held at module scope has to survive one loop ending, or it makes
    the object holding it unusable for the rest of the process.
    """

    def test_limiter_works_across_sequential_loops(self) -> None:
        """A single eagerly-built semaphore would raise on the second loop."""
        limiter = ConcurrencyLimiter(2)

        async def body() -> list[int]:
            async def op(i: int) -> int:
                async with limiter:
                    await asyncio.sleep(0.005)
                    return i

            return await asyncio.gather(*[op(i) for i in range(6)])

        for _ in range(3):
            assert len(asyncio.run(body())) == 6

    def test_semaphore_map_does_not_grow_across_loops(self) -> None:
        """A bound semaphore references its own loop, defeating the weak key.

        Closed loops are pruned explicitly, or the map grows for the life of a
        process that cycles event loops.
        """
        limiter = ConcurrencyLimiter(2)

        async def body() -> None:
            async def op() -> None:
                async with limiter:
                    await asyncio.sleep(0.005)

            await asyncio.gather(*[op() for _ in range(4)])

        for _ in range(4):
            asyncio.run(body())
            assert limiter.bound_loop_count == 1

    def test_decorator_form_also_survives_a_new_loop(self) -> None:
        """`limit` acquires and releases the same per-loop semaphore."""
        limiter = ConcurrencyLimiter(2)

        class Worker:
            @limiter.limit
            async def run(self) -> int:
                await asyncio.sleep(0.005)
                return 1

        async def body() -> int:
            worker = Worker()
            return sum(await asyncio.gather(*[worker.run() for _ in range(4)]))

        assert asyncio.run(body()) == 4
        assert asyncio.run(body()) == 4
