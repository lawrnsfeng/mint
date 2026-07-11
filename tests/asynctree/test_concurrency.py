"""Tests for asynctree concurrency module."""

import asyncio
import time

import pytest

from mint.asynctree.concurrency import ConcurrencyGate

_MAX_CONCURRENT = 2
_RATE_MIN_ELAPSED = 0.35
_RATE_MAX_ELAPSED = 0.7
_NO_RATE_MAX_ELAPSED = 0.2
_BURST_FIRST_MAX = 0.1
_BURST_GAP_MIN = 0.2
_BURST_COUNT = 3


def test_concurrency_gate_invalid_max_at_once() -> None:
    """Test ConcurrencyGate rejects invalid max_at_once."""
    with pytest.raises(ValueError, match="max_at_once must be > 0"):
        ConcurrencyGate(max_at_once=0)

    with pytest.raises(ValueError, match="max_at_once must be > 0"):
        ConcurrencyGate(max_at_once=-1)


def test_concurrency_gate_invalid_max_per_second() -> None:
    """Test ConcurrencyGate rejects negative max_per_second."""
    with pytest.raises(ValueError, match="max_per_second must be >= 0"):
        ConcurrencyGate(max_at_once=1, max_per_second=-1.0)


@pytest.mark.asyncio
async def test_semaphore_limits_concurrent() -> None:
    """Test semaphore limits concurrent operations."""
    gate = ConcurrencyGate(max_at_once=2)
    active_count = 0
    max_active = 0

    async def task() -> None:
        nonlocal active_count, max_active
        async with gate.acquire():
            active_count += 1
            max_active = max(max_active, active_count)
            await asyncio.sleep(0.1)
            active_count -= 1

    await asyncio.gather(*(task() for _ in range(5)))
    assert max_active == _MAX_CONCURRENT


@pytest.mark.asyncio
async def test_rate_limit_paces_operations() -> None:
    """Test rate limit paces operations per second."""
    gate = ConcurrencyGate(max_at_once=10, max_per_second=5.0)
    start_time = time.monotonic()

    async def task() -> None:
        async with gate.acquire():
            pass

    await asyncio.gather(*(task() for _ in range(3)))

    elapsed = time.monotonic() - start_time
    assert elapsed >= _RATE_MIN_ELAPSED
    assert elapsed <= _RATE_MAX_ELAPSED


@pytest.mark.asyncio
async def test_no_rate_limit() -> None:
    """Test gate with no rate limit (max_per_second=0)."""
    gate = ConcurrencyGate(max_at_once=5, max_per_second=0.0)
    start_time = time.monotonic()

    async def task() -> None:
        async with gate.acquire():
            await asyncio.sleep(0.01)

    await asyncio.gather(*(task() for _ in range(5)))

    elapsed = time.monotonic() - start_time
    assert elapsed < _NO_RATE_MAX_ELAPSED


@pytest.mark.asyncio
async def test_combined_limits() -> None:
    """Test semaphore and rate limit work together."""
    gate = ConcurrencyGate(max_at_once=2, max_per_second=10.0)
    active_count = 0
    max_active = 0

    async def task() -> None:
        nonlocal active_count, max_active
        async with gate.acquire():
            active_count += 1
            max_active = max(max_active, active_count)
            await asyncio.sleep(0.05)
            active_count -= 1

    await asyncio.gather(*(task() for _ in range(4)))
    assert max_active <= _MAX_CONCURRENT


@pytest.mark.asyncio
async def test_burst_respects_rate() -> None:
    """Test burst of tasks respects rate limit spacing."""
    gate = ConcurrencyGate(max_at_once=100, max_per_second=4.0)
    start_time = time.monotonic()
    timestamps: list[float] = []

    async def task() -> None:
        async with gate.acquire():
            timestamps.append(time.monotonic() - start_time)

    await asyncio.gather(*(task() for _ in range(3)))

    assert timestamps[0] < _BURST_FIRST_MAX
    if len(timestamps) >= _MAX_CONCURRENT:
        assert timestamps[1] - timestamps[0] >= _BURST_GAP_MIN
    if len(timestamps) >= _BURST_COUNT:
        assert timestamps[_MAX_CONCURRENT] - timestamps[1] >= _BURST_GAP_MIN
