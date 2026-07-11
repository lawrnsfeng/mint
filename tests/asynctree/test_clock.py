"""Tests for asynctree clock module."""

import asyncio

import pytest

from mint.asynctree.clock import DynamicClock, StaticClock

_STATIC_TOTAL = 10.0
_ELAPSED_MIN_MS = 100
_DYNAMIC_BASE = 30.0


def test_static_clock_invalid_total() -> None:
    """Test StaticClock rejects invalid total_seconds."""
    with pytest.raises(ValueError, match="total_seconds must be > 0"):
        StaticClock(total_seconds=0)
    with pytest.raises(ValueError, match="total_seconds must be > 0"):
        StaticClock(total_seconds=-1.0)


def test_static_clock_initial_state() -> None:
    """Test StaticClock initial state."""
    clock = StaticClock(total_seconds=10.0)
    assert clock.is_cancelled is False
    assert clock.elapsed_ms == 0
    assert clock.remaining_seconds() == _STATIC_TOTAL


def test_static_clock_start_tracks_time() -> None:
    """Test StaticClock tracks elapsed time after start."""
    clock = StaticClock(total_seconds=_STATIC_TOTAL)
    clock.start()
    assert clock.elapsed_ms >= 0
    assert clock.remaining_seconds() <= _STATIC_TOTAL


def test_static_clock_cancel() -> None:
    """Test StaticClock cancel marks as cancelled."""
    clock = StaticClock(total_seconds=5.0)
    clock.cancel()
    assert clock.is_cancelled is True


@pytest.mark.asyncio
async def test_static_clock_wait_cancels_after_timeout() -> None:
    """Test StaticClock wait_for_timeout cancels after duration."""
    clock = StaticClock(total_seconds=0.1)
    clock.start()
    await clock.wait_for_timeout()
    assert clock.is_cancelled is True
    assert clock.elapsed_ms >= _ELAPSED_MIN_MS


@pytest.mark.asyncio
async def test_static_clock_remaining_decreases() -> None:
    """Test StaticClock remaining_seconds decreases over time."""
    clock = StaticClock(total_seconds=1.0)
    clock.start()
    initial = clock.remaining_seconds()
    await asyncio.sleep(0.1)
    assert clock.remaining_seconds() < initial


def test_dynamic_clock_invalid_base() -> None:
    """Test DynamicClock rejects invalid base_seconds."""
    with pytest.raises(ValueError, match="base_seconds must be > 0"):
        DynamicClock(base_seconds=0)


def test_dynamic_clock_invalid_per_level() -> None:
    """Test DynamicClock rejects invalid per_level_seconds."""
    with pytest.raises(ValueError, match="per_level_seconds must be >= 0"):
        DynamicClock(base_seconds=10.0, per_level_seconds=-1.0)


def test_dynamic_clock_initial_state() -> None:
    """Test DynamicClock initial state."""
    clock = DynamicClock(base_seconds=30.0, per_level_seconds=15.0)
    assert clock.is_cancelled is False
    assert clock.elapsed_ms == 0
    assert clock.remaining_seconds() == _DYNAMIC_BASE


@pytest.mark.asyncio
async def test_dynamic_clock_extends_on_new_depth() -> None:
    """Test DynamicClock extends budget on new max depth."""
    clock = DynamicClock(base_seconds=30.0, per_level_seconds=10.0)
    clock.start()
    initial = clock.remaining_seconds()
    await clock.notify_depth(1)
    assert clock.remaining_seconds() > initial
    after_1 = clock.remaining_seconds()
    await clock.notify_depth(2)
    assert clock.remaining_seconds() > after_1


@pytest.mark.asyncio
async def test_dynamic_clock_no_extend_on_same_depth() -> None:
    """Test DynamicClock does not extend for repeated same depth."""
    clock = DynamicClock(base_seconds=30.0, per_level_seconds=10.0)
    clock.start()
    await clock.notify_depth(2)
    budget_after = clock.remaining_seconds()
    await asyncio.sleep(0.05)
    await clock.notify_depth(2)
    assert clock.remaining_seconds() < budget_after


@pytest.mark.asyncio
async def test_dynamic_clock_no_extend_on_lower_depth() -> None:
    """Test DynamicClock does not extend for lower depth."""
    clock = DynamicClock(base_seconds=30.0, per_level_seconds=10.0)
    clock.start()
    await clock.notify_depth(3)
    budget_after_3 = clock.remaining_seconds()
    await asyncio.sleep(0.05)
    await clock.notify_depth(1)
    assert clock.remaining_seconds() < budget_after_3


@pytest.mark.asyncio
async def test_dynamic_clock_wait_checks_budget() -> None:
    """Test DynamicClock wait_for_timeout respects growing budget."""
    clock = DynamicClock(base_seconds=0.2, per_level_seconds=0.1)
    clock.start()
    timeout_task = asyncio.create_task(clock.wait_for_timeout())
    await asyncio.sleep(0.05)
    await clock.notify_depth(1)
    assert not timeout_task.done()
    await asyncio.wait_for(timeout_task, timeout=0.5)
    assert clock.is_cancelled is True
