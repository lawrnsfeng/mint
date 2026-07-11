"""Tests targeting coverage gaps in asynctree."""

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
)
from mint.asynctree.clock import DynamicClock
from mint.asynctree.graph_dump import _serialize_node, dump_execution_graph
from mint.asynctree.models import TreeNode
from mint.asynctree.retry import RetryConfig, _HookedWait, build_retrying

_FLAKY_SUCCEED_ON = 3
_RETRY_MIN_WAIT = 0.01
_HOOK_WAIT_RETURN = 5.0
_HOOK_WAIT_CAPPED = 60.0
_HOOK_WAIT_OVER_CAP = 100.0
_BASE_WAIT_DEFAULT = 1.5


class SampleItem:
    """Sample item for tree nodes."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


@pytest.mark.asyncio
async def test_cancellation_marks_node_as_partial() -> None:
    """Test timeout causes in-flight tasks to be marked partial/cancelled."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Root spawns a slow child that will be cancelled by timeout."""
        if depth == 0:
            return FetchResult(child_refs=[ChildRef(id="slow-child")])
        await asyncio.sleep(5.0)
        return FetchResult(items=[SampleItem("never")])

    config = AsyncTreeExecutorConfig(max_at_once=5, level_timeout=0.1)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert report.timed_out is True
    assert report.cancelled_nodes >= 1
    cancelled_errors = [e for e in report.errors if e.kind == "cancelled"]
    assert len(cancelled_errors) >= 1
    assert cancelled_errors[0].node_id == "slow-child"

    slow_child = tree.children[0] if tree.children else None
    if slow_child:
        assert slow_child.partial is True
        assert slow_child.error is not None
        assert slow_child.error.kind == "cancelled"


@pytest.mark.asyncio
async def test_early_cancel_check_in_fetch() -> None:
    """Test that fetch checks clock cancellation before starting."""
    call_count = 0

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Root spawns children; semaphore forces some to wait for clock."""
        nonlocal call_count
        call_count += 1
        if depth == 0:
            return FetchResult(
                child_refs=[ChildRef(id=f"child-{i}") for i in range(20)],
            )
        await asyncio.sleep(0.5)
        return FetchResult()

    config = AsyncTreeExecutorConfig(max_at_once=1, level_timeout=0.1)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert report.timed_out is True


@pytest.mark.asyncio
async def test_short_traceback_no_truncation() -> None:
    """Test traceback that fits within max_lines is not truncated."""

    async def fetcher(_ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Single-line raise for short traceback."""
        msg = "short"
        raise ValueError(msg)

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=1)

    assert report.failed_nodes == 1
    tb = report.errors[0].traceback
    assert tb is not None
    assert "truncated" not in tb


def test_serialize_node_partial_status() -> None:
    """Test serialization of a partial node (no error, but partial=True)."""
    node: TreeNode[SampleItem] = TreeNode(
        ref=ChildRef(id="partial-node"),
        partial=True,
        depth=1,
    )
    result = _serialize_node(node)
    assert result["status"] == "partial"


@pytest.mark.asyncio
async def test_retry_after_hook_called() -> None:
    """Test retry_after_hook callback is invoked with exception."""
    hook_calls: list[BaseException] = []

    def hook(exc: BaseException) -> float | None:
        hook_calls.append(exc)
        return _RETRY_MIN_WAIT

    config = RetryConfig(
        max_attempts=_FLAKY_SUCCEED_ON,
        min_wait=_RETRY_MIN_WAIT,
        max_wait=0.02,
        retry_after_hook=hook,
    )
    retrying = build_retrying(config)
    call_count = 0

    async def flaky() -> str:
        nonlocal call_count
        call_count += 1
        if call_count < _FLAKY_SUCCEED_ON:
            msg = "retry me"
            raise RuntimeError(msg)
        return "done"

    result = None
    async for attempt in retrying:
        with attempt:
            result = await flaky()

    assert result == "done"
    assert len(hook_calls) >= 1
    assert all(isinstance(e, RuntimeError) for e in hook_calls)


def test_hooked_wait_no_outcome() -> None:
    """Test _HookedWait falls back to base when outcome is None."""
    base = MagicMock(return_value=_BASE_WAIT_DEFAULT)
    hook = MagicMock(return_value=5.0)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome = None

    result = hw(state)
    assert result == _BASE_WAIT_DEFAULT
    hook.assert_not_called()


def test_hooked_wait_no_exception() -> None:
    """Test _HookedWait falls back to base when outcome has no exception."""
    base = MagicMock(return_value=_BASE_WAIT_DEFAULT)
    hook = MagicMock(return_value=5.0)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome.exception.return_value = None

    result = hw(state)
    assert result == _BASE_WAIT_DEFAULT
    hook.assert_not_called()


def test_hooked_wait_hook_returns_none() -> None:
    """Test _HookedWait falls back to base when hook returns None."""
    base = MagicMock(return_value=_BASE_WAIT_DEFAULT)
    hook = MagicMock(return_value=None)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome.exception.return_value = RuntimeError("err")

    result = hw(state)
    assert result == _BASE_WAIT_DEFAULT


def test_hooked_wait_hook_returns_zero() -> None:
    """Test _HookedWait falls back to base when hook returns zero."""
    base = MagicMock(return_value=_BASE_WAIT_DEFAULT)
    hook = MagicMock(return_value=0.0)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome.exception.return_value = RuntimeError("err")

    result = hw(state)
    assert result == _BASE_WAIT_DEFAULT


def test_hooked_wait_applies_hook_wait() -> None:
    """Test _HookedWait returns hook's value when positive."""
    base = MagicMock(return_value=1.5)
    hook = MagicMock(return_value=_HOOK_WAIT_RETURN)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome.exception.return_value = RuntimeError("err")

    result = hw(state)
    assert result == _HOOK_WAIT_RETURN
    base.assert_not_called()


def test_hooked_wait_caps_at_60_seconds() -> None:
    """Test _HookedWait caps hook return value at 60 seconds."""
    base = MagicMock(return_value=1.5)
    hook = MagicMock(return_value=_HOOK_WAIT_OVER_CAP)
    hw = _HookedWait(base, hook)

    state = MagicMock()
    state.outcome.exception.return_value = RuntimeError("err")

    result = hw(state)
    assert result == _HOOK_WAIT_CAPPED
    base.assert_not_called()


@pytest.mark.asyncio
async def test_dynamic_clock_cancel_stops_wait() -> None:
    """Test DynamicClock wait_for_timeout exits when cancelled externally."""
    clock = DynamicClock(base_seconds=10.0, per_level_seconds=5.0)
    clock.start()

    async def cancel_after_delay() -> None:
        await asyncio.sleep(0.1)
        clock.cancel()

    _cancel_task = asyncio.create_task(cancel_after_delay())
    await asyncio.wait_for(clock.wait_for_timeout(), timeout=2.0)
    await _cancel_task
    assert clock.is_cancelled is True


def _deep_k() -> None:
    """Leaf of deep call chain."""
    msg = "deep"
    raise RuntimeError(msg)


def _deep_j() -> None:
    """Deep call chain step j."""
    _deep_k()


def _deep_i() -> None:
    """Deep call chain step i."""
    _deep_j()


def _deep_h() -> None:
    """Deep call chain step h."""
    _deep_i()


def _deep_g() -> None:
    """Deep call chain step g."""
    _deep_h()


def _deep_f() -> None:
    """Deep call chain step f."""
    _deep_g()


def _deep_e() -> None:
    """Deep call chain step e."""
    _deep_f()


def _deep_d() -> None:
    """Deep call chain step d."""
    _deep_e()


def _deep_c() -> None:
    """Deep call chain step c."""
    _deep_d()


def _deep_b() -> None:
    """Deep call chain step b."""
    _deep_c()


def _deep_a() -> None:
    """Entry point of deep call chain."""
    _deep_b()


@pytest.mark.asyncio
async def test_long_traceback_is_truncated() -> None:
    """Test traceback exceeding max_lines is truncated."""

    async def fetcher(_ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Invoke a deep call chain to produce a long traceback."""
        _deep_a()
        return FetchResult()

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=1)

    assert report.failed_nodes == 1
    tb = report.errors[0].traceback
    assert tb is not None
    assert "truncated" in tb


@pytest.mark.asyncio
async def test_dump_after_timeout_execution(tmp_path: Path) -> None:
    """Test graph dump after a timed-out execution has partial nodes."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Slow fetcher to trigger timeout."""
        if depth == 0:
            return FetchResult(child_refs=[ChildRef(id="slow")])
        await asyncio.sleep(5.0)
        return FetchResult()

    config = AsyncTreeExecutorConfig(max_at_once=5, level_timeout=0.1)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    output_path = tmp_path / "timeout_graph.json"
    dump_execution_graph(tree, report, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["report"]["timed_out"] is True
