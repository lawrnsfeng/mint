"""Tests for asynctree with deep trees (stress test)."""

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
)

_DEEP_DEPTH = 50
_DEEP_MAX_DEPTH_SEEN = 49
_FAIL_DEPTH = 10


class SampleItem:
    """Sample item for tree nodes."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


@pytest.mark.asyncio
async def test_deep_tree_depth_50() -> None:
    """Test deep tree (depth=50) does not overflow or hang."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Return one child to build a linear chain."""
        return FetchResult(
            items=[SampleItem(f"item-d{depth}")],
            child_refs=[ChildRef(id=f"d{depth + 1}")],
        )

    config = AsyncTreeExecutorConfig(max_at_once=5, level_timeout=60.0)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_bounded(
        ChildRef(id="d0"),
        depth=_DEEP_DEPTH,
    )

    assert report.total_nodes == _DEEP_DEPTH
    assert report.max_depth_seen == _DEEP_MAX_DEPTH_SEEN
    assert report.failed_nodes == 0
    assert report.timed_out is False


@pytest.mark.asyncio
async def test_deep_tree_path_tracking() -> None:
    """Test full parent path at deep failure has correct ancestor chain."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Fail at a specific depth."""
        if depth == _FAIL_DEPTH:
            msg = "deep fail"
            raise RuntimeError(msg)
        return FetchResult(child_refs=[ChildRef(id=f"n{depth + 1}")])

    config = AsyncTreeExecutorConfig(
        retry_max_attempts=1,
        max_at_once=5,
        level_timeout=30.0,
    )
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(ChildRef(id="n0"), depth=15)

    assert report.failed_nodes == 1
    error = report.errors[0]
    assert error.node_id == f"n{_FAIL_DEPTH}"
    assert error.path == [f"n{i}" for i in range(_FAIL_DEPTH + 1)]
