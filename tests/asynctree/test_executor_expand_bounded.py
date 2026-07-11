"""Tests for asynctree executor bounded expansion."""

import asyncio
import time

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
)

_SINGLE_LEVEL_CHILDREN = 2
_SINGLE_LEVEL_NODES = 3
_ASYM_ROOT_CHILDREN = 2
_ASYM_TOTAL_NODES = 6
_ASYM_MAX_DEPTH = 2
_ASYM_MAX_SIBLING_GAP = 0.2
_DEPTH_LIMIT_NODES = 3
_DEPTH_LIMIT_MAX_DEPTH = 2
_WIDE_TREE_MAX_ACTIVE = 4
_WIDE_TREE_TOTAL = 11
_RATE_MIN_ELAPSED = 0.5
_RATE_TOTAL_NODES = 4
_ZERO_CHILDREN_NODES = 3
_ZERO_CHILDREN_MAX_DEPTH = 1


class SampleItem:
    """Sample item for tree nodes."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


@pytest.mark.asyncio
async def test_expand_bounded_empty_tree() -> None:
    """Test bounded expansion with empty tree (no children)."""

    async def fetcher(ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Return items but no children."""
        return FetchResult(
            items=[SampleItem(f"file-at-{ref.id}")],
            child_refs=[],
        )

    config = AsyncTreeExecutorConfig(max_at_once=5, level_timeout=1.0)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    root_ref = ChildRef(id="root")
    tree, report = await executor.expand_bounded(root_ref, depth=3)

    assert tree.ref == root_ref
    assert tree.result is not None
    assert tree.result.items[0].name == "file-at-root"
    assert len(tree.children) == 0
    assert report.total_nodes == 1
    assert report.max_depth_seen == 0


@pytest.mark.asyncio
async def test_expand_bounded_single_level() -> None:
    """Test bounded expansion with single level of children."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Return children at depth 0 only."""
        if depth == 0:
            return FetchResult(
                items=[SampleItem("root-file")],
                child_refs=[ChildRef(id="child-1"), ChildRef(id="child-2")],
            )
        return FetchResult(items=[SampleItem(f"file-{ref.id}")])

    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher)
    tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert len(tree.children) == _SINGLE_LEVEL_CHILDREN
    assert report.total_nodes == _SINGLE_LEVEL_NODES
    assert report.max_depth_seen == 1


@pytest.mark.asyncio
async def test_expand_bounded_asymmetric_tree() -> None:
    """Test async spawn ordering in an asymmetric tree."""
    fetch_times: dict[str, float] = {}

    async def fetcher(ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Asymmetric tree with controlled delays."""
        start = time.monotonic()
        structure: dict[str, tuple[float, list[str], list[str]]] = {
            "D1": (0.01, ["D2", "D3"], ["F1"]),
            "D2": (0.02, ["D4"], ["F2"]),
            "D3": (0.15, ["D5", "D6"], ["F3"]),
            "D4": (0.01, [], ["F4"]),
            "D5": (0.01, [], ["F5"]),
            "D6": (0.01, [], ["F6"]),
        }
        fetch_times[ref.id] = start
        if ref.id in structure:
            delay, folders, files = structure[ref.id]
            await asyncio.sleep(delay)
            return FetchResult(
                items=[SampleItem(f) for f in files],
                child_refs=[ChildRef(id=fid) for fid in folders],
            )
        return FetchResult()

    config = AsyncTreeExecutorConfig(max_at_once=10, level_timeout=5.0)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    tree, report = await executor.expand_bounded(ChildRef(id="D1"), depth=5)

    assert len(tree.children) == _ASYM_ROOT_CHILDREN
    assert report.total_nodes == _ASYM_TOTAL_NODES
    assert report.max_depth_seen == _ASYM_MAX_DEPTH

    d5_d6_gap = abs(fetch_times["D5"] - fetch_times["D6"])
    assert d5_d6_gap < _ASYM_MAX_SIBLING_GAP


@pytest.mark.asyncio
async def test_expand_bounded_depth_limit() -> None:
    """Test bounded expansion respects depth limit."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Return a single child at each level."""
        return FetchResult(
            items=[SampleItem(f"item-d{depth}")],
            child_refs=[ChildRef(id=f"node-d{depth + 1}")],
        )

    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher)
    tree, report = await executor.expand_bounded(
        ChildRef(id="node-d0"),
        depth=3,
    )

    assert report.total_nodes == _DEPTH_LIMIT_NODES
    assert report.max_depth_seen == _DEPTH_LIMIT_MAX_DEPTH
    assert len(tree.children[0].children[0].children) == 0


@pytest.mark.asyncio
async def test_expand_bounded_wide_tree_concurrency() -> None:
    """Test wide tree respects max_at_once."""
    active_count = 0
    max_active = 0

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Wide tree fetcher tracking concurrency."""
        nonlocal active_count, max_active
        active_count += 1
        max_active = max(max_active, active_count)
        await asyncio.sleep(0.05)
        active_count -= 1
        if depth == 0:
            return FetchResult(
                child_refs=[ChildRef(id=f"c-{i}") for i in range(10)],
            )
        return FetchResult()

    config = AsyncTreeExecutorConfig(max_at_once=3, level_timeout=5.0)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert max_active <= _WIDE_TREE_MAX_ACTIVE
    assert report.total_nodes == _WIDE_TREE_TOTAL


@pytest.mark.asyncio
async def test_expand_bounded_rate_limit() -> None:
    """Test bounded expansion respects rate limit."""
    start_time = time.monotonic()

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Return 3 children at root."""
        if depth == 0:
            return FetchResult(
                child_refs=[ChildRef(id=f"c{i}") for i in range(3)],
            )
        return FetchResult()

    config = AsyncTreeExecutorConfig(
        max_at_once=10,
        max_per_second=5.0,
        level_timeout=5.0,
    )
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    elapsed = time.monotonic() - start_time
    assert elapsed >= _RATE_MIN_ELAPSED
    assert report.total_nodes == _RATE_TOTAL_NODES


@pytest.mark.asyncio
async def test_expand_bounded_invalid_depth() -> None:
    """Test expand_bounded rejects invalid depth."""

    async def fetcher(_ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Never called."""
        return FetchResult()

    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher)
    with pytest.raises(ValueError, match="depth must be >= 1"):
        await executor.expand_bounded(ChildRef(id="root"), depth=0)


@pytest.mark.asyncio
async def test_expand_bounded_zero_children_mid_tree() -> None:
    """Test node with items but no child_refs stops expansion."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Root has children, but children have no sub-children."""
        if depth == 0:
            return FetchResult(
                items=[SampleItem("root-item")],
                child_refs=[ChildRef(id="leaf-1"), ChildRef(id="leaf-2")],
            )
        return FetchResult(items=[SampleItem(f"leaf-item-{ref.id}")])

    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher)
    tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=5)

    assert report.total_nodes == _ZERO_CHILDREN_NODES
    assert report.max_depth_seen == _ZERO_CHILDREN_MAX_DEPTH
    for child in tree.children:
        assert child.result is not None
        assert len(child.children) == 0
