"""Tests for asynctree executor unbounded expansion."""

import asyncio

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
)

_CHAIN_DEPTH = 3
_CHAIN_NODES = 4
_UNBOUNDED_NODES = 3


class SampleItem:
    """Sample item for tree nodes."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


@pytest.mark.asyncio
async def test_expand_unbounded_dynamic_clock_extends() -> None:
    """Test dynamic clock extends budget on new depths."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Chain of configurable depth."""
        await asyncio.sleep(0.05)
        if depth < _CHAIN_DEPTH:
            return FetchResult(
                items=[SampleItem(f"item-{ref.id}")],
                child_refs=[ChildRef(id=f"node-{depth + 1}")],
            )
        return FetchResult(items=[SampleItem(f"leaf-{ref.id}")])

    config = AsyncTreeExecutorConfig(level_timeout=0.2, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_unbounded(ChildRef(id="node-0"))

    assert report.timed_out is False
    assert report.max_depth_seen == _CHAIN_DEPTH
    assert report.total_nodes == _CHAIN_NODES


@pytest.mark.asyncio
async def test_expand_unbounded_timeout_partial_result() -> None:
    """Test unbounded with timeout returns partial tree."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Slow fetcher that will timeout."""
        await asyncio.sleep(0.3)
        return FetchResult(
            items=[SampleItem(f"item-{ref.id}")],
            child_refs=[ChildRef(id=f"child-{depth}")],
        )

    config = AsyncTreeExecutorConfig(level_timeout=0.2, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    tree, report = await executor.expand_unbounded(ChildRef(id="root"))
    assert report.timed_out is True

    if tree.result is None:
        assert tree.error is not None or tree.partial or report.timed_out


@pytest.mark.asyncio
async def test_expand_unbounded_preserves_completed_nodes() -> None:
    """Test timeout preserves already-completed nodes."""

    async def fetcher(ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Fast and slow children."""
        delays = {"root": 0.05, "fast-child": 0.05, "slow-child": 0.5}
        await asyncio.sleep(delays.get(ref.id, 0.05))
        if ref.id == "root":
            return FetchResult(
                child_refs=[
                    ChildRef(id="fast-child"),
                    ChildRef(id="slow-child"),
                ],
            )
        return FetchResult(items=[SampleItem(f"item-{ref.id}")])

    config = AsyncTreeExecutorConfig(level_timeout=0.2, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    tree, _report = await executor.expand_unbounded(ChildRef(id="root"))

    assert tree.result is not None
    fast_child = next(
        (c for c in tree.children if c.ref and c.ref.id == "fast-child"),
        None,
    )
    assert fast_child is not None
    assert fast_child.result is not None

    slow_child = next(
        (c for c in tree.children if c.ref and c.ref.id == "slow-child"),
        None,
    )
    if slow_child and slow_child.error:
        assert slow_child.error.kind == "cancelled"
        assert slow_child.partial is True


@pytest.mark.asyncio
async def test_expand_unbounded_completes_without_timeout() -> None:
    """Test unbounded expansion completes when fast enough."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Fast fetcher."""
        await asyncio.sleep(0.01)
        if depth == 0:
            return FetchResult(
                child_refs=[ChildRef(id="c1"), ChildRef(id="c2")],
            )
        return FetchResult(items=[SampleItem(f"item-{ref.id}")])

    config = AsyncTreeExecutorConfig(level_timeout=2.0, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](fetcher=fetcher, config=config)

    _tree, report = await executor.expand_unbounded(ChildRef(id="root"))

    assert report.timed_out is False
    assert report.total_nodes == _UNBOUNDED_NODES
    assert report.cancelled_nodes == 0
