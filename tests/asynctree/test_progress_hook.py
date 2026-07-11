"""Tests for ProgressHook protocol integration with AsyncTreeExecutor."""

import asyncio

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
    ProgressHook,
)
from mint.asynctree.exceptions import TraversalAbortedError
from mint.asynctree.types import Fetcher, OnNodeError

_FAST_TIMEOUT = 0.3


class SampleItem:
    """Sample tree item."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


class _TrackingHook:
    """Hook implementation that records all calls."""

    def __init__(self) -> None:
        """Initialize empty tracking lists."""
        self.discovered: list[tuple[str, list[str]]] = []
        self.completed: list[str] = []

    def on_children_discovered(
        self,
        parent_id: str,
        child_ids: list[str],
        /,
    ) -> None:
        """Record a children-discovered event."""
        self.discovered.append((parent_id, list(child_ids)))

    def on_node_complete(self, node_id: str, /) -> None:
        """Record a node-complete event."""
        self.completed.append(node_id)


class _RaisingHook:
    """Hook that always raises — must not crash executor."""

    def on_children_discovered(
        self,
        _parent_id: str,
        _child_ids: list[str],
        /,
    ) -> None:
        """Raise unconditionally."""
        msg = "hook exploded on discovered"
        raise RuntimeError(msg)

    def on_node_complete(self, _node_id: str, /) -> None:
        """Raise unconditionally."""
        msg = "hook exploded on complete"
        raise RuntimeError(msg)


def _make_executor(
    fetcher: Fetcher[SampleItem],
    hook: ProgressHook | None = None,
    *,
    timeout: float = 5.0,
    retry_max: int = 1,
    on_node_error: OnNodeError = "skip_mark",
) -> AsyncTreeExecutor[SampleItem]:
    """Build a configured executor with optional hook."""
    cfg = AsyncTreeExecutorConfig(
        max_at_once=10,
        level_timeout=timeout,
        retry_max_attempts=retry_max,
    )
    return AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=cfg,
        on_node_error=on_node_error,
        progress_hook=hook,
    )


@pytest.mark.asyncio
async def test_no_hook_does_not_crash() -> None:
    """Executor with progress_hook=None runs successfully."""

    async def fetcher(
        _ref: ChildRef,
        _depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Leaf fetcher."""
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=None)
    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)
    assert report.total_nodes == 1


@pytest.mark.asyncio
async def test_raising_hook_does_not_crash_executor() -> None:
    """Hook that raises must not propagate — executor finishes normally."""
    hook = _RaisingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Return one child at depth 0, leaf at depth 1."""
        if depth == 0:
            return FetchResult(items=[], child_refs=[ChildRef(id="child-1")])
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook)
    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)
    assert report.total_nodes >= 1


@pytest.mark.asyncio
async def test_hook_not_called_on_empty_children() -> None:
    """on_children_discovered NOT fired when a node returns no children."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        _depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Leaf fetcher — no children."""
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook)
    await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert hook.discovered == []
    assert "root" in hook.completed


@pytest.mark.asyncio
async def test_hook_fires_on_root_expansion() -> None:
    """on_children_discovered fires for the root node when it has children."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Root returns two children; children are leaves."""
        if depth == 0:
            return FetchResult(
                items=[],
                child_refs=[ChildRef(id="a"), ChildRef(id="b")],
            )
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook)
    await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert len(hook.discovered) == 1
    parent_id, child_ids = hook.discovered[0]
    assert parent_id == "root"
    assert set(child_ids) == {"a", "b"}


@pytest.mark.asyncio
async def test_hook_fires_on_leaf_nodes() -> None:
    """on_node_complete fires for every leaf node."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Root returns two children; both are leaves."""
        if depth == 0:
            return FetchResult(
                items=[],
                child_refs=[ChildRef(id="x"), ChildRef(id="y")],
            )
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook)
    await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert set(hook.completed) == {"x", "y"}
    assert "root" not in hook.completed


@pytest.mark.asyncio
async def test_hook_timeout_interaction() -> None:
    """Executor times out cleanly with a hook attached — no crash."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Root returns a slow child that hangs until cancelled."""
        if depth == 0:
            return FetchResult(items=[], child_refs=[ChildRef(id="slow")])
        await asyncio.sleep(10)
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook, timeout=_FAST_TIMEOUT)
    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert report.timed_out is True


@pytest.mark.asyncio
async def test_hook_abort_interaction() -> None:
    """Executor abort policy works with a hook attached."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Root returns a child that fails."""
        if depth == 0:
            return FetchResult(items=[], child_refs=[ChildRef(id="bad")])
        msg = "fetch failed"
        raise ValueError(msg)

    executor = _make_executor(
        fetcher,
        hook=hook,
        retry_max=1,
        on_node_error="abort",
    )
    with pytest.raises(TraversalAbortedError):
        await executor.expand_bounded(ChildRef(id="root"), depth=2)


@pytest.mark.asyncio
async def test_dedup_skipped_nodes_do_not_fire_complete_twice() -> None:
    """Duplicate refs are skipped; on_node_complete fires once for it."""
    hook = _TrackingHook()

    async def fetcher(
        _ref: ChildRef,
        depth: int,
        /,
    ) -> FetchResult[SampleItem]:
        """Root returns same child id twice; second is a dup."""
        if depth == 0:
            return FetchResult(
                items=[],
                child_refs=[ChildRef(id="dup"), ChildRef(id="dup")],
            )
        return FetchResult(items=[], child_refs=[])

    executor = _make_executor(fetcher, hook=hook)
    await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert hook.completed.count("dup") == 1
