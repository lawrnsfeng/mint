"""Tests for asynctree executor failure modes."""

import asyncio
import time

import pytest

from mint.asynctree import (
    AsyncTreeExecutor,
    AsyncTreeExecutorConfig,
    ChildRef,
    FetchResult,
    TraversalAbortedError,
)

_SKIP_MARK_TOTAL = 3
_ABORT_MAX_ELAPSED = 2.0
_FAIL_DEPTH = 2
_RETRY_ATTEMPTS = 3


class SampleItem:
    """Sample item for tree nodes."""

    def __init__(self, name: str) -> None:
        """Initialize SampleItem."""
        self.name = name


@pytest.mark.asyncio
async def test_skip_mark_continues_siblings() -> None:
    """Test on_node_error='skip_mark' continues with error marker."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Fail on bad-child."""
        if ref.id == "bad-child":
            msg = "Simulated failure"
            raise RuntimeError(msg)
        if depth == 0:
            return FetchResult(
                child_refs=[
                    ChildRef(id="good-1"),
                    ChildRef(id="bad-child"),
                    ChildRef(id="good-2"),
                ],
            )
        return FetchResult(items=[SampleItem(f"item-{ref.id}")])

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert report.total_nodes == _SKIP_MARK_TOTAL
    assert report.failed_nodes == 1
    assert len(report.errors) == 1

    error = report.errors[0]
    assert error.kind == "fetch_failed"
    assert error.node_id == "bad-child"
    assert error.last_exception_type == "RuntimeError"
    assert error.attempts == 1
    assert "bad-child" in error.path

    bad = next(
        (c for c in tree.children if c.ref and c.ref.id == "bad-child"),
        None,
    )
    assert bad is not None
    assert bad.error is not None
    assert bad.result is None


@pytest.mark.asyncio
async def test_abort_cancels_all() -> None:
    """Test on_node_error='abort' cancels all tasks and raises."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Fail on fail-child."""
        if ref.id == "fail-child":
            msg = "Abort trigger"
            raise ValueError(msg)
        await asyncio.sleep(0.1)
        if depth == 0:
            return FetchResult(
                child_refs=[
                    ChildRef(id="child-1"),
                    ChildRef(id="fail-child"),
                    ChildRef(id="child-2"),
                ],
            )
        return FetchResult(items=[SampleItem(f"item-{ref.id}")])

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="abort",
    )

    with pytest.raises(TraversalAbortedError) as exc_info:
        await executor.expand_bounded(ChildRef(id="root"), depth=2)

    assert exc_info.value.node_id == "fail-child"
    assert isinstance(exc_info.value.original_error, ValueError)


@pytest.mark.asyncio
async def test_concurrent_abort_cancels_within_bounded_time() -> None:
    """Test abort cancels all in-flight tasks promptly."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Long-running children, one fails quickly."""
        if ref.id == "fail":
            msg = "abort!"
            raise RuntimeError(msg)
        if depth == 0:
            return FetchResult(
                child_refs=[
                    ChildRef(id="slow-1"),
                    ChildRef(id="slow-2"),
                    ChildRef(id="fail"),
                ],
            )
        await asyncio.sleep(10.0)
        return FetchResult()

    config = AsyncTreeExecutorConfig(
        retry_max_attempts=1,
        max_at_once=10,
        level_timeout=30.0,
    )
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="abort",
    )

    start = time.monotonic()
    with pytest.raises(TraversalAbortedError):
        await executor.expand_bounded(ChildRef(id="root"), depth=2)
    elapsed = time.monotonic() - start

    assert elapsed < _ABORT_MAX_ELAPSED


@pytest.mark.asyncio
async def test_error_has_traceback() -> None:
    """Test failed node error includes truncated traceback."""

    async def fetcher(ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Raise with nested calls for stack depth."""

        def inner() -> None:
            msg = "Deep error"
            raise TypeError(msg)

        def outer() -> None:
            inner()

        if ref.id == "root":
            outer()
        return FetchResult()

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=1)

    assert report.failed_nodes == 1
    error = report.errors[0]
    assert error.traceback is not None
    assert len(error.traceback) > 0
    assert "Traceback" in error.traceback


@pytest.mark.asyncio
async def test_error_has_full_path() -> None:
    """Test node error includes full ancestor path."""

    async def fetcher(_ref: ChildRef, depth: int) -> FetchResult[SampleItem]:
        """Fail at depth 2."""
        if depth == _FAIL_DEPTH:
            msg = f"Fail at depth {_FAIL_DEPTH}"
            raise RuntimeError(msg)
        return FetchResult(child_refs=[ChildRef(id=f"node-{depth + 1}")])

    config = AsyncTreeExecutorConfig(retry_max_attempts=1, max_at_once=5)
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(
        ChildRef(id="node-0"),
        depth=3,
    )

    assert report.failed_nodes == 1
    error = report.errors[0]
    assert error.node_id == "node-2"
    assert error.path == ["node-0", "node-1", "node-2"]


@pytest.mark.asyncio
async def test_retry_exhausted_recorded() -> None:
    """Test retry exhaustion is recorded in error."""
    attempts = 0

    async def fetcher(_ref: ChildRef, _depth: int) -> FetchResult[SampleItem]:
        """Raise on every call."""
        nonlocal attempts
        attempts += 1
        msg = f"Attempt {attempts}"
        raise RuntimeError(msg)

    config = AsyncTreeExecutorConfig(
        retry_max_attempts=_RETRY_ATTEMPTS,
        max_at_once=5,
    )
    executor = AsyncTreeExecutor[SampleItem](
        fetcher=fetcher,
        config=config,
        on_node_error="skip_mark",
    )

    _tree, report = await executor.expand_bounded(ChildRef(id="root"), depth=1)

    assert report.failed_nodes == 1
    assert report.errors[0].attempts == _RETRY_ATTEMPTS
