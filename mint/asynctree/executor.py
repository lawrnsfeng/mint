"""Async tree executor for recursive traversal."""

import asyncio
import contextlib
import traceback
from dataclasses import dataclass
from typing import Any

from .clock import DynamicClock, GrandClock, StaticClock
from .concurrency import ConcurrencyGate
from .config import AsyncTreeExecutorConfig
from .exceptions import AsyncTreeFetcherError, TraversalAbortedError
from .models import (
    ChildRef,
    FetchResult,
    NodeError,
    TraversalReport,
    TreeNode,
)
from .retry import RetryConfig, build_retrying
from .types import Fetcher, OnNodeError, ProgressHook


@dataclass
class _SpawnContext[Item]:
    """Context bundle passed to child-spawning logic.

    Attributes:
        report: Mutable traversal report accumulating stats.
        live_tasks: Set of currently running asyncio tasks.
        node_map: Mapping from task to tree node.
        max_depth: Maximum traversal depth (None = unbounded).
        clock: Grand timeout clock.

    """

    report: TraversalReport
    live_tasks: set[asyncio.Task[Any]]
    node_map: dict[asyncio.Task[Any], "TreeNode[Item]"]
    max_depth: int | None
    clock: GrandClock


class AsyncTreeExecutor[Item]:
    """Executor for async recursive tree traversal.

    Orchestrates recursive fetching with retry, concurrency control,
    grand timeout, configurable error handling, deduplication, and
    full ancestor path tracking.
    """

    def __init__(
        self,
        fetcher: Fetcher[Item],
        config: AsyncTreeExecutorConfig | None = None,
        *,
        on_node_error: OnNodeError = "skip_mark",
        retry_config: RetryConfig | None = None,
        progress_hook: ProgressHook | None = None,
    ) -> None:
        """Initialize AsyncTreeExecutor.

        Args:
            fetcher: Async callable that fetches children for a node.
            config: Executor tuning (defaults when omitted).
            on_node_error: Error handling policy.
            retry_config: Override retry configuration (uses config values
                if None).
            progress_hook: Optional hook called on child discovery and
                leaf completion.

        """
        cfg = config or AsyncTreeExecutorConfig()
        self._fetcher = fetcher
        self._retry_config = retry_config or RetryConfig(
            max_attempts=cfg.retry_max_attempts,
        )
        self._gate = ConcurrencyGate(cfg.max_at_once, cfg.max_per_second)
        self._level_timeout = cfg.level_timeout
        self._on_node_error = on_node_error
        self._progress_hook = progress_hook
        self._parent_map: dict[str, str | None] = {}
        self._seen_refs: set[str] = set()

    async def expand_bounded(
        self,
        root_ref: ChildRef,
        depth: int,
    ) -> tuple[TreeNode[Item], TraversalReport]:
        """Expand tree with bounded depth using a static timeout.

        Args:
            root_ref: Reference to the root node.
            depth: Maximum depth to traverse (must be >= 1).

        Returns:
            Tuple of (root TreeNode, TraversalReport).

        Raises:
            ValueError: If depth < 1.
            TraversalAbortedError: If on_node_error="abort" and a node fails.

        """
        if depth < 1:
            msg = "depth must be >= 1"
            raise ValueError(msg)

        self._reset_state()
        clock = StaticClock(depth * self._level_timeout)
        return await self._execute(root_ref, depth, clock)

    async def expand_unbounded(
        self,
        root_ref: ChildRef,
    ) -> tuple[TreeNode[Item], TraversalReport]:
        """Expand tree with unbounded depth using a dynamic timeout.

        Args:
            root_ref: Reference to the root node.

        Returns:
            Tuple of (root TreeNode, TraversalReport).

        Raises:
            TraversalAbortedError: If on_node_error="abort" and a node fails.

        """
        self._reset_state()
        clock = DynamicClock(
            base_seconds=self._level_timeout,
            per_level_seconds=self._level_timeout,
        )
        return await self._execute(root_ref, None, clock)

    def _reset_state(self) -> None:
        """Reset per-run mutable state."""
        self._parent_map = {}
        self._seen_refs = set()

    async def _execute(
        self,
        root_ref: ChildRef,
        max_depth: int | None,
        clock: GrandClock,
    ) -> tuple[TreeNode[Item], TraversalReport]:
        """Core traversal loop.

        Args:
            root_ref: Root node reference.
            max_depth: Maximum depth (None = unbounded).
            clock: Grand timeout clock.

        Returns:
            Tuple of (root TreeNode, TraversalReport).

        """
        clock.start()

        root_node: TreeNode[Item] = TreeNode(ref=root_ref, depth=0)
        report = TraversalReport()
        live_tasks: set[asyncio.Task[Any]] = set()
        node_map: dict[asyncio.Task[Any], TreeNode[Item]] = {}

        self._parent_map[root_ref.id] = None
        self._seen_refs.add(root_ref.id)

        timeout_task = asyncio.create_task(clock.wait_for_timeout())
        live_tasks.add(timeout_task)

        ctx: _SpawnContext[Item] = _SpawnContext(
            report=report,
            live_tasks=live_tasks,
            node_map=node_map,
            max_depth=max_depth,
            clock=clock,
        )

        root_task = self._create_fetch_task(root_node, clock)
        live_tasks.add(root_task)
        node_map[root_task] = root_node

        while live_tasks:
            done, live_tasks = await asyncio.wait(
                live_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            ctx.live_tasks = live_tasks

            for task in done:
                if task is timeout_task:
                    await self._handle_timeout(report, live_tasks, node_map)
                    break

                node = node_map.pop(task)

                try:
                    fetch_result = await task
                    self._handle_success_and_spawn_children(
                        node,
                        fetch_result,
                        ctx,
                    )
                except AsyncTreeFetcherError as fetcher_err:
                    await self._handle_fetch_error(
                        node,
                        fetcher_err.original_error,
                        report,
                        live_tasks,
                    )

            if live_tasks == {timeout_task}:
                timeout_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await timeout_task
                live_tasks.clear()
                break

        report.elapsed_ms = clock.elapsed_ms
        return root_node, report

    async def _handle_timeout(
        self,
        report: TraversalReport,
        live_tasks: set[asyncio.Task[Any]],
        node_map: dict[asyncio.Task[Any], "TreeNode[Item]"],
    ) -> None:
        """Cancel all live tasks on grand timeout and mark nodes."""
        report.timed_out = True
        for task in live_tasks:
            task.cancel()
        await asyncio.gather(*live_tasks, return_exceptions=True)
        for task in list(live_tasks):
            node = node_map.pop(task)
            self._handle_cancellation(node, report)
        live_tasks.clear()

    def _handle_cancellation(
        self,
        node: "TreeNode[Item]",
        report: TraversalReport,
    ) -> None:
        """Mark a cancelled node as partial with error info."""
        node.partial = True
        node.error = NodeError(
            kind="cancelled",
            attempts=0,
            last_exception_type="CancelledError",
            last_exception_repr="Task cancelled",
            node_id=node.ref.id if node.ref else "root",
            path=self._build_path(node),
        )
        report.cancelled_nodes += 1
        report.errors.append(node.error)

    async def _handle_fetch_error(
        self,
        node: "TreeNode[Item]",
        exc: Exception,
        report: TraversalReport,
        live_tasks: set[asyncio.Task[Any]],
    ) -> None:
        """Handle fetch error — skip_mark or abort depending on policy.

        Raises:
            TraversalAbortedError: If on_node_error="abort".

        """
        node_id = node.ref.id if node.ref else "root"
        node_error = NodeError(
            kind="fetch_failed",
            attempts=self._retry_config.max_attempts,
            last_exception_type=type(exc).__name__,
            last_exception_repr=repr(exc),
            traceback=self._truncate_traceback(exc),
            node_id=node_id,
            path=self._build_path(node),
        )
        node.error = node_error
        report.failed_nodes += 1
        report.errors.append(node_error)

        if self._on_node_error == "abort":
            for task in live_tasks:
                task.cancel()
            await asyncio.gather(*live_tasks, return_exceptions=True)
            live_tasks.clear()
            raise TraversalAbortedError(
                message=f"Traversal aborted due to error at node {node_id}",
                node_id=node_id,
                original_error=exc,
            ) from exc

    def _handle_success_and_spawn_children(
        self,
        node: "TreeNode[Item]",
        fetch_result: "FetchResult[Item]",
        ctx: "_SpawnContext[Item]",
    ) -> None:
        """Attach fetch result to node and spawn child tasks."""
        node.result = fetch_result
        ctx.report.total_nodes += 1
        ctx.report.max_depth_seen = max(
            ctx.report.max_depth_seen,
            node.depth,
        )

        if isinstance(ctx.clock, DynamicClock):
            task = asyncio.create_task(ctx.clock.notify_depth(node.depth))
            task.add_done_callback(
                lambda t: t.exception() if not t.cancelled() else None,
            )

        parent_id = node.ref.id if node.ref else "root"

        if not fetch_result.child_refs:
            self._hook_node_complete(parent_id)
            return

        child_depth = node.depth + 1
        if ctx.max_depth is not None and child_depth >= ctx.max_depth:
            self._hook_node_complete(parent_id)
            return

        spawned_ids: list[str] = []
        for child_ref in fetch_result.child_refs:
            if child_ref.id in self._seen_refs:
                self._handle_duplicate(child_ref, node, ctx.report)
                continue
            self._seen_refs.add(child_ref.id)
            self._parent_map[child_ref.id] = parent_id

            child_node: TreeNode[Item] = TreeNode(
                ref=child_ref,
                depth=child_depth,
            )
            node.children.append(child_node)

            child_task = self._create_fetch_task(child_node, ctx.clock)
            ctx.live_tasks.add(child_task)
            ctx.node_map[child_task] = child_node
            spawned_ids.append(child_ref.id)

        if spawned_ids:
            self._hook_children_discovered(parent_id, spawned_ids)
        else:
            self._hook_node_complete(parent_id)

    def _hook_children_discovered(
        self,
        parent_id: str,
        child_ids: list[str],
    ) -> None:
        """Call progress hook on_children_discovered.

        Suppresses any exception raised by the hook.
        """
        if self._progress_hook is None:
            return
        with contextlib.suppress(Exception):
            self._progress_hook.on_children_discovered(parent_id, child_ids)

    def _hook_node_complete(self, node_id: str) -> None:
        """Call progress hook on_node_complete, suppressing exceptions."""
        if self._progress_hook is None:
            return
        with contextlib.suppress(Exception):
            self._progress_hook.on_node_complete(node_id)

    def _handle_duplicate(
        self,
        child_ref: ChildRef,
        parent_node: "TreeNode[Item]",
        report: TraversalReport,
    ) -> None:
        """Emit a warning NodeError for duplicate child ref and skip it."""
        node_error = NodeError(
            kind="duplicate_skipped",
            attempts=0,
            last_exception_type="DuplicateRef",
            last_exception_repr=(
                f"Ref '{child_ref.id}' already seen, skipped"
            ),
            node_id=child_ref.id,
            path=[*self._build_path(parent_node), child_ref.id],
        )
        report.skipped_nodes += 1
        report.errors.append(node_error)

    def _create_fetch_task(
        self,
        node: "TreeNode[Item]",
        clock: GrandClock,
    ) -> "asyncio.Task[FetchResult[Item]]":
        """Create a fetch task for a node."""
        return asyncio.create_task(self._fetch_with_retry(node, clock))

    async def _fetch_with_retry(
        self,
        node: "TreeNode[Item]",
        clock: GrandClock,
    ) -> "FetchResult[Item]":
        """Fetch a node with retry and concurrency control.

        Raises:
            AsyncTreeFetcherError: If fetch fails after retries.
            asyncio.CancelledError: If traversal is cancelled.

        """
        node_id = node.ref.id if node.ref else "root"

        try:
            async with self._gate.acquire():
                self._check_cancelled(clock)
                retrying = build_retrying(self._retry_config)
                return await retrying(self._fetch_once, node, clock)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise AsyncTreeFetcherError(node_id, exc) from exc

    async def _fetch_once(
        self,
        node: "TreeNode[Item]",
        clock: GrandClock,
    ) -> "FetchResult[Item]":
        """Execute a single fetch attempt, checking cancellation first.

        Args:
            node: Tree node to fetch children for.
            clock: Grand clock used to detect external cancellation.

        Returns:
            Fetch result from the user-provided fetcher.

        """
        self._check_cancelled(clock)
        return await self._fetcher(
            node.ref or ChildRef(id="root"),
            node.depth,
        )

    def _check_cancelled(self, clock: GrandClock) -> None:
        """Raise CancelledError if the grand clock has fired."""
        if clock.is_cancelled:
            raise asyncio.CancelledError

    def _build_path(self, node: "TreeNode[Item]") -> list[str]:
        """Build full ancestor path from root to this node.

        Args:
            node: Node to build path for.

        Returns:
            List of node IDs from root to this node.

        """
        path: list[str] = []
        node_id: str | None = node.ref.id if node.ref else "root"
        while node_id is not None:
            path.insert(0, node_id)
            node_id = self._parent_map.get(node_id)
        return path

    def _truncate_traceback(
        self,
        exc: Exception,
        *,
        max_lines: int = 20,
    ) -> str:
        """Extract and truncate exception traceback.

        Args:
            exc: Exception to extract traceback from.
            max_lines: Maximum lines to keep.

        Returns:
            Truncated traceback string.

        """
        tb_lines = traceback.format_exception(
            type(exc),
            exc,
            exc.__traceback__,
        )
        if len(tb_lines) <= max_lines:
            return "".join(tb_lines)
        return "".join(tb_lines[:max_lines]) + "\n... (truncated)"
