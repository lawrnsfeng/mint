"""Async recursive tree traversal framework.

A generic, platform-agnostic framework for recursively fetching hierarchical
tree structures with configurable retry, concurrency gating, timeout policies,
partial-result recovery on cancellation, and rich error reporting.

This package is standalone — it has zero imports from other mint modules
and can be extracted to its own package.
"""

from .config import AsyncTreeExecutorConfig, AsyncTreeSettings
from .exceptions import (
    AsyncTreeError,
    AsyncTreeFetcherError,
    GrandTimeoutExceededError,
    TraversalAbortedError,
)
from .executor import AsyncTreeExecutor
from .graph_dump import dump_execution_graph
from .models import (
    ChildRef,
    FetchResult,
    NodeError,
    TraversalReport,
    TreeNode,
)
from .retry import RetryConfig
from .types import (
    Fetcher,
    OnNodeError,
    ProgressHook,
    RetryAfterHook,
    RetryPredicate,
)

__all__ = [
    "AsyncTreeError",
    "AsyncTreeExecutor",
    "AsyncTreeExecutorConfig",
    "AsyncTreeFetcherError",
    "AsyncTreeSettings",
    "ChildRef",
    "FetchResult",
    "Fetcher",
    "GrandTimeoutExceededError",
    "NodeError",
    "OnNodeError",
    "ProgressHook",
    "RetryAfterHook",
    "RetryConfig",
    "RetryPredicate",
    "TraversalAbortedError",
    "TraversalReport",
    "TreeNode",
    "dump_execution_graph",
]
