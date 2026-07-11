"""Data models for async tree traversal."""

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class ChildRef:
    """Reference to a child node to be recursively fetched.

    Attributes:
        id: Unique identifier for the child node.
        payload: Caller-defined data attached to this reference.

    """

    id: str
    payload: Any = None


@dataclass
class FetchResult[Item]:
    """Result from fetching a node's children.

    Attributes:
        items: List of leaf items (files, messages, etc.) at this node.
        child_refs: List of child references to recurse into.
        extra: Optional caller-defined metadata.

    """

    items: list[Item] = field(default_factory=list)
    child_refs: list[ChildRef] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class NodeError:
    """Error information for a failed or skipped node.

    Attributes:
        kind: Type of error that occurred.
        attempts: Number of retry attempts made.
        last_exception_type: Type name of the last exception.
        last_exception_repr: String representation of the last exception.
        node_id: ID of the node that failed.
        traceback: Truncated traceback string (optional).
        path: Full ancestor path from root to this node.

    """

    kind: Literal[
        "fetch_failed",
        "timeout",
        "cancelled",
        "aborted",
        "duplicate_skipped",
    ]
    attempts: int
    last_exception_type: str
    last_exception_repr: str
    node_id: str
    traceback: str | None = None
    path: list[str] = field(default_factory=list)


@dataclass
class TreeNode[Item]:
    """A node in the fetched tree.

    Attributes:
        ref: Reference to this node (None for synthetic root).
        result: Fetch result for this node (None if failed/cancelled).
        children: List of child TreeNodes.
        error: Error info if this node failed (None on success).
        partial: True if this subtree was cancelled mid-traversal.
        depth: Depth of this node in the tree (0 for root).

    """

    ref: ChildRef | None = None
    result: "FetchResult[Item] | None" = None
    children: "list[TreeNode[Item]]" = field(default_factory=list)
    error: NodeError | None = None
    partial: bool = False
    depth: int = 0


@dataclass
class TraversalReport:
    """Summary of a tree traversal operation.

    Attributes:
        total_nodes: Total number of nodes successfully visited.
        failed_nodes: Number of nodes that failed to fetch.
        cancelled_nodes: Number of nodes cancelled due to timeout.
        skipped_nodes: Number of duplicate nodes skipped.
        max_depth_seen: Maximum depth reached during traversal.
        elapsed_ms: Total elapsed time in milliseconds.
        timed_out: True if grand timeout was exceeded.
        errors: List of all errors encountered.

    """

    total_nodes: int = 0
    failed_nodes: int = 0
    cancelled_nodes: int = 0
    skipped_nodes: int = 0
    max_depth_seen: int = 0
    elapsed_ms: int = 0
    timed_out: bool = False
    errors: list[NodeError] = field(default_factory=list)
