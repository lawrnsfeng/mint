"""Tests for asynctree models."""

import dataclasses
import json
from typing import Literal, cast, get_args

import pytest

from mint.asynctree.models import (
    ChildRef,
    FetchResult,
    NodeError,
    TraversalReport,
    TreeNode,
)

_ITEMS_TWO = 2
_ATTEMPTS_THREE = 3
_DEPTH_TWO = 2
_TOTAL_NODES = 10
_SKIPPED_NODES = 2


class FileItem:
    """Test item type for generic parametrization."""

    def __init__(self, name: str, size: int) -> None:
        """Initialize FileItem."""
        self.name = name
        self.size = size


class FolderItem:
    """Alternative test item type."""

    def __init__(self, title: str) -> None:
        """Initialize FolderItem."""
        self.title = title


def test_child_ref_minimal() -> None:
    """Test ChildRef with minimal required fields."""
    ref = ChildRef(id="folder-123")
    assert ref.id == "folder-123"
    assert ref.payload is None


def test_child_ref_with_payload() -> None:
    """Test ChildRef with payload."""
    payload = {"name": "Documents", "type": "folder"}
    ref = ChildRef(id="folder-456", payload=payload)
    assert ref.id == "folder-456"
    assert ref.payload == payload


def test_child_ref_json_roundtrip() -> None:
    """Test ChildRef JSON serialization roundtrip via dataclasses.asdict."""
    original = ChildRef(id="test-id", payload={"key": "value"})
    as_dict = dataclasses.asdict(original)
    json_str = json.dumps(as_dict)
    restored_dict = json.loads(json_str)
    restored = ChildRef(**restored_dict)
    assert restored.id == original.id
    assert restored.payload == original.payload


def test_fetch_result_empty() -> None:
    """Test FetchResult with no items or children."""
    result: FetchResult[FileItem] = FetchResult()
    assert result.items == []
    assert result.child_refs == []
    assert result.extra == {}


def test_fetch_result_with_items() -> None:
    """Test FetchResult with items."""
    items = [
        FileItem(name="doc.pdf", size=1024),
        FileItem(name="image.png", size=2048),
    ]
    result: FetchResult[FileItem] = FetchResult(items=items)
    assert len(result.items) == _ITEMS_TWO
    assert result.items[0].name == "doc.pdf"


def test_fetch_result_with_child_refs() -> None:
    """Test FetchResult with child references."""
    refs = [ChildRef(id="child-1"), ChildRef(id="child-2")]
    result: FetchResult[FileItem] = FetchResult(child_refs=refs)
    assert len(result.child_refs) == _ITEMS_TWO
    assert result.child_refs[1].id == "child-2"


def test_fetch_result_generic_parametrization() -> None:
    """Test FetchResult works with different Item types."""
    result_files: FetchResult[FileItem] = FetchResult(
        items=[FileItem("a.txt", 100)],
    )
    result_folders: FetchResult[FolderItem] = FetchResult(
        items=[FolderItem("Project")],
    )
    assert result_files.items[0].name == "a.txt"
    assert result_folders.items[0].title == "Project"


def test_node_error_required_fields() -> None:
    """Test NodeError with required fields."""
    error = NodeError(
        kind="fetch_failed",
        attempts=3,
        last_exception_type="HTTPError",
        last_exception_repr="HTTPError(500)",
        node_id="node-123",
    )
    assert error.kind == "fetch_failed"
    assert error.attempts == _ATTEMPTS_THREE
    assert error.node_id == "node-123"
    assert error.path == []
    assert error.traceback is None


def test_node_error_with_path_and_traceback() -> None:
    """Test NodeError with path and traceback."""
    error = NodeError(
        kind="timeout",
        attempts=1,
        last_exception_type="TimeoutError",
        last_exception_repr="TimeoutError()",
        node_id="node-456",
        path=["root", "parent", "node-456"],
        traceback="Traceback (most recent call last):\n...",
    )
    assert error.path == ["root", "parent", "node-456"]
    assert error.traceback is not None


NodeErrorKind = Literal[
    "fetch_failed",
    "timeout",
    "cancelled",
    "aborted",
    "duplicate_skipped",
]


@pytest.mark.parametrize("kind", list(get_args(NodeErrorKind)))
def test_node_error_kinds(kind: str) -> None:
    """Test NodeError accepts all valid kinds."""
    error = NodeError(
        kind=cast("NodeErrorKind", kind),
        attempts=1,
        last_exception_type="Error",
        last_exception_repr="Error()",
        node_id="node",
    )
    assert error.kind == kind


def test_tree_node_root() -> None:
    """Test TreeNode for root (no ref)."""
    node: TreeNode[FileItem] = TreeNode()
    assert node.ref is None
    assert node.result is None
    assert node.children == []
    assert node.error is None
    assert node.partial is False
    assert node.depth == 0


def test_tree_node_with_ref_and_result() -> None:
    """Test TreeNode with ref and result."""
    ref = ChildRef(id="folder-1")
    result: FetchResult[FileItem] = FetchResult(
        items=[FileItem("file.txt", 512)],
    )
    node: TreeNode[FileItem] = TreeNode(
        ref=ref,
        result=result,
        depth=_DEPTH_TWO,
    )
    assert node.ref == ref
    assert node.result == result
    assert node.depth == _DEPTH_TWO


def test_tree_node_with_children() -> None:
    """Test TreeNode with child nodes."""
    parent: TreeNode[FileItem] = TreeNode(depth=1)
    child1: TreeNode[FileItem] = TreeNode(ref=ChildRef(id="child-1"), depth=2)
    child2: TreeNode[FileItem] = TreeNode(ref=ChildRef(id="child-2"), depth=2)
    parent.children = [child1, child2]
    assert len(parent.children) == _ITEMS_TWO
    assert parent.children[0].ref is not None
    assert parent.children[0].ref.id == "child-1"


def test_tree_node_with_error() -> None:
    """Test TreeNode with error marker."""
    error = NodeError(
        kind="cancelled",
        attempts=0,
        last_exception_type="CancelledError",
        last_exception_repr="Task cancelled",
        node_id="node-x",
    )
    node: TreeNode[FileItem] = TreeNode(
        ref=ChildRef(id="node-x"),
        error=error,
        partial=True,
    )
    assert node.error is not None
    assert node.error.kind == "cancelled"
    assert node.partial is True


def test_traversal_report_defaults() -> None:
    """Test TraversalReport default values."""
    report = TraversalReport()
    assert report.total_nodes == 0
    assert report.failed_nodes == 0
    assert report.cancelled_nodes == 0
    assert report.skipped_nodes == 0
    assert report.max_depth_seen == 0
    assert report.elapsed_ms == 0
    assert report.timed_out is False
    assert report.errors == []


def test_traversal_report_with_data() -> None:
    """Test TraversalReport with actual data."""
    error = NodeError(
        kind="fetch_failed",
        attempts=4,
        last_exception_type="HTTPError",
        last_exception_repr="500",
        node_id="failed-node",
    )
    report = TraversalReport(
        total_nodes=10,
        failed_nodes=1,
        cancelled_nodes=0,
        skipped_nodes=2,
        max_depth_seen=3,
        elapsed_ms=1500,
        timed_out=False,
        errors=[error],
    )
    assert report.total_nodes == _TOTAL_NODES
    assert report.skipped_nodes == _SKIPPED_NODES
    assert report.errors[0].node_id == "failed-node"
