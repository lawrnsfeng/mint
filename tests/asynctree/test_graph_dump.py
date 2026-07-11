"""Tests for asynctree graph dump module."""

import json
from pathlib import Path

import pytest

from mint.asynctree import AsyncTreeExecutor, AsyncTreeExecutorConfig
from mint.asynctree.graph_dump import dump_execution_graph
from mint.asynctree.models import (
    ChildRef,
    FetchResult,
    NodeError,
    TraversalReport,
    TreeNode,
)

_TREE_TOTAL_NODES = 2
_TREE_CHILDREN = 2
_ELAPSED_MS = 500
_REAL_TREE_TOTAL = 3
_REAL_TREE_CHILDREN = 2


class DummyItem:
    """Dummy item for testing."""

    def __init__(self, name: str) -> None:
        """Initialize DummyItem."""
        self.name = name


def _build_sample_tree() -> tuple[TreeNode[DummyItem], TraversalReport]:
    """Build a sample tree with mixed success/failure for testing."""
    root: TreeNode[DummyItem] = TreeNode(
        ref=ChildRef(id="root"),
        result=FetchResult(items=[DummyItem("file1"), DummyItem("file2")]),
        depth=0,
    )
    good_child: TreeNode[DummyItem] = TreeNode(
        ref=ChildRef(id="good-child"),
        result=FetchResult(items=[DummyItem("child-file")]),
        depth=1,
    )
    bad_child: TreeNode[DummyItem] = TreeNode(
        ref=ChildRef(id="bad-child"),
        error=NodeError(
            kind="fetch_failed",
            attempts=3,
            last_exception_type="HTTPError",
            last_exception_repr="HTTPError(500)",
            node_id="bad-child",
            path=["root", "bad-child"],
            traceback="Traceback...",
        ),
        depth=1,
    )
    root.children = [good_child, bad_child]

    assert bad_child.error is not None
    report = TraversalReport(
        total_nodes=_TREE_TOTAL_NODES,
        failed_nodes=1,
        cancelled_nodes=0,
        skipped_nodes=0,
        max_depth_seen=1,
        elapsed_ms=_ELAPSED_MS,
        timed_out=False,
        errors=[bad_child.error],
    )
    return root, report


def test_dump_execution_graph_creates_file(tmp_path: Path) -> None:
    """Test dump_execution_graph creates a JSON file."""
    tree, report = _build_sample_tree()
    output_path = tmp_path / "debug" / "graph.json"

    dump_execution_graph(tree, report, output_path)

    assert output_path.exists()
    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert "tree" in data
    assert "report" in data
    assert "errors" in data


def test_dump_execution_graph_tree_structure(tmp_path: Path) -> None:
    """Test dumped tree has correct nested structure."""
    tree, report = _build_sample_tree()
    output_path = tmp_path / "graph.json"

    dump_execution_graph(tree, report, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    tree_data = data["tree"]
    assert tree_data["node_id"] == "root"
    assert tree_data["depth"] == 0
    assert tree_data["status"] == "success"
    assert tree_data["items_count"] == _TREE_TOTAL_NODES
    assert len(tree_data["children"]) == _TREE_CHILDREN

    good = tree_data["children"][0]
    assert good["node_id"] == "good-child"
    assert good["status"] == "success"

    bad = tree_data["children"][1]
    assert bad["node_id"] == "bad-child"
    assert bad["status"] == "fetch_failed"
    assert "error" in bad


def test_dump_execution_graph_report_fields(tmp_path: Path) -> None:
    """Test dumped report has all expected fields."""
    tree, report = _build_sample_tree()
    output_path = tmp_path / "graph.json"

    dump_execution_graph(tree, report, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    r = data["report"]
    assert r["total_nodes"] == _TREE_TOTAL_NODES
    assert r["failed_nodes"] == 1
    assert r["elapsed_ms"] == _ELAPSED_MS
    assert r["timed_out"] is False
    assert r["skipped_nodes"] == 0


def test_dump_execution_graph_errors_flat_array(tmp_path: Path) -> None:
    """Test errors are a flat array for quick scanning."""
    tree, report = _build_sample_tree()
    output_path = tmp_path / "graph.json"

    dump_execution_graph(tree, report, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    errors = data["errors"]
    assert len(errors) == 1
    assert errors[0]["node_id"] == "bad-child"
    assert errors[0]["kind"] == "fetch_failed"
    assert errors[0]["path"] == ["root", "bad-child"]


def test_dump_execution_graph_roundtrip(tmp_path: Path) -> None:
    """Test JSON can be re-loaded and validated."""
    tree, report = _build_sample_tree()
    output_path = tmp_path / "graph.json"

    dump_execution_graph(tree, report, output_path)

    raw = output_path.read_text(encoding="utf-8")
    data = json.loads(raw)

    re_serialized = json.dumps(data, indent=2, ensure_ascii=False)
    assert json.loads(re_serialized) == data


@pytest.mark.asyncio
async def test_dump_after_real_execution(tmp_path: Path) -> None:
    """Test graph dump after a real executor run."""

    async def fetcher(ref: ChildRef, depth: int) -> FetchResult[DummyItem]:
        """Return a two-level tree."""
        if depth == 0:
            return FetchResult(
                items=[DummyItem("root-file")],
                child_refs=[ChildRef(id="child-1"), ChildRef(id="child-2")],
            )
        return FetchResult(items=[DummyItem(f"file-{ref.id}")])

    config = AsyncTreeExecutorConfig(max_at_once=5, level_timeout=2.0)
    executor = AsyncTreeExecutor[DummyItem](fetcher=fetcher, config=config)

    tree_result, report_result = await executor.expand_bounded(
        ChildRef(id="root"),
        depth=2,
    )

    output_path = tmp_path / "real_graph.json"
    dump_execution_graph(tree_result, report_result, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["report"]["total_nodes"] == _REAL_TREE_TOTAL
    assert len(data["tree"]["children"]) == _REAL_TREE_CHILDREN


def test_dump_empty_tree(tmp_path: Path) -> None:
    """Test dump with a minimal tree (no result yet)."""
    tree: TreeNode[DummyItem] = TreeNode(ref=ChildRef(id="empty"))
    report = TraversalReport()
    output_path = tmp_path / "empty.json"

    dump_execution_graph(tree, report, output_path)

    data = json.loads(output_path.read_text(encoding="utf-8"))
    assert data["tree"]["status"] == "pending"
    assert data["tree"]["items_count"] == 0
