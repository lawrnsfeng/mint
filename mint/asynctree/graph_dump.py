"""Execution graph JSON dump for debugging."""

import json
from pathlib import Path
from typing import Any

from .models import NodeError, TraversalReport, TreeNode


def _serialize_node[Item](node: TreeNode[Item]) -> dict[str, Any]:
    """Recursively serialize a TreeNode to a dict.

    Args:
        node: TreeNode to serialize.

    Returns:
        Dict representation suitable for JSON encoding.

    """
    status = "success"
    if node.error is not None:
        status = node.error.kind
    elif node.partial:
        status = "partial"
    elif node.result is None:
        status = "pending"

    result: dict[str, Any] = {
        "node_id": node.ref.id if node.ref else "root",
        "depth": node.depth,
        "status": status,
        "items_count": len(node.result.items) if node.result else 0,
        "children": [_serialize_node(child) for child in node.children],
    }

    if node.error is not None:
        result["error"] = _serialize_error(node.error)

    return result


def _serialize_error(error: NodeError) -> dict[str, Any]:
    """Serialize a NodeError to a dict.

    Args:
        error: NodeError to serialize.

    Returns:
        Dict representation of the error.

    """
    return {
        "node_id": error.node_id,
        "kind": error.kind,
        "attempts": error.attempts,
        "last_exception_type": error.last_exception_type,
        "last_exception_repr": error.last_exception_repr,
        "traceback": error.traceback,
        "path": error.path,
    }


def _serialize_report(report: TraversalReport) -> dict[str, Any]:
    """Serialize a TraversalReport to a dict.

    Args:
        report: TraversalReport to serialize.

    Returns:
        Dict representation of the report.

    """
    return {
        "total_nodes": report.total_nodes,
        "failed_nodes": report.failed_nodes,
        "cancelled_nodes": report.cancelled_nodes,
        "skipped_nodes": report.skipped_nodes,
        "max_depth_seen": report.max_depth_seen,
        "elapsed_ms": report.elapsed_ms,
        "timed_out": report.timed_out,
    }


def dump_execution_graph[Item](
    tree: TreeNode[Item],
    report: TraversalReport,
    path: Path,
) -> None:
    """Dump the execution graph as JSON for debugging.

    Produces a JSON file with:
    - ``tree``: nested tree structure mirroring runtime hierarchy
    - ``report``: summary statistics
    - ``errors``: flat array of all errors for quick scanning

    Args:
        tree: Root TreeNode from the traversal.
        report: TraversalReport from the traversal.
        path: File path to write the JSON output to.

    """
    output: dict[str, Any] = {
        "tree": _serialize_node(tree),
        "report": _serialize_report(report),
        "errors": [_serialize_error(e) for e in report.errors],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(output, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
