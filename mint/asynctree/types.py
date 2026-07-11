"""Type definitions for async tree traversal."""

from collections.abc import Callable
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from .models import ChildRef, FetchResult


OnNodeError = Literal["skip_mark", "abort"]


class Fetcher[Item](Protocol):
    """Protocol for fetching child items from a node.

    A fetcher is an async callable that takes a node reference and its depth,
    returning a FetchResult containing items and child references.
    """

    async def __call__(
        self,
        ref: "ChildRef",
        depth: int,
        /,
    ) -> "FetchResult[Item]":
        """Fetch children for the given node reference."""
        ...


class ProgressHook(Protocol):
    """Protocol for receiving progress events from AsyncTreeExecutor.

    Implementations track tree traversal progress. All methods are called
    synchronously from the executor event loop — keep them fast and
    non-blocking. Exceptions raised inside hook methods are suppressed;
    they must not crash the executor.
    """

    def on_children_discovered(
        self,
        parent_id: str,
        child_ids: list[str],
        /,
    ) -> None:
        """Fire when a node's children are discovered and spawned.

        Args:
            parent_id: ID of the node whose children were just fetched.
            child_ids: IDs of the child nodes that will be traversed
                (duplicates already excluded).

        """
        ...

    def on_node_complete(self, node_id: str, /) -> None:
        """Fire when a leaf node completes with no children to spawn.

        Args:
            node_id: ID of the completed leaf node.

        """
        ...


RetryPredicate = Callable[[BaseException], bool]

RetryAfterHook = Callable[[BaseException], float | None]
