"""Protocol for the canvas graph + result store."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from mint.worker.canvas.models import AnyNode, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus


@dataclass(frozen=True)
class GroupProgress:
    """Outcome of atomically recording one group child as done.

    ``added`` is False when this child was already recorded (a redelivery);
    ``fired`` is True at most once per *unreleased* guard — exactly when a
    completion finds every child done and the callback-fired guard still free —
    so callers never need their own de-duplication. A redelivery can therefore
    still fire the callback, but only after ``reset_group_fired`` released the
    guard because the previous dispatch never reached the broker.
    """

    added: bool
    done_count: int
    fired: bool


class ICanvasStore(Protocol):
    """Durable store for canvas graphs, node results, and fan-in progress."""

    async def create_canvas(self, canvas_id: str, nodes: Mapping[str, AnyNode]) -> None:
        """Persist every node of a freshly built canvas in one call."""
        ...

    async def get_node(self, canvas_id: str, node_id: str) -> AnyNode | None:
        """Look up a single node, or None if it does not exist."""
        ...

    async def set_node_status(self, canvas_id: str, node_id: str, status: NodeStatus) -> None:
        """Update a node's status. A no-op if the node does not exist."""
        ...

    async def cancel_nodes(self, canvas_id: str, node_ids: Sequence[str]) -> None:
        """Mark every listed node CANCELLED."""
        ...

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Persist a node's terminal outcome."""
        ...

    async def get_result(self, canvas_id: str, node_id: str) -> NodeOutcome | None:
        """Look up a single node's outcome, or None if it has not finished."""
        ...

    async def get_results(
        self,
        canvas_id: str,
        node_ids: Sequence[str],
    ) -> dict[str, NodeOutcome]:
        """Look up outcomes for every listed node that has one recorded."""
        ...

    async def mark_child_done(
        self,
        canvas_id: str,
        group_id: str,
        child_id: str,
        num_children: int,
    ) -> GroupProgress:
        """Atomically record one group child as done and report fan-in progress."""
        ...

    async def claim_group_terminal(self, canvas_id: str, group_id: str) -> bool:
        """Claim the right to emit this group's single terminal outcome. True if won.

        Shares the callback-fired guard, because a group emits exactly one terminal
        event: either it fires its callback, or it aborts/propagates — never both.
        Without it, two legs failing concurrently under PROPAGATE both bubble a
        group-level ERROR and the enclosing container advances twice.
        """
        ...

    async def reset_group_fired(self, canvas_id: str, group_id: str) -> None:
        """Release a group's callback-fired guard so the next completion can re-fire it.

        Called only when a callback dispatch that ``mark_child_done`` authorised
        failed to publish — without this the guard stays burned and the callback
        is never dispatched at all. See ``CanvasEngine.rollback``.
        """
        ...

    async def get_canvas_status(self, canvas_id: str) -> CanvasStatus:
        """Return a canvas's status, defaulting to RUNNING if never set."""
        ...

    async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
        """Update a canvas's overall status."""
        ...

    async def close(self) -> None:
        """Release any underlying connections/resources."""
        ...
