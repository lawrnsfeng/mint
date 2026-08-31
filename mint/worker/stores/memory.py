"""In-memory ``ICanvasStore`` — single-process deployments and tests."""

import asyncio
from collections.abc import Mapping, Sequence

from mint.worker.canvas.models import AnyNode, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.stores.interface import GroupProgress


class MemoryCanvasStore:
    """Process-local canvas store backed by plain dicts, guarded by one lock.

    Correct for a single process (including concurrent asyncio tasks within
    it, which is what makes fan-in race tests meaningful without a real
    broker); not shared across processes. See ``RedisCanvasStore`` for the
    multi-process equivalent.
    """

    def __init__(self) -> None:
        """Start with an empty store."""
        self._nodes: dict[tuple[str, str], AnyNode] = {}
        self._results: dict[tuple[str, str], NodeOutcome] = {}
        self._group_done: dict[tuple[str, str], set[str]] = {}
        self._group_fired: set[tuple[str, str]] = set()
        self._canvas_status: dict[str, CanvasStatus] = {}
        self._lock = asyncio.Lock()

    async def create_canvas(self, canvas_id: str, nodes: Mapping[str, AnyNode]) -> None:
        """Persist every node of a freshly built canvas in one call."""
        async with self._lock:
            # A retry under a caller-supplied canvas_id must start from nothing.
            # Resetting only the status is not enough: a burned fan-in guard from
            # the previous attempt makes mark_child_done report fired=False forever,
            # so the chord's callback is never dispatched and the canvas stalls
            # RUNNING — a stall the retry existed to escape.
            self._purge(canvas_id)
            for node_id, node in nodes.items():
                self._nodes[(canvas_id, node_id)] = node
            self._canvas_status[canvas_id] = CanvasStatus.RUNNING

    def _purge(self, canvas_id: str) -> None:
        """Drop everything a previous attempt under this canvas id left behind."""
        for mapping in (self._nodes, self._results):
            for key in [key for key in mapping if key[0] == canvas_id]:
                del mapping[key]
        for key in [key for key in self._group_done if key[0] == canvas_id]:
            del self._group_done[key]
        self._group_fired -= {key for key in self._group_fired if key[0] == canvas_id}

    async def get_node(self, canvas_id: str, node_id: str) -> AnyNode | None:
        """Look up a single node, or None if it does not exist."""
        return self._nodes.get((canvas_id, node_id))

    async def set_node_status(self, canvas_id: str, node_id: str, status: NodeStatus) -> None:
        """Move a node to ``status`` from a non-terminal one. A no-op otherwise.

        Guarded exactly as ``RedisCanvasStore.set_node_status`` is. Concurrent
        handler tasks are the whole reason this store takes a lock, so the race is
        reachable here too: task A reads node N as RUNNING inside `_complete`, task
        B's ABORT cancels N, and A's `_record` then stamps N back to FINISHED —
        after which `_complete`'s CANCELLED guard never fires and the engine advances
        a branch it had given up on. Under Redis it would not, and a stand-in store
        that diverges makes every test written against it prove the wrong thing.
        """
        async with self._lock:
            key = (canvas_id, node_id)
            node = self._nodes.get(key)
            if node is None or node.status not in (NodeStatus.PENDING, NodeStatus.RUNNING):
                return
            self._nodes[key] = node.model_copy(update={"status": status})

    async def mark_node_running(self, canvas_id: str, node_id: str) -> None:
        """Move a node from PENDING to RUNNING. A no-op from any other status."""
        async with self._lock:
            key = (canvas_id, node_id)
            node = self._nodes.get(key)
            if node is None or node.status != NodeStatus.PENDING:
                return
            self._nodes[key] = node.model_copy(update={"status": NodeStatus.RUNNING})

    async def cancel_nodes(self, canvas_id: str, node_ids: Sequence[str]) -> None:
        """Mark every listed node CANCELLED, unless it already reached a terminal status.

        Guarded exactly as ``RedisCanvasStore.cancel_nodes`` is: a leg that
        genuinely FINISHED before the cancellation reached it did run, and stamping
        it CANCELLED erases that — and `_complete` then discards a redelivery of its
        outcome. Cancellation expands through whole subtrees, so grandchildren that
        already completed are routinely in the list.

        Divergence here would be worse than the bug: this store exists to be a
        faithful single-process stand-in, so the same canvas must behave the same
        way under both.
        """
        async with self._lock:
            for node_id in node_ids:
                key = (canvas_id, node_id)
                node = self._nodes.get(key)
                if node is None or node.status not in (NodeStatus.PENDING, NodeStatus.RUNNING):
                    continue
                self._nodes[key] = node.model_copy(update={"status": NodeStatus.CANCELLED})

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Persist a node's terminal outcome."""
        async with self._lock:
            self._results[(canvas_id, node_id)] = outcome

    async def get_result(self, canvas_id: str, node_id: str) -> NodeOutcome | None:
        """Look up a single node's outcome, or None if it has not finished."""
        return self._results.get((canvas_id, node_id))

    async def get_results(
        self,
        canvas_id: str,
        node_ids: Sequence[str],
    ) -> dict[str, NodeOutcome]:
        """Look up outcomes for every listed node that has one recorded."""
        mp_str_outcome: dict[str, NodeOutcome] = {}
        for node_id in node_ids:
            outcome = self._results.get((canvas_id, node_id))
            if outcome is not None:
                mp_str_outcome[node_id] = outcome
        return mp_str_outcome

    async def mark_child_done(
        self,
        canvas_id: str,
        group_id: str,
        child_id: str,
        num_children: int,
    ) -> GroupProgress:
        """Atomically record one group child as done and report fan-in progress."""
        async with self._lock:
            key = (canvas_id, group_id)
            done = self._group_done.setdefault(key, set())
            added = child_id not in done
            done.add(child_id)
            done_count = len(done)
            fired = False
            # Mirrors FAN_IN_SCRIPT exactly, `added` deliberately not part of the
            # predicate: the fired-guard alone is what makes firing exactly-once,
            # and gating on `added` would stop a redelivery from re-firing a
            # callback whose dispatch was authorised but never published.
            if done_count == num_children and key not in self._group_fired:
                self._group_fired.add(key)
                fired = True
            return GroupProgress(added=added, done_count=done_count, fired=fired)

    async def claim_group_terminal(self, canvas_id: str, group_id: str) -> bool:
        """Claim the right to emit this group's single terminal outcome. True if won."""
        async with self._lock:
            key = (canvas_id, group_id)
            if key in self._group_fired:
                return False
            self._group_fired.add(key)
            return True

    async def reset_group_fired(self, canvas_id: str, group_id: str) -> None:
        """Release this group's callback-fired guard so a redelivery can re-fire it."""
        async with self._lock:
            self._group_fired.discard((canvas_id, group_id))

    async def get_canvas_status(self, canvas_id: str) -> CanvasStatus:
        """Return a canvas's status, defaulting to RUNNING if never set."""
        return self._canvas_status.get(canvas_id, CanvasStatus.RUNNING)

    async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
        """Update a canvas's overall status."""
        async with self._lock:
            self._canvas_status[canvas_id] = status

    async def close(self) -> None:
        """Release any underlying connections/resources — a no-op for an in-memory store."""
        return
