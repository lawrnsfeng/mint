"""CanvasEngine: pure transition rules for advancing a canvas graph.

The engine holds every rule for what happens when one node finishes — chain
sequencing, group fan-in, error policy, cancellation — in one place, so it
can be exercised without a broker and reused unchanged by both an embedded
Worker and a future centralized Coordinator.

The graph is walked with an explicit loop (not recursion) so pathological
nesting cannot blow the stack, and a per-call visited set turns a corrupted
(cyclic) graph into a raised error instead of an infinite loop.
"""

from collections.abc import Sequence
from typing import Final, assert_never

from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.models import (
    AnyNode,
    ChainNode,
    ChildResult,
    FanIn,
    GroupNode,
    NodeOutcome,
    TaskNode,
)
from mint.worker.enums import CanvasStatus, ErrorPolicy, NodeStatus
from mint.worker.exc import (
    CallbackNotFoundError,
    CanvasCycleError,
    InvalidParentTypeError,
    NodeNotFoundError,
    ParentNotFoundError,
    ResultTooLargeError,
    WorkerError,
)
from mint.worker.stores.interface import ICanvasStore


class CanvasEngine:
    """Advances a canvas graph one node-completion at a time."""

    DEFAULT_MAX_RESULT_BYTES: Final[int] = 256_000

    def __init__(
        self,
        store: ICanvasStore,
        max_result_bytes: int = DEFAULT_MAX_RESULT_BYTES,
    ) -> None:
        """Build an engine over ``store``, rejecting any result over ``max_result_bytes``."""
        self.store = store
        self.max_result_bytes = max_result_bytes

    async def complete(
        self,
        canvas_id: str,
        node_id: str,
        outcome: NodeOutcome,
    ) -> list[Dispatch]:
        """Record a node's outcome and return whatever must be published next.

        Idempotent under at-least-once redelivery of the same outcome: a
        duplicate leg of a group is recorded but produces no dispatch, and a
        canvas that already reached a terminal state short-circuits to no-op.

        The one exception is deliberate: if a previous callback dispatch was
        authorised but never published, the caller releases the group's fan-in
        guard via ``rollback()``, and the redelivery that follows *does* produce
        the callback dispatch again. Without that, a single failed publish loses
        a chord's callback permanently.
        """
        if await self.store.get_canvas_status(canvas_id) != CanvasStatus.RUNNING:
            return []
        try:
            return await self._complete(canvas_id, node_id, outcome)
        except WorkerError:
            await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)
            raise

    async def rollback(self, dispatches: Sequence[Dispatch]) -> None:
        """Undo the fan-in bookkeeping behind dispatches that failed to publish.

        ``complete()`` burns a group's one-shot callback-fired guard *before* its
        caller gets a chance to publish the resulting dispatch. If that publish
        fails and the delivery is redelivered, the replayed ``complete()`` would
        find the guard already burned, dispatch nothing, and let the caller ack —
        stranding the chord's callback forever. Releasing the guard here is what
        makes the redelivery re-fire it, preserving the "ack only after the store
        write *and* the publish succeed" contract this package relies on.

        Only callback dispatches carry a ``group_id``; everything else is a no-op.
        """
        for dispatch in dispatches:
            if dispatch.group_id is not None:
                await self.store.reset_group_fired(dispatch.canvas_id, dispatch.group_id)

    async def _complete(
        self,
        canvas_id: str,
        node_id: str,
        outcome: NodeOutcome,
    ) -> list[Dispatch]:
        node = await self._require_node(canvas_id, node_id)
        await self._record(canvas_id, node_id, outcome)

        visited = {node_id}
        current_id, current_outcome, parent_id = node_id, outcome, node.parent_id

        while parent_id is not None:
            if parent_id in visited:
                raise CanvasCycleError(canvas_id=canvas_id, node_id=parent_id)
            visited.add(parent_id)

            parent = await self._require_parent(canvas_id, current_id, parent_id)

            match parent:
                case TaskNode():
                    raise InvalidParentTypeError(node_id=parent.id, canvas_id=canvas_id)
                case ChainNode():
                    dispatch, bubbled = await self._advance_chain(
                        canvas_id,
                        parent,
                        current_id,
                        current_outcome,
                    )
                case GroupNode() if current_id == parent.callback:
                    dispatch = None
                    bubbled = current_outcome.model_copy(update={"node_id": parent.id})
                case GroupNode():
                    dispatch, bubbled = await self._advance_group(
                        canvas_id,
                        parent,
                        current_id,
                        current_outcome,
                    )
                case _ as unreachable:  # pragma: no cover
                    # AnyNode is exactly Task | Chain | Group, all three handled above;
                    # this exists so a future node type is a loud typed failure here
                    # instead of a silent match fallthrough leaving dispatch/bubbled
                    # unbound below. Provably unreachable given the closed union —
                    # excluded from coverage rather than tested, same as any other
                    # exhaustiveness guard.
                    assert_never(unreachable)

            if dispatch is not None:
                return [dispatch]
            if bubbled is None:
                return []

            await self._record(canvas_id, parent.id, bubbled)
            current_id, current_outcome, parent_id = parent.id, bubbled, parent.parent_id

        final = (
            CanvasStatus.ERROR
            if current_outcome.status == NodeStatus.ERROR
            else CanvasStatus.FINISHED
        )
        await self.store.set_canvas_status(canvas_id, final)
        return []

    async def _advance_chain(
        self,
        canvas_id: str,
        chain: ChainNode,
        finished_child_id: str,
        outcome: NodeOutcome,
    ) -> tuple[Dispatch | None, NodeOutcome | None]:
        if outcome.status == NodeStatus.ERROR:
            remaining = self._chain_remaining(chain, finished_child_id)
            if chain.error_policy == ErrorPolicy.ABORT:
                await self._abort_canvas(canvas_id, remaining)
                return None, None
            if chain.error_policy == ErrorPolicy.PROPAGATE:
                await self.store.cancel_nodes(canvas_id, remaining)
                return None, NodeOutcome(
                    node_id=chain.id,
                    status=NodeStatus.ERROR,
                    error=outcome.error,
                )
            # CONTINUE: sequencing carries on despite the error.

        next_id = chain.next_id(finished_child_id)
        if next_id is None:
            final_status = (
                NodeStatus.ERROR if outcome.status == NodeStatus.ERROR else NodeStatus.FINISHED
            )
            return None, NodeOutcome(
                node_id=chain.id,
                status=final_status,
                result=outcome.result,
                error=outcome.error,
            )

        next_node = await self._require_node(canvas_id, next_id)
        if not isinstance(next_node, TaskNode):
            raise InvalidParentTypeError(node_id=next_node.id, canvas_id=canvas_id)
        return (
            Dispatch(
                topic=next_node.topic,
                node_id=next_node.id,
                canvas_id=canvas_id,
                body=outcome.result or "{}",
            ),
            None,
        )

    async def _advance_group(
        self,
        canvas_id: str,
        group: GroupNode,
        finished_child_id: str,
        outcome: NodeOutcome,
    ) -> tuple[Dispatch | None, NodeOutcome | None]:
        if outcome.status == NodeStatus.ERROR and group.error_policy == ErrorPolicy.ABORT:
            await self._abort_canvas(canvas_id, await self._unfinished(canvas_id, group))
            return None, None

        progress = await self.store.mark_child_done(
            canvas_id,
            group.id,
            finished_child_id,
            group.num_children,
        )
        if not progress.fired:
            return None, None

        results = await self.store.get_results(canvas_id, group.children)
        children: list[ChildResult] = []
        for child_id in group.children:  # declared order, not completion order
            result = results.get(child_id)
            children.append(
                ChildResult(
                    node_id=child_id,
                    ok=result is not None and result.ok,
                    value=result.result if result is not None else None,
                    error=result.error if result is not None else None,
                ),
            )
        fan_in = FanIn(children=children, input=group.input)

        if group.callback is not None:
            entry = await self._entry_task(canvas_id, group, group.callback)
            # group_id travels with the dispatch so a caller whose publish fails can
            # hand it to rollback() and release the fan-in guard this call just burned.
            return (
                Dispatch(
                    topic=entry.topic,
                    node_id=entry.id,
                    canvas_id=canvas_id,
                    body=fan_in.model_dump_json(),
                    group_id=group.id,
                ),
                None,
            )

        any_error = any(not child.ok for child in children)
        final_status = (
            NodeStatus.ERROR
            if any_error and group.error_policy != ErrorPolicy.CONTINUE
            else NodeStatus.FINISHED
        )
        # No callback means nothing reads this group's aggregate — carrying the full
        # FanIn forward as `.result` is what makes nested-group fan-in re-encode an
        # already-encoded string at every level (O(2^depth); see bug #15). A caller
        # that genuinely needs a nested leg's own children's outcomes can query the
        # store directly via get_results(canvas_id, group.children).
        return None, NodeOutcome(node_id=group.id, status=final_status, result=None)

    async def _entry_task(self, canvas_id: str, group: GroupNode, callback_id: str) -> TaskNode:
        """Resolve a group's callback down to the concrete task to dispatch to."""
        node = await self._require_callback(canvas_id, group, callback_id)
        if isinstance(node, TaskNode):
            return node
        if isinstance(node, ChainNode):
            first = await self._require_node(canvas_id, node.children[0])
            if not isinstance(first, TaskNode):
                raise InvalidParentTypeError(node_id=first.id, canvas_id=canvas_id)
            return first
        raise InvalidParentTypeError(node_id=node.id, canvas_id=canvas_id)

    async def _abort_canvas(self, canvas_id: str, remaining: Sequence[str]) -> None:
        if remaining:
            await self.store.cancel_nodes(canvas_id, remaining)
        await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)

    async def _record(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        if outcome.result is not None:
            size = len(outcome.result.encode())
            if size > self.max_result_bytes:
                raise ResultTooLargeError(node_id=node_id, size=size, limit=self.max_result_bytes)
        await self.store.set_result(canvas_id, node_id, outcome)
        await self.store.set_node_status(canvas_id, node_id, outcome.status)

    async def _require_node(self, canvas_id: str, node_id: str) -> AnyNode:
        node = await self.store.get_node(canvas_id, node_id)
        if node is None:
            raise NodeNotFoundError(node_id=node_id, canvas_id=canvas_id)
        return node

    async def _require_parent(self, canvas_id: str, node_id: str, parent_id: str) -> AnyNode:
        parent = await self.store.get_node(canvas_id, parent_id)
        if parent is None:
            raise ParentNotFoundError(node_id=node_id, parent_id=parent_id, canvas_id=canvas_id)
        return parent

    async def _require_callback(
        self,
        canvas_id: str,
        group: GroupNode,
        callback_id: str,
    ) -> AnyNode:
        node = await self.store.get_node(canvas_id, callback_id)
        if node is None:
            raise CallbackNotFoundError(
                group_id=group.id,
                callback_id=callback_id,
                canvas_id=canvas_id,
            )
        return node

    async def _unfinished(self, canvas_id: str, group: GroupNode) -> list[str]:
        """Return the group's children that have no recorded outcome yet.

        Cancelling every sibling indiscriminately would overwrite legs that already
        FINISHED with CANCELLED, destroying the record that they ran — legs whose
        side effects really happened. Only what never completed can be cancelled.
        ``_chain_remaining`` gets the equivalent right by slicing the not-yet-run
        tail; a group has no ordering to slice, so it asks the store instead.
        """
        results = await self.store.get_results(canvas_id, group.children)
        return [child_id for child_id in group.children if child_id not in results]

    @staticmethod
    def _chain_remaining(chain: ChainNode, finished_child_id: str) -> list[str]:
        idx = chain.children.index(finished_child_id)
        return chain.children[idx + 1 :]
