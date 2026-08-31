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
from dataclasses import replace
from typing import Final, assert_never

from mint.logger import get_logger
from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.models import (
    AnyNode,
    ChainNode,
    ChildResult,
    FanIn,
    GroupNode,
    NodeOutcome,
    TaskNode,
    TerminalStatus,
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

logger = get_logger(__name__)


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

        Note the limit of that guarantee: only *group fan-in* is deduplicated, by
        the fired guard. **Chain sequencing is not.** Replaying a chain step's
        outcome dispatches the next step again — so a step whose ack fails after
        the canvas advanced re-runs its successor. That is inherent to
        at-least-once without an idempotency key on the work itself; a chain step
        that must not run twice needs to be idempotent in its own right.
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

        Releases every guard the call burned, not only the dispatching group's — a
        single walk can claim an inner group's terminal slot and then fire an outer
        group's callback, and leaving the inner one burned strands the redelivery
        just as surely.
        """
        for dispatch in dispatches:
            for group_id in dispatch.claimed_groups:
                await self.store.reset_group_fired(dispatch.canvas_id, group_id)

    async def _complete(
        self,
        canvas_id: str,
        node_id: str,
        outcome: NodeOutcome,
    ) -> list[Dispatch]:
        node = await self._require_node(canvas_id, node_id)
        if node.status == NodeStatus.CANCELLED:
            # Already-dispatched work under a cancelled subtree still reports when it
            # finishes. Advancing from it dispatches the *next* step of a branch that
            # was cancelled — side effects happening after the cancellation was
            # recorded. Recording its outcome would also overwrite the CANCELLED
            # status that says why it stopped.
            logger.info(
                "Discarding an outcome for a cancelled node",
                node_id=node_id,
                canvas_id=canvas_id,
            )
            return []
        await self._record(canvas_id, node_id, outcome)

        visited = {node_id}
        # Every group guard burned during this walk, so a caller whose publish fails
        # can release all of them rather than only the one that dispatched.
        claimed: list[str] = []
        try:
            return await self._walk(canvas_id, node, outcome, visited, claimed)
        except BaseException:
            # A guard burned mid-walk stays burned unless something releases it. Only
            # WorkerError is handled above; a driver-level error (a Redis blip during
            # `_record`, say) escapes to the caller's catch-all, which requeues — and
            # the redelivery then finds the guard burned, dispatches nothing, and acks.
            # The canvas is RUNNING forever with its callback never sent. This is the
            # same release `rollback()` performs for a failed publish.
            await self._release(canvas_id, claimed)
            raise

    async def _release(self, canvas_id: str, group_ids: Sequence[str]) -> None:
        """Release fan-in guards, never masking the failure that prompted it."""
        for group_id in group_ids:
            try:
                await self.store.reset_group_fired(canvas_id, group_id)
            except Exception:
                logger.exception(
                    "Could not release a fan-in guard",
                    canvas_id=canvas_id,
                    group_id=group_id,
                )

    async def _walk(
        self,
        canvas_id: str,
        node: AnyNode,
        outcome: NodeOutcome,
        visited: set[str],
        claimed: list[str],
    ) -> list[Dispatch]:
        """Walk from a completed node up to its root, dispatching whatever comes next."""
        current_id, current_outcome, parent_id = node.id, outcome, node.parent_id

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
                    bubbled = await self._callback_outcome(canvas_id, parent, current_outcome)
                case GroupNode():
                    dispatch, bubbled = await self._advance_group(
                        canvas_id,
                        parent,
                        current_id,
                        current_outcome,
                        claimed,
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
                return [replace(dispatch, claimed_groups=tuple(claimed))]
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
                # Recorded *before* the canvas goes terminal, not after. A terminal
                # canvas status makes RedisCanvasStore expire every key it tracks —
                # so a later write lands a plain SET on a node key (clearing the TTL
                # just applied) and a result key into an already-expiring registry.
                # Both then live forever, unreclaimed and unreferenced.
                #
                # Recorded but not bubbled: ABORT ends the canvas, so nothing above
                # should advance — but the chain itself must not be left reading
                # PENDING with no result while its children are ERROR/CANCELLED.
                # PROPAGATE already records this by bubbling.
                await self._record(
                    canvas_id,
                    chain.id,
                    NodeOutcome(node_id=chain.id, status=NodeStatus.ERROR, error=outcome.error),
                )
                await self._abort_canvas(canvas_id, remaining)
                return None, None
            if chain.error_policy == ErrorPolicy.PROPAGATE:
                await self._cancel_subtrees(canvas_id, remaining)
                return None, NodeOutcome(
                    node_id=chain.id,
                    status=NodeStatus.ERROR,
                    error=outcome.error,
                )
            # CONTINUE: sequencing carries on despite the error.

        next_id = chain.next_id(finished_child_id)
        if next_id is None:
            final_status: TerminalStatus = (
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
        claimed: list[str],
    ) -> tuple[Dispatch | None, NodeOutcome | None]:
        terminal = await self._apply_group_error_policy(
            canvas_id,
            group,
            finished_child_id,
            outcome,
            claimed,
        )
        if terminal is not None:
            return terminal

        progress = await self.store.mark_child_done(
            canvas_id,
            group.id,
            finished_child_id,
            group.num_children,
        )
        if not progress.fired:
            return None, None
        claimed.append(group.id)

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
            return (
                Dispatch(
                    topic=entry.topic,
                    node_id=entry.id,
                    canvas_id=canvas_id,
                    body=fan_in.model_dump_json(),
                ),
                None,
            )

        any_error = any(not child.ok for child in children)
        final_status: TerminalStatus = (
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
        await self._cancel_subtrees(canvas_id, remaining)
        await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)

    async def _cancel_subtrees(self, canvas_id: str, node_ids: Sequence[str]) -> None:
        """Cancel these nodes and everything beneath them.

        Marking only the named node leaves a compound leg's children untouched, so a
        step already in flight inside a cancelled chain completes, is recorded, and
        dispatches the next step — work running, and having side effects, under a
        branch the engine has already given up on.
        """
        if not node_ids:
            return
        await self.store.cancel_nodes(canvas_id, await self._descendants(canvas_id, node_ids))

    async def _descendants(self, canvas_id: str, node_ids: Sequence[str]) -> list[str]:
        """Return ``node_ids`` plus every node underneath them, breadth-first."""
        collected: list[str] = []
        seen: set[str] = set()
        queue = list(node_ids)
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            collected.append(current)
            node = await self.store.get_node(canvas_id, current)
            match node:
                case ChainNode() | GroupNode():
                    queue.extend(node.children)
                    if isinstance(node, GroupNode) and node.callback is not None:
                        queue.append(node.callback)
                case _:
                    pass
        return collected

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

    async def _callback_outcome(
        self,
        canvas_id: str,
        group: GroupNode,
        outcome: NodeOutcome,
    ) -> NodeOutcome | None:
        """Turn a callback's own outcome into its group's, honouring the error policy.

        ``error_policy`` was only ever consulted for *leg* completions, so a group
        whose callback failed bubbled that failure regardless — which happens to
        match PROPAGATE, and silently downgrades ABORT to it. ABORT promises to mark
        the whole canvas ERROR immediately so nothing further dispatches; without
        this an enclosing CONTINUE container carried on and fired its own callback.
        """
        if outcome.status == NodeStatus.ERROR and group.error_policy == ErrorPolicy.ABORT:
            await self._record(
                canvas_id,
                group.id,
                NodeOutcome(node_id=group.id, status=NodeStatus.ERROR, error=outcome.error),
            )
            await self._abort_canvas(canvas_id, [])
            return None
        return outcome.model_copy(update={"node_id": group.id})

    async def _apply_group_error_policy(
        self,
        canvas_id: str,
        group: GroupNode,
        finished_child_id: str,
        outcome: NodeOutcome,
        claimed: list[str],
    ) -> tuple[Dispatch | None, NodeOutcome | None] | None:
        """Handle a failed child under ABORT/PROPAGATE, or None if the group continues.

        Both branches end the group early, before ``mark_child_done`` — which is a
        group's only de-duplication — so each claims the group's single terminal
        slot first. Two legs failing concurrently would otherwise each act on it:
        under PROPAGATE both bubble a group-level ERROR and the enclosing container
        advances twice.
        """
        if outcome.status != NodeStatus.ERROR:
            return None
        if group.error_policy == ErrorPolicy.ABORT:
            if not await self.store.claim_group_terminal(canvas_id, group.id):
                return None, None
            claimed.append(group.id)
            # Cancels the callback along with the unfinished legs — an aborting group
            # never dispatches it, so leaving it PENDING misreports it as expected.
            await self._cancel_group_remainder(canvas_id, group, finished_child_id)
            # Same reasoning as the chain's ABORT branch: record, don't bubble — and
            # record before the canvas goes terminal, or the write outlives the TTL
            # sweep it should have been part of.
            await self._record(
                canvas_id,
                group.id,
                NodeOutcome(node_id=group.id, status=NodeStatus.ERROR, error=outcome.error),
            )
            await self.store.set_canvas_status(canvas_id, CanvasStatus.ERROR)
            return None, None
        if group.error_policy == ErrorPolicy.PROPAGATE:
            if not await self.store.claim_group_terminal(canvas_id, group.id):
                return None, None
            claimed.append(group.id)
            # Mirrors _advance_chain's PROPAGATE branch. Without this the policy was
            # only ever consulted on the ABORT pre-check and the callback-less
            # final_status, so a group *with* a callback treated PROPAGATE exactly
            # like CONTINUE: the callback fired with the failed leg present, the
            # group's own outcome became the callback's, and the enclosing container
            # advanced as though nothing had failed.
            await self._cancel_group_remainder(canvas_id, group, finished_child_id)
            return None, NodeOutcome(
                node_id=group.id,
                status=NodeStatus.ERROR,
                error=outcome.error,
            )
        return None

    async def _cancel_group_remainder(
        self,
        canvas_id: str,
        group: GroupNode,
        finished_child_id: str,
    ) -> None:
        """Cancel everything in ``group`` that will now never run: legs and callback.

        The callback is included because a propagating group never dispatches it —
        leaving it PENDING would misreport it as still expected.
        """
        remaining = [
            child_id
            for child_id in await self._unfinished(canvas_id, group)
            if child_id != finished_child_id
        ]
        if group.callback is not None:
            remaining.append(group.callback)
        await self._cancel_subtrees(canvas_id, remaining)

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
        return chain.children[chain.index_of(finished_child_id) + 1 :]
