"""CanvasEngine: pure transition rules, hand-built graphs, no I/O.

Every test builds its graph directly against the store (bypassing the
builder DSL) so each one exercises exactly the transition rule under test.
Builder-specific concerns (entry-dispatch targeting, flattening, id
collisions, publish ordering) live in test_builder.py instead.
"""

import asyncio
import sys

import pytest

from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import (
    AnyNode,
    ChainNode,
    ErrorInfo,
    FanIn,
    GroupNode,
    NodeOutcome,
    TaskNode,
)
from mint.worker.enums import CanvasStatus, ErrorPolicy, NodeStatus
from mint.worker.exc import (
    CallbackNotFoundError,
    CanvasCycleError,
    ChildNotInParentError,
    InvalidParentTypeError,
    NodeNotFoundError,
    ParentNotFoundError,
    ResultTooLargeError,
)
from mint.worker.stores.memory import MemoryCanvasStore

CANVAS = "c1"


def ok_outcome(node_id: str, result: str = "{}") -> NodeOutcome:
    """Build a FINISHED outcome for ``node_id``."""
    return NodeOutcome(node_id=node_id, status=NodeStatus.FINISHED, result=result)


def err_outcome(node_id: str, message: str = "boom") -> NodeOutcome:
    """Build an ERROR outcome for ``node_id``."""
    return NodeOutcome(
        node_id=node_id,
        status=NodeStatus.ERROR,
        error=ErrorInfo(type="TestError", message=message),
    )


async def seed(store: MemoryCanvasStore, *nodes: AnyNode) -> None:
    """Persist a hand-built graph in one call, as the builder would."""
    await store.create_canvas(CANVAS, {node.id: node for node in nodes})


def task(node_id: str, parent_id: str | None, topic: str = "topic") -> TaskNode:
    """Build a TaskNode with the given id/parent, defaulting its topic."""
    return TaskNode(id=node_id, canvas_id=CANVAS, parent_id=parent_id, topic=f"{topic}-{node_id}")


class TestChain:
    """Chain sequencing: order, root results, and error policies."""

    async def test_linear_three_node_chain_runs_in_order(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Each hop dispatches to the next node with the previous node's result as body."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1", "t2", "t3"],
        )
        await seed(store, task("t1", "chain"), task("t2", "chain"), task("t3", "chain"), chain)

        first = await engine.complete(CANVAS, "t1", ok_outcome("t1", '{"v":1}'))
        assert first == [
            Dispatch(topic="topic-t2", node_id="t2", canvas_id=CANVAS, body='{"v":1}'),
        ]

        second = await engine.complete(CANVAS, "t2", ok_outcome("t2", '{"v":2}'))
        assert second == [
            Dispatch(topic="topic-t3", node_id="t3", canvas_id=CANVAS, body='{"v":2}'),
        ]

        third = await engine.complete(CANVAS, "t3", ok_outcome("t3", '{"v":3}'))
        assert third == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED
        chain_result = await store.get_result(CANVAS, "chain")
        assert chain_result is not None
        assert chain_result.result == '{"v":3}'

    async def test_single_step_chain_reports_to_its_parent(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A chain of length one still records its own aggregate result."""
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["t1"])
        await seed(store, task("t1", "chain"), chain)

        result = await engine.complete(CANVAS, "t1", ok_outcome("t1"))

        assert result == []
        chain_result = await store.get_result(CANVAS, "chain")
        assert chain_result is not None
        assert chain_result.ok

    async def test_bare_root_task_still_records_its_result(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A single root task (no parent at all) must not be dropped. Regression for bug #5."""
        await seed(store, task("only", None))

        result = await engine.complete(CANVAS, "only", ok_outcome("only", '{"done":true}'))

        assert result == []
        stored = await store.get_result(CANVAS, "only")
        assert stored is not None
        assert stored.result == '{"done":true}'
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED

    async def test_middle_error_under_propagate_cancels_remainder(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """PROPAGATE stops the chain, cancels what has not run, and errors the canvas."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1", "t2", "t3"],
            error_policy=ErrorPolicy.PROPAGATE,
        )
        await seed(store, task("t1", "chain"), task("t2", "chain"), task("t3", "chain"), chain)
        await engine.complete(CANVAS, "t1", ok_outcome("t1"))

        result = await engine.complete(CANVAS, "t2", err_outcome("t2"))

        assert result == []
        t3 = await store.get_node(CANVAS, "t3")
        assert t3 is not None
        assert t3.status == NodeStatus.CANCELLED
        chain_result = await store.get_result(CANVAS, "chain")
        assert chain_result is not None
        assert chain_result.status == NodeStatus.ERROR
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_middle_error_under_continue_keeps_the_chain_going(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """CONTINUE ignores the error and dispatches to the next step regardless."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1", "t2", "t3"],
            error_policy=ErrorPolicy.CONTINUE,
        )
        await seed(store, task("t1", "chain"), task("t2", "chain"), task("t3", "chain"), chain)
        await engine.complete(CANVAS, "t1", ok_outcome("t1"))

        result = await engine.complete(CANVAS, "t2", err_outcome("t2"))

        assert result == [
            Dispatch(topic="topic-t3", node_id="t3", canvas_id=CANVAS, body="{}"),
        ]
        t3 = await store.get_node(CANVAS, "t3")
        assert t3 is not None
        assert t3.status == NodeStatus.PENDING

    async def test_last_step_error_under_abort_has_no_siblings_left_to_cancel(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """ABORT on a chain's only step must still error the canvas with nothing to cancel."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1"],
            error_policy=ErrorPolicy.ABORT,
        )
        await seed(store, task("t1", "chain"), chain)

        result = await engine.complete(CANVAS, "t1", err_outcome("t1"))

        assert result == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR
        # The chain itself is recorded ERROR: it really did fail, and leaving it
        # PENDING while its children read ERROR/CANCELLED misreports the compound
        # node as never started. Recorded, not bubbled — ABORT ends the canvas, so
        # nothing above it should advance.
        chain_outcome = await store.get_result(CANVAS, "chain")
        assert chain_outcome is not None
        assert chain_outcome.status == NodeStatus.ERROR

    async def test_middle_error_under_abort_stops_everything_immediately(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """ABORT cancels the remainder and fails the canvas with no further dispatch."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1", "t2", "t3"],
            error_policy=ErrorPolicy.ABORT,
        )
        await seed(store, task("t1", "chain"), task("t2", "chain"), task("t3", "chain"), chain)
        await engine.complete(CANVAS, "t1", ok_outcome("t1"))

        result = await engine.complete(CANVAS, "t2", err_outcome("t2"))

        assert result == []
        t3 = await store.get_node(CANVAS, "t3")
        assert t3 is not None
        assert t3.status == NodeStatus.CANCELLED
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR
        # The chain itself is recorded ERROR: it really did fail, and leaving it
        # PENDING while its children read ERROR/CANCELLED misreports the compound
        # node as never started. Recorded, not bubbled — ABORT ends the canvas, so
        # nothing above it should advance.
        chain_outcome = await store.get_result(CANVAS, "chain")
        assert chain_outcome is not None
        assert chain_outcome.status == NodeStatus.ERROR


class TestGroup:
    """Chord fan-in: ordering, idempotency, error policies, and no-callback propagation."""

    def _group(
        self,
        children: list[str],
        callback: str | None,
        *,
        parent_id: str | None = None,
        error_policy: ErrorPolicy = ErrorPolicy.CONTINUE,
    ) -> GroupNode:
        return GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=parent_id,
            children=children,
            callback=callback,
            error_policy=error_policy,
        )

    async def test_callback_fires_once_in_declared_order_not_completion_order(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Completion order is leg2, leg1, leg3; the callback body must list leg1, leg2, leg3."""
        group = self._group(["leg1", "leg2", "leg3"], callback="cb")
        cb = task("cb", "g", topic="callback")
        legs = [task(leg, "g") for leg in ("leg1", "leg2", "leg3")]
        await seed(store, *legs, cb, group)

        r1 = await engine.complete(CANVAS, "leg2", ok_outcome("leg2", '"leg2"'))
        r2 = await engine.complete(CANVAS, "leg1", ok_outcome("leg1", '"leg1"'))
        r3 = await engine.complete(CANVAS, "leg3", ok_outcome("leg3", '"leg3"'))

        assert r1 == []
        assert r2 == []
        assert len(r3) == 1
        fan_in = FanIn.model_validate_json(r3[0].body)
        assert [child.node_id for child in fan_in.children] == ["leg1", "leg2", "leg3"]
        assert [child.value for child in fan_in.children] == ['"leg1"', '"leg2"', '"leg3"']

    async def test_callback_fires_once_regardless_of_reverse_completion_order(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Completing leg3, leg2, leg1 still yields declared order leg1, leg2, leg3."""
        group = self._group(["leg1", "leg2", "leg3"], callback="cb")
        cb = task("cb", "g", topic="callback")
        legs = [task(leg, "g") for leg in ("leg1", "leg2", "leg3")]
        await seed(store, *legs, cb, group)

        await engine.complete(CANVAS, "leg3", ok_outcome("leg3"))
        await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))
        final = await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))

        assert len(final) == 1
        fan_in = FanIn.model_validate_json(final[0].body)
        assert [child.node_id for child in fan_in.children] == ["leg1", "leg2", "leg3"]

    async def test_duplicate_delivery_of_a_leg_is_not_double_counted(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Redelivering leg1's outcome must not fire the callback early. Regression for bug #3."""
        group = self._group(["leg1", "leg2"], callback="cb")
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        duplicate = await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        final = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert duplicate == []
        assert len(final) == 1

    async def test_duplicate_delivery_of_the_final_leg_fires_callback_only_once(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Redelivering the leg that completes the group must not fire the callback twice."""
        group = self._group(["leg1", "leg2"], callback="cb")
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        first = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))
        second = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert len(first) == 1
        assert second == []

    async def test_concurrent_completion_of_the_last_two_legs_fires_exactly_once(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Two legs racing to be 'the last one' under real concurrency yield one dispatch total."""
        group = self._group(["leg1", "leg2"], callback="cb")
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        results = await asyncio.gather(
            engine.complete(CANVAS, "leg1", ok_outcome("leg1")),
            engine.complete(CANVAS, "leg2", ok_outcome("leg2")),
        )

        total_dispatches = sum(len(r) for r in results)
        assert total_dispatches == 1

    async def test_a_callback_dispatch_names_the_guard_it_burned(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Only a callback dispatch is rollback-able, so only it names its group."""
        group = self._group(["leg1"], callback="cb")
        await seed(store, task("leg1", "g"), task("cb", "g", topic="callback"), group)

        dispatches = await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))

        assert dispatches[0].claimed_groups == ("g",)

    async def test_a_chain_dispatch_claims_nothing(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A chain's next-step dispatch burns no fan-in guard, so there is nothing to roll back."""
        chain = ChainNode(id="ch", canvas_id=CANVAS, parent_id=None, children=["s1", "s2"])
        await seed(store, task("s1", "ch"), task("s2", "ch"), chain)

        dispatches = await engine.complete(CANVAS, "s1", ok_outcome("s1"))

        assert dispatches[0].claimed_groups == ()

    async def test_rollback_lets_a_redelivered_leg_re_fire_a_callback_that_never_published(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The lost-callback bug: an authorised dispatch that fails to publish must survive.

        complete() burns the fan-in guard before its caller ever gets to publish.
        Without rollback the redelivery finds the guard burned, dispatches nothing,
        and the caller acks — stranding the chord's callback forever.
        """
        group = self._group(["leg1", "leg2"], callback="cb")
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        authorised = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))
        # the caller's publish raised, so it rolls back and nacks for redelivery
        await engine.rollback(authorised)
        redelivered = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert len(authorised) == 1
        assert len(redelivered) == 1
        assert redelivered[0].node_id == "cb"

    async def test_without_a_rollback_a_redelivered_leg_still_fires_only_once(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Dropping `added` from the fire predicate must not weaken bug #3's guarantee."""
        group = self._group(["leg1", "leg2"], callback="cb")
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))
        replays = [
            await engine.complete(CANVAS, "leg1", ok_outcome("leg1")),
            await engine.complete(CANVAS, "leg2", ok_outcome("leg2")),
        ]

        assert all(replay == [] for replay in replays)

    async def test_rollback_ignores_dispatches_with_no_group(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A chain dispatch has no fan-in guard behind it — rolling it back is a no-op."""
        await seed(store, task("solo", None))

        await engine.rollback(
            [Dispatch(topic="t", node_id="solo", canvas_id=CANVAS, body="{}")],
        )

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.RUNNING

    async def test_continue_policy_fires_callback_with_failed_children_present(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """CONTINUE still reaches num_children and fires, carrying the failure through.

        This is the exact scenario that forces easyrag's BoxReferenceSyncHandler to call
        the private ``_check_next_step`` from ``on_failure`` today.
        """
        group = self._group(["leg1", "leg2"], callback="cb", error_policy=ErrorPolicy.CONTINUE)
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", err_outcome("leg1", "download failed"))
        final = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert len(final) == 1
        fan_in = FanIn.model_validate_json(final[0].body)
        by_id = {child.node_id: child for child in fan_in.children}
        assert by_id["leg1"].ok is False
        assert by_id["leg1"].error is not None
        assert by_id["leg1"].error.message == "download failed"
        assert by_id["leg2"].ok is True

    async def test_all_children_erroring_still_fires_the_callback(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Every leg failing must not silently skip the callback."""
        group = self._group(["leg1", "leg2"], callback="cb", error_policy=ErrorPolicy.CONTINUE)
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            group,
        )

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))
        final = await engine.complete(CANVAS, "leg2", err_outcome("leg2"))

        assert len(final) == 1
        fan_in = FanIn.model_validate_json(final[0].body)
        assert all(not child.ok for child in fan_in.children)

    async def test_abort_policy_cancels_pending_legs_and_never_fires_callback(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """ABORT cancels whatever has not run, errors the canvas, and skips the callback."""
        group = self._group(
            ["leg1", "leg2", "leg3"],
            callback="cb",
            error_policy=ErrorPolicy.ABORT,
        )
        legs = [task(leg, "g") for leg in ("leg1", "leg2", "leg3")]
        await seed(store, *legs, task("cb", "g", topic="callback"), group)
        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))

        result = await engine.complete(CANVAS, "leg2", err_outcome("leg2"))

        assert result == []
        leg3 = await store.get_node(CANVAS, "leg3")
        assert leg3 is not None
        assert leg3.status == NodeStatus.CANCELLED
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR
        # Recorded ERROR for the same reason the chain's ABORT branch records one.
        group_outcome = await store.get_result(CANVAS, "g")
        assert group_outcome is not None
        assert group_outcome.status == NodeStatus.ERROR

        # The canvas is now terminal: a late leg3 delivery must be a pure no-op.
        late = await engine.complete(CANVAS, "leg3", ok_outcome("leg3"))
        assert late == []

    async def test_no_callback_group_propagates_its_own_completion_to_its_parent(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A callback-less group finishes as a unit and advances whatever contains it.

        Regression for bug #2: the original engine returned early here and the enclosing
        chain (or an outer chord) never advanced.

        The dispatch body carries no payload (bug #15's fix: a callback-less group's
        own result is never the encoded FanIn) — a downstream step that needs the
        legs' actual outcomes queries the store directly, which this also verifies.
        """
        outer_chain = ChainNode(
            id="post",
            canvas_id=CANVAS,
            parent_id=None,
            children=["g", "t_after"],
        )
        group = self._group(["leg1", "leg2"], callback=None, parent_id="post")
        t_after = task("t_after", "post")
        await seed(store, task("leg1", "g"), task("leg2", "g"), t_after, group, outer_chain)

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        result = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert len(result) == 1
        dispatch = result[0]
        assert dispatch.node_id == "t_after"
        assert dispatch.topic == t_after.topic
        assert dispatch.body == "{}"
        leg_results = await store.get_results(CANVAS, ["leg1", "leg2"])
        assert {node_id for node_id, r in leg_results.items() if r.ok} == {"leg1", "leg2"}

    async def test_chord_nested_in_a_chord_increments_the_outer_group(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The inner chord's completion counts as exactly one leg of the outer chord."""
        outer = self._group(["leg_a", "inner"], callback=None, parent_id=None)
        inner = self._group(["leg_b", "leg_c"], callback=None, parent_id="outer")
        inner = inner.model_copy(update={"id": "inner"})
        outer = outer.model_copy(update={"id": "outer"})
        leg_a = task("leg_a", "outer")
        leg_b = TaskNode(id="leg_b", canvas_id=CANVAS, parent_id="inner", topic="topic-leg_b")
        leg_c = TaskNode(id="leg_c", canvas_id=CANVAS, parent_id="inner", topic="topic-leg_c")
        await seed(store, leg_a, leg_b, leg_c, inner, outer)

        await engine.complete(CANVAS, "leg_a", ok_outcome("leg_a"))
        await engine.complete(CANVAS, "leg_b", ok_outcome("leg_b"))
        final = await engine.complete(CANVAS, "leg_c", ok_outcome("leg_c"))

        assert final == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED
        outer_result = await store.get_result(CANVAS, "outer")
        assert outer_result is not None
        assert outer_result.ok
        assert outer_result.result is None  # bug #15: no callback, nothing carried forward
        leg_results = await store.get_results(CANVAS, ["leg_a", "inner"])
        assert leg_results["leg_a"].ok
        assert leg_results["inner"].ok

    async def test_chain_leg_counts_only_after_the_whole_chain_finishes(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A chord leg that is a chain must not increment fan-in until its last step finishes."""
        outer = self._group(["chain_leg", "plain_leg"], callback=None, parent_id=None)
        chain_leg = ChainNode(
            id="chain_leg",
            canvas_id=CANVAS,
            parent_id="g",
            children=["t1", "t2"],
        )
        t1 = TaskNode(id="t1", canvas_id=CANVAS, parent_id="chain_leg", topic="topic-t1")
        t2 = TaskNode(id="t2", canvas_id=CANVAS, parent_id="chain_leg", topic="topic-t2")
        plain_leg = task("plain_leg", "g")
        await seed(store, t1, t2, plain_leg, chain_leg, outer)

        await engine.complete(CANVAS, "plain_leg", ok_outcome("plain_leg"))
        mid_chain = await engine.complete(CANVAS, "t1", ok_outcome("t1", '{"step":1}'))

        # Still mid-chain: must dispatch to t2, not fire the (already-1-of-2) group.
        assert len(mid_chain) == 1
        assert mid_chain[0].node_id == "t2"

        final = await engine.complete(CANVAS, "t2", ok_outcome("t2"))

        assert final == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED
        group_result = await store.get_result(CANVAS, "g")
        assert group_result is not None
        assert group_result.result is None  # bug #15: no callback, nothing carried forward
        leg_results = await store.get_results(CANVAS, ["chain_leg", "plain_leg"])
        assert all(r.ok for r in leg_results.values())

    async def test_single_child_group_fires_on_first_completion(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """num_children == 1 must fire on the very first (and only) leg."""
        group = self._group(["only_leg"], callback="cb")
        await seed(store, task("only_leg", "g"), task("cb", "g", topic="callback"), group)

        result = await engine.complete(CANVAS, "only_leg", ok_outcome("only_leg"))

        assert len(result) == 1
        assert result[0].node_id == "cb"


class TestGroupPropagatePolicy:
    """PROPAGATE must actually propagate for a group, callback or not."""

    def _group(self, *, callback: str | None) -> GroupNode:
        return GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id="chain",
            children=["leg1", "leg2"],
            callback=callback,
            error_policy=ErrorPolicy.PROPAGATE,
        )

    async def _seed(self, store: MemoryCanvasStore, *, callback: str | None) -> None:
        nodes: list[AnyNode] = [
            task("leg1", "g"),
            task("leg2", "g"),
            self._group(callback=callback),
            task("after", "chain", topic="after"),
            ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["g", "after"]),
        ]
        if callback is not None:
            nodes.append(task(callback, "g", topic="callback"))
        await seed(store, *nodes)

    async def test_a_failed_leg_does_not_dispatch_the_callback(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """error_policy was only read on the ABORT pre-check and the no-callback path.

        A group *with* a callback therefore treated PROPAGATE exactly like
        CONTINUE — the callback fired with the failed leg present as ok=False.
        """
        await self._seed(store, callback="cb")

        dispatches = await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        assert dispatches == []

    async def test_a_failed_leg_records_the_group_as_errored(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The group's own outcome used to become the callback's — a success."""
        await self._seed(store, callback="cb")

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        recorded = await store.get_result(CANVAS, "g")
        assert recorded is not None
        assert recorded.status == NodeStatus.ERROR

    async def test_a_failed_leg_cancels_the_pending_legs_and_the_callback(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Nothing in a propagating group runs after the failure, so nothing stays PENDING."""
        await self._seed(store, callback="cb")

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        for node_id in ("leg2", "cb"):
            node = await store.get_node(CANVAS, node_id)
            assert node is not None
            assert node.status == NodeStatus.CANCELLED

    async def test_the_failure_reaches_the_enclosing_chain(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The enclosing chain used to advance to its next step as if nothing failed."""
        await self._seed(store, callback="cb")

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        after = await store.get_node(CANVAS, "after")
        assert after is not None
        assert after.status == NodeStatus.CANCELLED
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_a_callback_less_group_still_propagates(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The no-callback path already honoured the policy; it must keep doing so."""
        await self._seed(store, callback=None)

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        recorded = await store.get_result(CANVAS, "g")
        assert recorded is not None
        assert recorded.status == NodeStatus.ERROR

    async def test_a_successful_leg_under_propagate_is_unaffected(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """PROPAGATE only changes what an *error* means — success still fans in."""
        await self._seed(store, callback="cb")

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        dispatches = await engine.complete(CANVAS, "leg2", ok_outcome("leg2"))

        assert len(dispatches) == 1
        assert dispatches[0].node_id == "cb"


class TestGroupAbortPreservesFinishedLegs:
    """ABORT must cancel only what never ran, never overwrite a real outcome."""

    def _group(self, children: list[str]) -> GroupNode:
        return GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=children,
            callback=None,
            error_policy=ErrorPolicy.ABORT,
        )

    async def test_abort_leaves_an_already_finished_sibling_finished(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A leg that already succeeded really ran — CANCELLED would erase that record."""
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("leg3", "g"),
            self._group(["leg1", "leg2", "leg3"]),
        )
        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))

        await engine.complete(CANVAS, "leg2", err_outcome("leg2"))

        finished = await store.get_node(CANVAS, "leg1")
        assert finished is not None
        assert finished.status == NodeStatus.FINISHED

    async def test_abort_still_cancels_the_legs_that_never_ran(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Preserving finished legs must not stop the genuinely pending ones being cancelled."""
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("leg3", "g"),
            self._group(["leg1", "leg2", "leg3"]),
        )
        await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))

        await engine.complete(CANVAS, "leg2", err_outcome("leg2"))

        pending = await store.get_node(CANVAS, "leg3")
        assert pending is not None
        assert pending.status == NodeStatus.CANCELLED
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_abort_does_not_cancel_the_failed_leg_itself(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The leg that triggered the abort has its own ERROR outcome to keep."""
        await seed(store, task("leg1", "g"), task("leg2", "g"), self._group(["leg1", "leg2"]))

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        failed = await store.get_node(CANVAS, "leg1")
        assert failed is not None
        assert failed.status == NodeStatus.ERROR


class TestMalformedGraph:
    """Lookup failures against a corrupted or incomplete graph."""

    async def test_unknown_node_id_raises_and_errors_the_canvas(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A completion for a node the store has never heard of must not be swallowed."""
        with pytest.raises(NodeNotFoundError):
            await engine.complete(CANVAS, "ghost", ok_outcome("ghost"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_missing_parent_raises_and_errors_the_canvas(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A node whose declared parent was never persisted must raise, not return silently."""
        await seed(store, task("t1", "ghost_parent"))

        with pytest.raises(ParentNotFoundError):
            await engine.complete(CANVAS, "t1", ok_outcome("t1"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_missing_callback_raises_and_errors_the_canvas(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A group whose callback id does not resolve must raise once fan-in fires."""
        group = GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1"],
            callback="ghost_cb",
        )
        await seed(store, task("leg1", "g"), group)

        with pytest.raises(CallbackNotFoundError):
            await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_parent_that_is_a_task_node_is_rejected(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A node whose declared parent is itself a plain task (which can't own children)."""
        bad_parent = task("bad_parent", None)
        child = task("child", "bad_parent")
        await seed(store, bad_parent, child)

        with pytest.raises(InvalidParentTypeError):
            await engine.complete(CANVAS, "child", ok_outcome("child"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_chain_next_step_that_is_not_a_task_is_rejected(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A chain whose declared next child is a group (not a flattened task) must raise."""
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["t1", "not_a_task"],
        )
        not_a_task = GroupNode(
            id="not_a_task",
            canvas_id=CANVAS,
            parent_id="chain",
            children=["x"],
            callback=None,
        )
        await seed(store, task("t1", "chain"), not_a_task, chain)

        with pytest.raises(InvalidParentTypeError):
            await engine.complete(CANVAS, "t1", ok_outcome("t1"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_callback_chain_whose_first_step_is_not_a_task_is_rejected(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A Chain callback whose first declared child is a group (malformed graph)."""
        group = GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1"],
            callback="cb_chain",
        )
        cb_chain = ChainNode(
            id="cb_chain",
            canvas_id=CANVAS,
            parent_id="g",
            children=["not_a_task"],
        )
        not_a_task = GroupNode(
            id="not_a_task",
            canvas_id=CANVAS,
            parent_id="cb_chain",
            children=["x"],
            callback=None,
        )
        await seed(store, task("leg1", "g"), not_a_task, cb_chain, group)

        with pytest.raises(InvalidParentTypeError):
            await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_callback_that_is_a_group_is_rejected(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A group cannot itself be another group's callback (malformed graph)."""
        group = GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1"],
            callback="cb_group",
        )
        cb_group = GroupNode(
            id="cb_group",
            canvas_id=CANVAS,
            parent_id="g",
            children=["x"],
            callback=None,
        )
        await seed(store, task("leg1", "g"), cb_group, group)

        with pytest.raises(InvalidParentTypeError):
            await engine.complete(CANVAS, "leg1", ok_outcome("leg1"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR

    async def test_completion_for_a_terminal_canvas_is_a_pure_no_op(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A late outcome for an already-finished/errored canvas must not mutate anything."""
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["t1", "t2"])
        await seed(store, task("t1", "chain"), task("t2", "chain"), chain)
        await store.set_canvas_status(CANVAS, CanvasStatus.ERROR)

        result = await engine.complete(CANVAS, "t1", ok_outcome("t1"))

        assert result == []
        assert await store.get_result(CANVAS, "t1") is None

    async def test_deeply_nested_graph_resolves_without_recursion(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Ancestor depth well past Python's default recursion limit still resolves in one call.

        The engine walks ancestors with an explicit while-loop rather than recursive
        method calls, so a depth comfortably above ``sys.getrecursionlimit()`` (1000
        by default) must still succeed. Depth alone is the proof: if ``_complete``
        ever regressed to one Python call per ancestor level, this would raise a
        natural ``RecursionError`` instead of completing.

        Deliberately does NOT mutate ``sys.setrecursionlimit`` — CPython's own
        machinery (asyncio's task stepping, pytest-asyncio, assertion rewriting,
        traceback formatting) needs far more headroom than a handful of levels, and
        starving it of that causes cascading RecursionErrors during cleanup instead
        of testing anything about this code.

        Deliberately pure chain-of-chains, not alternating chain/group: a ChainNode
        passes ``.result`` through unchanged, so nesting depth here is orthogonal to
        payload size. Nested *groups* have a separate, much smaller depth bound —
        see ``TestResultSizeGuard`` — because their fan-in used to double the result
        size at every level (bug #15). Mixing the two concerns in one test is what
        actually OOM-killed pytest twice while this suite was being written.
        """
        depth = sys.getrecursionlimit() + 500
        nodes: dict[str, AnyNode] = {}
        current_id = "leaf"
        nodes[current_id] = TaskNode(
            id=current_id,
            canvas_id=CANVAS,
            parent_id=None,
            topic="leaf-topic",
        )
        for level in range(depth):
            container_id = f"level{level}"
            nodes[current_id] = nodes[current_id].model_copy(update={"parent_id": container_id})
            nodes[container_id] = ChainNode(
                id=container_id,
                canvas_id=CANVAS,
                parent_id=None,
                children=[current_id],
            )
            current_id = container_id
        await store.create_canvas(CANVAS, nodes)

        result = await engine.complete(CANVAS, "leaf", ok_outcome("leaf"))

        assert result == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED

    async def test_cycle_in_the_graph_is_detected_and_raised(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A corrupted graph where two chains point at each other must not loop forever."""
        chain_a = ChainNode(id="chain_a", canvas_id=CANVAS, parent_id="chain_b", children=["t"])
        chain_b = ChainNode(
            id="chain_b",
            canvas_id=CANVAS,
            parent_id="chain_a",
            children=["chain_a"],
        )
        t = task("t", "chain_a")
        await seed(store, t, chain_a, chain_b)

        with pytest.raises(CanvasCycleError):
            await engine.complete(CANVAS, "t", ok_outcome("t"))
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestResultSizeGuard:
    """Regression tests for bug #15: nested-group fan-in doubled its payload every level.

    Found live during this session — see the plan's incident writeup. The fix has
    two parts, one test each, plus a permanent regression test at the depth the
    incident's bounded simulation used.
    """

    async def test_no_callback_group_outcome_never_carries_the_full_fan_in(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """A callback-less group's own result must be None, not the encoded FanIn.

        Nothing reads it on this path — there is no callback — so carrying it
        forward serves no purpose except being the thing an outer group's fan-in
        would have to re-escape and double in size.
        """
        group = GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1", "leg2"],
            callback=None,
        )
        await seed(store, task("leg1", "g"), task("leg2", "g"), group)

        await engine.complete(CANVAS, "leg1", ok_outcome("leg1", '{"big":"payload"}'))
        await engine.complete(CANVAS, "leg2", ok_outcome("leg2", '{"big":"payload"}'))

        group_result = await store.get_result(CANVAS, "g")
        assert group_result is not None
        assert group_result.result is None

    async def test_nested_groups_stay_bounded_in_payload_size(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """22 levels of callback=None group nesting must not grow the stored result at all.

        Permanent regression test at the same depth the incident's bounded
        simulation used to demonstrate 2x-per-level growth (67MB by level 21,
        before the fix). After the fix, every intermediate group's own result is
        None the whole way up — nothing to grow.
        """
        depth = 22
        nodes: dict[str, AnyNode] = {}
        current_id = "leaf"
        nodes[current_id] = TaskNode(
            id=current_id,
            canvas_id=CANVAS,
            parent_id=None,
            topic="leaf-topic",
        )
        for level in range(depth):
            container_id = f"level{level}"
            nodes[current_id] = nodes[current_id].model_copy(update={"parent_id": container_id})
            nodes[container_id] = GroupNode(
                id=container_id,
                canvas_id=CANVAS,
                parent_id=None,
                children=[current_id],
                callback=None,
            )
            current_id = container_id
        await store.create_canvas(CANVAS, nodes)

        result = await engine.complete(CANVAS, "leaf", ok_outcome("leaf", '{"x":1}'))

        assert result == []
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED
        for level in range(depth):
            outcome = await store.get_result(CANVAS, f"level{level}")
            assert outcome is not None
            assert outcome.result is None

    async def test_oversized_result_raises_immediately(
        self,
        store: MemoryCanvasStore,
    ) -> None:
        """A result over the configured max_result_bytes must raise, not silently store.

        Defense in depth alongside the structural fix above: this catches any other
        way a large payload could reach the store, not just the nested-group path.
        """
        engine = CanvasEngine(store, max_result_bytes=16)
        await seed(store, task("only", None))

        with pytest.raises(ResultTooLargeError):
            await engine.complete(
                CANVAS,
                "only",
                ok_outcome("only", '{"more_than_16_bytes":true}'),
            )

        assert await store.get_result(CANVAS, "only") is None
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestChildNotListedByItsParent:
    """A node whose parent doesn't list it must fail as a WorkerError, not a ValueError."""

    async def test_a_chain_that_does_not_list_the_finished_node_raises_a_worker_error(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """`list.index` raised a bare ValueError that escaped every `except WorkerError`.

        It landed in the driver's generic handler instead, so the delivery burned
        its retries and dead-lettered while the canvas stayed RUNNING with no
        status recorded — the exact stall the error handling exists to prevent.
        Reachable by reusing a canvas id for a different graph, which
        `apply(canvas_id=...)` supports.
        """
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["other"])
        await seed(store, task("stray", "chain"), task("other", "chain"), chain)

        with pytest.raises(ChildNotInParentError):
            await engine.complete(CANVAS, "stray", ok_outcome("stray"))

    async def test_that_failure_marks_the_canvas_errored(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Routing through WorkerError is what gets the canvas a terminal status."""
        chain = ChainNode(id="chain", canvas_id=CANVAS, parent_id=None, children=["other"])
        await seed(store, task("stray", "chain"), task("other", "chain"), chain)

        with pytest.raises(ChildNotInParentError):
            await engine.complete(CANVAS, "stray", ok_outcome("stray"))

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestGroupTerminalBranchesAreNotDeduplicated:
    """ABORT/PROPAGATE return before mark_child_done, the group's only de-dup."""

    def _group(self, policy: ErrorPolicy) -> GroupNode:
        return GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id=None,
            children=["leg1", "leg2"],
            callback="cb",
            error_policy=policy,
        )

    async def test_abort_cancels_the_callback_as_well_as_the_legs(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """An aborting group never dispatches its callback, so PENDING misreports it."""
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            self._group(ErrorPolicy.ABORT),
        )

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        callback = await store.get_node(CANVAS, "cb")
        assert callback is not None
        assert callback.status == NodeStatus.CANCELLED

    async def test_abort_still_errors_the_canvas(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Cancelling the callback must not lose the canvas-level failure."""
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            task("cb", "g", topic="callback"),
            self._group(ErrorPolicy.ABORT),
        )

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestConcurrentTerminalLegsEmitOnce:
    """A group emits exactly one terminal outcome, however many legs fail at once."""

    def _group(self, policy: ErrorPolicy) -> GroupNode:
        return GroupNode(
            id="g",
            canvas_id=CANVAS,
            parent_id="chain",
            children=["leg1", "leg2"],
            callback=None,
            error_policy=policy,
        )

    async def _seed(self, store: MemoryCanvasStore, policy: ErrorPolicy) -> None:
        await seed(
            store,
            task("leg1", "g"),
            task("leg2", "g"),
            self._group(policy),
            task("after", "chain", topic="after"),
            ChainNode(
                id="chain",
                canvas_id=CANVAS,
                parent_id=None,
                children=["g", "after"],
                error_policy=ErrorPolicy.CONTINUE,
            ),
        )

    async def test_two_legs_failing_concurrently_advance_the_parent_once(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """PROPAGATE returns before mark_child_done, the group's only de-duplication.

        Both failing legs took the branch, both bubbled a group-level ERROR, and the
        enclosing CONTINUE chain dispatched its next step twice.
        """
        await self._seed(store, ErrorPolicy.PROPAGATE)

        results = await asyncio.gather(
            engine.complete(CANVAS, "leg1", err_outcome("leg1")),
            engine.complete(CANVAS, "leg2", err_outcome("leg2")),
        )

        assert sum(len(dispatches) for dispatches in results) == 1

    async def test_two_legs_aborting_concurrently_abort_once(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """ABORT has the same shape; it just happened to be masked by the status guard."""
        await self._seed(store, ErrorPolicy.ABORT)

        results = await asyncio.gather(
            engine.complete(CANVAS, "leg1", err_outcome("leg1")),
            engine.complete(CANVAS, "leg2", err_outcome("leg2")),
        )

        assert all(dispatches == [] for dispatches in results)
        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestRollbackReleasesEveryGuardBurned:
    """One complete() can burn several group guards; rollback must release all of them."""

    async def _nested(self, store: MemoryCanvasStore) -> None:
        """Outer chord (CONTINUE, with callback) whose legs are an inner chord and a task."""
        await seed(
            store,
            task("inner1", "inner"),
            task("inner2", "inner"),
            GroupNode(
                id="inner",
                canvas_id=CANVAS,
                parent_id="outer",
                children=["inner1", "inner2"],
                callback=None,
                error_policy=ErrorPolicy.PROPAGATE,
            ),
            task("sibling", "outer"),
            task("cb", "outer", topic="callback"),
            GroupNode(
                id="outer",
                canvas_id=CANVAS,
                parent_id=None,
                children=["inner", "sibling"],
                callback="cb",
                error_policy=ErrorPolicy.CONTINUE,
            ),
        )

    async def test_a_dispatch_names_both_the_inner_and_outer_guards(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """The inner group claims its terminal slot, then the outer fires its callback."""
        await self._nested(store)
        await engine.complete(CANVAS, "sibling", ok_outcome("sibling"))

        dispatches = await engine.complete(CANVAS, "inner1", err_outcome("inner1"))

        assert set(dispatches[0].claimed_groups) == {"inner", "outer"}

    async def test_rollback_lets_the_whole_walk_replay(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Releasing only the dispatching group leaves the inner guard burned.

        The redelivery then stops at the inner group's failed claim, returns no
        dispatch at all, and the caller acks — the callback is lost and the canvas
        stays RUNNING forever, which is the exact stranding rollback exists to
        prevent.
        """
        await self._nested(store)
        await engine.complete(CANVAS, "sibling", ok_outcome("sibling"))
        authorised = await engine.complete(CANVAS, "inner1", err_outcome("inner1"))

        await engine.rollback(authorised)
        redelivered = await engine.complete(CANVAS, "inner1", err_outcome("inner1"))

        assert len(authorised) == 1
        assert len(redelivered) == 1
        assert redelivered[0].node_id == "cb"


class TestCancelledSubtreesStopRunning:
    """Cancelling a compound leg must stop everything inside it, not just label it."""

    async def _nested(self, store: MemoryCanvasStore) -> None:
        """Build root=[grp, tail] (CONTINUE), grp=PROPAGATE over [a, B], B=chain [b1, b2]."""
        await seed(
            store,
            task("a", "grp"),
            task("b1", "B"),
            task("b2", "B"),
            ChainNode(id="B", canvas_id=CANVAS, parent_id="grp", children=["b1", "b2"]),
            GroupNode(
                id="grp",
                canvas_id=CANVAS,
                parent_id="root",
                children=["a", "B"],
                callback=None,
                error_policy=ErrorPolicy.PROPAGATE,
            ),
            task("tail", "root", topic="tail"),
            ChainNode(
                id="root",
                canvas_id=CANVAS,
                parent_id=None,
                children=["grp", "tail"],
                error_policy=ErrorPolicy.CONTINUE,
            ),
        )

    async def test_cancelling_a_compound_leg_cancels_its_children(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Only the leg's own node was marked; its steps were left PENDING."""
        await self._nested(store)

        await engine.complete(CANVAS, "a", err_outcome("a"))

        for node_id in ("B", "b1", "b2"):
            node = await store.get_node(CANVAS, node_id)
            assert node is not None
            assert node.status == NodeStatus.CANCELLED

    async def test_work_already_in_flight_under_a_cancelled_leg_dispatches_nothing(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """`b1` was already dispatched when `B` was cancelled, and still reports.

        Advancing from it dispatched `b2` — side effects under a branch the engine
        had already given up on.
        """
        await self._nested(store)
        await engine.complete(CANVAS, "a", err_outcome("a"))

        dispatches = await engine.complete(CANVAS, "b1", ok_outcome("b1"))

        assert dispatches == []

    async def test_a_cancelled_nodes_outcome_is_not_recorded(
        self,
        engine: CanvasEngine,
        store: MemoryCanvasStore,
    ) -> None:
        """Recording it would overwrite the CANCELLED status that says why it stopped."""
        await self._nested(store)
        await engine.complete(CANVAS, "a", err_outcome("a"))

        await engine.complete(CANVAS, "b1", ok_outcome("b1"))

        assert await store.get_result(CANVAS, "b1") is None
        node = await store.get_node(CANVAS, "b1")
        assert node is not None
        assert node.status == NodeStatus.CANCELLED


class TestAbortRecordsBeforeTheCanvasGoesTerminal:
    """A terminal canvas status is what triggers Redis's TTL sweep."""

    async def test_the_container_outcome_is_written_before_the_canvas_status(
        self,
    ) -> None:
        """Writing after left the container's keys unreclaimed forever in Redis.

        `set_canvas_status(ERROR)` expires every key the canvas registry tracks, so
        a later `set_result` lands in an already-expiring registry and a later
        `set_node_status` clears the TTL just applied to the node key.
        """
        order: list[str] = []

        class OrderingStore(MemoryCanvasStore):
            async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
                if node_id == "chain":
                    order.append("record")
                await super().set_result(canvas_id, node_id, outcome)

            async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
                order.append(f"canvas:{status.value}")
                await super().set_canvas_status(canvas_id, status)

        ordering = OrderingStore()
        engine = CanvasEngine(ordering)
        chain = ChainNode(
            id="chain",
            canvas_id=CANVAS,
            parent_id=None,
            children=["s1", "s2"],
            error_policy=ErrorPolicy.ABORT,
        )
        await ordering.create_canvas(
            CANVAS,
            {
                "s1": task("s1", "chain"),
                "s2": task("s2", "chain"),
                "chain": chain,
            },
        )

        await engine.complete(CANVAS, "s1", err_outcome("s1"))

        assert order.index("record") < order.index("canvas:error")

    async def test_a_group_abort_records_before_the_canvas_status_too(
        self,
    ) -> None:
        """The group branch had the identical ordering."""
        order: list[str] = []

        class OrderingStore(MemoryCanvasStore):
            async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
                if node_id == "g":
                    order.append("record")
                await super().set_result(canvas_id, node_id, outcome)

            async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
                order.append(f"canvas:{status.value}")
                await super().set_canvas_status(canvas_id, status)

        ordering = OrderingStore()
        engine = CanvasEngine(ordering)
        await ordering.create_canvas(
            CANVAS,
            {
                "leg1": task("leg1", "g"),
                "leg2": task("leg2", "g"),
                "g": GroupNode(
                    id="g",
                    canvas_id=CANVAS,
                    parent_id=None,
                    children=["leg1", "leg2"],
                    callback=None,
                    error_policy=ErrorPolicy.ABORT,
                ),
            },
        )

        await engine.complete(CANVAS, "leg1", err_outcome("leg1"))

        assert order.index("record") < order.index("canvas:error")
