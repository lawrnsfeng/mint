"""MemoryCanvasStore: the in-process ICanvasStore used by every other test in this suite."""

from mint.worker.canvas.models import NodeOutcome, TaskNode
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.stores.memory import MemoryCanvasStore

CANVAS = "c1"


def task(node_id: str) -> TaskNode:
    """Build a minimal root task node for store-level tests."""
    return TaskNode(id=node_id, canvas_id=CANVAS, parent_id=None, topic="topic")


class TestNodes:
    """create_canvas / get_node / set_node_status / cancel_nodes."""

    async def test_get_node_on_unknown_id_returns_none(self) -> None:
        """No canvas or node has been created yet."""
        store = MemoryCanvasStore()

        assert await store.get_node(CANVAS, "ghost") is None

    async def test_create_canvas_then_get_node_round_trips(self) -> None:
        """Nodes persisted via create_canvas must be retrievable by id."""
        store = MemoryCanvasStore()
        node = task("t1")

        await store.create_canvas(CANVAS, {"t1": node})

        assert await store.get_node(CANVAS, "t1") == node

    async def test_set_node_status_updates_an_existing_node(self) -> None:
        """The stored node's status field must change; other fields are untouched."""
        store = MemoryCanvasStore()
        await store.create_canvas(CANVAS, {"t1": task("t1")})

        await store.set_node_status(CANVAS, "t1", NodeStatus.RUNNING)

        node = await store.get_node(CANVAS, "t1")
        assert node is not None
        assert node.status == NodeStatus.RUNNING

    async def test_set_node_status_on_a_missing_node_is_a_no_op(self) -> None:
        """A status update for a node that was never created must not raise."""
        store = MemoryCanvasStore()

        await store.set_node_status(CANVAS, "ghost", NodeStatus.RUNNING)

        assert await store.get_node(CANVAS, "ghost") is None

    async def test_cancel_nodes_marks_every_listed_node_cancelled(self) -> None:
        """A mix of existing and missing ids: existing get cancelled, missing ones are no-ops."""
        store = MemoryCanvasStore()
        await store.create_canvas(CANVAS, {"t1": task("t1"), "t2": task("t2")})

        await store.cancel_nodes(CANVAS, ["t1", "t2", "ghost"])

        t1 = await store.get_node(CANVAS, "t1")
        t2 = await store.get_node(CANVAS, "t2")
        assert t1 is not None
        assert t2 is not None
        assert t1.status == NodeStatus.CANCELLED
        assert t2.status == NodeStatus.CANCELLED


class TestResults:
    """set_result / get_result / get_results."""

    async def test_get_result_on_unknown_node_returns_none(self) -> None:
        """No result has been recorded for this node."""
        store = MemoryCanvasStore()

        assert await store.get_result(CANVAS, "t1") is None

    async def test_set_result_then_get_result_round_trips(self) -> None:
        """A recorded outcome must be retrievable by node id."""
        store = MemoryCanvasStore()
        outcome = NodeOutcome(node_id="t1", status=NodeStatus.FINISHED, result='{"x":1}')

        await store.set_result(CANVAS, "t1", outcome)

        assert await store.get_result(CANVAS, "t1") == outcome

    async def test_get_results_returns_only_the_ones_that_exist(self) -> None:
        """A partial set of recorded results: missing ids are simply absent, not None entries."""
        store = MemoryCanvasStore()
        await store.set_result(CANVAS, "t1", NodeOutcome(node_id="t1", status=NodeStatus.FINISHED))

        results = await store.get_results(CANVAS, ["t1", "t2"])

        assert set(results) == {"t1"}


class TestGroupFanIn:
    """mark_child_done: the atomic completed-child-set the engine's fan-in relies on."""

    async def test_first_of_two_children_does_not_fire(self) -> None:
        """One of two required children is not enough to fire the callback."""
        store = MemoryCanvasStore()

        progress = await store.mark_child_done(CANVAS, "g", "leg1", 2)

        assert progress.added
        assert progress.done_count == 1
        assert not progress.fired

    async def test_second_distinct_child_fires_exactly_once(self) -> None:
        """The second, distinct child reaching done_count == num_children fires."""
        store = MemoryCanvasStore()
        await store.mark_child_done(CANVAS, "g", "leg1", 2)

        progress = await store.mark_child_done(CANVAS, "g", "leg2", 2)

        assert progress.added
        assert progress.done_count == 2
        assert progress.fired

    async def test_redelivery_of_an_already_done_child_never_fires_again(self) -> None:
        """Redelivering the child that already fired must not report `fired` a second time."""
        store = MemoryCanvasStore()
        await store.mark_child_done(CANVAS, "g", "leg1", 2)
        await store.mark_child_done(CANVAS, "g", "leg2", 2)

        progress = await store.mark_child_done(CANVAS, "g", "leg2", 2)

        assert not progress.added
        assert not progress.fired


class TestCanvasStatus:
    """get_canvas_status / set_canvas_status."""

    async def test_unknown_canvas_defaults_to_running(self) -> None:
        """A canvas that was never explicitly set is RUNNING by default."""
        store = MemoryCanvasStore()

        assert await store.get_canvas_status("unknown") == CanvasStatus.RUNNING

    async def test_set_canvas_status_then_get_round_trips(self) -> None:
        """An explicit status update must be reflected on the next read."""
        store = MemoryCanvasStore()

        await store.set_canvas_status(CANVAS, CanvasStatus.ERROR)

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.ERROR


class TestCanvasIdReuse:
    """apply(canvas_id=...) exists for idempotent retries, so a reused id must start clean."""

    async def test_recreating_a_canvas_resets_a_terminal_status(self) -> None:
        """`setdefault` left the retry inheriting the failed attempt's ERROR status.

        Every completion would then short-circuit on the non-RUNNING guard, ack,
        and the retried canvas would silently never advance at all.
        """
        store = MemoryCanvasStore()
        node = TaskNode(id="t1", canvas_id="c1", topic="t")
        await store.create_canvas("c1", {"t1": node})
        await store.set_canvas_status("c1", CanvasStatus.ERROR)

        await store.create_canvas("c1", {"t1": node})

        assert await store.get_canvas_status("c1") == CanvasStatus.RUNNING
