"""Client DSL: entry-dispatch targeting, flattening, id collisions, apply() ordering."""

import pytest

from mint.worker.canvas.builder import Chain, Chord, Node
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import ChainNode, FanIn, GroupNode, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.envelope import Envelope
from mint.worker.exc import DuplicateNodeIdError, MissingInputError
from mint.worker.stores.memory import MemoryCanvasStore
from tests.worker.conftest import FailingPublishSpy, OrderSpy, PublishSpy, SpiedMemoryCanvasStore


def ok_outcome(node_id: str, result: str = "{}") -> NodeOutcome:
    """Build a FINISHED outcome for ``node_id``."""
    return NodeOutcome(node_id=node_id, status=NodeStatus.FINISHED, result=result)


class TestEntryDispatch:
    """Which topic and node id get published for the very first message(s)."""

    async def test_chain_as_a_chord_leg_dispatches_to_its_first_task_and_runs_to_completion(
        self,
        store: MemoryCanvasStore,
        publish_spy: PublishSpy,
    ) -> None:
        """The initial publish must target step 1's topic/id, not the wrapping chain's.

        Regression for bug #1: the original engine stamped the *chain's* id on the
        first message, so the receiving worker looked up the chain node and jumped
        straight to its parent — steps 2..n never ran.
        """
        n1 = Node(topic="t1", input='"a"', id="n1")
        n2 = Node(topic="t2", id="n2")
        chain = Chain([n1, n2], id="chain1")
        chord = Chord([chain], callback=None, id="chord1")

        canvas_id = await chord.apply(store, publish_spy)

        assert len(publish_spy.calls) == 1
        topic, body = publish_spy.calls[0]
        assert topic == "t1"
        envelope = Envelope.from_bytes(body)
        assert envelope.node_id == "n1"
        assert envelope.canvas_id == canvas_id
        assert envelope.body == '"a"'

        engine = CanvasEngine(store)
        after_n1 = await engine.complete(canvas_id, "n1", ok_outcome("n1", '"b"'))
        assert len(after_n1) == 1
        assert after_n1[0].node_id == "n2"
        assert after_n1[0].topic == "t2"

        after_n2 = await engine.complete(canvas_id, "n2", ok_outcome("n2"))
        assert after_n2 == []
        assert await store.get_canvas_status(canvas_id) == CanvasStatus.FINISHED

    async def test_chain_as_a_chord_callback_runs_every_step_when_triggered(
        self,
        store: MemoryCanvasStore,
        publish_spy: PublishSpy,
    ) -> None:
        """A multi-step callback chain must run all of its steps, not just the first."""
        leg = Node(topic="leg", input="{}", id="leg1")
        cb1 = Node(topic="cb1", id="cb1")
        cb2 = Node(topic="cb2", id="cb2")
        callback_chain = Chain([cb1, cb2], id="cbchain")
        chord = Chord([leg], callback=callback_chain, id="chord1")

        canvas_id = await chord.apply(store, publish_spy)

        # The callback is never published up front — only the leg is.
        assert len(publish_spy.calls) == 1
        assert publish_spy.calls[0][0] == "leg"

        engine = CanvasEngine(store)
        after_leg = await engine.complete(canvas_id, "leg1", ok_outcome("leg1", '{"x":1}'))
        assert len(after_leg) == 1
        assert after_leg[0].node_id == "cb1"
        fan_in = FanIn.model_validate_json(after_leg[0].body)
        assert fan_in.children[0].node_id == "leg1"

        after_cb1 = await engine.complete(canvas_id, "cb1", ok_outcome("cb1"))
        assert len(after_cb1) == 1
        assert after_cb1[0].node_id == "cb2"

        after_cb2 = await engine.complete(canvas_id, "cb2", ok_outcome("cb2"))
        assert after_cb2 == []
        assert await store.get_canvas_status(canvas_id) == CanvasStatus.FINISHED

    async def test_nested_chord_leg_publishes_every_one_of_its_own_legs(
        self,
        store: MemoryCanvasStore,
        publish_spy: PublishSpy,
    ) -> None:
        """A Chord nested as another Chord's leg must fan out, not publish one message."""
        inner = Chord(
            [Node(topic="b1", input="{}", id="b1"), Node(topic="b2", input="{}", id="b2")],
            callback=None,
            id="inner",
        )
        outer = Chord([Node(topic="a1", input="{}", id="a1"), inner], callback=None, id="outer")

        await outer.apply(store, publish_spy)

        topics = {topic for topic, _ in publish_spy.calls}
        assert topics == {"a1", "b1", "b2"}


class TestFlattening:
    """A chain nested inside another chain collapses to one flat step list."""

    def test_nested_chain_flattens_but_preserves_order(self) -> None:
        """steps=[z, [a, b], c] must flatten to [z, a, b, c], not keep the nested chain."""
        inner = Chain([Node(topic="a", id="a"), Node(topic="b", id="b")])
        outer = Chain([Node(topic="z", id="z", input="{}"), inner, Node(topic="c", id="c")])

        nodes: dict = {}
        outer.build("canvas1", None, nodes)

        chain_node = nodes[outer.id]
        assert isinstance(chain_node, ChainNode)
        assert chain_node.children == ["z", "a", "b", "c"]


class TestDuplicateIds:
    """Duplicate node ids must fail fast at build time, never at dispatch time."""

    def test_duplicate_ids_among_chord_legs_are_rejected(self) -> None:
        """Two distinct leg objects sharing an id must not silently collide in the graph."""
        chord = Chord(
            [Node(topic="t1", id="dup"), Node(topic="t2", id="dup")],
            callback=None,
        )

        with pytest.raises(DuplicateNodeIdError):
            chord.build("canvas1", None, {})

    def test_duplicate_ids_within_a_chain_are_rejected(self) -> None:
        """The same node reused twice in one chain must not silently collide in the graph."""
        shared = Node(topic="t", id="same")
        chain = Chain([shared, shared])

        with pytest.raises(DuplicateNodeIdError):
            chain.build("canvas1", None, {})

    def test_a_chain_whose_own_id_collides_with_an_existing_node_is_rejected(self) -> None:
        """The chain's *own* id, not just a step's, must be checked against the graph so far."""
        nodes: dict = {}
        Node(topic="other", id="dup").build("canvas1", None, nodes)
        chain = Chain([Node(topic="t1", id="n1")], id="dup")

        with pytest.raises(DuplicateNodeIdError):
            chain.build("canvas1", None, nodes)

    def test_a_chord_whose_own_id_collides_with_an_existing_node_is_rejected(self) -> None:
        """The chord's *own* id, not just a leg's, must be checked against the graph so far."""
        nodes: dict = {}
        Node(topic="other", id="dup").build("canvas1", None, nodes)
        chord = Chord([Node(topic="t1", id="n1")], callback=None, id="dup")

        with pytest.raises(DuplicateNodeIdError):
            chord.build("canvas1", None, nodes)


class TestApplyOrdering:
    """apply() must persist the whole graph before publishing anything, and fail loudly."""

    async def test_standalone_chain_apply_persists_and_publishes_successfully(
        self,
        store: MemoryCanvasStore,
        publish_spy: PublishSpy,
    ) -> None:
        """A Chain used directly as the whole canvas (not wrapped in a Chord) must succeed."""
        chain = Chain([Node(topic="t1", input='"a"', id="n1"), Node(topic="t2", id="n2")])

        canvas_id = await chain.apply(store, publish_spy)

        assert canvas_id
        assert await store.get_node(canvas_id, chain.id) is not None
        assert len(publish_spy.calls) == 1
        assert publish_spy.calls[0][0] == "t1"

    async def test_writes_every_node_before_publishing_any_leg(
        self,
        order_spy: OrderSpy,
    ) -> None:
        """Regression for bug #6: legs used to publish in a bare loop with no prior durability."""
        spied_store = SpiedMemoryCanvasStore(order_spy)
        publish = PublishSpy(order_spy)
        chord = Chord(
            [Node(topic="t1", input="1", id="n1"), Node(topic="t2", input="2", id="n2")],
            callback=None,
        )

        await chord.apply(spied_store, publish)

        assert order_spy.events == ["create_canvas", "publish:t1", "publish:t2"]

    async def test_publish_failure_partway_through_errors_the_canvas_without_losing_structure(
        self,
        store: MemoryCanvasStore,
    ) -> None:
        """A leg 2 publish failure must not strand a group whose graph is half-written."""
        canvas_id = "known-canvas"
        publish = FailingPublishSpy(fail_at=2)
        chord = Chord(
            [
                Node(topic="t1", input="1", id="n1"),
                Node(topic="t2", input="2", id="n2"),
                Node(topic="t3", input="3", id="n3"),
            ],
            callback=None,
            id="chord1",
        )

        with pytest.raises(RuntimeError, match="publish failed"):
            await chord.apply(store, publish, canvas_id=canvas_id)

        assert await store.get_canvas_status(canvas_id) == CanvasStatus.ERROR
        group_node = await store.get_node(canvas_id, "chord1")
        assert isinstance(group_node, GroupNode)
        assert group_node.children == ["n1", "n2", "n3"]
        # Only the first leg was actually published before the failure.
        assert len(publish.calls) == 2

    async def test_chain_with_no_input_on_its_first_step_raises_a_typed_error(
        self,
        store: MemoryCanvasStore,
        publish_spy: PublishSpy,
    ) -> None:
        """A missing entry input must be a typed MissingInputError, not a bare ValueError."""
        chain = Chain([Node(topic="t1", id="n1"), Node(topic="t2", id="n2", input="{}")])

        with pytest.raises(MissingInputError):
            await chain.apply(store, publish_spy, canvas_id="canvas1")

        assert await store.get_canvas_status("canvas1") == CanvasStatus.ERROR


class TestCanvasIdPropagation:
    """Every node in a built graph, at any depth, carries the same canvas_id."""

    def test_canvas_id_propagates_to_every_node_at_every_depth(self) -> None:
        """A chain leg, a plain leg, and a callback must all share the group's canvas_id."""
        inner_chain = Chain([Node(topic="a", id="a"), Node(topic="b", id="b")])
        group = Chord(
            [inner_chain, Node(topic="c", id="c", input="{}")],
            callback=Node(topic="cb", id="cb"),
            id="group1",
        )

        nodes: dict = {}
        group.build("canvas-x", None, nodes)

        assert set(nodes) == {"a", "b", "c", inner_chain.id, "group1", "cb"}
        assert all(node.canvas_id == "canvas-x" for node in nodes.values())
