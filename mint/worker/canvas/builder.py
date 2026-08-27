"""Client DSL for building a canvas graph: ``Node``, ``Chain``, ``Chord``.

Every compound node's ``parent_id`` is the id of whatever Chain/Chord object
directly contains it — chain steps point at their chain, chord legs and the
chord's own callback both point at the chord. That single rule is what lets
the engine treat "a chain finished" and "a chord's callback finished" as the
same kind of event when it bubbles up to an enclosing container, so nesting
composes for free instead of needing a special case per level.

``build``/``publish_entries`` are the shared construction protocol every DSL
node implements (deliberately not underscore-prefixed: Chain and Chord call
each other's, and each other's children's, uniformly while assembling and
launching a graph). ``publish_entries`` is polymorphic rather than
"return one (topic, id, body) tuple" specifically so a Chord nested as
another Chord's leg fans out all of *its* legs in turn, instead of only
being able to publish a single message.
"""

from collections.abc import Awaitable, Callable
from typing import Union
from uuid import uuid4

from mint.worker.canvas.models import AnyNode, ChainNode, GroupNode, TaskNode
from mint.worker.enums import CanvasStatus, ErrorPolicy
from mint.worker.envelope import Envelope
from mint.worker.exc import DuplicateNodeIdError, MissingInputError
from mint.worker.stores.interface import ICanvasStore

type PublishFn = Callable[[str, bytes], Awaitable[None]]


class Node:
    """A single task: one message published to one topic."""

    def __init__(self, topic: str, input: str | None = None, id: str | None = None) -> None:
        """Build a task node targeting ``topic``, optionally with a fixed id and input."""
        self.id = id or str(uuid4())
        self.topic = topic
        self.input = input

    def build(self, canvas_id: str, parent_id: str | None, nodes: dict[str, AnyNode]) -> None:
        """Add this node's persisted representation to ``nodes``."""
        if self.id in nodes:
            raise DuplicateNodeIdError(node_id=self.id)
        nodes[self.id] = TaskNode(
            id=self.id,
            canvas_id=canvas_id,
            parent_id=parent_id,
            topic=self.topic,
            input=self.input,
        )

    async def publish_entries(self, canvas_id: str, publish: PublishFn) -> None:
        """Publish the single message that starts this node."""
        if self.input is None:
            raise MissingInputError(node_id=self.id)
        await publish(self.topic, _envelope(self.id, canvas_id, self.input))


class Chain:
    """An ordered sequence of steps, run one after another.

    Nested chains are flattened at construction time so ``children`` is
    always a flat list of task leaves — the engine never has to resolve a
    chain-within-a-chain at dispatch time.
    """

    def __init__(
        self,
        steps: list[Union[Node, "Chain"]],
        *,
        error_policy: ErrorPolicy = ErrorPolicy.PROPAGATE,
        id: str | None = None,
    ) -> None:
        """Build a chain from ``steps``, flattening any nested chains in order."""
        self.id = id or str(uuid4())
        self.error_policy = error_policy
        self.steps: list[Node] = []
        for step in steps:
            if isinstance(step, Chain):
                self.steps.extend(step.steps)
            else:
                self.steps.append(step)

    def build(self, canvas_id: str, parent_id: str | None, nodes: dict[str, AnyNode]) -> None:
        """Add every step's persisted representation, then this chain's own."""
        if self.id in nodes:
            raise DuplicateNodeIdError(node_id=self.id)
        for step in self.steps:
            step.build(canvas_id, self.id, nodes)
        nodes[self.id] = ChainNode(
            id=self.id,
            canvas_id=canvas_id,
            parent_id=parent_id,
            children=[step.id for step in self.steps],
            error_policy=self.error_policy,
        )

    async def publish_entries(self, canvas_id: str, publish: PublishFn) -> None:
        """Publish the single message that starts this chain: its first step."""
        await self.steps[0].publish_entries(canvas_id, publish)

    async def apply(
        self,
        store: ICanvasStore,
        publish: PublishFn,
        *,
        canvas_id: str | None = None,
    ) -> str:
        """Persist this chain as a canvas and publish its first message.

        ``canvas_id`` may be pre-assigned by the caller (idempotent retries,
        pre-generated trace ids); a fresh one is generated otherwise.
        """
        canvas_id = canvas_id or str(uuid4())
        nodes: dict[str, AnyNode] = {}
        self.build(canvas_id, None, nodes)
        await store.create_canvas(canvas_id, nodes)
        try:
            await self.publish_entries(canvas_id, publish)
        except Exception:
            await store.set_canvas_status(canvas_id, CanvasStatus.ERROR)
            raise
        return canvas_id


class Chord:
    """A fan-out of legs, optionally aggregated by a callback."""

    def __init__(
        self,
        legs: list[Union[Node, Chain, "Chord"]],
        callback: Node | Chain | None = None,
        *,
        error_policy: ErrorPolicy = ErrorPolicy.CONTINUE,
        id: str | None = None,
    ) -> None:
        """Build a chord fanning out to ``legs``, optionally aggregated by ``callback``."""
        self.id = id or str(uuid4())
        self.legs = legs
        self.callback = callback
        self.error_policy = error_policy

    def build(self, canvas_id: str, parent_id: str | None, nodes: dict[str, AnyNode]) -> None:
        """Add every leg's and the callback's persisted representation, then this group's own."""
        if self.id in nodes:
            raise DuplicateNodeIdError(node_id=self.id)
        leg_ids: list[str] = []
        for leg in self.legs:
            if leg.id in leg_ids:
                raise DuplicateNodeIdError(node_id=leg.id)
            leg_ids.append(leg.id)
            leg.build(canvas_id, self.id, nodes)
        if self.callback is not None:
            self.callback.build(canvas_id, self.id, nodes)
        nodes[self.id] = GroupNode(
            id=self.id,
            canvas_id=canvas_id,
            parent_id=parent_id,
            children=leg_ids,
            callback=self.callback.id if self.callback is not None else None,
            error_policy=self.error_policy,
        )

    async def publish_entries(self, canvas_id: str, publish: PublishFn) -> None:
        """Publish every leg's starting message (recursing into nested chords)."""
        for leg in self.legs:
            await leg.publish_entries(canvas_id, publish)

    async def apply(
        self,
        store: ICanvasStore,
        publish: PublishFn,
        *,
        canvas_id: str | None = None,
    ) -> str:
        """Persist this chord as a canvas, then publish every leg's first message.

        Every node is written before any leg is published, so a publish
        failure partway through never leaves a group whose graph is
        incomplete — only some of its legs undispatched, which is what
        ``ErrorPolicy`` and redelivery exist to recover from. ``canvas_id``
        may be pre-assigned by the caller; a fresh one is generated otherwise.
        """
        canvas_id = canvas_id or str(uuid4())
        nodes: dict[str, AnyNode] = {}
        self.build(canvas_id, None, nodes)
        await store.create_canvas(canvas_id, nodes)
        try:
            await self.publish_entries(canvas_id, publish)
        except Exception:
            await store.set_canvas_status(canvas_id, CanvasStatus.ERROR)
            raise
        return canvas_id


def _envelope(node_id: str, canvas_id: str, body: str) -> bytes:
    return Envelope(node_id=node_id, canvas_id=canvas_id, body=body).to_bytes()
