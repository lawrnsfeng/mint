"""Persisted canvas graph: nodes, results, and fan-in payloads."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter

from mint.worker.enums import ErrorPolicy, NodeStatus, NodeType


class ErrorInfo(BaseModel):
    """Serializable snapshot of a failure."""

    type: str
    message: str


class BaseNode(BaseModel):
    """Fields shared by every canvas node."""

    id: str
    canvas_id: str
    status: NodeStatus = NodeStatus.PENDING
    parent_id: str | None = None


class TaskNode(BaseNode):
    """A single unit of work dispatched to one broker topic."""

    type: Literal[NodeType.TASK] = NodeType.TASK
    topic: str
    input: str | None = None


class ChainNode(BaseNode):
    """An ordered sequence of task nodes run one after another."""

    type: Literal[NodeType.CHAIN] = NodeType.CHAIN
    children: list[str] = Field(min_length=1)
    error_policy: ErrorPolicy = ErrorPolicy.PROPAGATE

    def next_id(self, node_id: str) -> str | None:
        """Return the id of the node after ``node_id``, or None if it was last."""
        idx = self.children.index(node_id) + 1
        if idx >= len(self.children):
            return None
        return self.children[idx]


class GroupNode(BaseNode):
    """A fan-out of children, optionally aggregated by a callback."""

    type: Literal[NodeType.GROUP] = NodeType.GROUP
    children: list[str] = Field(min_length=1)
    callback: str | None = None
    input: str | None = None
    error_policy: ErrorPolicy = ErrorPolicy.CONTINUE

    @property
    def num_children(self) -> int:
        """Number of fan-out legs this group waits on."""
        return len(self.children)


AnyNode = Annotated[TaskNode | ChainNode | GroupNode, Field(discriminator="type")]
NodeAdapter: TypeAdapter[AnyNode] = TypeAdapter(AnyNode)


class NodeOutcome(BaseModel):
    """Recorded result of a node (or a compound chain/group) reaching a terminal state."""

    node_id: str
    status: Literal[NodeStatus.FINISHED, NodeStatus.ERROR]
    result: str | None = None
    error: ErrorInfo | None = None

    @property
    def ok(self) -> bool:
        """Whether this outcome represents success."""
        return self.status == NodeStatus.FINISHED


class ChildResult(BaseModel):
    """One fan-out leg's outcome, as seen from a group's callback."""

    node_id: str
    ok: bool
    value: str | None = None
    error: ErrorInfo | None = None


class FanIn(BaseModel):
    """Payload delivered to a chord's callback: every leg's result plus the group input."""

    children: list[ChildResult]
    input: str | None = None
