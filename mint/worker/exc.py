"""Exception hierarchy for mint.worker."""

from dataclasses import dataclass

from mint.exc import TemplatedError
from mint.worker.enums import ErrorPolicy


@dataclass
class WorkerError(TemplatedError):
    """Generic mint.worker error."""


@dataclass
class NodeNotFoundError(WorkerError):
    """A referenced canvas node does not exist in the store."""

    TEMPLATE = "Node {node_id} not found in canvas {canvas_id}"
    node_id: str
    canvas_id: str


@dataclass
class ParentNotFoundError(WorkerError):
    """A node's declared parent does not exist in the store."""

    TEMPLATE = "Parent {parent_id} of node {node_id} not found in canvas {canvas_id}"
    node_id: str
    parent_id: str
    canvas_id: str


@dataclass
class CallbackNotFoundError(WorkerError):
    """A group's declared callback does not exist in the store."""

    TEMPLATE = "Callback {callback_id} of group {group_id} not found in canvas {canvas_id}"
    group_id: str
    callback_id: str
    canvas_id: str


@dataclass
class InvalidParentTypeError(WorkerError):
    """A node that cannot own children (a task) was referenced as a parent."""

    TEMPLATE = "Node {node_id} cannot be a parent in canvas {canvas_id}"
    node_id: str
    canvas_id: str


@dataclass
class DuplicateNodeIdError(WorkerError):
    """Two nodes in the same canvas graph share an id."""

    TEMPLATE = "Duplicate node id {node_id} in canvas graph"
    node_id: str


@dataclass
class ConflictingErrorPolicyError(WorkerError):
    """A nested chain declared an error policy the chain flattening it does not share.

    Flattening dissolves the nested chain into its parent's step list, so it keeps
    no policy of its own — raising here is what stops the caller's explicit choice
    from being silently reversed.
    """

    TEMPLATE = (
        "Nested chain {nested_id} declares error policy {nested_policy}, "
        "but chain {chain_id} flattening it uses {policy}"
    )
    chain_id: str
    nested_id: str
    policy: ErrorPolicy
    nested_policy: ErrorPolicy


@dataclass
class MissingInputError(WorkerError):
    """An entry-point node has no input set and cannot be published."""

    TEMPLATE = "Node {node_id} has no input and cannot be an entry point"
    node_id: str


@dataclass
class EmptyContainerError(WorkerError):
    """A Chain or Chord was built with no children.

    Caught in the DSL so it surfaces as a ``WorkerError`` like every other builder
    failure. Left to ``build()`` it becomes a raw pydantic ``ValidationError`` from
    the node's ``min_length=1``, and ``publish_entries`` would ``IndexError`` before
    that — neither of which any caller is watching for.
    """

    TEMPLATE = "{container} {container_id} has no children"
    container: str
    container_id: str


@dataclass
class ChildNotInParentError(WorkerError):
    """A node's parent does not list it as one of its children.

    Reachable when a canvas id is reused for a different graph — which
    ``Chain.apply``/``Chord.apply``'s ``canvas_id`` argument makes possible — so it
    has to raise as a ``WorkerError`` and route through the engine's normal error
    handling rather than escaping as a bare ``ValueError``.
    """

    TEMPLATE = "Node {node_id} is not a child of {parent_id} in canvas {canvas_id}"
    node_id: str
    parent_id: str
    canvas_id: str


@dataclass
class CanvasCycleError(WorkerError):
    """A cycle was detected while walking a canvas graph."""

    TEMPLATE = "Cycle detected in canvas {canvas_id} at node {node_id}"
    canvas_id: str
    node_id: str


@dataclass
class WorkerNotBoundError(WorkerError):
    """A worker method was called before WorkerApp.register() bound its dependencies."""

    TEMPLATE = "Worker {worker} is not bound to a broker/store; register it with a WorkerApp first"
    worker: str


@dataclass
class MissingWorkerConfigError(WorkerError):
    """A worker subclass did not declare a required class attribute."""

    TEMPLATE = "Worker {worker} is missing required class attribute {attribute}"
    worker: str
    attribute: str


@dataclass
class DuplicateTopicError(WorkerError):
    """Two workers registered on the same app claim the same topic."""

    TEMPLATE = "Topic {topic} is already claimed by another registered worker"
    topic: str


@dataclass
class AppAlreadyRunningError(WorkerError):
    """WorkerApp.run() was called while it was already running."""

    TEMPLATE = "WorkerApp is already running"


@dataclass
class CoordinatorAlreadyRunningError(WorkerError):
    """Coordinator.run() was called while it was already running."""

    TEMPLATE = "Coordinator is already running"


@dataclass
class ResultTooLargeError(WorkerError):
    """A node's result exceeds the engine's configured size guard.

    Raised immediately instead of letting a node's payload keep growing —
    most concretely, a callback-less group re-embedding an already-serialized
    child result doubles in size at every level of nesting (O(2^depth)).
    """

    TEMPLATE = "Result for node {node_id} is {size} bytes, over the {limit} byte limit"
    node_id: str
    size: int
    limit: int


@dataclass
class UnpicklableTaskError(WorkerError):
    """A callable or its input cannot be pickled to cross a process pool boundary.

    Raised immediately, before ever submitting to the pool — a bound method whose
    ``self`` holds an unpicklable dependency (a DB client, an open socket) would
    otherwise fail deep inside the worker process, or hang the pool's result
    queue waiting on a submission that never truly went anywhere.
    """

    TEMPLATE = "{fn} (or its input) cannot be pickled for a process pool: {detail}"
    fn: str
    detail: str


@dataclass
class RemoteMethodNotFoundError(WorkerError):
    """A remote executor's configured method name does not exist on its stub."""

    TEMPLATE = "Method {method} not found on stub {stub}"
    stub: str
    method: str


@dataclass
class RemoteCallTimeoutError(WorkerError):
    """An AMQP RPC call's reply did not arrive within its configured timeout."""

    TEMPLATE = "No reply on queue {queue} within {timeout}s"
    queue: str
    timeout: float
