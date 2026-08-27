"""Enumerations shared across the mint.worker package."""

from enum import StrEnum


class NodeType(StrEnum):
    """Discriminator for the three canvas node shapes."""

    TASK = "task"
    CHAIN = "chain"
    GROUP = "group"


class NodeStatus(StrEnum):
    """Lifecycle status of a single canvas node."""

    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"
    CANCELLED = "cancelled"


class CanvasStatus(StrEnum):
    """Lifecycle status of an entire canvas (the root node and everything under it)."""

    RUNNING = "running"
    FINISHED = "finished"
    ERROR = "error"


class ErrorPolicy(StrEnum):
    """How a chain or group reacts when one of its children errors.

    CONTINUE: record the error, keep going as if nothing happened.
    PROPAGATE: stop this container, mark it errored, cancel what has not run yet,
        but still let its own parent decide what to do next.
    ABORT: cancel every pending sibling and fail the whole canvas immediately.
    """

    CONTINUE = "continue"
    PROPAGATE = "propagate"
    ABORT = "abort"


class DeliveryGuarantee(StrEnum):
    """The delivery guarantee a broker implementation actually provides."""

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
