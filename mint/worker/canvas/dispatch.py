"""What the canvas engine tells its caller to publish next."""

from dataclasses import dataclass

from mint.worker.envelope import Envelope


@dataclass(frozen=True)
class Dispatch:
    """One message the engine wants published to advance a canvas.

    ``group_id`` is set only on a chord callback's dispatch, and names the group
    whose fan-in guard was burned to authorise it. The caller hands it back to
    ``CanvasEngine.rollback`` when the publish fails, so the callback stays
    dispatchable on redelivery instead of being lost for good.
    """

    topic: str
    node_id: str
    canvas_id: str
    body: str
    group_id: str | None = None

    def to_envelope(self) -> Envelope:
        """Build the wire envelope for this dispatch."""
        return Envelope(node_id=self.node_id, canvas_id=self.canvas_id, body=self.body)
