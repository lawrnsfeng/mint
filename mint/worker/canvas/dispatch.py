"""What the canvas engine tells its caller to publish next."""

from dataclasses import dataclass

from mint.worker.envelope import Envelope


@dataclass(frozen=True)
class Dispatch:
    """One message the engine wants published to advance a canvas.

    ``claimed_groups`` names *every* group whose one-shot guard was burned while
    producing this dispatch — not just the group that dispatched. A single
    ``complete()`` can burn several: an inner group claiming its terminal slot,
    bubbling an outcome outwards, and an outer group then firing its callback.
    The caller hands the whole set back to ``CanvasEngine.rollback`` when the
    publish fails; releasing only the last one leaves the inner guards burned, so
    the redelivery stops at the first of them and the dispatch is lost anyway.
    """

    topic: str
    node_id: str
    canvas_id: str
    body: str
    claimed_groups: tuple[str, ...] = ()

    def to_envelope(self, trace_id: str | None = None) -> Envelope:
        """Build the wire envelope for this dispatch, carrying ``trace_id`` forward.

        Every non-entry message in a canvas is produced here, so dropping the
        caller's trace id killed it at the first hop — leaving a multi-node canvas
        impossible to correlate in logs even when its entry envelope had one.
        """
        return Envelope(
            node_id=self.node_id,
            canvas_id=self.canvas_id,
            trace_id=trace_id,
            body=self.body,
        )
