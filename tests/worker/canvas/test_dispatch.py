"""Dispatch: what the engine tells its caller to publish next."""

from mint.worker.canvas.dispatch import Dispatch


class TestDispatch:
    """Conversion to the wire envelope that actually gets published."""

    def test_to_envelope_carries_topic_target_and_body(self) -> None:
        """to_envelope() must preserve node_id/canvas_id/body; topic isn't part of the envelope."""
        dispatch = Dispatch(topic="t1", node_id="n1", canvas_id="c1", body='{"x":1}')

        envelope = dispatch.to_envelope()

        assert envelope.node_id == "n1"
        assert envelope.canvas_id == "c1"
        assert envelope.body == '{"x":1}'


class TestTracePropagation:
    """Every non-entry message is built here, so a dropped trace id dies at hop one."""

    def test_to_envelope_carries_the_trace_id_forward(self) -> None:
        """Without this, correlating a multi-node canvas in logs is impossible."""
        dispatch = Dispatch(topic="t", node_id="n1", canvas_id="c1", body="{}")

        envelope = dispatch.to_envelope(trace_id="trace-abc")

        assert envelope.trace_id == "trace-abc"

    def test_to_envelope_without_a_trace_id_leaves_it_unset(self) -> None:
        """A canvas started with no trace id must not invent one."""
        dispatch = Dispatch(topic="t", node_id="n1", canvas_id="c1", body="{}")

        assert dispatch.to_envelope().trace_id is None
