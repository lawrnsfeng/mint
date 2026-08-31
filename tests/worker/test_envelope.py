"""Envelope: the wire format for one delivery attempt targeting one canvas node."""

from mint.worker.envelope import Envelope


class TestEnvelope:
    """Serialization round-trip and JSON decoding."""

    def test_round_trips_through_bytes(self) -> None:
        """to_bytes() then from_bytes() must reproduce the same fields."""
        envelope = Envelope(node_id="n1", canvas_id="c1", body='{"x":1}', trace_id="t1")

        restored = Envelope.from_bytes(envelope.to_bytes())

        assert restored.id == envelope.id
        assert restored.node_id == "n1"
        assert restored.canvas_id == "c1"
        assert restored.body == '{"x":1}'
        assert restored.trace_id == "t1"

    def test_data_decodes_the_body_as_json(self) -> None:
        """The ``data`` property must parse ``body`` rather than returning raw text."""
        envelope = Envelope(node_id="n1", canvas_id="c1", body='{"x":1,"y":[1,2]}')

        assert envelope.data == {"x": 1, "y": [1, 2]}

    def test_id_and_attempt_default(self) -> None:
        """A freshly built envelope gets a generated id and attempt 1."""
        envelope = Envelope(node_id="n1", canvas_id="c1", body="{}")

        assert envelope.id
        assert envelope.attempt == 1
        assert envelope.trace_id is None
