"""Wire format: one delivery attempt targeting one canvas node."""

import json
from datetime import UTC, datetime
from typing import Self
from uuid import uuid4

from pydantic import BaseModel, Field


class Envelope(BaseModel):
    """A single message on the broker.

    ``id`` identifies this delivery attempt; ``node_id`` identifies the canvas
    node it targets. The two are deliberately distinct — conflating them is
    what let a chain-as-chord-leg dispatch stamp the wrong id on its message.
    """

    id: str = Field(default_factory=lambda: str(uuid4()))
    node_id: str
    canvas_id: str
    attempt: int = 1
    trace_id: str | None = None
    published_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    body: str

    @property
    def data(self) -> object:
        """Return the body decoded as JSON."""
        return json.loads(self.body)

    def to_bytes(self) -> bytes:
        """Serialize this envelope for publishing on a broker."""
        return self.model_dump_json().encode()

    @classmethod
    def from_bytes(cls, raw: bytes) -> Self:
        """Deserialize an envelope previously produced by ``to_bytes``."""
        return cls.model_validate_json(raw)
