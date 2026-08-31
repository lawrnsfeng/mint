"""Protocol for the message broker: publish/consume with explicit delivery semantics.

Unlike the original implementation, delivery guarantee is a declared property of
each broker (``guarantee``), not something callers have to discover the hard way —
and a ``Worker`` only ever acks after both the canvas store write and the dispatch
publish succeed, so an at-least-once broker is what makes idempotent fan-in
(``ICanvasStore.mark_child_done``) actually pay off.
"""

from collections.abc import AsyncIterator, Mapping
from typing import ClassVar, Protocol

from mint.worker.enums import DeliveryGuarantee


class Delivery(Protocol):
    """One received message, with ack/nack control over its own redelivery."""

    body: bytes
    attempt: int

    async def ack(self) -> None:
        """Acknowledge this message as fully processed."""
        ...

    async def nack(self, *, requeue: bool) -> None:
        """Reject this message. ``requeue=True`` redelivers it; ``False`` dead-letters it."""
        ...


class IBroker(Protocol):
    """Publish/consume protocol every broker implementation satisfies."""

    guarantee: ClassVar[DeliveryGuarantee]

    async def publish(
        self,
        topic: str,
        message: bytes,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        """Publish ``message`` to ``topic``."""
        ...

    def consume(self, topic: str) -> AsyncIterator[Delivery]:
        """Yield deliveries from ``topic`` until the broker is closed."""
        ...

    async def close(self) -> None:
        """Release any underlying connections/resources."""
        ...
