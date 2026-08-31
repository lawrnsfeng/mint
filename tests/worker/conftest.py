"""Shared fixtures for mint.worker tests: no Docker, no sleeps.

``MemoryCanvasStore`` and the spies here are what let the entire engine and
builder suite run in-process — containerised stores/brokers are exercised
separately, only for what genuinely needs real concurrency or a real wire.
"""

from collections.abc import Awaitable, Callable, Mapping

import pytest

from mint.worker.canvas.dispatch import Dispatch
from mint.worker.canvas.engine import CanvasEngine
from mint.worker.canvas.models import AnyNode
from mint.worker.stores.memory import MemoryCanvasStore


class OrderSpy:
    """Records the order in which labeled events occur across collaborators."""

    def __init__(self) -> None:
        """Start with an empty event log."""
        self.events: list[str] = []

    def record(self, label: str) -> None:
        """Append one event label."""
        self.events.append(label)


class DispatchSpy:
    """Accumulates every ``Dispatch`` returned across a sequence of calls."""

    def __init__(self) -> None:
        """Start with an empty dispatch log."""
        self.dispatches: list[Dispatch] = []

    def record(self, dispatches: list[Dispatch]) -> None:
        """Append a batch of dispatches."""
        self.dispatches.extend(dispatches)


class PublishSpy:
    """A ``PublishFn`` that records every call it receives."""

    def __init__(self, order_spy: OrderSpy | None = None) -> None:
        """Start with an empty call log, optionally also reporting to ``order_spy``."""
        self.calls: list[tuple[str, bytes]] = []
        self._order_spy = order_spy

    async def __call__(self, topic: str, body: bytes) -> None:
        """Record one publish call."""
        if self._order_spy is not None:
            self._order_spy.record(f"publish:{topic}")
        self.calls.append((topic, body))


class FailingPublishSpy(PublishSpy):
    """A ``PublishFn`` that raises on its ``fail_at``-th call (1-indexed)."""

    def __init__(self, fail_at: int, order_spy: OrderSpy | None = None) -> None:
        """Configure this spy to raise on its ``fail_at``-th call."""
        super().__init__(order_spy)
        self.fail_at = fail_at

    async def __call__(self, topic: str, body: bytes) -> None:
        """Record the call, then raise if this is the configured failure point."""
        await super().__call__(topic, body)
        if len(self.calls) == self.fail_at:
            raise RuntimeError("publish failed")


class SpiedMemoryCanvasStore(MemoryCanvasStore):
    """A ``MemoryCanvasStore`` that reports ``create_canvas`` to an ``OrderSpy``."""

    def __init__(self, order_spy: OrderSpy) -> None:
        """Wrap a fresh in-memory store, reporting ``create_canvas`` to ``order_spy``."""
        super().__init__()
        self._order_spy = order_spy

    async def create_canvas(self, canvas_id: str, nodes: Mapping[str, AnyNode]) -> None:
        """Record the event, then persist as usual."""
        self._order_spy.record("create_canvas")
        await super().create_canvas(canvas_id, nodes)


@pytest.fixture
def order_spy() -> OrderSpy:
    """Return a fresh event-order recorder."""
    return OrderSpy()


@pytest.fixture
def dispatch_spy() -> DispatchSpy:
    """Return a fresh dispatch recorder."""
    return DispatchSpy()


@pytest.fixture
def store() -> MemoryCanvasStore:
    """Return a fresh in-memory canvas store."""
    return MemoryCanvasStore()


@pytest.fixture
def engine(store: MemoryCanvasStore) -> CanvasEngine:
    """Return a canvas engine backed by a fresh in-memory store."""
    return CanvasEngine(store)


@pytest.fixture
def publish_spy() -> PublishSpy:
    """Return a fresh publish recorder usable directly as a ``PublishFn``."""
    return PublishSpy()


type PublishFn = Callable[[str, bytes], Awaitable[None]]
