"""Canvas graph stores: the ``ICanvasStore`` protocol and its implementations."""

from .interface import GroupProgress, ICanvasStore
from .memory import MemoryCanvasStore

__all__ = [
    "GroupProgress",
    "ICanvasStore",
    "MemoryCanvasStore",
]
