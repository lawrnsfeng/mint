"""Executors: the ``ITaskExecutor`` protocol and its implementations."""

from .inline import InlineExecutor
from .interface import ITaskExecutor

__all__ = [
    "ITaskExecutor",
    "InlineExecutor",
]
