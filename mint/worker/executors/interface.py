"""Protocol for how a Worker actually runs its ``process`` coroutine.

Swapping executors is what lets a blocking or CPU-bound ``process`` run without
stalling the worker's event loop, without ``process`` itself knowing or caring —
``ThreadPoolExecutor``/``ProcessPoolExecutor`` offload it; ``InlineExecutor`` (the
default) just awaits it directly.
"""

from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable


class ITaskExecutor[T, RT](Protocol):
    """Runs ``fn(input_)`` and returns its result, however this executor chooses to.

    A *remote* executor (``GRPCExecutor``, ``AMQPRPCExecutor``) satisfies this same
    shape but ignores ``fn`` entirely — it replaces ``process`` rather than wrapping
    it, dispatching ``input_`` to a remote service instead. Sharing one signature
    means ``Worker`` never has to branch on which kind of executor is bound.
    """

    async def execute(self, fn: Callable[[T], Awaitable[RT]], input_: T) -> RT:
        """Run ``fn(input_)`` and return its result."""
        ...


@runtime_checkable
class IClosableExecutor(Protocol):
    """An executor holding a resource (a pool, a connection) that must be released.

    Optional: ``InlineExecutor`` and ``GRPCExecutor`` hold nothing persistent and
    don't implement this. ``WorkerApp`` checks for it structurally at shutdown
    rather than requiring every executor to have a (possibly no-op) ``aclose``.
    """

    async def aclose(self) -> None:
        """Release whatever this executor holds. Never called from ``__del__``."""
        ...
