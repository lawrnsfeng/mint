"""gRPC remote executor — dispatches to a remote gRPC service instead of running locally.

Regression fix for bug #12: the original's ``iscoroutinefunction`` check was
inverted — it raised on the exact case (an async stub method) that should have
worked, and did nothing to catch the case that actually matters (a bad method
name). Awaiting whatever the stub method returns works uniformly whether that
method is an ``async def`` (returns a coroutine) or one of grpc.aio's actual
generated stub methods (whose call returns an awaitable ``Call`` object, never a
coroutine function at all — ``iscoroutinefunction`` was never the right test here).
So the type check is removed rather than fixed: it was never needed.
"""

from typing import Protocol

from grpc.aio import Channel, insecure_channel

from mint.worker.exc import RemoteMethodNotFoundError


class Stub(Protocol):
    """The shape a generated gRPC stub class must have: constructible from a channel."""

    def __init__(self, channel: Channel) -> None:
        """Bind this stub to ``channel``, as every generated gRPC stub class does."""


class GRPCExecutor[T, RT]:
    """Dispatches ``input_`` to ``method`` on a gRPC stub over ``uri``, per call.

    A fresh channel is opened per call rather than pooled — that mirrors gRPC's own
    guidance (channels are cheap and already multiplex calls internally; the
    complexity of pooling them buys nothing gRPC's client doesn't already do).
    """

    def __init__(self, uri: str, stub: type[Stub], method: str) -> None:
        """Configure a dispatch target: ``stub(channel).method(input_)`` over ``uri``."""
        self.uri = uri
        self._stub_cls = stub
        self._method_name = method

    async def execute(self, fn: object, input_: T) -> RT:
        """Call the configured remote method with ``input_`` and return its reply.

        ``fn`` is unused: a ``GRPCExecutor`` replaces ``process`` entirely rather
        than wrapping it, and takes it only to satisfy the same call signature every
        executor shares — so ``Worker`` never has to branch on which kind is bound.
        """
        del fn
        async with insecure_channel(self.uri) as channel:
            stub = self._stub_cls(channel)
            try:
                method = getattr(stub, self._method_name)
            except AttributeError as exc:
                raise RemoteMethodNotFoundError(
                    stub=self._stub_cls.__name__,
                    method=self._method_name,
                ) from exc
            return await method(input_)
