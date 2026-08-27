"""GRPCExecutor: dispatches to a remote gRPC stub method instead of running locally.

Regression coverage for bug #12: the original inverted ``iscoroutinefunction``
check rejected exactly the case that should have worked. There is no such check
here at all — both an ``async def`` stub method and grpc.aio's actual call shape
(a plain callable whose *return value*, not itself, is awaitable) must both work.
"""

from collections.abc import Generator
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

from mint.worker.exc import RemoteMethodNotFoundError
from mint.worker.executors.grpc import GRPCExecutor

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

URI = "localhost:50051"


class _FakeCall:
    """Mimics grpc.aio's UnaryUnaryCall: awaitable, but not itself a coroutine function."""

    def __init__(self, result: object) -> None:
        """Store the value this call resolves to."""
        self._result = result

    def __await__(self) -> Generator[Any, None, object]:
        """Resolve to the stored result when awaited."""

        async def _resolve() -> object:
            return self._result

        return _resolve().__await__()


class AsyncStub:
    """A stub whose method is a genuine ``async def`` (a coroutine function)."""

    def __init__(self, channel: object) -> None:
        """Record the channel it was constructed with."""
        self.channel = channel

    async def Call(self, request: object) -> str:  # noqa: N802
        """Echo the request back, prefixed, as a real coroutine function."""
        return f"echo:{request}"


class MultiCallableStub:
    """A stub whose method mimics grpc.aio's real shape: sync callable, awaitable result."""

    def __init__(self, channel: object) -> None:
        """Record the channel it was constructed with."""
        self.channel = channel

    def Call(self, request: object) -> _FakeCall:  # noqa: N802
        """Return an awaitable call object, exactly like a real grpc.aio stub method."""
        return _FakeCall(f"echo:{request}")


def unused_fn(_input: object) -> object:
    """Stand in for the ``fn`` param GRPCExecutor deliberately ignores."""
    raise AssertionError("GRPCExecutor must never call fn")


@pytest.fixture
def mock_insecure_channel(mocker: "MockerFixture") -> MagicMock:
    """Patch insecure_channel to an async-context-manager yielding a sentinel channel."""
    channel_cm = mocker.AsyncMock()
    channel_cm.__aenter__.return_value = mocker.sentinel.channel
    return mocker.patch(
        "mint.worker.executors.grpc.insecure_channel",
        return_value=channel_cm,
    )


class TestExecute:
    """Both an async-def stub method and a sync-multicallable-shaped one must work."""

    async def test_a_coroutine_function_stub_method_is_accepted(
        self,
        mock_insecure_channel: MagicMock,
    ) -> None:
        """The inverted check used to reject exactly this case."""
        executor = GRPCExecutor(URI, AsyncStub, "Call")

        result = await executor.execute(unused_fn, "hello")

        assert result == "echo:hello"
        mock_insecure_channel.assert_called_once_with(URI)

    async def test_a_sync_multicallable_shaped_stub_method_is_accepted(
        self,
        mock_insecure_channel: MagicMock,
    ) -> None:
        """grpc.aio's real stub methods are sync callables returning an awaitable Call."""
        executor = GRPCExecutor(URI, MultiCallableStub, "Call")

        result = await executor.execute(unused_fn, "world")

        assert result == "echo:world"
        mock_insecure_channel.assert_called_once_with(URI)


class TestMissingMethod:
    """A method name that doesn't exist on the stub must fail clearly, not obscurely."""

    async def test_unknown_method_raises_a_typed_error(
        self,
        mock_insecure_channel: MagicMock,
    ) -> None:
        """The exact bug #12 gap: a bad method name used to go undetected until the call."""
        executor = GRPCExecutor(URI, AsyncStub, "DoesNotExist")

        with pytest.raises(RemoteMethodNotFoundError):
            await executor.execute(unused_fn, "hello")
        mock_insecure_channel.assert_called_once_with(URI)
