"""Tests for the loop-keyed client cache shared by the storage backends.

These exercise cache mechanics only, through a stub backend -- no container and
no vendor SDK, so they are fast and deterministic. Backend-specific behaviour
lives in test_s3.py / test_abs.py.
"""

import asyncio
import gc
import threading
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from typing import TYPE_CHECKING, ClassVar, cast
from unittest.mock import MagicMock

import pytest

from mint.fs.asynk import lifecycle as lifecycle_module
from mint.fs.asynk import provider as provider_module
from mint.fs.asynk.client_protocols import IS3Client, ensure_conforms, missing_members
from mint.fs.asynk.lifecycle import (
    atexit_sweep,
    register_atexit_once,
    running_loop,
)
from mint.fs.asynk.provider import ClientFactory, ClientProviderBase
from mint.fs.exc import (
    FactoryNotConfiguredError,
    IncompatibleClientError,
    ProviderClosedError,
)

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

SECRET = "super-secret-key"  # noqa: S105


class FakeConnector:
    """Stands in for an aiohttp connector during the exit sweep."""

    def __init__(self, *, closed: bool = False, explodes: bool = False) -> None:
        """Start open unless told otherwise."""
        self.closed = closed
        self.explodes = explodes
        self.sync_closes = 0

    def _close(self) -> None:
        """Mimic aiohttp's synchronous connector teardown."""
        if self.explodes:
            raise OSError("connector refused to close")
        self.sync_closes += 1
        self.closed = True


class FakeSession:
    """Stands in for an aiohttp ClientSession during the exit sweep."""

    def __init__(self, connector: FakeConnector | None) -> None:
        """Hold the connector the sweep will reach for."""
        self.connector = connector


class FakeClient:
    """A stand-in client that satisfies IS3Client and records its closure.

    A real object rather than a spec'd mock: a spec'd mock reports the spec as
    its ``__class__``, so it passes ``isinstance`` without actually carrying the
    members, which would make the conformance tests vacuous.
    """

    def __init__(self) -> None:
        """Start out open."""
        self.closed = False
        self.close_raises: BaseException | None = None
        self.sessions: tuple[FakeSession, ...] = ()

    async def aclose(self) -> None:
        """Mark the client closed, or fail the way a double close would."""
        if self.close_raises is not None:
            raise self.close_raises
        self.closed = True

    async def list_objects_v2(self, **kwargs: object) -> object:
        """List objects under a prefix."""

    async def get_object(self, **kwargs: object) -> object:
        """Fetch an object."""

    async def put_object(self, **kwargs: object) -> object:
        """Store an object."""

    async def head_object(self, **kwargs: object) -> object:
        """Fetch object metadata."""

    async def copy_object(self, **kwargs: object) -> object:
        """Copy an object."""

    async def delete_object(self, **kwargs: object) -> object:
        """Delete one object."""

    async def delete_objects(self, **kwargs: object) -> object:
        """Delete objects in bulk."""

    async def create_bucket(self, **kwargs: object) -> object:
        """Create the bucket."""

    async def generate_presigned_url(self, *args: object, **kwargs: object) -> object:
        """Build a presigned URL."""


class FakeProvider(ClientProviderBase[FakeClient]):
    """Minimal concrete provider over FakeClient."""

    BACKEND: ClassVar[str] = "fake"
    CLIENT_PROTOCOL: ClassVar[type] = IS3Client

    def __init__(  # noqa: PLR0913
        self,
        endpoint: str = "https://endpoint.invalid",
        credential: str = SECRET,
        *,
        client: FakeClient | None = None,
        client_factory: ClientFactory[FakeClient] | None = None,
        max_pool_connections: int | None = None,
        idle_ttl_seconds: float | None = ClientProviderBase.DEFAULT_IDLE_TTL_SECONDS,
        auto_shutdown: bool = True,
        probe_raises: bool = False,
        slow_build: bool = False,
    ) -> None:
        """Initialize with a stubbed endpoint and credential."""
        self._endpoint = endpoint
        self._credential = credential
        self.probe_raises = probe_raises
        self.slow_build = slow_build
        self.build_raises: BaseException | None = None
        self.build_hook: Callable[[FakeClient], Awaitable[None]] | None = None
        super().__init__(
            client=client,
            client_factory=client_factory,
            max_pool_connections=max_pool_connections,
            idle_ttl_seconds=idle_ttl_seconds,
            auto_shutdown=auto_shutdown,
        )

    def _is_client_open(self, client: FakeClient) -> bool:
        """Probe the stub client's own closed flag.

        Raises:
            RuntimeError: When `probe_raises` is set, standing in for an SDK
                whose internals moved under a version bump.

        """
        if self.probe_raises:
            raise RuntimeError("probe blew up")
        return not client.closed

    @property
    def endpoint(self) -> str:
        """Stub endpoint."""
        return self._endpoint

    @property
    def credential_digest(self) -> str:
        """Digest over the stub credential."""
        return self.digest(self._credential)

    async def _build(self, stack: AsyncExitStack) -> FakeClient:
        """Build a FakeClient and register its teardown."""
        if self.slow_build:
            # Yield so concurrent borrowers actually interleave; without this
            # the first one runs to completion before the second starts, and
            # the post-lock re-check is never reached.
            await asyncio.sleep(0)
        if self.build_raises is not None:
            raise self.build_raises
        client = FakeClient()
        stack.push_async_callback(client.aclose)
        if self.build_hook is not None:
            # Runs while the build is still in flight, which is the window a
            # concurrent aclose() exploits.
            await self.build_hook(client)
        return client

    def _aiohttp_sessions(self, _client: FakeClient) -> tuple[FakeSession, ...]:
        """Expose the stub sessions the exit sweep should reach for."""
        return _client.sessions


class MinimalProvider(ClientProviderBase[FakeClient]):
    """Implements only the abstract surface, overriding nothing optional."""

    BACKEND: ClassVar[str] = "minimal"
    CLIENT_PROTOCOL: ClassVar[type] = IS3Client

    @property
    def endpoint(self) -> str:
        """Stub endpoint."""
        return "https://minimal.invalid"

    @property
    def credential_digest(self) -> str:
        """Stub credential digest."""
        return self.digest("minimal")

    def _is_client_open(self, client: FakeClient) -> bool:
        """Probe the stub client's own closed flag."""
        return not client.closed

    async def _build(self, stack: AsyncExitStack) -> FakeClient:
        """Build a FakeClient and register its teardown."""
        client = FakeClient()
        stack.push_async_callback(client.aclose)
        return client


def _spec_client() -> MagicMock:
    """Build a spec'd double that satisfies IS3Client."""
    return MagicMock(spec=IS3Client)


class TestReuse:
    """A single client serves every borrow on one loop."""

    async def test_repeated_borrow_returns_same_client(self) -> None:
        """Two sequential borrows share one client."""
        provider = FakeProvider()
        async with provider.borrow() as first:
            pass
        async with provider.borrow() as second:
            pass
        assert first is second
        assert provider.created_count == 1

    async def test_concurrent_cold_borrow_builds_one_client(self) -> None:
        """50 racing first-borrows must not build 50 clients."""
        provider = FakeProvider()

        async def borrow_once() -> int:
            async with provider.borrow() as client:
                return id(client)

        ids = await asyncio.gather(*[borrow_once() for _ in range(50)])
        assert len(set(ids)) == 1
        assert provider.created_count == 1

    async def test_nested_borrow_is_reentrant(self) -> None:
        """A borrow inside a borrow yields the same client."""
        provider = FakeProvider()
        async with provider.borrow() as outer, provider.borrow() as inner:
            assert outer is inner
        assert provider.created_count == 1


class TestCacheKey:
    """What does and does not count as the same client."""

    async def test_distinct_endpoints_get_distinct_clients(self) -> None:
        """Endpoint is part of the key."""
        one = FakeProvider(endpoint="https://a.invalid")
        two = FakeProvider(endpoint="https://b.invalid")
        assert one.config_digest != two.config_digest

    async def test_distinct_credentials_get_distinct_clients(self) -> None:
        """Credentials are part of the key even on one endpoint."""
        one = FakeProvider(credential="first")
        two = FakeProvider(credential="second")
        assert one.config_digest != two.config_digest

    async def test_identical_config_shares_a_digest(self) -> None:
        """Same configuration means the same identity."""
        assert FakeProvider().config_digest == FakeProvider().config_digest

    async def test_pool_size_participates_in_the_key(self) -> None:
        """A different pool size is a different client."""
        one = FakeProvider(max_pool_connections=10)
        two = FakeProvider(max_pool_connections=64)
        assert one.config_digest != two.config_digest

    async def test_key_never_carries_the_raw_secret(self) -> None:
        """A key is safe to log: the credential appears only as a digest."""
        provider = FakeProvider()
        async with provider.borrow():
            pass
        keys = repr(list(provider._entries))
        assert SECRET not in keys
        assert provider.credential_digest in keys
        assert SECRET not in provider.config_digest


class TestEventLoopAffinity:
    """aiohttp connectors bind the loop that built them."""

    def test_each_loop_gets_its_own_client(self) -> None:
        """A client is never handed across event loops."""
        provider = FakeProvider()

        async def borrow_once() -> int:
            async with provider.borrow() as client:
                return id(client)

        first = asyncio.run(borrow_once())
        second = asyncio.run(borrow_once())
        assert first != second
        assert provider.created_count == 2

    def test_dead_loop_entry_is_dropped(self) -> None:
        """An entry whose loop has closed is evicted, not reused.

        Uses `auto_shutdown=False` so the entry actually survives its loop --
        with the shutdown fallback armed there would be nothing left to drop.
        """
        provider = FakeProvider(auto_shutdown=False)

        async def borrow_once() -> None:
            async with provider.borrow():
                pass

        asyncio.run(borrow_once())
        assert provider.cached_count == 1
        asyncio.run(borrow_once())
        assert provider.cached_count == 1
        assert provider.created_count == 2


class TestIdleEviction:
    """TTL sweeping is lazy, and never terminal for a live borrow."""

    async def test_idle_client_is_evicted_and_closed(self) -> None:
        """With ttl=0 the next borrow builds a fresh client."""
        provider = FakeProvider(idle_ttl_seconds=0)
        async with provider.borrow() as first:
            pass
        async with provider.borrow() as second:
            pass
        assert first is not second
        assert first.closed is True

    async def test_evicted_client_is_never_handed_back(self) -> None:
        """A closed client must not be reused.

        Azure's AioHttpTransport raises once closed, so recycling an evicted
        client would be a hard failure rather than a slow path.
        """
        provider = FakeProvider(idle_ttl_seconds=0)
        seen: list[FakeClient] = []
        for _ in range(5):
            async with provider.borrow() as client:
                assert client.closed is False
                seen.append(client)
        assert len({id(c) for c in seen}) == 5

    async def test_inflight_client_is_not_swept(self) -> None:
        """A borrow in flight survives another coroutine's sweep."""
        provider = FakeProvider(idle_ttl_seconds=0)

        async def hold() -> bool:
            async with provider.borrow() as client:
                await asyncio.sleep(0.05)
                return client.closed

        async def churn() -> None:
            await asyncio.sleep(0.01)
            async with provider.borrow():
                pass

        closed_during_use, _ = await asyncio.gather(hold(), churn())
        assert closed_during_use is False

    async def test_ttl_none_never_evicts(self) -> None:
        """Eviction is opt-out."""
        provider = FakeProvider(idle_ttl_seconds=None)
        async with provider.borrow() as first:
            pass
        async with provider.borrow() as second:
            pass
        assert first is second


class TestShutdown:
    """aclose() semantics."""

    async def test_aclose_closes_owned_clients(self) -> None:
        """Clients the provider built are closed."""
        provider = FakeProvider()
        async with provider.borrow() as client:
            pass
        await provider.aclose()
        assert client.closed is True
        assert provider.cached_count == 0

    async def test_aclose_is_idempotent(self) -> None:
        """A second aclose is a no-op."""
        provider = FakeProvider()
        async with provider.borrow():
            pass
        await provider.aclose()
        await provider.aclose()
        assert provider.is_closed is True

    async def test_borrow_after_aclose_raises(self) -> None:
        """A closed provider hands out nothing."""
        provider = FakeProvider()
        await provider.aclose()
        with pytest.raises(ProviderClosedError):
            async with provider.borrow():
                pass

    async def test_shared_registry_reuses_one_provider(self) -> None:
        """Equivalent configurations resolve to one shared provider."""
        first = FakeProvider.shared(FakeProvider())
        second = FakeProvider.shared(FakeProvider())
        try:
            assert first is second
        finally:
            await FakeProvider.aclose_shared()

    async def test_shared_registry_replaces_a_closed_provider(self) -> None:
        """A closed shared provider is not handed out again."""
        first = FakeProvider.shared(FakeProvider())
        await first.aclose()
        second = FakeProvider.shared(FakeProvider())
        try:
            assert second is not first
        finally:
            await FakeProvider.aclose_shared()


class TestInjection:
    """Caller-supplied clients and factories."""

    async def test_injected_client_is_used_verbatim(self) -> None:
        """Injection bypasses the cache."""
        injected = _spec_client()
        provider = FakeProvider(client=injected)
        async with provider.borrow() as client:
            assert client is injected
        assert provider.created_count == 0

    async def test_injected_client_is_never_closed(self) -> None:
        """Mint does not close what it does not own."""
        injected = FakeClient()
        provider = FakeProvider(client=injected)
        async with provider.borrow():
            pass
        await provider.aclose()
        assert injected.closed is False

    async def test_non_conforming_client_is_rejected_at_construction(self) -> None:
        """A wrong shape fails at wiring time, naming what is missing."""
        with pytest.raises(IncompatibleClientError) as excinfo:
            FakeProvider(client=cast("FakeClient", object()))
        assert "get_object" in str(excinfo.value)

    async def test_bare_magicmock_is_rejected(self) -> None:
        """A bare MagicMock satisfies no protocol; doubles must be spec'd."""
        with pytest.raises(IncompatibleClientError):
            FakeProvider(client=cast("FakeClient", MagicMock()))

    async def test_factory_is_called_once_per_cache_miss(self) -> None:
        """The factory backs the cache; it is not invoked per operation."""
        calls = 0

        def factory() -> FakeClient:
            nonlocal calls
            calls += 1
            return FakeClient()

        provider = FakeProvider(client_factory=factory)
        for _ in range(5):
            async with provider.borrow():
                pass
        assert calls == 1
        assert provider.created_count == 1

    async def test_factory_may_return_an_async_context_manager(self) -> None:
        """A factory yielding a context manager has its teardown adopted."""
        exited = False

        @asynccontextmanager
        async def make() -> AsyncIterator[FakeClient]:
            nonlocal exited
            try:
                yield FakeClient()
            finally:
                exited = True

        provider = FakeProvider(client_factory=make)
        async with provider.borrow():
            pass
        assert exited is False
        await provider.aclose()
        assert exited is True

    async def test_factory_result_is_validated(self) -> None:
        """A factory returning the wrong shape is rejected too."""
        provider = FakeProvider(
            client_factory=cast("ClientFactory[FakeClient]", object),
        )
        with pytest.raises(IncompatibleClientError):
            async with provider.borrow():
                pass

    async def test_adopt_without_factory_raises(self) -> None:
        """The adoption helper refuses to run without a factory."""
        provider = FakeProvider()
        with pytest.raises(FactoryNotConfiguredError):
            async with AsyncExitStack() as stack:
                await provider._adopt_factory_client(stack)


class TestProtocolNarrowing:
    """narrow() / missing_members() diagnostics."""

    def test_real_client_conforms(self) -> None:
        """An object carrying the members passes the check."""
        ensure_conforms(FakeClient(), IS3Client)

    def test_spec_mock_conforms(self) -> None:
        """A spec'd double passes too."""
        ensure_conforms(_spec_client(), IS3Client)

    def test_non_conforming_is_reported_with_detail(self) -> None:
        """The error names the protocol, the actual type, and the gaps."""
        with pytest.raises(IncompatibleClientError) as excinfo:
            ensure_conforms(object(), IS3Client)
        message = str(excinfo.value)
        assert "IS3Client" in message
        assert "object" in message
        assert "put_object" in message

    def test_missing_members_lists_every_gap(self) -> None:
        """A plain object is missing the whole surface."""
        missing = missing_members(object(), IS3Client)
        assert "get_object" in missing
        assert "generate_presigned_url" in missing

    def test_missing_members_empty_for_conforming(self) -> None:
        """A client that really carries the members reports no gaps."""
        assert missing_members(FakeClient(), IS3Client) == []


class TestShutdownFallback:
    """The loop-teardown hook and the interpreter-exit last resort.

    `loop.shutdown_asyncgens()` finalizes async generators *before*
    `loop.close()` and while the loop is still running, which is the only
    window in which a cached client can actually be closed. `asyncio.run`,
    `asyncio.Runner`, uvicorn and pytest-asyncio all call it.
    """

    def test_loop_teardown_closes_the_client(self) -> None:
        """A caller who never calls aclose still gets a clean shutdown."""
        provider = FakeProvider()
        held: dict[str, FakeClient] = {}

        async def body() -> None:
            async with provider.borrow() as client:
                held["client"] = client

        asyncio.run(body())

        assert held["client"].closed is True
        assert provider.cached_count == 0

    def test_loop_teardown_closes_on_keyboard_interrupt(self) -> None:
        """An interrupted program still releases its clients."""
        provider = FakeProvider()
        held: dict[str, FakeClient] = {}

        async def body() -> None:
            async with provider.borrow() as client:
                held["client"] = client
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            asyncio.run(body())

        assert held["client"].closed is True

    def test_auto_shutdown_false_leaves_the_client_open(self) -> None:
        """Opting out really opts out -- proving the hook did the closing."""
        provider = FakeProvider(auto_shutdown=False)
        held: dict[str, FakeClient] = {}

        async def body() -> None:
            async with provider.borrow() as client:
                held["client"] = client

        asyncio.run(body())

        assert held["client"].closed is False
        assert provider.cached_count == 1

    def test_provider_stays_usable_after_teardown(self) -> None:
        """The hook must not permanently close the provider.

        It calls `aclose_loop`, not `aclose`: a process running several loops
        in sequence -- pytest-asyncio, for one -- must get a fresh client each
        time rather than a ProviderClosedError.
        """
        provider = FakeProvider()

        async def body() -> str:
            async with provider.borrow() as client:
                return type(client).__name__

        assert asyncio.run(body()) == "FakeClient"
        assert asyncio.run(body()) == "FakeClient"
        assert provider.is_closed is False
        assert provider.created_count == 2

    async def test_aclose_loop_is_idempotent(self) -> None:
        """Closing an already-closed loop is a no-op, not an error."""
        provider = FakeProvider()
        async with provider.borrow():
            pass
        await provider.aclose_loop()
        await provider.aclose_loop()
        assert provider.cached_count == 0
        assert provider.is_closed is False

    async def test_already_closed_client_is_not_closed_again(self) -> None:
        """The probe gates the close.

        aiobotocore asserts on a second close, so evicting a client someone
        else already closed must not attempt one.
        """
        provider = FakeProvider()
        async with provider.borrow() as client:
            pass
        client.closed = True

        await provider.aclose()

        assert provider.cached_count == 0

    async def test_probe_failure_is_treated_as_open(self) -> None:
        """An unreadable client is assumed open, because leaking is worse."""
        provider = FakeProvider()
        async with provider.borrow() as client:
            pass
        provider.probe_raises = True

        with pytest.raises(RuntimeError, match="probe blew up"):
            await provider.aclose()

        assert client.closed is False

    async def test_foreign_live_loop_entry_is_left_alone(self) -> None:
        """A client owned by another live loop is skipped, not awaited.

        aiohttp queues socket teardown on the owning loop, so closing across
        loops would leave a callback that never runs and a future bound to the
        wrong loop. The foreign loop is driven on a thread because a loop
        cannot be run from inside another running loop.
        """
        provider = FakeProvider(auto_shutdown=False)
        box: dict[str, FakeClient] = {}
        loop_a = asyncio.new_event_loop()

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)

            async def borrow_once() -> None:
                async with provider.borrow() as client:
                    box["client"] = client

            loop_a.run_until_complete(borrow_once())

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()

        try:
            assert provider.cached_count == 1
            assert not loop_a.is_closed()

            await provider.aclose_loop()  # runs on the test's own loop

            assert box["client"].closed is False
            assert provider.cached_count == 1
        finally:
            loop_a.close()

    async def test_injected_client_survives_every_shutdown_path(self) -> None:
        """Mint never closes a client it does not own, even at shutdown."""
        injected = FakeClient()
        provider = FakeProvider(client=injected)
        async with provider.borrow():
            pass

        await provider.aclose_loop()
        await provider.aclose()
        provider._exit_sweep()

        assert injected.closed is False

    async def test_exit_sweep_counts_only_genuinely_open_clients(self) -> None:
        """The exit report must not cry wolf over clients already closed."""
        provider = FakeProvider(auto_shutdown=False)
        async with provider.borrow() as client:
            pass

        assert provider._exit_sweep() == 1

        client.closed = True
        assert provider._exit_sweep() == 0

    def test_exit_sweep_reports_but_does_not_fake_close_dead_loops(self) -> None:
        """A dead loop's client is reported, never marked closed.

        Synchronously closing a connector whose loop is gone would flip its
        `_closed` flag and silence aiohttp's warnings without ever sending a
        FIN -- hiding the leak instead of fixing it.
        """
        provider = FakeProvider(auto_shutdown=False)
        box: dict[str, FakeClient] = {}

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                box["client"] = client

        asyncio.run(borrow_once())  # loop is closed on return

        assert provider._exit_sweep() == 1
        assert box["client"].closed is False


class TestExitSweepTeardown:
    """The interpreter-exit last resort: report, and release what it may."""

    def _stranded(self, *, connector: FakeConnector | None) -> FakeProvider:
        """Build a provider holding one open client on a dead loop."""
        provider = FakeProvider(auto_shutdown=False)

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                client.sessions = (FakeSession(connector),)

        asyncio.run(borrow_once())
        return provider

    def test_live_loop_connector_is_closed_synchronously(self) -> None:
        """With the loop alive, sockets are released without awaiting."""
        provider = FakeProvider(auto_shutdown=False)
        connector = FakeConnector()

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                client.sessions = (FakeSession(connector),)
                # Sweep from inside the loop, so entry.loop is alive.
                assert provider._exit_sweep() == 1

        asyncio.run(borrow_once())

        assert connector.sync_closes == 1
        assert connector.closed is True

    def test_dead_loop_connector_is_left_untouched(self) -> None:
        """A dead loop's connector is reported, never flipped to closed.

        Calling `_close()` there would silence aiohttp's warnings without ever
        sending a FIN, hiding the leak instead of fixing it.
        """
        connector = FakeConnector()
        provider = self._stranded(connector=connector)

        assert provider._exit_sweep() == 1
        assert connector.sync_closes == 0
        assert connector.closed is False

    def test_already_closed_connector_is_skipped(self) -> None:
        """A connector someone else closed is not closed twice."""
        provider = FakeProvider(auto_shutdown=False)
        connector = FakeConnector(closed=True)

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                client.sessions = (FakeSession(connector),)
                provider._exit_sweep()

        asyncio.run(borrow_once())
        assert connector.sync_closes == 0

    def test_session_without_a_connector_is_skipped(self) -> None:
        """A detached session has nothing to release."""
        provider = FakeProvider(auto_shutdown=False)

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                client.sessions = (FakeSession(None),)
                assert provider._exit_sweep() == 1

        asyncio.run(borrow_once())

    def test_connector_failure_is_swallowed(self) -> None:
        """Exit is the worst place to raise, so a bad connector is tolerated."""
        provider = FakeProvider(auto_shutdown=False)
        connector = FakeConnector(explodes=True)

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                client.sessions = (FakeSession(connector),)
                assert provider._exit_sweep() == 1

        asyncio.run(borrow_once())
        assert connector.closed is False

    def test_atexit_sweep_reports_stranded_clients(self) -> None:
        """The module-level handler walks every live provider."""
        provider = self._stranded(connector=FakeConnector())
        try:
            atexit_sweep()
        finally:
            asyncio.run(provider.aclose())

    def test_atexit_sweep_survives_a_broken_provider(self) -> None:
        """One provider raising must not stop the others being swept."""
        provider = self._stranded(connector=FakeConnector())
        provider.probe_raises = True
        try:
            atexit_sweep()  # must not propagate
        finally:
            provider.probe_raises = False
            asyncio.run(provider.aclose())

    def test_atexit_sweep_is_quiet_when_nothing_is_open(self) -> None:
        """A tidy process gets no exit noise."""
        provider = FakeProvider()

        async def borrow_once() -> None:
            async with provider.borrow():
                pass

        asyncio.run(borrow_once())  # hook closes it
        assert provider._exit_sweep() == 0
        atexit_sweep()

    def test_registering_atexit_twice_is_harmless(self) -> None:
        """Every provider calls the registrar; only the first arms it."""
        register_atexit_once()
        register_atexit_once()
        assert FakeProvider() is not None

    def test_running_loop_helper_outside_a_loop(self) -> None:
        """The helper reports None rather than raising off-loop."""
        assert running_loop() is None

    async def test_running_loop_helper_inside_a_loop(self) -> None:
        """And returns the loop when there is one."""
        assert running_loop() is asyncio.get_running_loop()


class TestEvictionEdges:
    """Branches of `_evict` that only a deliberate setup reaches."""

    async def test_evicting_an_unknown_key_is_a_noop(self) -> None:
        """A double eviction must not raise."""
        provider = FakeProvider()
        async with provider.borrow():
            pass
        key = next(iter(provider._entries))
        await provider._evict(key)
        await provider._evict(key)
        assert provider.cached_count == 0

    async def test_close_assertion_error_is_downgraded(self) -> None:
        """Aiobotocore asserts on a double close; that must not be a traceback.

        The open-probe makes this a narrow race rather than the normal path,
        but the race is real, so eviction absorbs it quietly.
        """
        provider = FakeProvider()
        async with provider.borrow() as client:
            client.close_raises = AssertionError("Session was never entered")

        await provider.aclose()

        assert provider.cached_count == 0

    async def test_other_close_errors_are_logged_not_raised(self) -> None:
        """An unexpected close failure must not break shutdown."""
        provider = FakeProvider()
        async with provider.borrow() as client:
            client.close_raises = OSError("boom")

        await provider.aclose()

        assert provider.cached_count == 0

    def test_dead_loop_entry_is_dropped_without_closing(self) -> None:
        """An entry whose loop died is dropped, not closed."""
        provider = FakeProvider(auto_shutdown=False)
        box: dict[str, FakeClient] = {}

        async def borrow_once() -> None:
            async with provider.borrow() as client:
                box["client"] = client

        asyncio.run(borrow_once())  # loop closes on return
        asyncio.run(provider.aclose())  # a different, live loop

        assert provider.cached_count == 0
        assert box["client"].closed is False


class TestForeignLoopEviction:
    """`aclose` walks every entry, including other loops' -- and must skip them."""

    async def test_aclose_skips_entries_owned_by_another_live_loop(self) -> None:
        """Closing across live loops would hang, so those entries are left.

        aiohttp queues socket teardown on the owning loop; from here that
        callback would never run and the future would belong to the wrong loop.
        """
        provider = FakeProvider(auto_shutdown=False)
        box: dict[str, FakeClient] = {}
        loop_a = asyncio.new_event_loop()

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)

            async def borrow_once() -> None:
                async with provider.borrow() as client:
                    box["client"] = client

            loop_a.run_until_complete(borrow_once())

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()

        try:
            assert not loop_a.is_closed()

            await provider.aclose()  # walks all entries, including loop_a's

            assert box["client"].closed is False
            assert provider.cached_count == 1
        finally:
            loop_a.close()


class TestDefaultSessionDiscovery:
    """A backend that supplies no sessions degrades to report-only."""

    async def test_base_returns_no_sessions(self) -> None:
        """The default keeps the exit sweep from guessing at internals."""
        provider = MinimalProvider()
        async with provider.borrow() as client:
            assert provider._aiohttp_sessions(client) == ()
        await provider.aclose()


class TestBuildRace:
    """The per-key lock, exercised with builds that genuinely interleave."""

    async def test_interleaved_cold_borrows_build_one_client(self) -> None:
        """Racing borrowers share the client the lock winner built.

        `slow_build` forces a yield inside `_build`; without it the first
        borrower runs to completion before the second starts, and the
        post-lock re-check is never reached.
        """
        provider = FakeProvider(slow_build=True)

        async def borrow_once() -> int:
            async with provider.borrow() as client:
                return id(client)

        ids = await asyncio.gather(*[borrow_once() for _ in range(25)])

        assert len(set(ids)) == 1
        assert provider.created_count == 1


class _RegistryPoppedElsewhere(dict):  # type: ignore[type-arg]
    """A registry whose entries vanish before this thread can pop them."""

    def pop(self, _key: object, default: object = None) -> object:
        """Report every key as already taken by another thread."""
        return default


class TestSharedRegistryRaces:
    """`_SHARED` is process-global and reachable from several threads."""

    async def test_aclose_shared_tolerates_a_concurrent_pop(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """Two threads shutting down at once must not trip over each other.

        `aclose_shared` lists the keys it owns, then pops them one at a time;
        another thread can win the pop in between, leaving None.
        """
        stolen = _RegistryPoppedElsewhere(
            {(FakeProvider.__qualname__, "digest"): FakeProvider()},
        )
        mocker.patch.object(lifecycle_module, "SHARED", stolen)
        mocker.patch.object(provider_module, "SHARED", stolen)

        await FakeProvider.aclose_shared()  # must not raise

    async def test_aclose_shared_closes_every_provider_despite_one_failure(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A provider that fails to close must not strand the others."""
        bad = FakeProvider.shared(FakeProvider(endpoint="https://bad.invalid"))
        good = FakeProvider.shared(FakeProvider(endpoint="https://good.invalid"))
        mocker.patch.object(bad, "aclose", side_effect=OSError("nope"))

        await FakeProvider.aclose_shared()

        assert good.is_closed is True


class TestShutdownHardening:
    """Edges found by review: races and keying around the shutdown paths."""

    def test_sentinel_cache_is_weakly_keyed(self) -> None:
        """A loop torn down without shutdown_asyncgens leaves no entry behind.

        An `id(loop)`-keyed map would keep it forever, and a later loop at the
        same address would look already-armed and silently get no fallback.
        """
        provider = FakeProvider(auto_shutdown=False)

        async def body() -> None:
            async with provider.borrow():
                pass

        asyncio.run(body())
        provider._entries.clear()  # the last strong ref to that loop
        gc.collect()

        assert len(provider._sentinels) == 0

    def test_each_loop_arms_its_own_sentinel(self) -> None:
        """Sequential loops each get the fallback, not just the first."""
        provider = FakeProvider()
        closed: list[bool] = []

        async def body() -> None:
            async with provider.borrow() as client:
                pass
            closed.append(client.closed)

        for _ in range(3):
            asyncio.run(body())

        # Each run's client is closed by that run's own teardown.
        assert provider.cached_count == 0
        assert provider.created_count == 3

    async def test_evict_never_touches_the_build_lock(self) -> None:
        """Eviction must not drop a lock; only loop shutdown may.

        `locked()` reads False while waiters are still queued, so eviction has
        no safe signal to prune on. See `TestBuildLockLifetime` for what goes
        wrong when it tries.
        """
        provider = FakeProvider(slow_build=True)
        key = provider._key(asyncio.get_running_loop())
        lock = provider._locks.setdefault(key, asyncio.Lock())

        await provider._evict(key)

        assert provider._locks.get(key) is lock

    async def test_build_after_close_is_refused(self) -> None:
        """A provider closed while a borrow waited must not gain a client.

        `borrow` checks `_closed` once up front and then awaits; without a
        re-check inside the per-key lock the new client would land in a closed
        provider and escape shutdown entirely.
        """
        provider = FakeProvider()
        # Calling `_entry` directly bypasses borrow's up-front check, which is
        # exactly the window a concurrent `aclose()` opens.
        provider._closed = True

        with pytest.raises(ProviderClosedError):
            await provider._entry()

        assert provider.cached_count == 0


class TestShutdownRaces:
    """Windows opened by `aclose()` being async and `_build` awaiting."""

    async def test_client_built_during_aclose_is_closed_not_stranded(self) -> None:
        """`_closed` must be re-checked *after* the build, not only before.

        The build is where the awaiting happens, so `aclose()` can run to
        completion in that window. Publishing then would leave a live client in
        a closed provider that nothing can ever reach: `borrow` refuses on
        `_closed`, so no sweep runs, and a second `aclose` is a no-op.
        """
        provider = FakeProvider(slow_build=True)
        built: list[FakeClient] = []

        async def close_mid_build(client: FakeClient) -> None:
            built.append(client)
            await provider.aclose()  # completes while this build is in flight

        provider.build_hook = close_mid_build

        with pytest.raises(ProviderClosedError):
            async with provider.borrow():
                pass

        assert provider.cached_count == 0, "client must not be stranded"
        assert built, "the build did run"
        assert built[0].closed is True, "and its client must be closed"

    async def test_repeated_failed_builds_cache_nothing(self) -> None:
        """A build that never succeeds leaves no entry behind.

        Its lock deliberately stays -- see `TestBuildLockLifetime` -- and is
        cleaned up at loop shutdown instead.
        """
        provider = FakeProvider()
        provider.build_raises = OSError("build failed")

        for _ in range(3):
            with pytest.raises(OSError, match="build failed"):
                async with provider.borrow():
                    pass

        assert provider.cached_count == 0
        assert provider.created_count == 0

        await provider.aclose_loop()
        assert provider._locks == {}

    async def test_aclose_keeps_sentinels_for_loops_it_left_open(self) -> None:
        """Dropping a foreign loop's sentinel would close its clients via GC.

        `_sentinels` holds the only strong reference to each generator, and
        asyncio's registry is a WeakSet -- so clearing the map lets GC finalize
        them, which schedules `aclose_loop()` on that loop at a nondeterministic
        point, potentially mid-operation.
        """
        provider = FakeProvider()
        loop_a = asyncio.new_event_loop()
        box: dict[str, FakeClient] = {}

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)

            async def borrow_once() -> None:
                async with provider.borrow() as client:
                    box["client"] = client

            loop_a.run_until_complete(borrow_once())

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()

        try:
            assert len(provider._sentinels) == 1

            await provider.aclose()
            gc.collect()

            assert loop_a in provider._sentinels, "foreign loop keeps its sentinel"
            assert box["client"].closed is False
        finally:
            loop_a.close()

    async def test_aclose_drops_sentinels_for_loops_it_closed(self) -> None:
        """The map must not grow for loops that were actually cleaned up."""
        provider = FakeProvider()
        async with provider.borrow():
            pass

        assert len(provider._sentinels) == 1

        await provider.aclose()

        assert len(provider._sentinels) == 0


class TestBuildLockLifetime:
    """Locks are dropped only at shutdown, never opportunistically.

    `asyncio.Lock.release()` clears `_locked` before the woken waiter runs, so
    `locked()` reads False while waiters are still queued. Deleting on that
    signal lets a later caller create a second lock for the same key and build
    concurrently -- orphaning one of the two clients with its connector open.
    """

    async def test_failed_build_does_not_orphan_a_concurrent_client(self) -> None:
        """The exact race the review reproduced: fail one build, race two more."""
        provider = FakeProvider(slow_build=True)
        provider.build_raises = OSError("first build fails")
        seen: list[FakeClient | None] = []

        async def borrow() -> None:
            try:
                async with provider.borrow() as client:
                    seen.append(client)
            except OSError:
                seen.append(None)
                provider.build_raises = None  # only the first attempt fails

        await asyncio.gather(*[borrow() for _ in range(3)])

        survivors = [c for c in seen if c is not None]
        assert provider.created_count == 1, "the lock must serialise the rebuild"
        assert len({id(c) for c in survivors}) == 1, "all borrowers share one client"
        assert provider.cached_count == 1

    async def test_lock_survives_a_failed_build(self) -> None:
        """A failed build leaves the lock in place for the next builder."""
        provider = FakeProvider()
        provider.build_raises = OSError("nope")
        key = provider._key(asyncio.get_running_loop())

        with pytest.raises(OSError, match="nope"):
            async with provider.borrow():
                pass

        assert key in provider._locks

    async def test_locks_are_dropped_on_loop_shutdown(self) -> None:
        """They are cleaned up, just at a moment when no builder can be waiting."""
        provider = FakeProvider()
        async with provider.borrow():
            pass

        assert provider._locks != {}

        await provider.aclose_loop()

        assert provider._locks == {}

    async def test_aclose_keeps_locks_for_loops_it_left_open(self) -> None:
        """A foreign live loop may still be building; leave its lock alone."""
        provider = FakeProvider(auto_shutdown=False)
        loop_a = asyncio.new_event_loop()

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)

            async def borrow_once() -> None:
                async with provider.borrow():
                    pass

            loop_a.run_until_complete(borrow_once())

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()

        try:
            await provider.aclose()
            assert any(k.loop_id == id(loop_a) for k in provider._locks)
        finally:
            loop_a.close()


class TestProviderIdentity:
    """Lifetime policy is part of what makes two providers interchangeable."""

    def test_idle_ttl_separates_shared_providers(self) -> None:
        """`shared()` must not hand back a different eviction policy."""
        default = FakeProvider()
        never = FakeProvider(idle_ttl_seconds=None)

        assert default.config_digest != never.config_digest

    def test_identical_policy_still_shares(self) -> None:
        """The guard must not fragment providers that really do match."""
        assert FakeProvider().config_digest == FakeProvider().config_digest


class TestSharedRegistryRejectsInjection:
    """An injected client is caller-specific and must never be shared.

    `config_digest` covers configuration, not injection, so registering such a
    provider would let an unrelated storage with matching credentials silently
    borrow someone else's client.
    """

    async def test_injected_provider_is_not_registered(self) -> None:
        """`shared()` hands the candidate straight back."""
        injected = FakeProvider(client=FakeClient())
        returned = FakeProvider.shared(injected)
        try:
            plain = FakeProvider.shared(FakeProvider())
            assert returned is injected
            assert plain is not injected
        finally:
            await FakeProvider.aclose_shared()
            await injected.aclose()

    async def test_factory_provider_is_not_registered(self) -> None:
        """Same for a provider backed by a caller's factory."""
        with_factory = FakeProvider(client_factory=FakeClient)
        returned = FakeProvider.shared(with_factory)
        try:
            plain = FakeProvider.shared(FakeProvider())
            assert returned is with_factory
            assert plain is not with_factory
        finally:
            await FakeProvider.aclose_shared()
            await with_factory.aclose()

    async def test_plain_providers_still_share(self) -> None:
        """The guard must not stop ordinary configuration-only sharing."""
        first = FakeProvider.shared(FakeProvider())
        try:
            assert FakeProvider.shared(FakeProvider()) is first
        finally:
            await FakeProvider.aclose_shared()
