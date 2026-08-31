"""Tests for the per-backend "is this client still open?" probes.

These gate every shutdown path: eviction, the loop-teardown fallback and the
interpreter-exit sweep all refuse to touch a client the probe reports closed.
The probes read private SDK state because neither SDK exposes a public flag,
so they are worth testing directly against real client objects -- which is
possible offline, since building a client makes no network call.
"""

import asyncio
import gc
import itertools
import threading
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

import pytest
from azure.identity.aio import ClientSecretCredential
from azure.storage.blob._shared.constants import CONNECTION_TIMEOUT, READ_TIMEOUT
from azure.storage.blob.aio import BlobServiceClient

from mint.fs.asynk.abs import AzureBlobStorage
from mint.fs.asynk.abs_provider import BlobClientProvider
from mint.fs.asynk.client_protocols import (
    IBlobServiceClient,
    ensure_conforms,
    missing_members,
)
from mint.fs.asynk.lifecycle import TRANSPORT_UNWRAP_LIMIT
from mint.fs.asynk.s3 import S3Storage
from mint.fs.asynk.s3_provider import S3ClientProvider
from mint.fs.asynk.structs import AzureCredentialMode
from mint.fs.exc import (
    ClientNotInitializedError,
    ConflictingClientSourceError,
    IncompatibleClientError,
    InvalidArgumentsError,
    ProviderClosedError,
)

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from azure.core.pipeline.transport import AioHttpTransport
    from pytest_mock.plugin import MockerFixture
    from types_aiobotocore_s3.client import S3Client

    from mint.fs.asynk.provider import ClientFactory

AZURITE_CS = (
    "DefaultEndpointsProtocol=http;AccountName=dev;AccountKey=a2V5;"
    "BlobEndpoint=http://127.0.0.1:1/dev;"
)
UNREACHABLE_S3 = "http://127.0.0.1:1"

_DETACHED_LOOP = asyncio.new_event_loop()
"""A loop used only as a cache key where no client is ever driven."""


def _s3_provider(*, auto_shutdown: bool = True) -> S3ClientProvider:
    """Build an S3 provider pointed at an endpoint nothing answers.

    Args:
        auto_shutdown: Whether to arm the loop-teardown fallback.

    Returns:
        A provider whose clients can be built but never connect.

    """
    return S3ClientProvider(
        endpoint_url=UNREACHABLE_S3,
        access_key="a",
        secret_key="b",  # noqa: S106
        region_name="us-east-1",
        auto_shutdown=auto_shutdown,
    )


class TestS3OpenProbe:
    """`S3ClientProvider._is_client_open` against real aiobotocore clients."""

    async def test_entered_client_reports_open(self) -> None:
        """A client the provider built and entered is open."""
        provider = _s3_provider(auto_shutdown=False)
        async with provider.borrow() as client:
            assert provider._is_client_open(client) is True
        await provider.aclose()

    async def test_closed_client_reports_closed(self) -> None:
        """Aiobotocore nulls its `_sessions` dict on exit, which the probe reads.

        This is the case that makes the probe mandatory rather than an
        optimisation: aiobotocore asserts on a second close.
        """
        provider = _s3_provider(auto_shutdown=False)
        async with provider.borrow() as client:
            pass
        await provider.aclose()

        assert provider._is_client_open(client) is False

    async def test_unreadable_client_is_assumed_open(self) -> None:
        """An unrecognised shape errs toward closing, because leaking is worse.

        If an SDK upgrade moves the internals, mint should still attempt the
        close rather than silently abandon a live connection pool.
        """
        provider = _s3_provider(auto_shutdown=False)

        unreadable = cast("S3Client", object())
        assert provider._is_client_open(unreadable) is True
        assert provider._aiohttp_sessions(unreadable) == ()

    async def test_sessions_are_empty_before_any_request(self) -> None:
        """Aiobotocore creates its aiohttp session lazily, on first request."""
        provider = _s3_provider(auto_shutdown=False)
        async with provider.borrow() as client:
            assert provider._aiohttp_sessions(client) == ()
        await provider.aclose()

    async def test_double_close_would_raise_without_the_probe(self) -> None:
        """Pin the SDK behaviour the probe exists to avoid.

        If aiobotocore ever makes this idempotent the probe stops being
        load-bearing, and this test is how we would find out.
        """
        provider = _s3_provider(auto_shutdown=False)
        async with provider.borrow() as client:
            pass
        await provider.aclose()

        with pytest.raises(AssertionError):
            await client.close()


class TestAzureOpenProbe:
    """`BlobClientProvider._is_client_open` against real blob clients."""

    async def test_never_opened_client_reports_closed(self) -> None:
        """A client that never issued a request holds no session to close.

        `AioHttpTransport` creates its session lazily in `open()`, so reporting
        it closed is correct: there is nothing to release.
        """
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)
        client = BlobServiceClient.from_connection_string(AZURITE_CS)
        try:
            assert provider._is_client_open(client) is False
            assert provider._aiohttp_sessions(client) == ()
        finally:
            await client.close()

    async def test_pool_limited_client_reports_open_then_closed(self) -> None:
        """Supplying a bounded connector creates the session eagerly."""
        provider = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=8,
        )
        client = provider._create_client(asyncio.get_running_loop())

        assert provider._is_client_open(client) is True
        assert len(provider._aiohttp_sessions(client)) == 1

        await client.close()

        assert provider._is_client_open(client) is False
        assert provider._aiohttp_sessions(client) == ()

    async def test_child_clients_unwrap_to_the_parent_transport(self) -> None:
        """`AsyncTransportWrapper` nests, so unwrapping needs a bounded loop.

        A container client wraps the service client's transport, and a blob
        client wraps that wrapper -- but all three must resolve to the one real
        transport, since only it owns the socket.
        """
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)
        service = BlobServiceClient.from_connection_string(AZURITE_CS)
        try:
            real = provider._transport_of(service)
            container = service.get_container_client("c")
            blob = container.get_blob_client("b")

            assert provider._transport_of(container) is real
            assert provider._transport_of(blob) is real
        finally:
            await service.close()

    async def test_unreadable_object_yields_no_transport(self) -> None:
        """An unrecognised object is reported open, and offers no sessions."""
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)

        unreadable = cast("BlobServiceClient", object())
        assert provider._transport_of(unreadable) is None
        assert provider._is_client_open(unreadable) is True
        assert provider._aiohttp_sessions(unreadable) == ()


class TestAzureCredentialShutdown:
    """The credential cache is keyed by loop and lives outside `_entries`.

    Nothing but an explicit close reaches it, which is exactly the leak the
    provider exists to fix: the old code built a credential per operation and
    never closed one.
    """

    @staticmethod
    def _provider() -> BlobClientProvider:
        """Build a provider in client-secret mode, which caches a credential."""
        return BlobClientProvider(
            "dev",
            tenant_id="tenant",
            client_id="client",
            client_secret="secret",  # noqa: S106
        )

    async def test_credential_is_cached_per_loop(self) -> None:
        """One credential per loop, so its token cache is shared."""
        provider = self._provider()
        loop = asyncio.get_running_loop()

        assert provider._token_credential(loop) is provider._token_credential(loop)

        await provider.aclose()

    async def test_aclose_loop_closes_this_loop_s_credential(self) -> None:
        """Per-loop shutdown must not leave the credential behind."""
        provider = self._provider()
        credential = provider._token_credential(asyncio.get_running_loop())

        await provider.aclose_loop()

        transport = provider._transport_of(credential)
        assert transport is not None, "credential transport must be reachable"
        assert transport.session is None, "credential was not actually closed"
        assert provider._credentials == {}
        assert provider.is_closed is False

    async def test_close_credential_ignores_none(self) -> None:
        """No credential cached for this loop is not an error."""
        provider = self._provider()
        await provider._close_credential(None)
        await provider.aclose()

    async def test_close_credential_skips_an_already_closed_one(self) -> None:
        """A credential someone else closed is left alone."""
        provider = self._provider()
        credential = ClientSecretCredential("t", "c", "s")
        await credential.close()

        await provider._close_credential(credential)

        await provider.aclose()

    async def test_aclose_closes_every_cached_credential(self) -> None:
        """Full shutdown drains the credential cache."""
        provider = self._provider()
        provider._token_credential(asyncio.get_running_loop())

        await provider.aclose()

        assert provider._credentials == {}
        assert provider.is_closed is True


class TestAzureCredentialModes:
    """Every credential mode must resolve and build a client offline.

    These paths decide which credential object gets cached and therefore what
    shutdown has to close, so they are part of the shutdown surface.
    """

    def test_sas_token_mode(self) -> None:
        """A SAS token needs no credential object to close."""
        provider = BlobClientProvider("dev", sas_token="sig=abc")  # noqa: S106
        assert provider.mode is AzureCredentialMode.SharedAccessSignature
        assert provider._create_client(_DETACHED_LOOP) is not None
        assert provider._credentials == {}

    def test_shared_access_key_mode(self) -> None:
        """A shared key is a plain string, so nothing is cached to close."""
        provider = BlobClientProvider("dev", shared_access_key="a2V5")
        assert provider.mode is AzureCredentialMode.SharedAccessKey
        assert provider._create_client(_DETACHED_LOOP) is not None
        assert provider._credentials == {}

    def test_client_secret_mode_caches_a_credential(self) -> None:
        """Service-principal auth is the mode that creates a closeable object."""
        provider = BlobClientProvider(
            "dev",
            tenant_id="t",
            client_id="c",
            client_secret="s",  # noqa: S106
        )
        assert provider.mode is AzureCredentialMode.ClientSecret
        loop = asyncio.new_event_loop()
        try:
            assert provider._create_client(loop) is not None
            assert loop in provider._credentials
        finally:
            loop.close()

    def test_env_shared_access_key_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The env-var fallbacks resolve the same way as explicit arguments.

        Args:
            monkeypatch: pytest fixture for patching the environment.

        """
        monkeypatch.setenv(BlobClientProvider.AzureStorageAccessKey, "a2V5")
        monkeypatch.delenv(BlobClientProvider.AzureStorageConnectionString, raising=False)
        provider = BlobClientProvider("dev")
        assert provider.mode is AzureCredentialMode.EnvVarSharedAccessKey

    def test_env_connection_string_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Connection string via environment.

        Args:
            monkeypatch: pytest fixture for patching the environment.

        """
        monkeypatch.delenv(BlobClientProvider.AzureStorageAccessKey, raising=False)
        monkeypatch.setenv(BlobClientProvider.AzureStorageConnectionString, AZURITE_CS)
        provider = BlobClientProvider("dev")
        assert provider.mode is AzureCredentialMode.EnvVarConnectionString
        assert provider._create_client(_DETACHED_LOOP) is not None

    def test_default_mode_caches_a_credential(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """With nothing configured, DefaultAzureCredential is cached and owned.

        Args:
            monkeypatch: pytest fixture for patching the environment.

        """
        monkeypatch.delenv(BlobClientProvider.AzureStorageAccessKey, raising=False)
        monkeypatch.delenv(BlobClientProvider.AzureStorageConnectionString, raising=False)
        provider = BlobClientProvider("dev")
        assert provider.mode is AzureCredentialMode.Default
        loop = asyncio.new_event_loop()
        try:
            assert provider._create_client(loop) is not None
            assert loop in provider._credentials
        finally:
            loop.close()

    def test_unsupported_mode_is_rejected(self) -> None:
        """An unreachable mode raises rather than building a broken client."""
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)
        provider.mode = cast("AzureCredentialMode", "nonsense")
        with pytest.raises(InvalidArgumentsError):
            provider._create_client(_DETACHED_LOOP)

    async def test_pool_limit_supplies_an_explicit_transport(self) -> None:
        """A bounded connector requires handing the SDK our own session."""
        provider = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=4,
        )
        transport = provider._transport()
        assert transport is not None
        assert transport.session is not None
        await transport.close()

    def test_no_pool_limit_uses_the_sdk_default_transport(self) -> None:
        """Without a limit mint does not override the transport at all."""
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)
        assert provider._transport() is None


class TestAzureCredentialCloseIsReached:
    """An *open* credential really is closed, not just dropped."""

    async def test_open_credential_is_closed(self, mocker: "MockerFixture") -> None:
        """Force the transport to look open so the close path is taken.

        A credential only opens its session on a real token request, which this
        suite deliberately never makes.

        Args:
            mocker: pytest-mock fixture.

        """
        provider = BlobClientProvider(
            "dev",
            tenant_id="t",
            client_id="c",
            client_secret="s",  # noqa: S106
        )
        credential = provider._token_credential(asyncio.get_running_loop())
        transport = provider._transport_of(credential)
        assert transport is not None
        mocker.patch.object(transport, "session", object())
        close_spy = mocker.patch.object(credential, "close", new_callable=mocker.AsyncMock)

        await provider.aclose()

        close_spy.assert_awaited_once()


class _FakeWrapper:
    """Mimics `AsyncTransportWrapper`: delegates, owns no session."""

    def __init__(self, inner: object) -> None:
        """Wrap another transport."""
        self._transport = inner


class _FakePipelineHolder:
    """Mimics the `_pipeline._transport` shape the unwrap walks."""

    def __init__(self, transport: object) -> None:
        """Expose a pipeline carrying the given transport."""
        self._pipeline = SimpleNamespace(_transport=transport)


class TestTransportUnwrapLimits:
    """The unwrap loop is bounded and defensive by design.

    `AsyncTransportWrapper` nests, and only the innermost real transport owns a
    session -- so the walk must terminate on both a chain that ends in
    something unrecognised and a chain that is absurdly deep.
    """

    @staticmethod
    def _provider() -> BlobClientProvider:
        """Build any provider; the unwrap does not depend on credentials."""
        return BlobClientProvider("dev", connection_string=AZURITE_CS)

    def test_chain_ending_without_a_real_transport(self) -> None:
        """A wrapper around something inert yields no transport."""
        holder = _FakePipelineHolder(_FakeWrapper(object()))
        assert self._provider()._transport_of(holder) is None

    def test_chain_deeper_than_the_bound(self) -> None:
        """An implausibly deep chain stops rather than looping forever."""
        node: object = object()
        for _ in range(TRANSPORT_UNWRAP_LIMIT + 2):
            node = _FakeWrapper(node)
        holder = _FakePipelineHolder(node)

        assert self._provider()._transport_of(holder) is None

    def test_chain_exactly_at_the_bound_still_resolves(self) -> None:
        """The bound is generous enough for any real nesting.

        Real nesting is at most service -> container -> blob, so a chain this
        deep already means something is wrong; it must still not hang.
        """
        provider = self._provider()
        service = BlobServiceClient.from_connection_string(AZURITE_CS)
        try:
            real = provider._transport_of(service)
            node: object = real
            for _ in range(TRANSPORT_UNWRAP_LIMIT - 1):
                node = _FakeWrapper(node)
            assert provider._transport_of(_FakePipelineHolder(node)) is real
        finally:
            asyncio.run(service.close())


class TestStorageProviderDelegation:
    """The storage classes expose their provider's resolved state.

    `mode` and `params` used to be instance attributes; they now delegate, so
    callers and existing tests that read them keep working after the provider
    took over credential resolution.
    """

    def test_s3_storage_delegates_mode_and_params(self) -> None:
        """S3Storage forwards credential state to its provider."""
        storage = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
            region_name="us-east-1",
        )
        assert storage.mode is storage.provider.mode
        assert storage.params == storage.provider.params
        assert storage.params["aws_access_key_id"] == "a"

    def test_azure_storage_delegates_mode_and_params(self) -> None:
        """AzureBlobStorage forwards credential state to its provider."""
        storage = AzureBlobStorage(
            container_name="c",
            storage_account_name="dev",
            connection_string=AZURITE_CS,
        )
        assert storage.mode is storage.provider.mode
        assert storage.params == storage.provider.params
        assert storage.account_url == storage.provider.account_url

    def test_client_property_raises_outside_an_operation(self) -> None:
        """Reaching for the client with nothing bound names the storage class.

        Replaces a bare `RuntimeError` from before the provider existed.
        """
        s3 = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
        )
        abs_storage = AzureBlobStorage(
            container_name="c",
            storage_account_name="dev",
            connection_string=AZURITE_CS,
        )

        with pytest.raises(ClientNotInitializedError, match="S3Storage"):
            _ = s3.client
        with pytest.raises(ClientNotInitializedError, match="AzureBlobStorage"):
            _ = abs_storage.client

    def test_auto_shutdown_reaches_the_provider(self) -> None:
        """The opt-out must be usable without building a provider by hand.

        Most callers configure storage with plain credential kwargs and never
        touch a provider, so an opt-out only reachable through the provider
        constructor would be unreachable for them.
        """
        default = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
        )
        opted_out = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
            auto_shutdown=False,
        )

        assert default.provider.auto_shutdown is True
        assert opted_out.provider.auto_shutdown is False

    def test_auto_shutdown_separates_shared_providers(self) -> None:
        """Storages that disagree about shutdown must not share a provider.

        Otherwise whichever was constructed first would silently decide the
        behaviour for the other.
        """
        on = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
        )
        off = S3Storage(
            bucket_name="b",
            endpoint_url=UNREACHABLE_S3,
            access_key="a",
            secret_key="b",  # noqa: S106
            auto_shutdown=False,
        )

        assert on.provider is not off.provider

    def test_azure_auto_shutdown_reaches_the_provider(self) -> None:
        """Same passthrough on the Azure backend."""
        storage = AzureBlobStorage(
            container_name="c",
            storage_account_name="dev",
            connection_string=AZURITE_CS,
            auto_shutdown=False,
        )
        assert storage.provider.auto_shutdown is False


class TestSessionLoopAffinity:
    """The aiobotocore session cache must not outlive its loop.

    A refreshable credential carries an `asyncio.Lock` bound to the loop that
    first awaited it, and CPython reuses the address of a collected loop -- so
    an `id(loop)`-keyed cache can hand a new loop a session wired to a dead one.
    """

    def test_each_loop_gets_its_own_session(self) -> None:
        """Three sequential loops must never share an AioSession."""
        provider = _s3_provider()
        seen: list[object] = []

        async def body() -> None:
            async with provider.borrow():
                loop = asyncio.get_running_loop()
                seen.append(provider._sessions[loop][provider.credential_digest])

        for _ in range(3):
            asyncio.run(body())

        assert all(a is not b for a, b in itertools.combinations(seen, 2))
        assert provider.created_count == 3

    def test_session_cache_is_pruned_on_loop_shutdown(self) -> None:
        """`aclose_loop` drops the session, not just the client.

        The session cache lives outside `_entries`, so per-loop shutdown has to
        reach it explicitly or it survives for a later loop to stumble on.
        """
        provider = _s3_provider()

        async def body() -> None:
            async with provider.borrow():
                assert len(provider._sessions) == 1

        asyncio.run(body())
        assert len(provider._sessions) == 0

    def test_session_cache_is_weakly_keyed(self) -> None:
        """The session cache cannot outlive the loop, so it cannot grow forever.

        With the fallback opted out, the cached entry still references its loop
        -- which is itself why no address reuse can occur while that entry
        lives. Once the entry goes, the weak key lets the session go too.
        """
        provider = _s3_provider(auto_shutdown=False)

        async def body() -> None:
            async with provider.borrow():
                pass

        asyncio.run(body())
        assert len(provider._sessions) == 1, "entry still pins its loop"

        provider._entries.clear()  # the only remaining strong ref to the loop
        gc.collect()

        assert len(provider._sessions) == 0


class TestPoolLimitedTransportParity:
    """A pool-limited Azure client must behave like a default one.

    Supplying a transport bypasses `_create_pipeline`, which is where the SDK
    applies its own timeouts -- so anything it would have set has to be set here.
    """

    async def test_timeouts_match_the_sdk_defaults(self) -> None:
        """Without this, a stalled request hangs for 300s instead of 60s."""
        pooled = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=8,
        )
        transport = pooled._transport()
        assert transport is not None
        try:
            assert transport.connection_config.timeout == CONNECTION_TIMEOUT
            assert transport.connection_config.read_timeout == READ_TIMEOUT
        finally:
            await transport.close()

    async def test_timeouts_match_an_unpooled_client(self) -> None:
        """Compared against what the SDK actually builds for itself."""
        default = BlobClientProvider("dev", connection_string=AZURITE_CS)
        client = default._create_client(_DETACHED_LOOP)
        try:
            transport = default._transport_of(client)
            assert transport is not None
            assert transport.connection_config.timeout == CONNECTION_TIMEOUT
            assert transport.connection_config.read_timeout == READ_TIMEOUT
        finally:
            await client.close()

    async def test_proxy_environment_is_honoured(self) -> None:
        """`trust_env` off would silently ignore HTTPS_PROXY / NO_PROXY."""
        pooled = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=8,
        )
        transport = pooled._transport()
        assert transport is not None
        try:
            assert transport.session is not None
            assert transport.session.trust_env is True
        finally:
            await transport.close()


class TestConflictingClientSources:
    """Two sources for one client is a wiring mistake, not a preference."""

    def test_provider_with_client_is_rejected(self) -> None:
        """Silently dropping the injected client would be a debugging trap."""
        provider = _s3_provider()
        with pytest.raises(ConflictingClientSourceError, match="provider, client"):
            S3Storage(bucket_name="b", provider=provider, client=cast("S3Client", object()))

    def test_client_with_factory_is_rejected(self) -> None:
        """Same for the two injection routes."""
        with pytest.raises(ConflictingClientSourceError):
            S3Storage(
                bucket_name="b",
                client=cast("S3Client", object()),
                client_factory=cast("ClientFactory[S3Client]", object),
            )

    def test_azure_rejects_conflicts_too(self) -> None:
        """The guard is on both backends."""
        provider = BlobClientProvider("dev", connection_string=AZURITE_CS)
        with pytest.raises(ConflictingClientSourceError, match="AzureBlobStorage"):
            AzureBlobStorage(
                container_name="c",
                storage_account_name="dev",
                provider=provider,
                client=cast("BlobServiceClient", object()),
            )

    def test_a_single_source_is_fine(self) -> None:
        """The guard must not fire on the normal paths."""
        assert S3Storage(bucket_name="b", provider=_s3_provider()) is not None
        assert S3Storage(bucket_name="b", endpoint_url=UNREACHABLE_S3) is not None


class TestSharedProviderDeregistration:
    """Closing a shared provider must not poison later lookups."""

    async def test_closed_shared_provider_is_replaced(self) -> None:
        """A new storage gets a working provider, not the closed one."""
        first = S3Storage(
            bucket_name="b",
            endpoint_url="http://127.0.0.1:2",
            access_key="dereg",
            secret_key="dereg",  # noqa: S106
        )
        closed = first.provider
        await closed.aclose()

        second = S3Storage(
            bucket_name="b",
            endpoint_url="http://127.0.0.1:2",
            access_key="dereg",
            secret_key="dereg",  # noqa: S106
        )
        try:
            assert second.provider is not closed
            assert second.provider.is_closed is False
        finally:
            await second.provider.aclose()

    async def test_aclose_is_still_idempotent_after_deregistration(self) -> None:
        """De-registering twice must not raise."""
        provider = _s3_provider()
        await provider.aclose()
        await provider.aclose()
        assert provider.is_closed is True


class TestCredentialLoopAffinity:
    """Azure credentials get the same loop discipline as S3 sessions.

    A credential owns an `AioHttpTransport` wired to whichever loop first used
    it, so it can neither be closed from elsewhere nor handed to a later loop.
    """

    @staticmethod
    def _provider() -> BlobClientProvider:
        """Build a provider in the mode that caches a closeable credential."""
        return BlobClientProvider(
            "dev",
            tenant_id="t",
            client_id="c",
            client_secret="s",  # noqa: S106
        )

    def test_each_loop_gets_its_own_credential(self) -> None:
        """A collected loop's address is reused; a weak key is not."""
        provider = self._provider()
        seen: list[object] = []

        async def body() -> None:
            loop = asyncio.get_running_loop()
            seen.append(provider._token_credential(loop))
            await provider.aclose_loop()

        for _ in range(3):
            asyncio.run(body())

        assert all(a is not b for a, b in itertools.combinations(seen, 2))

    def test_credential_cache_is_weakly_keyed(self) -> None:
        """A credential cannot outlive the loop it was built for."""
        provider = self._provider()

        async def body() -> None:
            provider._token_credential(asyncio.get_running_loop())

        asyncio.run(body())
        gc.collect()

        assert len(provider._credentials) == 0

    async def test_aclose_leaves_a_foreign_live_loop_s_credential(self) -> None:
        """Closing across loops is exactly what the base class refuses for clients.

        The credential's transport is wired to the other loop, so closing it
        from here would queue teardown on a loop that may never run it -- and
        would strand that loop's still-open client with a dead credential.
        """
        provider = self._provider()
        loop_a = asyncio.new_event_loop()
        box: dict[str, object] = {}

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)
            box["credential"] = provider._token_credential(loop_a)

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()

        try:
            assert len(provider._credentials) == 1

            await provider.aclose()

            assert provider._credentials.get(loop_a) is box["credential"]
        finally:
            loop_a.close()

    async def test_aclose_still_closes_dead_loop_credentials(self) -> None:
        """A credential whose loop is gone is dropped rather than kept forever."""
        provider = self._provider()
        loop_a = asyncio.new_event_loop()

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)
            provider._token_credential(loop_a)

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()
        loop_a.close()

        await provider.aclose()

        assert len(provider._credentials) == 0


class TestSharedProviderRecovery:
    """`aclose_shared()` must not permanently brick a module-level storage."""

    async def test_storage_recovers_from_a_closed_shared_provider(self) -> None:
        """The documented shutdown call is safe to follow with more work.

        A storage created at import time captures the shared provider; closing
        it at the end of one event loop must not make every later operation
        raise for the rest of the process.
        """
        storage = S3Storage(
            bucket_name="b",
            endpoint_url="http://127.0.0.1:3",
            access_key="recover",
            secret_key="recover",  # noqa: S106
        )
        first = storage.provider
        await S3ClientProvider.aclose_shared()

        assert first.is_closed is True

        second = storage.provider
        try:
            assert second is not first
            assert second.is_closed is False
        finally:
            await S3ClientProvider.aclose_shared()

    async def test_caller_owned_provider_stays_closed(self) -> None:
        """A provider the caller passed in keeps its lifetime -- and its error.

        Silently replacing it would hide a genuine wiring mistake.
        """
        provider = _s3_provider()
        storage = S3Storage(bucket_name="b", provider=provider)
        await provider.aclose()

        assert storage.provider is provider
        with pytest.raises(ProviderClosedError):
            await storage.stat("anything")

    async def test_aclose_shared_survives_a_failing_provider(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """One provider raising must not strand the others still open."""
        bad = S3ClientProvider(endpoint_url="http://127.0.0.1:4", access_key="x", secret_key="x")  # noqa: S106
        good = S3ClientProvider(endpoint_url="http://127.0.0.1:5", access_key="y", secret_key="y")  # noqa: S106
        S3ClientProvider.shared(bad)
        S3ClientProvider.shared(good)
        mocker.patch.object(bad, "aclose", side_effect=OSError("nope"))

        await S3ClientProvider.aclose_shared()

        assert good.is_closed is True


class TestBuildFailureCleanup:
    """A client that fails to construct must not strand its session.

    `_transport()` builds an `aiohttp.ClientSession` eagerly, but nothing owns
    it until the client is constructed and entered on the exit stack.
    """

    async def test_failed_build_closes_the_pooled_session(self) -> None:
        """A malformed connection string must not leak a session per retry."""
        provider = BlobClientProvider(
            "dev",
            connection_string="this-is-not-a-connection-string",
            max_pool_connections=8,
        )

        for _ in range(3):
            with pytest.raises(ValueError, match=r"[Cc]onnection [Ss]tring"):
                async with provider.borrow():
                    pass

        assert provider.cached_count == 0
        assert provider.created_count == 0

    async def test_successful_build_keeps_its_session(self) -> None:
        """The cleanup path must not fire on the happy path."""
        provider = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=8,
        )
        try:
            async with provider.borrow() as client:
                assert provider._is_client_open(client) is True
        finally:
            await provider.aclose()


class TestCredentialDeadLoopHandling:
    """A dead loop's credential is dropped, never fake-closed."""

    async def test_dead_loop_credential_is_dropped_not_closed(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """Closing it would flip the connector's flag without sending a FIN.

        That is the "hides the leak rather than fixing it" behaviour the exit
        sweep explicitly refuses, so shutdown must refuse it too.
        """
        provider = BlobClientProvider(
            "dev",
            tenant_id="t",
            client_id="c",
            client_secret="s",  # noqa: S106
        )
        loop_a = asyncio.new_event_loop()
        box: dict[str, object] = {}

        def drive_foreign_loop() -> None:
            asyncio.set_event_loop(loop_a)
            box["credential"] = provider._token_credential(loop_a)

        thread = threading.Thread(target=drive_foreign_loop)
        thread.start()
        thread.join()
        loop_a.close()

        credential = box["credential"]
        close_spy = mocker.patch.object(credential, "close", new_callable=mocker.AsyncMock)

        await provider.aclose()

        close_spy.assert_not_awaited()
        assert len(provider._credentials) == 0


class TestCloneProviderOwnership:
    """A clone must inherit the parent's ability to recover from shutdown."""

    async def test_clone_recovers_from_aclose_shared(self) -> None:
        """Handing the clone a raw provider object would freeze it closed.

        The parent re-resolves through the registry; a clone given the object
        directly would raise ProviderClosedError forever while its parent
        silently recovered.
        """
        storage = S3Storage(
            bucket_name="b",
            endpoint_url="http://127.0.0.1:6",
            access_key="clone",
            secret_key="clone",  # noqa: S106
        )
        cloned = storage.clone()
        await S3ClientProvider.aclose_shared()

        try:
            assert cloned.provider.is_closed is False
            assert cloned.provider is storage.provider
        finally:
            await S3ClientProvider.aclose_shared()

    async def test_clone_of_an_injected_provider_keeps_it(self) -> None:
        """A caller-owned provider is still shared with the clone verbatim."""
        provider = _s3_provider()
        storage = S3Storage(bucket_name="b", provider=provider)

        assert storage.clone().provider is provider


class TestNoneValuedProtocolMember:
    """`isinstance` rejects a callable protocol member whose value is None.

    The diagnostic has to agree with the check, or a refused client is reported
    as missing nothing — from the helper whose whole job is explaining why.
    """

    class _NullClose:
        """Carries every IBlobServiceClient member, but `close` is None."""

        credential = object()
        account_name = "x"
        close = None

        def get_container_client(self, container: object) -> object:
            """Return a container client."""

        async def get_user_delegation_key(self, *args: object, **kwargs: object) -> object:
            """Return a user delegation key."""

    def test_isinstance_rejects_it(self) -> None:
        """Establish the behaviour the diagnostic must mirror."""
        assert isinstance(self._NullClose(), IBlobServiceClient) is False

    def test_diagnostic_names_the_none_member(self) -> None:
        """A refusal must never report `missing: []`."""
        assert missing_members(self._NullClose(), IBlobServiceClient) == ["close"]

    def test_error_message_is_actionable(self) -> None:
        """The raised error carries the member name."""
        with pytest.raises(IncompatibleClientError, match="close"):
            ensure_conforms(self._NullClose(), IBlobServiceClient)


class TestEnterFailureCleanup:
    """Entering the client is as unowned as constructing it.

    Until the client is on the exit stack nothing owns the session mint built
    for it, so a failure in `AioHttpTransport.open()` strands it exactly as a
    failed construction would.
    """

    async def test_failed_enter_releases_the_pooled_session(
        self,
        mocker: "MockerFixture",
    ) -> None:
        """A client that refuses to open must not leak its session."""
        provider = BlobClientProvider(
            "dev",
            connection_string=AZURITE_CS,
            max_pool_connections=8,
        )
        sessions: list[ClientSession] = []
        real_transport = provider._transport

        def capture_transport() -> "AioHttpTransport | None":
            transport = real_transport()
            assert transport is not None, "a pool limit was configured"
            assert transport.session is not None, "and its session built eagerly"
            sessions.append(transport.session)
            return transport

        mocker.patch.object(provider, "_transport", capture_transport)
        mocker.patch.object(
            BlobServiceClient,
            "__aenter__",
            side_effect=OSError("cannot open"),
        )

        with pytest.raises(OSError, match="cannot open"):
            async with provider.borrow():
                pass

        assert sessions, "a session was built"
        assert all(session.closed for session in sessions), "and released"
        assert provider.cached_count == 0
