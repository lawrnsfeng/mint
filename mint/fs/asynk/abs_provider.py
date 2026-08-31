"""Cached Azure Blob Storage client provider.

Unlike S3, constructing a ``BlobServiceClient`` is cheap (~0.2 ms). What is
expensive -- and what the previous per-operation construction threw away every
call -- is the **credential**: ``DefaultAzureCredential`` and
``ClientSecretCredential`` each own an in-memory ``TokenCache`` plus their own
``AioHttpTransport``. Rebuilding one per operation meant a fresh instance-metadata
token fetch each time and, because the old code never closed them, a leaked
aiohttp session per operation as well. This provider caches and owns both.

Eviction is terminal here: ``AioHttpTransport.open()`` raises once the transport
has been closed, so a closed client is dropped from the cache and never reused.
"""

import asyncio
import os
import weakref
from contextlib import AsyncExitStack
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

import aiohttp
from azure.core.pipeline.transport import AioHttpTransport
from azure.identity.aio import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob._shared.constants import CONNECTION_TIMEOUT, READ_TIMEOUT
from azure.storage.blob.aio import BlobServiceClient

from mint.fs.asynk.client_protocols import IBlobServiceClient
from mint.fs.asynk.lifecycle import (
    TRANSPORT_UNWRAP_LIMIT,
    SessionLike,
    running_loop,
)
from mint.fs.asynk.provider import ClientFactory, ClientProviderBase
from mint.fs.asynk.structs import AzureCredentialMode, AzureSessionParams
from mint.fs.exc import InvalidArgumentsError
from mint.logger import get_logger

if TYPE_CHECKING:
    from azure.core.credentials_async import AsyncTokenCredential

logger = get_logger(__name__)

type CredentialCache = weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    "AsyncTokenCredential",
]
"""Per-loop Azure token credentials, weakly keyed so none outlives its loop."""

_PIPELINE_PATHS: Final[tuple[tuple[str, ...], ...]] = (
    ("_client", "_client", "_pipeline", "_transport"),  # BlobServiceClient
    ("_client", "_pipeline", "_transport"),  # AadClient-backed credential
    ("_pipeline", "_transport"),  # anything holding a pipeline directly
)
"""Attribute paths from an SDK object down to its pipeline transport."""


class BlobClientProvider(ClientProviderBase[BlobServiceClient]):
    """Owns cached Azure credentials and blob service clients.

    Credential resolution order (unchanged from the original
    `AzureBlobStorage`):

    1. SAS token
    2. Shared access key
    3. Connection string
    4. Client secret (with tenant_id and client_id)
    5. ``AZURE_STORAGE_ACCESS_KEY``
    6. ``AZURE_STORAGE_CONNECTION_STRING``
    7. ``DefaultAzureCredential``
    """

    BACKEND: ClassVar[str] = "abs"
    CLIENT_PROTOCOL: ClassVar[type] = IBlobServiceClient

    AzureStorageAccessKey: Final[str] = "AZURE_STORAGE_ACCESS_KEY"
    AzureStorageConnectionString: Final[str] = "AZURE_STORAGE_CONNECTION_STRING"
    TmplAccountURL: Final[str] = "https://{storage_account_name}.blob.core.windows.net"

    def __init__(  # noqa: PLR0913
        self,
        storage_account_name: str,
        client_secret: str | None = None,
        shared_access_key: str | None = None,
        connection_string: str | None = None,
        sas_token: str | None = None,
        tenant_id: str | None = None,
        client_id: str | None = None,
        *,
        client: BlobServiceClient | None = None,
        client_factory: "ClientFactory[BlobServiceClient] | None" = None,
        max_pool_connections: int | None = None,
        idle_ttl_seconds: float | None = ClientProviderBase.DEFAULT_IDLE_TTL_SECONDS,
        auto_shutdown: bool = True,
    ) -> None:
        """Initialize the provider.

        Args:
            storage_account_name: Azure storage account name.
            client_secret: Client secret for service principal auth.
            shared_access_key: Storage account shared access key.
            connection_string: Full connection string.
            sas_token: Shared access signature token.
            tenant_id: Azure AD tenant ID for service principal.
            client_id: Azure AD client/application ID.
            client: A caller-owned client, used verbatim and never closed.
            client_factory: Builds a client per cache miss; mint owns the result.
            max_pool_connections: Per-client connection pool size. When set, the
                client is given an explicit ``AioHttpTransport`` over an
                ``aiohttp.TCPConnector`` with this limit; when None, the SDK's
                own default transport is used.
            idle_ttl_seconds: Idle eviction window; None disables it.
            auto_shutdown: Close this loop's clients when the loop tears down.

        """
        self.storage_account_name = storage_account_name
        self.sas_token = sas_token
        self.shared_access_key = shared_access_key
        self.connection_string = connection_string
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

        self.mode, self.params = self._init_credential_mode()
        self._credentials: CredentialCache = weakref.WeakKeyDictionary()
        super().__init__(
            client=client,
            client_factory=client_factory,
            max_pool_connections=max_pool_connections,
            idle_ttl_seconds=idle_ttl_seconds,
            auto_shutdown=auto_shutdown,
        )

    # -- credential resolution ---------------------------------------------

    def _init_credential_mode(  # noqa: PLR0911
        self,
    ) -> tuple[AzureCredentialMode, AzureSessionParams]:
        """Determine and initialize the credential mode.

        Returns:
            Tuple of (credential mode, session parameters).

        """
        params: AzureSessionParams = {}
        if self.sas_token is not None:
            params.update({"sas_token": self.sas_token})
            return AzureCredentialMode.SharedAccessSignature, params

        if self.shared_access_key is not None:
            params.update({"shared_access_key": self.shared_access_key})
            return AzureCredentialMode.SharedAccessKey, params

        if self.connection_string is not None:
            params.update({"connection_string": self.connection_string})
            return AzureCredentialMode.ConnectionString, params

        if all(
            field is not None for field in (self.tenant_id, self.client_id, self.client_secret)
        ):
            params.update(
                {
                    "tenant_id": self.tenant_id,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            return AzureCredentialMode.ClientSecret, params

        self.shared_access_key = os.getenv(self.AzureStorageAccessKey)
        if self.shared_access_key is not None:
            params.update({"shared_access_key": self.shared_access_key})
            return AzureCredentialMode.EnvVarSharedAccessKey, params

        self.connection_string = os.getenv(self.AzureStorageConnectionString)
        if self.connection_string is not None:
            params.update({"connection_string": self.connection_string})
            return AzureCredentialMode.EnvVarConnectionString, params

        return AzureCredentialMode.Default, params

    # -- lifecycle probes ---------------------------------------------------

    @staticmethod
    def _transport_of(obj: object) -> "AioHttpTransport | None":
        """Walk from a client or credential down to its real transport.

        Child clients (``service -> container -> blob``) wrap the parent's
        transport in ``AsyncTransportWrapper``, which has no ``session`` of its
        own and whose ``close()`` is a deliberate no-op -- and the wrapping
        nests, so unwrapping needs a bounded loop rather than one hop.

        The three shapes differ: a ``BlobServiceClient`` nests its generated
        client one level deeper than a credential does, and a child client is
        handed the pipeline directly.

        Args:
            obj: A ``BlobServiceClient``, a child client, or a credential.

        Returns:
            The underlying transport, or None if it cannot be reached.

        """
        transport = None
        for path in _PIPELINE_PATHS:
            node = cast("Any", obj)
            try:
                for attr in path:
                    node = getattr(node, attr)
            except AttributeError:
                continue
            transport = node
            break
        if transport is None:
            logger.debug("could not reach azure transport on %s", type(obj).__name__)
            return None
        for _ in range(TRANSPORT_UNWRAP_LIMIT):
            if isinstance(transport, AioHttpTransport):
                return transport
            try:
                transport = transport._transport  # noqa: SLF001
            except AttributeError:
                logger.debug("azure transport chain ended without a real transport")
                return None
        logger.debug("azure transport unwrap exceeded its bound")
        return None

    def _is_client_open(self, client: BlobServiceClient) -> bool:
        """Report whether the client's transport still holds a live session.

        ``AioHttpTransport`` nulls ``session`` on close but leaves
        ``_has_been_opened`` set, which is precisely why eviction is terminal
        here: reopening a closed transport raises.

        Args:
            client: A client this provider is holding.

        Returns:
            True if the transport looks open, or if it cannot be read.

        """
        transport = self._transport_of(client)
        if transport is None:
            return True
        return transport.session is not None

    def _aiohttp_sessions(self, _client: BlobServiceClient) -> tuple[SessionLike, ...]:
        """Return the client's live aiohttp session for exit-time cleanup.

        Args:
            _client: A client this provider is holding.

        Returns:
            Its session, or an empty tuple if none can be reached.

        """
        transport = self._transport_of(_client)
        if transport is None or transport.session is None:
            return ()
        return (transport.session,)

    # -- cache key ----------------------------------------------------------

    @property
    def account_url(self) -> str:
        """Blob endpoint for the configured storage account."""
        return self.TmplAccountURL.format(
            storage_account_name=self.storage_account_name,
        )

    @property
    def endpoint(self) -> str:
        """Account URL; part of the cache key."""
        return self.account_url

    @property
    def credential_digest(self) -> str:
        """Digest over the resolved credential material."""
        return self.digest(
            self.mode.value,
            self.params.get("sas_token"),
            self.params.get("shared_access_key"),
            self.params.get("connection_string"),
            self.params.get("tenant_id"),
            self.params.get("client_id"),
            self.params.get("client_secret"),
        )

    # -- client construction ------------------------------------------------

    def _token_credential(self, loop: asyncio.AbstractEventLoop) -> "AsyncTokenCredential":
        """Return the cached token credential for this loop, building it once.

        The credential owns a ``TokenCache`` and its own ``AioHttpTransport``,
        so it is cached per loop and closed on that loop's shutdown. Keyed by
        the loop *object* for the same reason the S3 session cache is: a
        collected loop's address gets reused, and an id-keyed map would then
        hand a new loop a credential wired to a dead one.

        Args:
            loop: The running event loop.

        Returns:
            A credential shared by every client built on this loop.

        """
        credential = self._credentials.get(loop)
        if credential is not None:
            return credential
        if self.mode is AzureCredentialMode.ClientSecret:
            credential = ClientSecretCredential(
                str(self.tenant_id),
                str(self.client_id),
                str(self.client_secret),
            )
        else:
            credential = DefaultAzureCredential()
        self._credentials[loop] = credential
        return credential

    def _transport(self) -> "AioHttpTransport | None":
        """Build a pool-bounded transport, or None to use the SDK default.

        The SDK's own transport builds an unbounded ``aiohttp`` session
        (``AioHttpTransport.open``), so a limit requires supplying the session.
        Everything else here exists to keep a pool-limited client behaving
        exactly like a default one: the session options mirror
        ``AioHttpTransport.open`` (notably ``auto_decompress=False``, which the
        storage layer relies on), and the timeouts restore the values
        ``_create_pipeline`` applies only to a transport it builds itself.

        Returns:
            A transport when a pool limit was requested, else None. Passing
            None leaves ``_create_pipeline`` to build the default transport.

        """
        if self.max_pool_connections is None:
            return None
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=self.max_pool_connections),
            cookie_jar=aiohttp.DummyCookieJar(),
            auto_decompress=False,
            # The SDK's own session sets this; without it a corporate proxy's
            # HTTPS_PROXY/NO_PROXY/netrc settings are silently ignored.
            trust_env=True,
        )
        return AioHttpTransport(
            session=session,
            # azure-storage applies these only when it builds the transport
            # itself, so supplying one would otherwise silently widen the
            # budget to azure-core's 300s/300s defaults.
            connection_timeout=CONNECTION_TIMEOUT,
            read_timeout=READ_TIMEOUT,
        )

    def _create_client(
        self,
        loop: asyncio.AbstractEventLoop,
        transport: "AioHttpTransport | None" = None,
    ) -> BlobServiceClient:
        """Build a BlobServiceClient for the resolved credential mode.

        Args:
            loop: The running event loop.
            transport: A transport to hand the SDK. When None one is built if
                a pool limit was configured; callers that need to release it on
                failure should build it themselves and pass it in.

        Returns:
            An unopened BlobServiceClient.

        Raises:
            InvalidArgumentsError: If the credential mode is unsupported.

        """
        transport = transport or self._transport()
        match self.mode:
            case AzureCredentialMode.SharedAccessSignature:
                return BlobServiceClient(
                    self.account_url,
                    credential=self.sas_token,
                    transport=transport,
                )
            case AzureCredentialMode.ClientSecret | AzureCredentialMode.Default:
                return BlobServiceClient(
                    self.account_url,
                    credential=self._token_credential(loop),
                    transport=transport,
                )
            case AzureCredentialMode.ConnectionString | AzureCredentialMode.EnvVarConnectionString:
                return BlobServiceClient.from_connection_string(
                    str(self.connection_string),
                    transport=transport,
                )
            case AzureCredentialMode.SharedAccessKey | AzureCredentialMode.EnvVarSharedAccessKey:
                return BlobServiceClient(
                    self.account_url,
                    credential=self.shared_access_key,
                    transport=transport,
                )
            case _:
                raise InvalidArgumentsError(detail=f"mode = {self.mode}")

    async def _build(self, stack: AsyncExitStack) -> BlobServiceClient:
        """Build a blob service client for the resolved credential mode.

        Args:
            stack: Exit stack owning the client's teardown.

        Returns:
            An entered BlobServiceClient bound to the running event loop.

        """
        transport = self._transport()
        try:
            client = self._create_client(asyncio.get_running_loop(), transport)
            # Entering is inside the guard too: until the client is on the
            # stack nothing owns the session, so a failure in `open()` would
            # strand it just as a failed construction would.
            return await stack.enter_async_context(client)
        except BaseException:
            # A malformed connection string raises here, and each retry would
            # otherwise strand another ClientSession and TCPConnector.
            if transport is not None:
                await transport.close()
            raise

    async def aclose_loop(self) -> None:
        """Close this loop's clients, then the credential they shared.

        The credential cache is keyed by loop and lives outside ``_entries``,
        so it would otherwise survive a per-loop shutdown and leak its own
        transport -- the very leak this provider exists to fix.
        """
        loop = asyncio.get_running_loop()
        await super().aclose_loop()
        await self._close_credential(self._credentials.pop(loop, None))

    async def aclose(self) -> None:
        """Close cached clients, then the credentials that outlived them.

        Credentials are closed last: each owns its own transport, and closing
        one before its clients would strand in-flight token refreshes.

        Like the base class, this closes only what the running loop owns.
        """
        await super().aclose()
        loop = running_loop()
        with self._guard:
            owners = list(self._credentials.keys())
        # Only this loop's credential can be closed from here. A credential on
        # another live loop owns a transport wired to that loop, so closing it
        # would queue teardown on a loop that may never run it -- the same
        # cross-loop hazard the base class refuses for clients. That loop's own
        # shutdown closes it.
        for owner in owners:
            if owner is loop:
                await self._close_credential(self._credentials.pop(owner, None))
            elif owner.is_closed():
                # Its transport is wired to a loop that will never run again.
                # Closing from here would flip the connector's closed flag
                # without sending a FIN, hiding the leak; drop it instead,
                # exactly as the base class does for a dead loop's client.
                self._credentials.pop(owner, None)

    async def _close_credential(self, credential: "AsyncTokenCredential | None") -> None:
        """Close a cached credential if it is still open.

        Args:
            credential: The credential to close, or None.

        """
        if credential is None:
            return
        transport = self._transport_of(credential)
        if transport is not None and transport.session is None:
            return
        await credential.close()
