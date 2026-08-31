"""Cached aiobotocore S3 client provider.

Two caches, both keyed by the running event loop:

- One ``AioSession`` per credential identity. botocore memoizes the resolved
  credentials (``botocore/session.py`` ``get_credentials``) and the parsed
  service-model JSON (``botocore/loaders.py`` ``instance_cache``) *on the
  session*, so sharing it is what stops the credential chain -- and its EC2
  instance-metadata probe -- from re-running on every operation.
- One ``S3Client`` per (endpoint, credentials, region, pool size, loop). The
  client owns its own ``TCPConnector``, so reusing it keeps the TLS pool warm.
"""

import asyncio
import os
import weakref
from contextlib import AsyncExitStack
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Final, cast

from mint.fs.asynk.client_protocols import IS3Client
from mint.fs.asynk.lifecycle import SessionLike
from mint.fs.asynk.provider import ClientFactory, ClientProviderBase
from mint.fs.asynk.s3_structs import S3CredentialMode, S3SessionParams
from mint.logger import get_logger

if TYPE_CHECKING:
    from aiobotocore.config import AioConfig
    from aiobotocore.httpsession import AIOHTTPSession
    from aiobotocore.session import AioSession
    from types_aiobotocore_s3.client import S3Client

logger = get_logger(__name__)

type SessionCache = weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    dict[str, "AioSession"],
]
"""Per-loop aiobotocore sessions, keyed by credential digest within each loop."""


class S3ClientProvider(ClientProviderBase["S3Client"]):
    """Owns cached aiobotocore sessions and S3 clients.

    Credential resolution order (unchanged from the original `S3Storage`):

    1. Explicit key pair (``access_key`` + ``secret_key``)
    2. Environment variables ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY``
    3. ``~/.aws/credentials`` shared profile
    4. IAM role / instance metadata
    """

    BACKEND: ClassVar[str] = "s3"
    CLIENT_PROTOCOL: ClassVar[type] = IS3Client

    AWSAccessKeyID: Final[str] = "AWS_ACCESS_KEY_ID"
    AWSSecretAccessKey: Final[str] = "AWS_SECRET_ACCESS_KEY"
    AWSSessionToken: Final[str] = "AWS_SESSION_TOKEN"
    S3Resource: Final[str] = "s3"

    DefaultMaxPoolConnections: Final[int] = 64
    """Pool size when the caller names none.

    botocore defaults to 10. That was survivable when every operation built its
    own client -- effective concurrency scaled with the fan-out -- but one
    cached client now serves the whole process, so the default would silently
    become a process-wide ceiling of 10 and queue everything past it on the
    connector. aiobotocore sets only socket timeouts, which do not cover
    connector-queue wait, so it would surface as latency rather than an error.
    """

    def __init__(  # noqa: PLR0913
        self,
        endpoint_url: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        session_token: str | None = None,
        profile_name: str = "default",
        region_name: str | None = None,
        *,
        client: "S3Client | None" = None,
        client_factory: "ClientFactory[S3Client] | None" = None,
        max_pool_connections: int | None = None,
        idle_ttl_seconds: float | None = ClientProviderBase.DEFAULT_IDLE_TTL_SECONDS,
        auto_shutdown: bool = True,
    ) -> None:
        """Initialize the provider.

        Args:
            endpoint_url: Custom S3 endpoint (LocalStack, MinIO). None for AWS.
            access_key: AWS access key ID.
            secret_key: AWS secret access key.
            session_token: AWS session token, for temporary credentials.
            profile_name: AWS shared-credentials profile name.
            region_name: AWS region name.
            client: A caller-owned client, used verbatim and never closed.
            client_factory: Builds a client per cache miss; mint owns the result.
            max_pool_connections: Per-client connection pool size. Defaults
                to ``DefaultMaxPoolConnections`` rather than botocore's 10,
                which would otherwise cap the whole process now that one client
                serves every operation.
            idle_ttl_seconds: Idle eviction window; None disables it.
            auto_shutdown: Close this loop's clients when the loop tears down.

        """
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = access_key
        self.aws_secret_access_key = secret_key
        self.aws_session_token = session_token
        self.profile_name = profile_name
        self.region_name = region_name

        self.mode, self.params = self._init_credential_mode()
        self._sessions: SessionCache = weakref.WeakKeyDictionary()
        super().__init__(
            client=client,
            client_factory=client_factory,
            max_pool_connections=max_pool_connections,
            idle_ttl_seconds=idle_ttl_seconds,
            auto_shutdown=auto_shutdown,
        )

    # -- credential resolution ---------------------------------------------

    def _init_credential_mode(self) -> tuple[S3CredentialMode, S3SessionParams]:
        """Determine credential mode and build session params.

        Returns:
            Tuple of (credential mode, session parameters).

        """
        params: S3SessionParams = {}

        if self.aws_access_key_id and self.aws_secret_access_key:
            params.update(self._key_pair_params())
            return S3CredentialMode.KeyPair, params

        self.aws_access_key_id = os.getenv(self.AWSAccessKeyID)
        self.aws_secret_access_key = os.getenv(self.AWSSecretAccessKey)
        self.aws_session_token = os.getenv(self.AWSSessionToken)
        if self.aws_access_key_id and self.aws_secret_access_key:
            params.update(self._key_pair_params())
            return S3CredentialMode.EnvVar, params

        if self._has_aws_profile(self.profile_name):
            params.update({"profile_name": self.profile_name})
            return S3CredentialMode.SharedCredentials, params

        return S3CredentialMode.IAMRole, params

    def _key_pair_params(self) -> S3SessionParams:
        """Build the explicit-credentials slice of the session params."""
        return {
            "aws_access_key_id": self.aws_access_key_id,
            "aws_secret_access_key": self.aws_secret_access_key,
            "aws_session_token": self.aws_session_token,
            "region_name": self.region_name,
        }

    @staticmethod
    def _has_aws_profile(profile_name: str) -> bool:
        """Check if the named AWS profile exists in ~/.aws/credentials.

        Args:
            profile_name: AWS shared credentials profile name.

        Returns:
            True if the profile file exists and declares the profile.

        """
        credentials_path = Path.home() / ".aws" / "credentials"
        if not credentials_path.exists():
            return False
        content = credentials_path.read_text(encoding="utf-8")
        header = "[default]" if profile_name == "default" else f"[{profile_name}]"
        return header in content

    # -- lifecycle probes ---------------------------------------------------

    def _http_session(self, client: "S3Client") -> "AIOHTTPSession | None":
        """Reach the aiobotocore HTTP session behind a client, defensively.

        Args:
            client: A client this provider is holding.

        Returns:
            The session, or None if the SDK's internals have moved.

        """
        # types-aiobotocore does not declare the endpoint/session internals;
        # their shape is verified against the installed aiobotocore instead.
        try:
            endpoint = cast("Any", client)._endpoint  # noqa: SLF001
            return cast("AIOHTTPSession", endpoint.http_session)
        except AttributeError:
            logger.debug("could not reach aiobotocore http session")
            return None

    def _is_client_open(self, client: "S3Client") -> bool:
        """Report whether the client still holds live aiohttp sessions.

        aiobotocore exposes no public flag, but ``AIOHTTPSession`` nulls its
        ``_sessions`` dict on exit, which is the same value ``__aexit__``
        asserts on. This probe is mandatory rather than an optimisation: that
        assertion means a second close *raises*, unlike azure-core's, whose
        close is guarded and idempotent.

        Args:
            client: A client this provider is holding.

        Returns:
            True if the client looks open, or if its state cannot be read.

        """
        session = self._http_session(client)
        if session is None:
            return True
        return cast("Any", session)._sessions is not None  # noqa: SLF001

    def _aiohttp_sessions(self, _client: "S3Client") -> tuple[SessionLike, ...]:
        """Return the client's live aiohttp sessions for exit-time cleanup.

        Args:
            _client: A client this provider is holding.

        Returns:
            Its sessions, or an empty tuple if none can be reached.

        """
        session = self._http_session(_client)
        if session is None:
            return ()
        live = cast("Any", session)._sessions or {}  # noqa: SLF001
        return tuple(live.values())

    # -- cache key ----------------------------------------------------------

    @property
    def endpoint(self) -> str:
        """Endpoint URL, or a marker for the AWS default endpoint."""
        return self.endpoint_url or "aws:default"

    @property
    def credential_digest(self) -> str:
        """Digest over the resolved credential material."""
        return self.digest(
            self.mode.value,
            self.params.get("aws_access_key_id"),
            self.params.get("aws_secret_access_key"),
            self.params.get("aws_session_token"),
            self.params.get("profile_name"),
        )

    @property
    def extra(self) -> str:
        """Region and pool size; the rest of the cache-key material."""
        return f"region={self.region_name}&pool={self.pool_size}"

    # -- client construction ------------------------------------------------

    def _session(self, loop: "asyncio.AbstractEventLoop") -> "AioSession":
        """Return the ``AioSession`` for these credentials on this loop.

        Sessions are keyed by the loop *object*, not by ``id(loop)``: a
        refreshable credential carries an ``asyncio.Lock`` bound to whichever
        loop first awaited it, and CPython reuses the address of a collected
        loop -- so an id-keyed cache can hand a new loop a session wired to a
        dead one. A weak-keyed map cannot: the entry disappears with the loop.

        Args:
            loop: The running event loop.

        Returns:
            A session whose credential and loader caches are shared by every
            client built from it.

        """
        from aiobotocore.session import AioSession  # noqa: PLC0415

        with self._guard:
            per_loop = self._sessions.setdefault(loop, {})
        session = per_loop.get(self.credential_digest)
        if session is None:
            profile = self.params.get("profile_name")
            session = AioSession(profile=profile) if profile else AioSession()
            per_loop[self.credential_digest] = session
        return session

    @property
    def pool_size(self) -> int:
        """Effective per-client connection pool size."""
        if self.max_pool_connections is None:
            return self.DefaultMaxPoolConnections
        return self.max_pool_connections

    def _config(self) -> "AioConfig":
        """Build the botocore config carrying the pool limit.

        Returns:
            An ``AioConfig`` bounding the client's connection pool.

        """
        from aiobotocore.config import AioConfig  # noqa: PLC0415

        return AioConfig(max_pool_connections=self.pool_size)

    async def _build(self, stack: AsyncExitStack) -> "S3Client":
        """Build an S3 client from the cached session for this loop.

        Args:
            stack: Exit stack owning the client's teardown.

        Returns:
            An entered ``S3Client`` bound to the running event loop.

        """
        session = self._session(asyncio.get_running_loop())
        ctx = session.create_client(
            "s3",
            region_name=self.params.get("region_name") or self.region_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.params.get("aws_access_key_id"),
            aws_secret_access_key=self.params.get("aws_secret_access_key"),
            aws_session_token=self.params.get("aws_session_token"),
            config=self._config(),
        )
        return await stack.enter_async_context(ctx)

    async def aclose_loop(self) -> None:
        """Close this loop's clients, then drop the session they shared.

        The session cache lives outside ``_entries``, so a per-loop shutdown
        would otherwise leave it behind for a later loop to stumble on.
        """
        loop = asyncio.get_running_loop()
        await super().aclose_loop()
        self._sessions.pop(loop, None)

    async def aclose(self) -> None:
        """Close cached clients, then drop the sessions they were built from."""
        await super().aclose()
        with self._guard:
            self._sessions.clear()
