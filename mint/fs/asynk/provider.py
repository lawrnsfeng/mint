"""Loop-keyed client cache shared by the async storage backends.

Backends used to build a brand-new SDK client for every top-level operation.
For S3 that meant a fresh ``AioSession`` per call, which re-ran the whole
botocore credential chain -- including the EC2 instance-metadata probe -- and
threw away a warm TLS pool each time. A provider owns the client instead, and
the backend borrows it.

The cache key carries the running event loop's identity on purpose: aiohttp's
``TCPConnector`` binds the loop it was built on, so a client may never cross
loops. Credential material is reduced to a digest so a key is safe to log.

This module deliberately imports no vendor SDK -- ``S3ClientProvider`` and
``BlobClientProvider`` live in sibling modules so that installing only the
``s3`` or only the ``azure`` dependency group stays sufficient.
"""

import asyncio
import hashlib
import threading
import time
import weakref
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
)
from dataclasses import dataclass
from typing import ClassVar, Final, Self, cast

from mint.fs.asynk.client_protocols import ensure_conforms
from mint.fs.asynk.lifecycle import (
    LIVE_PROVIDERS,
    SHARED,
    SHARED_GUARD,
    SessionLike,
    deregister_shared,
    register_atexit_once,
    running_loop,
)
from mint.fs.exc import FactoryNotConfiguredError, ProviderClosedError
from mint.logger import get_logger

logger = get_logger(__name__)

type ClientFactory[T] = Callable[[], T | AbstractAsyncContextManager[T]]

type SentinelCache = weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    AsyncGenerator[None],
]
"""Per-loop shutdown sentinels, weakly keyed so none outlives its loop."""

_FIELD_SEPARATOR: Final[bytes] = b"\x1f"
_NONE_MARKER: Final[bytes] = b"\x00"


@dataclass(frozen=True, slots=True)
class ClientCacheKey:
    """Identity of one cached client.

    Credential material appears only as ``credential_digest``, so a key may be
    logged or interpolated into an error message without leaking secrets.
    """

    backend: str
    endpoint: str
    credential_digest: str
    extra: str
    loop_id: int


@dataclass(slots=True)
class _CacheEntry[T]:
    """One cached client plus the bookkeeping the sweep needs."""

    client: T
    loop: asyncio.AbstractEventLoop
    stack: AsyncExitStack | None
    last_used: float
    inflight: int = 0


class ClientProviderBase[T](ABC):
    """Owns and reuses one SDK client per (configuration, event loop).

    Mirrors :class:`mint.db.asynk.database.Database`: a long-lived object that
    owns a pooled resource, accepts a pre-built one, and is injected into the
    things that use it.

    Subclasses supply the backend-specific pieces -- how to build a client, how
    to identify its endpoint and credentials, and which client protocol an
    injected object must satisfy.
    """

    DEFAULT_IDLE_TTL_SECONDS: Final[float] = 900.0

    BACKEND: ClassVar[str]
    CLIENT_PROTOCOL: ClassVar[type]

    def __init__(
        self,
        *,
        client: T | None = None,
        client_factory: ClientFactory[T] | None = None,
        max_pool_connections: int | None = None,
        idle_ttl_seconds: float | None = DEFAULT_IDLE_TTL_SECONDS,
        auto_shutdown: bool = True,
    ) -> None:
        """Initialize the provider.

        Args:
            client: A caller-owned client to use verbatim. It is never closed
                by mint, and bypasses the cache entirely. Test doubles must be
                spec'd (``MagicMock(spec=...)``) to satisfy the protocol check.
            client_factory: Called on each cache miss to build a client, so
                mint still gets one client per event loop rather than a single
                loop-bound object. May return the client or an async context
                manager yielding it; mint owns and closes whatever it returns.
            max_pool_connections: Per-client HTTP connection pool size. Relevant
                now that one client serves all concurrent operations.
            idle_ttl_seconds: Close and drop a cached client left unused for
                this long. ``None`` disables eviction. Swept lazily on borrow;
                a client with an operation in flight is never swept.
            auto_shutdown: Close this loop's clients automatically when the
                event loop tears down, as a fallback for callers who never get
                to :meth:`aclose`. Set False when something outside this
                provider owns the clients' lifetime.

        """
        if client is not None:
            ensure_conforms(client, self.CLIENT_PROTOCOL)
        self._injected = client
        self._client_factory = client_factory
        self.max_pool_connections = max_pool_connections
        self.idle_ttl_seconds = idle_ttl_seconds
        self.auto_shutdown = auto_shutdown
        # SHARED is process-global, so two threads each building a storage
        # with the same configuration get the *same* provider. asyncio.Lock
        # guards one loop's coroutines; this guards the maps across threads.
        # Held only for dict/int mutation, never across an await.
        self._guard = threading.Lock()
        self._entries: dict[ClientCacheKey, _CacheEntry[T]] = {}
        self._locks: dict[ClientCacheKey, asyncio.Lock] = {}
        self._sentinels: SentinelCache = weakref.WeakKeyDictionary()
        self._closed = False
        self._created = 0
        LIVE_PROVIDERS.add(self)
        register_atexit_once()

    # -- subclass contract --------------------------------------------------

    @abstractmethod
    async def _build(self, stack: AsyncExitStack) -> T:
        """Build a new client, registering its teardown on ``stack``.

        Only called when no ``client_factory`` was supplied -- the base class
        routes to the factory itself, so a subclass cannot forget to honour it.

        Args:
            stack: Exit stack owning this client's teardown. The provider
                closes it on eviction.

        Returns:
            A client bound to the running event loop.

        """

    @abstractmethod
    def _is_client_open(self, client: T) -> bool:
        """Report whether ``client`` still holds live network resources.

        The SDKs disagree about double-close: azure-core's transport guards
        ``close()`` with ``if self._session_owner and self.session``, but
        aiobotocore's ``AIOHTTPSession.__aexit__`` asserts ``_sessions is not
        None`` and so *raises* on a second close. Neither exposes a public
        "is it closed" flag, so each backend probes its own internals.

        Implementations must not raise: an unrecognised shape (an SDK upgrade
        moving an attribute) should report True, since attempting a close that
        turns out to be redundant is cheaper than leaking a connection pool.

        Args:
            client: A client this provider is holding.

        Returns:
            True if the client looks open, or if its state cannot be read.

        """

    @property
    @abstractmethod
    def endpoint(self) -> str:
        """Endpoint this provider talks to; part of the cache key."""

    @property
    @abstractmethod
    def credential_digest(self) -> str:
        """Digest over the credential material; part of the cache key."""

    @property
    def extra(self) -> str:
        """Remaining cache-key material (region, pool size, api version)."""
        return f"pool={self.max_pool_connections}"

    async def _adopt_factory_client(self, stack: AsyncExitStack) -> T:
        """Invoke the caller's factory and take ownership of what it returns.

        Args:
            stack: Exit stack owning the client's teardown.

        Returns:
            The factory's client, narrowed to this backend's protocol.

        Raises:
            FactoryNotConfiguredError: If no factory was supplied.

        """
        if self._client_factory is None:
            raise FactoryNotConfiguredError(provider=type(self).__name__)
        made = self._client_factory()
        if isinstance(made, AbstractAsyncContextManager):
            cm = cast("AbstractAsyncContextManager[T]", made)
            made = await stack.enter_async_context(cm)
        ensure_conforms(made, self.CLIENT_PROTOCOL)
        return made

    # -- public surface -----------------------------------------------------

    @property
    def is_closed(self) -> bool:
        """Whether :meth:`aclose` has been called."""
        return self._closed

    @property
    def created_count(self) -> int:
        """How many clients this provider has built. Diagnostics and tests."""
        return self._created

    @property
    def cached_count(self) -> int:
        """How many clients are currently cached."""
        return len(self._entries)

    @property
    def config_digest(self) -> str:
        """Identity of this provider's configuration, ignoring event loop."""
        return self.digest(
            self.BACKEND,
            self.endpoint,
            self.credential_digest,
            self.extra,
            # Two providers configured alike but disagreeing about lifetime
            # policy must not silently inherit whichever was built first.
            str(self.auto_shutdown),
            str(self.idle_ttl_seconds),
        )

    @staticmethod
    def digest(*parts: str | None) -> str:
        """Hash configuration parts into a stable, secret-free identifier.

        Args:
            parts: Values to fold in, in a fixed order. ``None`` is distinct
                from the empty string.

        Returns:
            Hex sha256 digest.

        """
        hasher = hashlib.sha256()
        for part in parts:
            hasher.update(_NONE_MARKER if part is None else part.encode("utf-8"))
            hasher.update(_FIELD_SEPARATOR)
        return hasher.hexdigest()

    @classmethod
    def shared(cls, candidate: Self) -> Self:
        """Return the process-default provider matching ``candidate``'s config.

        Building a provider is pure bookkeeping (no I/O), so callers construct a
        candidate and hand it here; an equivalent live provider wins, otherwise
        the candidate is registered and returned.

        A candidate carrying an injected client or factory is returned as-is
        and never registered: it belongs to whoever supplied that client.

        Args:
            candidate: A freshly built provider to share or register.

        Returns:
            The provider that should actually be used.

        """
        if candidate._injected is not None or candidate._client_factory is not None:
            # An injected client or factory is caller-specific and invisible to
            # config_digest, so registering it would let an unrelated storage
            # with matching credentials silently borrow someone else's client.
            return candidate
        key = (cls.__qualname__, candidate.config_digest)
        with SHARED_GUARD:
            existing = SHARED.get(key)
            if existing is not None and not existing.is_closed:
                return cast("Self", existing)
            SHARED[key] = candidate
        return candidate

    @classmethod
    async def aclose_shared(cls) -> None:
        """Close and forget every process-default provider of this class."""
        with SHARED_GUARD:
            mine = [key for key in SHARED if key[0] == cls.__qualname__]
        for key in mine:
            with SHARED_GUARD:
                provider = SHARED.pop(key, None)
            if provider is None:
                continue
            try:
                await provider.aclose()
            except Exception:
                # One provider failing must not strand the others still open.
                logger.exception("failed to close shared provider %s", key[0])

    @asynccontextmanager
    async def borrow(self) -> AsyncIterator[T]:
        """Lend a cached client for the duration of one logical operation.

        The borrow is counted, so an idle sweep can never close a client out
        from under an operation still using it.

        Yields:
            A client bound to the running event loop.

        Raises:
            ProviderClosedError: If the provider has been closed.

        """
        if self._closed:
            raise ProviderClosedError(provider=type(self).__name__)
        if self._injected is not None:
            yield self._injected
            return
        entry = await self._entry()
        entry.inflight += 1
        try:
            yield entry.client
        finally:
            entry.inflight -= 1
            entry.last_used = time.monotonic()

    async def aclose_loop(self) -> None:
        """Close the clients this provider holds for the *running* loop.

        Like :meth:`aclose`, this does not wait for in-flight borrows; when it
        runs as the loop-teardown fallback nothing else is executing, so that
        is moot, but a direct call should follow the same quiesce-first rule.

        Unlike :meth:`aclose` this leaves the provider usable, so a process
        that runs several event loops in sequence gets a fresh client on each
        rather than a :class:`ProviderClosedError`. Entries belonging to other
        loops are left alone -- they cannot be closed from here (see
        :meth:`_evict`).

        Idempotent: closing an already-closed loop is a no-op.
        """
        loop = asyncio.get_running_loop()
        with self._guard:
            mine = [key for key, entry in self._entries.items() if entry.loop is loop]
        for key in mine:
            await self._evict(key)
        self._sentinels.pop(loop, None)
        self._drop_locks_for(loop)

    async def aclose(self) -> None:
        """Close every client this provider owns. Injected clients are spared.

        Call this once traffic has quiesced. It does not wait for in-flight
        borrows: a client closed while an operation is still using it will fail
        that operation, so drain your requests first -- the same contract as
        disposing a database engine.

        Clients owned by another *live* event loop cannot be closed from here
        and are left cached and open; a warning names them, and that loop's own
        teardown will close them. This provider also de-registers itself as a
        process default, so a later storage with the same configuration gets a
        fresh provider rather than this closed one.

        Idempotent: a second call is a no-op.
        """
        self._closed = True
        with self._guard:
            keys = list(self._entries)
        for key in keys:
            await self._evict(key)
        # A sentinel is the only strong reference to its generator; dropping
        # one for a loop whose entries we deliberately left open would let GC
        # finalize it and close that loop's clients behind its back, possibly
        # mid-operation. Keep those, drop the rest.
        with self._guard:
            live_loops = {entry.loop for entry in self._entries.values()}
            sentinel_owners = list(self._sentinels)
        for owner in sentinel_owners:
            if owner not in live_loops:
                self._sentinels.pop(owner, None)
        with self._guard:
            live_ids = {id(owner) for owner in live_loops}
            for key in [k for k in self._locks if k.loop_id not in live_ids]:
                del self._locks[key]
        deregister_shared(self)
        if self._entries:
            logger.warning(
                "%s closed with %d client(s) still owned by other live event "
                "loops; each will be closed when its own loop tears down",
                type(self).__name__,
                len(self._entries),
            )

    # -- internals ----------------------------------------------------------

    def _aiohttp_sessions(self, _client: T) -> tuple[SessionLike, ...]:
        """Return the sessions ``client`` holds, for exit-time cleanup.

        Backends override this so :func:`_atexit_sweep` can shut sockets down
        synchronously. The default is empty, which degrades the exit sweep to
        report-only rather than doing something wrong.

        Args:
            client: A client this provider is holding.

        Returns:
            Live sessions, or an empty tuple if none can be found.

        """
        return ()

    def _exit_sweep(self) -> int:
        """Release what is still open, synchronously. Interpreter-exit only.

        Returns:
            How many clients were still open, whether or not they could be
            released -- the caller reports this so a missing ``aclose`` is
            visible rather than silently patched over.

        """
        stranded = 0
        with self._guard:
            entries = list(self._entries.values())
        for entry in entries:
            if entry.stack is None or not self._is_client_open(entry.client):
                continue
            stranded += 1
            if entry.loop.is_closed():
                continue
            self._close_sessions_sync(entry.client)
        return stranded

    def _close_sessions_sync(self, client: T) -> None:
        """Shut a client's connectors down without an event loop.

        ``BaseConnector._close()`` does the socket work inline; the public
        ``close()`` wraps the same call in a ``_DeprecationWaiter`` that warns
        from ``__del__`` when it is never awaited, which is exactly the
        situation here.

        Args:
            client: A client whose owning loop is still alive.

        """
        for session in self._aiohttp_sessions(client):
            connector = session.connector
            if connector is None or connector.closed:
                continue
            try:
                connector._close()  # noqa: SLF001
            except Exception:  # noqa: BLE001
                logger.debug("could not release connector at exit", exc_info=False)

    async def _shutdown_sentinel(self) -> AsyncGenerator[None]:
        """Yield once, then close this loop's clients when finalized.

        The loop finalizes its async generators in ``shutdown_asyncgens()``,
        which runs *before* ``loop.close()`` and while the loop is still
        running -- so, unlike an ``atexit`` hook, the ``finally`` here can
        actually await a close. ``asyncio.run``, ``asyncio.Runner``, uvicorn
        and pytest-asyncio all call it, including on ``KeyboardInterrupt``.
        """
        try:
            yield
        finally:
            await self.aclose_loop()

    async def _arm_shutdown(self, loop: asyncio.AbstractEventLoop) -> None:
        """Arm the loop-teardown fallback for ``loop``, once.

        Keyed on the loop object rather than ``id(loop)``: a loop torn down
        without ``shutdown_asyncgens()`` would otherwise leave an entry behind
        forever, and a later loop allocated at the same address would look
        already-armed and silently get no fallback at all.

        Args:
            loop: The running event loop to arm the fallback on.

        """
        if not self.auto_shutdown or loop in self._sentinels:
            return
        sentinel = self._shutdown_sentinel()
        # First iteration is what registers it with the loop's asyncgen hooks.
        await anext(sentinel)
        self._sentinels[loop] = sentinel

    def _drop_locks_for(self, loop: asyncio.AbstractEventLoop) -> None:
        """Forget the build locks belonging to ``loop``.

        Locks are only ever dropped here, during that loop's shutdown, never
        opportunistically. ``asyncio.Lock.release()`` clears ``_locked`` before
        the woken waiter runs, so ``locked()`` reads False while waiters are
        still queued -- deleting on that signal lets a later caller create a
        second lock for the same key and build concurrently, orphaning one of
        the two clients with its connector still open. There is at most one
        lock per (configuration, loop), so keeping them until shutdown costs
        nothing.

        Args:
            loop: The loop whose locks may be discarded.

        """
        with self._guard:
            for key in [k for k in self._locks if k.loop_id == id(loop)]:
                del self._locks[key]

    def _key(self, loop: asyncio.AbstractEventLoop) -> ClientCacheKey:
        return ClientCacheKey(
            backend=self.BACKEND,
            endpoint=self.endpoint,
            credential_digest=self.credential_digest,
            extra=self.extra,
            loop_id=id(loop),
        )

    async def _entry(self) -> _CacheEntry[T]:
        loop = asyncio.get_running_loop()
        await self._sweep()
        key = self._key(loop)
        cached = self._entries.get(key)
        if cached is not None:
            cached.last_used = time.monotonic()
            return cached

        with self._guard:
            lock = self._locks.setdefault(key, asyncio.Lock())
        return await self._build_entry(key, loop, lock)

    async def _build_entry(
        self,
        key: ClientCacheKey,
        loop: asyncio.AbstractEventLoop,
        lock: asyncio.Lock,
    ) -> _CacheEntry[T]:
        """Build and publish the entry for ``key`` under ``lock``.

        Args:
            key: Cache key being built.
            loop: The running event loop.
            lock: The per-key build lock.

        Returns:
            The cached entry, whether this call built it or lost the race.

        Raises:
            ProviderClosedError: If the provider closed before or during build.

        """
        async with lock:
            cached = self._entries.get(key)
            if cached is not None:
                cached.last_used = time.monotonic()
                return cached
            if self._closed:
                # aclose() may have run and finished iterating while this
                # coroutine waited for the lock; without this re-check the new
                # client would land in a closed provider and escape shutdown.
                raise ProviderClosedError(provider=type(self).__name__)
            stack = AsyncExitStack()
            try:
                client = (
                    await self._adopt_factory_client(stack)
                    if self._client_factory is not None
                    else await self._build(stack)
                )
            except BaseException:
                await stack.aclose()
                raise
            if self._closed:
                # Building is where the awaiting happens, so aclose() can run
                # to completion in that window. Publishing now would strand a
                # live client in a closed provider, unreachable forever.
                await stack.aclose()
                raise ProviderClosedError(provider=type(self).__name__)
            entry = _CacheEntry(
                client=client,
                loop=loop,
                stack=stack,
                last_used=time.monotonic(),
            )
            with self._guard:
                self._entries[key] = entry
                self._created += 1
            await self._arm_shutdown(loop)
            return entry

    async def _sweep(self) -> None:
        """Evict dead-loop and idle entries. Entries in flight are left alone."""
        ttl = self.idle_ttl_seconds
        now = time.monotonic()
        with self._guard:
            # Snapshot: another thread's loop may be mutating this map.
            candidates = list(self._entries.items())
        stale = [
            key
            for key, entry in candidates
            if entry.inflight == 0
            and (entry.loop.is_closed() or (ttl is not None and now - entry.last_used >= ttl))
        ]
        for key in stale:
            await self._evict(key)

    async def _evict(self, key: ClientCacheKey) -> None:
        """Remove an entry, then close it. Never recycles a closed client.

        Azure's ``AioHttpTransport`` raises on reuse after close, so eviction
        must be terminal: the entry leaves the cache before the close happens,
        and a closed client is never handed back out.

        Three cases are deliberately *not* closed here:

        - an injected client (``stack is None``), which mint does not own;
        - a client on a dead or foreign loop, which cannot be closed from the
          running loop at all -- aiohttp queues the socket teardown on the
          owning loop, so the callback would never run and the returned future
          would belong to the wrong loop;
        - a client something else already closed, since aiobotocore asserts on
          a second close.
        """
        entry = self._entries.get(key)
        if entry is not None and entry.stack is not None and entry.loop is not running_loop():
            if not entry.loop.is_closed():
                # Its own loop can still close it gracefully, and the exit
                # sweep needs to be able to see it. Dropping it here would
                # leak it silently.
                logger.debug("leaving client owned by another live loop: %s", key)
                return
            logger.debug("dropping client on a closed loop without closing: %s", key)

        with self._guard:
            entry = self._entries.pop(key, None)
        if entry is None or entry.stack is None:
            return
        if entry.loop is not running_loop():
            return
        if not self._is_client_open(entry.client):
            logger.debug("client already closed elsewhere; dropping entry: %s", key)
            return
        try:
            await entry.stack.aclose()
        except AssertionError:
            # aiobotocore's httpsession asserts when closed twice. The probe
            # above makes this a narrow race, not the normal path.
            logger.debug("client was closed concurrently: %s", key)
        except Exception:
            logger.exception("failed to close cached client: %s", key)
