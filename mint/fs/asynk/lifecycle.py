"""Process-lifetime concerns for the storage client cache.

Everything here outlives any single provider: the registry of process-default
providers, the interpreter-exit sweep, and the small helpers both depend on.
Kept apart from ``provider.py`` so that module stays about one provider's cache.

The dependency runs one way -- ``provider`` imports this, never the reverse at
runtime -- so the ``ClientProviderBase`` references below are type-only.
"""

import asyncio
import atexit
import threading
import weakref
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Final, Protocol

from mint.fs.exc import ConflictingClientSourceError
from mint.logger import get_logger

if TYPE_CHECKING:
    from mint.fs.asynk.provider import ClientProviderBase

logger = get_logger(__name__)

TRANSPORT_UNWRAP_LIMIT: Final[int] = 10
"""How many `AsyncTransportWrapper` layers to unwrap before giving up."""

SHARED: dict[tuple[str, str], "ClientProviderBase[Any]"] = {}
"""Process-default providers, keyed by (provider class, configuration digest)."""

SHARED_GUARD: Final[threading.Lock] = threading.Lock()
"""Guards `SHARED`; it is reachable from every thread that builds a storage."""

LIVE_PROVIDERS: "weakref.WeakSet[ClientProviderBase[Any]]" = weakref.WeakSet()
"""Every live provider, so the exit sweep sees explicitly built ones too."""

_atexit_registered = False


class ConnectorLike(Protocol):
    """The connector surface the exit sweep drives.

    Structural on purpose: ``BaseConnector._close()`` does the socket teardown
    inline, and the public ``close()`` wraps the same call in an awaitable that
    warns from ``__del__`` when never awaited -- which is exactly the situation
    at interpreter exit. Naming only what is used keeps the contract honest and
    lets a backend supply any equivalent object.
    """

    closed: bool

    def _close(self) -> None:
        """Tear the connector's transports down synchronously."""


class SessionLike(Protocol):
    """The session surface the exit sweep drives."""

    @property
    def connector(self) -> ConnectorLike | None:
        """The connector this session owns, if it still has one."""


def running_loop() -> asyncio.AbstractEventLoop | None:
    """Return the running loop, or None when called outside one."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def reject_conflicting_sources(
    *,
    storage: str,
    provider: object | None,
    client: object | None,
    client_factory: object | None,
) -> None:
    """Refuse an ambiguous client source instead of silently picking one.

    A caller who passes ``provider=`` alongside ``client=`` -- wiring a test
    double while also injecting a shared provider, say -- would otherwise have
    the client dropped without a word and every operation go to the provider's
    real cached client.

    Args:
        storage: Name of the storage class, for the error message.
        provider: The provider argument, if any.
        client: The client argument, if any.
        client_factory: The factory argument, if any.

    Raises:
        ConflictingClientSourceError: If more than one source was supplied.

    """
    given = [
        name
        for name, value in (
            ("provider", provider),
            ("client", client),
            ("client_factory", client_factory),
        )
        if value is not None
    ]
    if len(given) > 1:
        raise ConflictingClientSourceError(storage=storage, given=", ".join(given))


def deregister_shared(provider: "ClientProviderBase[Any]") -> None:
    """Drop a provider from the process-default registry.

    Without this, closing a provider reached through :meth:`shared` would leave
    it registered, and every later lookup for that configuration would hand
    back a closed provider.

    Args:
        provider: The provider to forget.

    """
    with SHARED_GUARD:
        for key, registered in list(SHARED.items()):
            if registered is provider:
                del SHARED[key]


def register_atexit_once() -> None:
    """Install the last-resort exit sweep, at most once per process."""
    global _atexit_registered  # noqa: PLW0603
    if _atexit_registered:
        return
    atexit.register(atexit_sweep)
    _atexit_registered = True


def atexit_sweep() -> None:
    """Report -- and where legitimate, tear down -- clients still open at exit.

    This runs with **no event loop**: by the time interpreter shutdown reaches
    here ``asyncio.run`` has already closed its loop, and on Python 3.14 even
    ``get_event_loop()`` raises rather than minting a new one. So nothing can
    be awaited. Two cases:

    - the owning loop is still alive -- close the aiohttp connectors
      synchronously, which is real work: ``BaseConnector._close()`` shuts the
      transports down inline and only its *return value* is awaitable.
    - the owning loop is already closed -- report only. Calling ``_close()``
      there would flip the connector's ``_closed`` flag and silence aiohttp's
      warnings without ever sending a FIN, which hides the leak rather than
      fixing it.

    The loop-teardown fallback (:meth:`ClientProviderBase._arm_shutdown`)
    normally gets there first; this only catches callers who drive a loop by
    hand and skip ``shutdown_asyncgens()``.
    """
    for provider in list(LIVE_PROVIDERS):
        try:
            stranded = provider._exit_sweep()  # noqa: SLF001
        except Exception:  # noqa: BLE001
            # Interpreter teardown is the worst possible place to raise.
            logger.debug("exit sweep failed for %s", type(provider).__name__)
            continue
        if not stranded:
            continue
        name = type(provider).__name__
        # Logging handlers may already be torn down at this point; an unhandled
        # traceback out of atexit would be worse than a missing warning.
        with suppress(Exception):
            logger.warning(
                "%d storage client(s) still open at interpreter exit for %s; "
                "close them from inside the event loop, e.g. "
                "`await %s.aclose_shared()`, so sockets shut down gracefully",
                stranded,
                name,
                name,
            )
