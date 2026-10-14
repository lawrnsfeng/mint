"""Retry policy for re-running a session scope after a transient failure."""

from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from sqlalchemy.exc import DBAPIError
from tenacity import (
    AsyncRetrying,
    Retrying,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

# serialization_failure, deadlock_detected: Postgres aborted the transaction
# to resolve a conflict and the same work is expected to succeed if re-run.
_RETRYABLE_SQLSTATES: Final[frozenset[str]] = frozenset({"40001", "40P01"})


@runtime_checkable
class IDriverError(Protocol):
    """Structural contract: a driver exception carrying a Postgres SQLSTATE.

    Both drivers mint.db uses expose it as ``pgcode`` — psycopg2 natively,
    asyncpg through SQLAlchemy's adapter.
    """

    pgcode: str | None


@dataclass(frozen=True)
class ScopeRetry:
    """How often, and how patiently, an outermost session scope is re-run.

    Attributes:
        retries: Extra attempts after the first. ``0`` disables retrying.
        min_wait: Backoff scale, in seconds. Each wait is drawn at random
            from zero up to this value doubled per attempt already made.
        max_wait: Upper bound on any single wait, in seconds.

    """

    retries: int
    min_wait: float = 0.05
    max_wait: float = 2.0

    @staticmethod
    def is_transient(exc: BaseException) -> bool:
        """Return whether ``exc`` is a failure a fresh transaction can outlive.

        Args:
            exc: The exception a session scope failed with.

        Returns:
            ``True`` for a serialization failure, a deadlock, or a
            connection SQLAlchemy has invalidated; ``False`` otherwise.

        """
        if not isinstance(exc, DBAPIError):
            return False
        if exc.connection_invalidated:
            return True
        return isinstance(exc.orig, IDriverError) and exc.orig.pgcode in _RETRYABLE_SQLSTATES

    @property
    def as_retrying(self) -> Retrying:
        """Return the tenacity controller for a sync scope."""
        return Retrying(
            stop=stop_after_attempt(self.retries + 1),
            wait=wait_random_exponential(multiplier=self.min_wait, max=self.max_wait),
            retry=retry_if_exception(self.is_transient),
            reraise=True,
        )

    @property
    def as_async_retrying(self) -> AsyncRetrying:
        """Return the tenacity controller for an async scope."""
        return AsyncRetrying(
            stop=stop_after_attempt(self.retries + 1),
            wait=wait_random_exponential(multiplier=self.min_wait, max=self.max_wait),
            retry=retry_if_exception(self.is_transient),
            reraise=True,
        )
