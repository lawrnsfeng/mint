"""Exception hierarchy for the mint.db repository layer."""

from dataclasses import dataclass
from typing import Any

from mint.exc import TemplatedError


@dataclass
class RepositoryError(TemplatedError):
    """Generic repository error."""


@dataclass
class SessionNotInitializedError(RepositoryError):
    """A repository method was called with no session bound."""

    TEMPLATE = "Session has not been initialized for {repository}"
    repository: str


@dataclass
class NotFoundError(RepositoryError):
    """A requested record does not exist."""

    TEMPLATE = "{schema} not found: {id_}"
    schema: str
    id_: Any


@dataclass
class OperationalError(RepositoryError):
    """Uncaught exception when running a database operation."""

    TEMPLATE = "Operational uncaught error: {error}"
    error: BaseException


@dataclass
class ConfigError(RepositoryError):
    """Repository or database configuration is invalid."""

    TEMPLATE = "Invalid repository configuration: {detail}"
    detail: str


@dataclass
class DBSchemaNotSetError(RepositoryError):
    """force_session_schema() was called without a configured dbschema."""

    TEMPLATE = "dbschema must be set on {repository} to force a session schema"
    repository: str


@dataclass
class ScopeSchemaMismatchError(RepositoryError):
    """A call joined a session scope opened for a different dbschema."""

    TEMPLATE = "dbschema {requested} requested inside a session scope opened for {active}"
    requested: str
    active: str | None


@dataclass
class ScopeAbortedError(RepositoryError):
    """A session scope tried to commit a transaction a handled error had aborted."""

    TEMPLATE = (
        "a statement failed inside this session scope and the error was handled "
        "without rolling back to a savepoint: the transaction is aborted and "
        "cannot be committed"
    )


@dataclass
class PoolExhaustedError(RepositoryError):
    """No pooled connection became free within the pool timeout."""

    TEMPLATE = (
        "no database connection became free within {timeout}s "
        "(pool size {pool_size}, overflow {overflow}, checked out {checked_out})"
    )
    pool_size: int
    overflow: int
    checked_out: int
    timeout: float


@dataclass
class CommitOutcomeUnknownError(RepositoryError):
    """The connection was lost while a session scope was committing.

    The server may or may not have applied the transaction, so the scope
    is never re-run on this error: doing so could apply its work twice.
    """

    TEMPLATE = (
        "the connection was lost while committing: the transaction may or may "
        "not have been applied"
    )
