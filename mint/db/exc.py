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
