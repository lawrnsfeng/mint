"""Unit tests for the transient-error classification behind scope retries."""

from dataclasses import dataclass

import pytest
from sqlalchemy.exc import DBAPIError

from mint.db.retry import ScopeRetry


@dataclass
class FakeDriverError(Exception):
    """Stand-in for a driver exception carrying a SQLSTATE."""

    pgcode: str | None


class PlainDriverError(Exception):
    """Stand-in for a driver exception with no SQLSTATE attribute at all."""


@pytest.mark.parametrize("pgcode", ["40001", "40P01"])
def test_serialization_failure_and_deadlock_are_transient(pgcode: str) -> None:
    """The two SQLSTATEs Postgres uses to abort a conflicting transaction retry."""
    exc = DBAPIError("select 1", {}, FakeDriverError(pgcode))

    assert ScopeRetry.is_transient(exc) is True


@pytest.mark.parametrize("pgcode", ["23505", "57014", None])
def test_other_sqlstates_are_not_transient(pgcode: str | None) -> None:
    """A constraint violation, a cancelled statement or no code at all do not retry."""
    exc = DBAPIError("select 1", {}, FakeDriverError(pgcode))

    assert ScopeRetry.is_transient(exc) is False


def test_invalidated_connection_is_transient() -> None:
    """A connection SQLAlchemy invalidated retries whatever the driver reported."""
    exc = DBAPIError("select 1", {}, PlainDriverError(), connection_invalidated=True)

    assert ScopeRetry.is_transient(exc) is True


def test_driver_error_without_sqlstate_is_not_transient() -> None:
    """A driver exception exposing no SQLSTATE is not guessed to be transient."""
    exc = DBAPIError("select 1", {}, PlainDriverError())

    assert ScopeRetry.is_transient(exc) is False


def test_non_database_error_is_not_transient() -> None:
    """An ordinary exception never retries."""
    assert ScopeRetry.is_transient(ValueError("boom")) is False
