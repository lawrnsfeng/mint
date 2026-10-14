"""Hardening tests: timeouts, pool exhaustion, scope retry, savepoints."""

import contextlib
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.scope import scoped
from mint.db.asynk.uow import UnitOfWork
from mint.db.exc import (
    CommitOutcomeUnknownError,
    PoolExhaustedError,
    ScopeSchemaMismatchError,
)
from mint.db.retry import IDriverError
from mint.db.settings import DatabaseSettings
from tests.db.schemas import TItem

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

_QUERY_CANCELED = "57014"


class TItemCreate(SQLModel):
    """Create-payload for TItem."""

    name: str


class TItemWithIdCreate(SQLModel):
    """Create-payload for TItem with a caller-supplied id, to force a conflict."""

    id: UUID
    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


class BoomError(Exception):
    """Deliberate test-only failure, raised mid-scope."""


@dataclass
class FakeDriverError(Exception):
    """Stand-in for a driver exception carrying a SQLSTATE."""

    pgcode: str | None


class TenantProbe:
    """A service that reports which dbschema the scope it runs in was opened for."""

    def __init__(self, db: Database) -> None:
        """Keep the database this probe works against."""
        self.db = db

    async def joins_only_tenant_x(self) -> bool:
        """Return whether the open scope accepts ``tenant_x`` and refuses ``tenant_y``."""
        async with self.db.session_scope(dbschema="tenant_x"):
            pass
        try:
            async with self.db.session_scope(dbschema="tenant_y"):
                pass
        except ScopeSchemaMismatchError:
            return True
        return False

    @scoped(dbschema="tenant_x")
    async def via_method(self) -> bool:
        """Run the check in a scope the method decorator opened for ``tenant_x``."""
        return await self.joins_only_tenant_x()


class FlakyService:
    """A service whose scoped method fails transiently a set number of times."""

    def __init__(self, db: Database, *, failures: int, sqlstate: str = "40001") -> None:
        """Keep the database and how many attempts should fail."""
        self.db = db
        self.calls = 0
        self._failures = failures
        self._sqlstate = sqlstate

    async def _write_then_maybe_fail(self) -> int:
        """Insert a row; fail with the configured SQLSTATE while failures remain."""
        self.calls += 1
        await TItemRepository(self.db).create(TItemCreate(name=f"attempt-{self.calls}"))
        if self.calls <= self._failures:
            await self.db.current_session.execute(
                text(
                    "DO $$ BEGIN RAISE EXCEPTION 'simulated' "
                    f"USING ERRCODE = '{self._sqlstate}'; END $$",
                ),
            )
        return self.calls

    @scoped(retries=2)
    async def run(self) -> int:
        """Run the flaky write with two retries."""
        return await self._write_then_maybe_fail()

    @scoped
    async def run_without_retry(self) -> int:
        """Run the flaky write with no retry."""
        return await self._write_then_maybe_fail()

    @scoped
    async def run_joined(self) -> int:
        """Open the scope, then call the retrying method, which joins it."""
        return await self.run()

    async def names(self) -> list[str]:
        """Return the committed row names, sorted."""
        rows = await TItemRepository(self.db).get_many(limit=100)
        return sorted(row.name for row in rows)


@pytest.mark.asyncio
@pytest.mark.parametrize("sqlstate", ["40001", "40P01"])
async def test_scope_is_rerun_after_a_transient_error(async_db: Database, sqlstate: str) -> None:
    """A scope failing transiently is rolled back and re-run until it succeeds."""
    service = FlakyService(async_db, failures=2, sqlstate=sqlstate)

    assert await service.run() == 3
    # the two failed attempts left nothing behind
    assert await service.names() == ["attempt-3"]


@pytest.mark.asyncio
async def test_retries_exhausted_reraises_the_database_error(async_db: Database) -> None:
    """When every attempt fails the original error surfaces, after 1 + retries runs."""
    service = FlakyService(async_db, failures=10)

    with pytest.raises(DBAPIError) as raised:
        await service.run()

    assert service.calls == 3
    assert isinstance(raised.value.orig, IDriverError)
    assert raised.value.orig.pgcode == "40001"
    assert await service.names() == []


@pytest.mark.asyncio
async def test_transient_error_without_retries_is_not_rerun(async_db: Database) -> None:
    """Retrying is opt-in: a bare scope runs once."""
    service = FlakyService(async_db, failures=1)

    with pytest.raises(DBAPIError):
        await service.run_without_retry()

    assert service.calls == 1


@pytest.mark.asyncio
async def test_joined_scope_does_not_retry(async_db: Database) -> None:
    """A retrying function that joined an outer scope leaves the retry to its owner."""
    service = FlakyService(async_db, failures=1)

    with pytest.raises(DBAPIError):
        await service.run_joined()

    assert service.calls == 1
    assert await service.names() == []


@pytest.mark.asyncio
async def test_non_transient_error_is_not_retried(async_db: Database) -> None:
    """A constraint violation or an application error fails on the first attempt."""
    repo = TItemRepository(async_db)
    existing = await repo.create(TItemCreate(name="existing"))
    calls: list[str] = []

    @async_db.scoped(retries=3)
    async def duplicate() -> None:
        calls.append("duplicate")
        await repo.create(TItemWithIdCreate(id=existing.id, name="again"))

    @async_db.scoped(retries=3)
    async def boom() -> None:
        calls.append("boom")
        raise BoomError

    with pytest.raises(IntegrityError):
        await duplicate()
    with pytest.raises(BoomError):
        await boom()

    assert calls == ["duplicate", "boom"]


@pytest.mark.asyncio
async def test_savepoint_holds_inside_a_scope(async_db: Database) -> None:
    """A savepoint rolled back inside a scope keeps the scope's other writes."""
    repo = TItemRepository(async_db)

    @async_db.scoped
    async def write_around_a_failed_savepoint() -> None:
        await repo.create(TItemCreate(name="before"))
        with contextlib.suppress(BoomError):
            async with async_db.current_session.begin_nested():
                await repo.create(TItemCreate(name="discarded"))
                raise BoomError
        await repo.create(TItemCreate(name="after"))

    await write_around_a_failed_savepoint()

    rows = await repo.get_many()
    assert sorted(row.name for row in rows) == ["after", "before"]


@pytest.mark.asyncio
async def test_statement_timeout_cancels_a_slow_statement(postgres_async_url: str) -> None:
    """STATEMENT_TIMEOUT_MS reaches the server and cancels a statement that overruns."""
    db = Database(postgres_async_url, settings=DatabaseSettings(STATEMENT_TIMEOUT_MS=50))
    try:
        with pytest.raises(DBAPIError) as raised:
            async with db.session_scope() as session:
                await session.execute(text("select pg_sleep(2)"))

        assert isinstance(raised.value.orig, IDriverError)
        assert raised.value.orig.pgcode == _QUERY_CANCELED
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_exhausted_pool_raises_pool_exhausted_error(postgres_async_url: str) -> None:
    """A checkout that outlasts POOL_TIMEOUT raises PoolExhaustedError, not a driver error."""
    settings = DatabaseSettings(POOL_SIZE=1, MAX_OVERFLOW=0, POOL_TIMEOUT=0.2)
    db = Database(postgres_async_url, settings=settings)
    try:
        async with db.session_scope() as held:
            await held.execute(text("select 1"))  # takes the only connection

            with pytest.raises(PoolExhaustedError) as raised:
                async with db.create_session() as second:
                    await second.execute(text("select 1"))
            assert raised.value.pool_size == 1
            assert raised.value.checked_out == 1
            assert raised.value.timeout == pytest.approx(0.2)

            with pytest.raises(PoolExhaustedError):
                async with UnitOfWork(db) as uow:
                    await uow.session.execute(text("select 1"))
    finally:
        await db.engine.dispose()


@pytest.mark.asyncio
async def test_pool_timeout_from_another_pool_is_left_alone(async_db: Database) -> None:
    """A pool timeout raised in the block while this pool has idle connections is not ours."""
    async with async_db.create_session() as first, async_db.create_session() as second:
        await first.execute(text("select 1"))
        await second.execute(text("select 1"))
    # two connections are now idle in the pool; the block below takes one

    async def fail_on_another_pool() -> None:
        async with async_db.create_session() as session:
            await session.execute(text("select 1"))
            raise PoolTimeoutError("another pool")

    with pytest.raises(PoolTimeoutError, match="another pool"):
        await fail_on_another_pool()


@pytest.mark.asyncio
async def test_connection_lost_while_committing_is_not_retried(
    async_db: Database,
    mocker: "MockerFixture",
) -> None:
    """A disconnect during COMMIT has an unknown outcome, so the scope is not re-run."""
    lost = DBAPIError("COMMIT", {}, FakeDriverError(None), connection_invalidated=True)
    mocker.patch.object(AsyncSession, "commit", new=mocker.AsyncMock(side_effect=lost))
    calls: list[str] = []

    @async_db.scoped(retries=3)
    async def write() -> None:
        calls.append("write")
        await async_db.current_session.execute(text("select 1"))

    with pytest.raises(CommitOutcomeUnknownError):
        await write()

    assert calls == ["write"]


@pytest.mark.asyncio
async def test_serialization_failure_at_commit_is_retried(
    async_db: Database,
    mocker: "MockerFixture",
) -> None:
    """A conflict reported at COMMIT rolled the transaction back, so the scope re-runs."""
    conflict = DBAPIError("COMMIT", {}, FakeDriverError("40001"))
    mocker.patch.object(
        AsyncSession,
        "commit",
        new=mocker.AsyncMock(side_effect=[conflict, None]),
    )
    calls: list[str] = []

    @async_db.scoped(retries=3)
    async def write() -> None:
        calls.append("write")
        await async_db.current_session.execute(text("select 1"))

    await write()

    assert calls == ["write", "write"]


@pytest.mark.asyncio
async def test_scope_options_open_the_scope_for_a_dbschema(async_db: Database) -> None:
    """``dbschema`` on either decorator, or on run_in_scope, opens the scope for it."""
    probe = TenantProbe(async_db)

    @async_db.scoped(dbschema="tenant_x")
    async def via_function() -> bool:
        return await probe.joins_only_tenant_x()

    assert await via_function() is True
    assert await probe.via_method() is True
    assert await async_db.run_in_scope(probe.joins_only_tenant_x, dbschema="tenant_x") is True
