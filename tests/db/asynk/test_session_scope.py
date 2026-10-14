"""Session-scope tests: one session and one transaction per outermost scope.

A scope opened on the :class:`Database` is shared by every function and
every repository running against that database in the same task, and only
its outermost level commits.
"""

import asyncio
import contextlib
from dataclasses import dataclass
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import func, insert, make_url, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import QueuePool
from sqlmodel import SQLModel

from mint.db.asynk.base import RepositoryBase
from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mixins import ResourceOwnerMixin
from mint.db.asynk.scope import scoped
from mint.db.asynk.uow import UnitOfWork
from mint.db.exc import (
    ScopeAbortedError,
    ScopeSchemaMismatchError,
    SessionNotInitializedError,
)
from tests.db.schemas import TDoc, TItem, TJob


class TItemCreate(SQLModel):
    """Create-payload for TItem."""

    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


class TDocCreate(SQLModel):
    """Create-payload for TDoc."""

    name: str


class TDocRepository(EntityRepository[TDoc, UUID]):
    """CRUD repository for TDoc."""

    Schema = TDoc


@dataclass
class TOwner:
    """Minimal IOwner-compatible owner for tests."""

    id: UUID
    is_scoped: bool = True


class TJobCreate(SQLModel):
    """Create-payload for TJob, including the owner column for test setup."""

    name: str
    created_by_user_id: UUID


class TJobRepository(ResourceOwnerMixin[TJob], EntityRepository[TJob, UUID]):
    """Owner-scoped repository for TJob."""

    Schema = TJob


class BoomError(Exception):
    """Deliberate test-only failure, raised mid-scope."""


class TwoStepRepository(TItemRepository):
    """Repository whose custom method writes twice, then fails."""

    @RepositoryBase.ensure_session
    async def create_two_then_fail(self) -> None:
        """Insert two rows through the public API, then raise."""
        await self.create(TItemCreate(name="first"))
        await self.create(TItemCreate(name="second"))
        raise BoomError


class ItemService:
    """A service holding its database, the shape the bare ``scoped`` targets."""

    def __init__(self, db: Database) -> None:
        """Keep the database this service works against."""
        self.db = db

    @scoped
    async def add_pair(self, first: str, second: str) -> AsyncSession:
        """Insert two rows through a repository and return the scope's session."""
        repo = TItemRepository(self.db)
        await repo.create(TItemCreate(name=first))
        await repo.create(TItemCreate(name=second))
        return self.db.current_session

    @scoped
    async def add_then_fail(self, name: str) -> None:
        """Insert one row, then raise."""
        await TItemRepository(self.db).create(TItemCreate(name=name))
        raise BoomError


class CommittedRows:
    """Counts rows through its own session, so it only ever sees committed data."""

    def __init__(self, db: Database) -> None:
        """Keep the database to count against."""
        self._db = db

    async def items(self) -> int:
        """Return the committed TItem row count."""
        async with self._db.create_session() as session:
            result = await session.execute(select(func.count()).select_from(TItem))
            return result.scalar_one()

    async def docs(self) -> int:
        """Return the committed TDoc row count."""
        async with self._db.create_session() as session:
            result = await session.execute(select(func.count()).select_from(TDoc))
            return result.scalar_one()


@pytest.mark.asyncio
async def test_scoped_function_commits_once_at_exit(async_db: Database) -> None:
    """A scoped function's statements commit together, when the function returns."""
    committed = CommittedRows(async_db)
    seen_before_exit: list[int] = []

    @async_db.scoped
    async def write_two() -> None:
        session = async_db.current_session
        await session.execute(insert(TItem).values(id=uuid4(), name="a"))
        await session.execute(insert(TItem).values(id=uuid4(), name="b"))
        seen_before_exit.append(await committed.items())

    await write_two()

    assert seen_before_exit == [0]
    assert await committed.items() == 2


@pytest.mark.asyncio
async def test_scope_rolls_back_every_repository_on_error(async_db: Database) -> None:
    """An error in a scope discards the writes of every repository inside it."""
    items = TItemRepository(async_db)
    docs = TDocRepository(async_db)
    committed = CommittedRows(async_db)

    @async_db.scoped
    async def write_then_fail() -> None:
        await items.create(TItemCreate(name="x"))
        await docs.create(TDocCreate(name="y"))
        raise BoomError

    with pytest.raises(BoomError):
        await write_then_fail()

    assert await committed.items() == 0
    assert await committed.docs() == 0


@pytest.mark.asyncio
async def test_repositories_in_one_scope_share_one_session(async_db: Database) -> None:
    """Two repositories called in one scope run on the scope's single session."""
    items = TItemRepository(async_db)
    docs = TDocRepository(async_db)
    seen: list[AsyncSession] = []

    @async_db.scoped
    async def write_both() -> None:
        await items.create(TItemCreate(name="x"))
        seen.append(items.session)
        await docs.create(TDocCreate(name="y"))
        seen.append(docs.session)
        seen.append(async_db.current_session)

    await write_both()

    assert len(seen) == 3
    assert seen[0] is seen[1] is seen[2]
    committed = CommittedRows(async_db)
    assert await committed.items() == 1
    assert await committed.docs() == 1


@pytest.mark.asyncio
async def test_nested_scopes_and_plain_helpers_share_the_session(async_db: Database) -> None:
    """A nested scope joins the outer one, through an undecorated frame in between."""
    seen: list[AsyncSession] = []

    @async_db.scoped
    async def inner() -> None:
        seen.append(async_db.current_session)

    async def plain_helper() -> None:
        seen.append(async_db.current_session)
        await inner()

    @async_db.scoped
    async def outer() -> None:
        seen.append(async_db.current_session)
        await plain_helper()
        # the nested scope returning must not have released the session
        seen.append(async_db.current_session)

    await outer()

    assert len(seen) == 4
    assert all(session is seen[0] for session in seen)


@pytest.mark.asyncio
async def test_nested_repository_write_does_not_commit_mid_scope(async_db: Database) -> None:
    """A repository write inside a scope stays uncommitted until the scope exits."""
    repo = TItemRepository(async_db)
    committed = CommittedRows(async_db)
    seen_before_exit: list[int] = []

    @async_db.scoped
    async def write_and_peek() -> None:
        await repo.create(TItemCreate(name="x"))
        seen_before_exit.append(await committed.items())

    await write_and_peek()

    assert seen_before_exit == [0]
    assert await committed.items() == 1


@pytest.mark.asyncio
async def test_custom_repository_method_with_several_writes_is_atomic(
    async_db: Database,
) -> None:
    """A repository method that writes twice and fails leaves no row behind."""
    repo = TwoStepRepository(async_db)

    with pytest.raises(BoomError):
        await repo.create_two_then_fail()

    assert await CommittedRows(async_db).items() == 0


@pytest.mark.asyncio
async def test_scoped_method_resolves_database_from_self(async_db: Database) -> None:
    """The bare ``scoped`` decorator scopes a method on ``self.db``."""
    service = ItemService(async_db)
    committed = CommittedRows(async_db)

    session = await service.add_pair("a", "b")

    assert isinstance(session, AsyncSession)
    assert await committed.items() == 2

    with pytest.raises(BoomError):
        await service.add_then_fail("c")

    assert await committed.items() == 2


@pytest.mark.asyncio
async def test_scope_without_commit_persists_nothing(async_db: Database) -> None:
    """``commit=False`` on the outermost scope wins over everything joining it."""
    repo = TItemRepository(async_db)

    @async_db.scoped
    async def joined_write() -> None:
        await repo.create(TItemCreate(name="x"))

    async with async_db.session_scope(commit=False):
        await joined_write()

    assert await CommittedRows(async_db).items() == 0


@pytest.mark.asyncio
async def test_scope_releases_session_and_connection_on_exit(async_db: Database) -> None:
    """After the outermost scope exits nothing stays bound or checked out."""
    pool = async_db.engine.pool
    assert isinstance(pool, QueuePool)
    checked_out_inside: list[int] = []

    @async_db.scoped
    async def touch() -> None:
        await async_db.current_session.execute(select(TItem))
        checked_out_inside.append(pool.checkedout())

    await touch()

    assert checked_out_inside == [1]
    assert pool.checkedout() == 0
    with pytest.raises(SessionNotInitializedError):
        _ = async_db.current_session


@pytest.mark.asyncio
async def test_failed_scope_leaves_next_scope_clean(async_db: Database) -> None:
    """A scope that raised does not leave a stale session bound for the next one."""
    repo = TItemRepository(async_db)

    @async_db.scoped
    async def fail() -> None:
        await repo.create(TItemCreate(name="lost"))
        raise BoomError

    with pytest.raises(BoomError):
        await fail()
    with pytest.raises(SessionNotInitializedError):
        _ = async_db.current_session

    created = await repo.create(TItemCreate(name="kept"))

    assert created.name == "kept"
    assert await CommittedRows(async_db).items() == 1


@pytest.mark.asyncio
async def test_joining_scope_with_another_dbschema_raises(async_db: Database) -> None:
    """A joiner asking for a dbschema the scope was not opened for is refused."""
    async with async_db.session_scope():
        with pytest.raises(ScopeSchemaMismatchError):
            async with async_db.session_scope(dbschema="tenant_x"):
                pass
        with pytest.raises(ScopeSchemaMismatchError):
            await TItemRepository(async_db, dbschema="tenant_x").get_many()


@pytest.mark.asyncio
async def test_joining_scope_with_same_or_no_dbschema_is_allowed(async_db: Database) -> None:
    """A joiner naming the scope's own dbschema, or none, shares its session."""
    async with async_db.session_scope(dbschema="tenant_x") as outer:
        async with async_db.session_scope(dbschema="tenant_x") as same:
            assert same is outer
        async with async_db.session_scope() as unspecified:
            assert unspecified is outer


@pytest.mark.asyncio
async def test_repository_joining_a_scope_keeps_its_own_scoping(async_db: Database) -> None:
    """A repository joining a session another one opened still applies its owner scoping."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    setup = TJobRepository(async_db)
    await setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    await setup.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))
    jobs = TJobRepository(async_db, owner=owner_a)

    @async_db.scoped
    async def read_twice() -> list[list[str]]:
        # the scope's session is first used by an unrelated repository
        await TItemRepository(async_db).get_many()
        first = await jobs.get_many()
        second = await jobs.get_many()
        return [[job.name for job in first], [job.name for job in second]]

    assert await read_twice() == [["a-job"], ["a-job"]]


@pytest.mark.asyncio
async def test_external_session_still_commits_per_statement(async_db: Database) -> None:
    """On an externally-owned session ``auto_commit`` keeps committing each write."""
    async with UnitOfWork(async_db) as uow:
        repo = TItemRepository(async_db, session=uow.session)
        await repo.create(TItemCreate(name="x"))

        assert await CommittedRows(async_db).items() == 1


@pytest.mark.concurrency
@pytest.mark.asyncio
async def test_concurrent_scopes_get_distinct_sessions(async_db: Database) -> None:
    """Concurrent tasks each get their own scope session on one shared database."""
    repo = TItemRepository(async_db)
    sessions: list[AsyncSession] = []

    @async_db.scoped
    async def create_and_verify(name: str) -> None:
        sessions.append(async_db.current_session)
        created = await repo.create(TItemCreate(name=name))
        await asyncio.sleep(0)
        assert async_db.current_session is repo.session
        fetched = await repo.get(created.id)
        assert fetched is not None
        assert fetched.name == name

    await asyncio.gather(*(create_and_verify(f"task-{i}") for i in range(50)))

    assert len({id(session) for session in sessions}) == 50
    assert await repo.count() == 50


class TItemWithIdCreate(SQLModel):
    """Create-payload for TItem with a caller-supplied id, to force a conflict."""

    id: UUID
    name: str


class JobsAroundItems(TJobRepository):
    """Owner-scoped repository whose method calls another repository mid-way."""

    @RepositoryBase.ensure_session
    async def names_around_an_item_read(self) -> list[list[str]]:
        """List jobs, read through another repository, list jobs again."""
        before = await self.get_many()
        await TItemRepository(self.db).get_many()
        after = await self.get_many()
        return [[job.name for job in before], [job.name for job in after]]


@pytest.mark.asyncio
async def test_owner_scoping_does_not_leak_between_repositories(async_db: Database) -> None:
    """Each repository in a shared scope sees rows by its own owner, not a neighbour's."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    setup = TJobRepository(async_db)
    await setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    await setup.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    @async_db.scoped
    async def read_as_each() -> list[list[str]]:
        as_a = await TJobRepository(async_db, owner=owner_a).get_many()
        as_b = await TJobRepository(async_db, owner=owner_b).get_many()
        unscoped = await TJobRepository(async_db).get_many()
        return [sorted(job.name for job in rows) for rows in (as_a, as_b, unscoped)]

    assert await read_as_each() == [["a-job"], ["b-job"], ["a-job", "b-job"]]


@pytest.mark.asyncio
async def test_scoping_is_restored_after_a_nested_repository_call(async_db: Database) -> None:
    """A repository keeps its own scoping after calling into another one."""
    owner_a = TOwner(id=uuid4())
    setup = TJobRepository(async_db)
    await setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    await setup.create(TJobCreate(name="b-job", created_by_user_id=uuid4()))

    names = await JobsAroundItems(async_db, owner=owner_a).names_around_an_item_read()

    assert names == [["a-job"], ["a-job"]]


@pytest.mark.asyncio
async def test_task_outliving_its_parent_scope_opens_its_own(async_db: Database) -> None:
    """A task started in a scope and run after it exited does not join the dead scope."""
    repo = TItemRepository(async_db)
    release = asyncio.Event()

    async def late_write() -> None:
        await release.wait()
        await repo.create(TItemCreate(name="late"))

    @async_db.scoped
    async def parent() -> asyncio.Task[None]:
        await repo.create(TItemCreate(name="parent"))
        return asyncio.create_task(late_write())

    task = await parent()
    release.set()
    await task

    assert await CommittedRows(async_db).items() == 2


@pytest.mark.asyncio
async def test_handled_statement_error_refuses_to_commit(async_db: Database) -> None:
    """Swallowing a database error in a scope raises at exit instead of losing the writes."""
    repo = TItemRepository(async_db)
    existing = await repo.create(TItemCreate(name="existing"))

    @async_db.scoped
    async def write_and_swallow() -> None:
        await repo.create(TItemCreate(name="would-be-lost"))
        with contextlib.suppress(IntegrityError):
            await repo.create(TItemWithIdCreate(id=existing.id, name="duplicate"))

    with pytest.raises(ScopeAbortedError):
        await write_and_swallow()

    assert await CommittedRows(async_db).items() == 1


@pytest.mark.asyncio
async def test_statement_error_inside_a_savepoint_still_commits(async_db: Database) -> None:
    """A failure rolled back to a savepoint leaves the scope committable."""
    repo = TItemRepository(async_db)
    existing = await repo.create(TItemCreate(name="existing"))

    @async_db.scoped
    async def write_around_a_failed_savepoint() -> None:
        await repo.create(TItemCreate(name="before"))
        with contextlib.suppress(IntegrityError):
            async with async_db.current_session.begin_nested():
                await repo.create(TItemWithIdCreate(id=existing.id, name="duplicate"))
        await repo.create(TItemCreate(name="after"))

    await write_around_a_failed_savepoint()

    rows = await repo.get_many()
    assert sorted(row.name for row in rows) == ["after", "before", "existing"]


@pytest.mark.asyncio
async def test_failure_to_connect_surfaces_unchanged(postgres_async_url: str) -> None:
    """An error raised before any connection exists passes through the scope as is."""
    url = make_url(postgres_async_url).set(database="mint_no_such_database")
    db = Database(url.render_as_string(hide_password=False))
    # SQLAlchemy 2.0 lets asyncpg's own error through at connect time; 2.1 wraps it.
    try:
        with pytest.raises((DBAPIError, asyncpg.PostgresError)):
            async with db.session_scope() as session:
                await session.execute(text("select 1"))
    finally:
        await db.engine.dispose()
