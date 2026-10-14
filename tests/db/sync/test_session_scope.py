"""Session-scope tests: one session and one transaction per outermost scope.

A scope opened on the :class:`Database` is shared by every function and
every repository running against that database in the same thread, and only
its outermost level commits.
"""

import contextlib
import contextvars
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, insert, make_url, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import QueuePool
from sqlmodel import SQLModel

from mint.db.exc import (
    ScopeAbortedError,
    ScopeSchemaMismatchError,
    SessionNotInitializedError,
)
from mint.db.sync.base import RepositoryBase
from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
from mint.db.sync.mixins import ResourceOwnerMixin
from mint.db.sync.scope import scoped
from mint.db.sync.uow import UnitOfWork
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
    def create_two_then_fail(self) -> None:
        """Insert two rows through the public API, then raise."""
        self.create(TItemCreate(name="first"))
        self.create(TItemCreate(name="second"))
        raise BoomError


class ItemService:
    """A service holding its database, the shape the bare ``scoped`` targets."""

    def __init__(self, db: Database) -> None:
        """Keep the database this service works against."""
        self.db = db

    @scoped
    def add_pair(self, first: str, second: str) -> Session:
        """Insert two rows through a repository and return the scope's session."""
        repo = TItemRepository(self.db)
        repo.create(TItemCreate(name=first))
        repo.create(TItemCreate(name=second))
        return self.db.current_session

    @scoped
    def add_then_fail(self, name: str) -> None:
        """Insert one row, then raise."""
        TItemRepository(self.db).create(TItemCreate(name=name))
        raise BoomError


class CommittedRows:
    """Counts rows through its own session, so it only ever sees committed data."""

    def __init__(self, db: Database) -> None:
        """Keep the database to count against."""
        self._db = db

    def items(self) -> int:
        """Return the committed TItem row count."""
        with self._db.create_session() as session:
            result = session.execute(select(func.count()).select_from(TItem))
            return result.scalar_one()

    def docs(self) -> int:
        """Return the committed TDoc row count."""
        with self._db.create_session() as session:
            result = session.execute(select(func.count()).select_from(TDoc))
            return result.scalar_one()


def test_scoped_function_commits_once_at_exit(sync_db: Database) -> None:
    """A scoped function's statements commit together, when the function returns."""
    committed = CommittedRows(sync_db)
    seen_before_exit: list[int] = []

    @sync_db.scoped
    def write_two() -> None:
        session = sync_db.current_session
        session.execute(insert(TItem).values(id=uuid4(), name="a"))
        session.execute(insert(TItem).values(id=uuid4(), name="b"))
        seen_before_exit.append(committed.items())

    write_two()

    assert seen_before_exit == [0]
    assert committed.items() == 2


def test_scope_rolls_back_every_repository_on_error(sync_db: Database) -> None:
    """An error in a scope discards the writes of every repository inside it."""
    items = TItemRepository(sync_db)
    docs = TDocRepository(sync_db)
    committed = CommittedRows(sync_db)

    @sync_db.scoped
    def write_then_fail() -> None:
        items.create(TItemCreate(name="x"))
        docs.create(TDocCreate(name="y"))
        raise BoomError

    with pytest.raises(BoomError):
        write_then_fail()

    assert committed.items() == 0
    assert committed.docs() == 0


def test_repositories_in_one_scope_share_one_session(sync_db: Database) -> None:
    """Two repositories called in one scope run on the scope's single session."""
    items = TItemRepository(sync_db)
    docs = TDocRepository(sync_db)
    seen: list[Session] = []

    @sync_db.scoped
    def write_both() -> None:
        items.create(TItemCreate(name="x"))
        seen.append(items.session)
        docs.create(TDocCreate(name="y"))
        seen.append(docs.session)
        seen.append(sync_db.current_session)

    write_both()

    assert len(seen) == 3
    assert seen[0] is seen[1] is seen[2]
    committed = CommittedRows(sync_db)
    assert committed.items() == 1
    assert committed.docs() == 1


def test_nested_scopes_and_plain_helpers_share_the_session(sync_db: Database) -> None:
    """A nested scope joins the outer one, through an undecorated frame in between."""
    seen: list[Session] = []

    @sync_db.scoped
    def inner() -> None:
        seen.append(sync_db.current_session)

    def plain_helper() -> None:
        seen.append(sync_db.current_session)
        inner()

    @sync_db.scoped
    def outer() -> None:
        seen.append(sync_db.current_session)
        plain_helper()
        # the nested scope returning must not have released the session
        seen.append(sync_db.current_session)

    outer()

    assert len(seen) == 4
    assert all(session is seen[0] for session in seen)


def test_nested_repository_write_does_not_commit_mid_scope(sync_db: Database) -> None:
    """A repository write inside a scope stays uncommitted until the scope exits."""
    repo = TItemRepository(sync_db)
    committed = CommittedRows(sync_db)
    seen_before_exit: list[int] = []

    @sync_db.scoped
    def write_and_peek() -> None:
        repo.create(TItemCreate(name="x"))
        seen_before_exit.append(committed.items())

    write_and_peek()

    assert seen_before_exit == [0]
    assert committed.items() == 1


def test_custom_repository_method_with_several_writes_is_atomic(
    sync_db: Database,
) -> None:
    """A repository method that writes twice and fails leaves no row behind."""
    repo = TwoStepRepository(sync_db)

    with pytest.raises(BoomError):
        repo.create_two_then_fail()

    assert CommittedRows(sync_db).items() == 0


def test_scoped_method_resolves_database_from_self(sync_db: Database) -> None:
    """The bare ``scoped`` decorator scopes a method on ``self.db``."""
    service = ItemService(sync_db)
    committed = CommittedRows(sync_db)

    session = service.add_pair("a", "b")

    assert isinstance(session, Session)
    assert committed.items() == 2

    with pytest.raises(BoomError):
        service.add_then_fail("c")

    assert committed.items() == 2


def test_scope_without_commit_persists_nothing(sync_db: Database) -> None:
    """``commit=False`` on the outermost scope wins over everything joining it."""
    repo = TItemRepository(sync_db)

    @sync_db.scoped
    def joined_write() -> None:
        repo.create(TItemCreate(name="x"))

    with sync_db.session_scope(commit=False):
        joined_write()

    assert CommittedRows(sync_db).items() == 0


def test_scope_releases_session_and_connection_on_exit(sync_db: Database) -> None:
    """After the outermost scope exits nothing stays bound or checked out."""
    pool = sync_db.engine.pool
    assert isinstance(pool, QueuePool)
    checked_out_inside: list[int] = []

    @sync_db.scoped
    def touch() -> None:
        sync_db.current_session.execute(select(TItem))
        checked_out_inside.append(pool.checkedout())

    touch()

    assert checked_out_inside == [1]
    assert pool.checkedout() == 0
    with pytest.raises(SessionNotInitializedError):
        _ = sync_db.current_session


def test_failed_scope_leaves_next_scope_clean(sync_db: Database) -> None:
    """A scope that raised does not leave a stale session bound for the next one."""
    repo = TItemRepository(sync_db)

    @sync_db.scoped
    def fail() -> None:
        repo.create(TItemCreate(name="lost"))
        raise BoomError

    with pytest.raises(BoomError):
        fail()
    with pytest.raises(SessionNotInitializedError):
        _ = sync_db.current_session

    created = repo.create(TItemCreate(name="kept"))

    assert created.name == "kept"
    assert CommittedRows(sync_db).items() == 1


def test_joining_scope_with_another_dbschema_raises(sync_db: Database) -> None:
    """A joiner asking for a dbschema the scope was not opened for is refused."""
    with sync_db.session_scope():
        with (
            pytest.raises(ScopeSchemaMismatchError),
            sync_db.session_scope(dbschema="tenant_x"),
        ):
            pass
        with pytest.raises(ScopeSchemaMismatchError):
            TItemRepository(sync_db, dbschema="tenant_x").get_many()


def test_joining_scope_with_same_or_no_dbschema_is_allowed(sync_db: Database) -> None:
    """A joiner naming the scope's own dbschema, or none, shares its session."""
    with sync_db.session_scope(dbschema="tenant_x") as outer:
        with sync_db.session_scope(dbschema="tenant_x") as same:
            assert same is outer
        with sync_db.session_scope() as unspecified:
            assert unspecified is outer


def test_repository_joining_a_scope_keeps_its_own_scoping(sync_db: Database) -> None:
    """A repository joining a session another one opened still applies its owner scoping."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    setup = TJobRepository(sync_db)
    setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    setup.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))
    jobs = TJobRepository(sync_db, owner=owner_a)

    @sync_db.scoped
    def read_twice() -> list[list[str]]:
        # the scope's session is first used by an unrelated repository
        TItemRepository(sync_db).get_many()
        first = jobs.get_many()
        second = jobs.get_many()
        return [[job.name for job in first], [job.name for job in second]]

    assert read_twice() == [["a-job"], ["a-job"]]


def test_external_session_still_commits_per_statement(sync_db: Database) -> None:
    """On an externally-owned session ``auto_commit`` keeps committing each write."""
    with UnitOfWork(sync_db) as uow:
        repo = TItemRepository(sync_db, session=uow.session)
        repo.create(TItemCreate(name="x"))

        assert CommittedRows(sync_db).items() == 1


@pytest.mark.concurrency
def test_concurrent_scopes_get_distinct_sessions(sync_db: Database) -> None:
    """Concurrent threads each get their own scope session on one shared database."""
    repo = TItemRepository(sync_db)
    sessions: list[Session] = []

    @sync_db.scoped
    def create_and_verify(name: str) -> None:
        sessions.append(sync_db.current_session)
        created = repo.create(TItemCreate(name=name))
        assert sync_db.current_session is repo.session
        fetched = repo.get(created.id)
        assert fetched is not None
        assert fetched.name == name

    names = [f"task-{i}" for i in range(50)]
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(create_and_verify, names))

    assert len({id(session) for session in sessions}) == 50
    assert repo.count() == 50


class TItemWithIdCreate(SQLModel):
    """Create-payload for TItem with a caller-supplied id, to force a conflict."""

    id: UUID
    name: str


class JobsAroundItems(TJobRepository):
    """Owner-scoped repository whose method calls another repository mid-way."""

    @RepositoryBase.ensure_session
    def names_around_an_item_read(self) -> list[list[str]]:
        """List jobs, read through another repository, list jobs again."""
        before = self.get_many()
        TItemRepository(self.db).get_many()
        after = self.get_many()
        return [[job.name for job in before], [job.name for job in after]]


def test_owner_scoping_does_not_leak_between_repositories(sync_db: Database) -> None:
    """Each repository in a shared scope sees rows by its own owner, not a neighbour's."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    setup = TJobRepository(sync_db)
    setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    setup.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    @sync_db.scoped
    def read_as_each() -> list[list[str]]:
        as_a = TJobRepository(sync_db, owner=owner_a).get_many()
        as_b = TJobRepository(sync_db, owner=owner_b).get_many()
        unscoped = TJobRepository(sync_db).get_many()
        return [sorted(job.name for job in rows) for rows in (as_a, as_b, unscoped)]

    assert read_as_each() == [["a-job"], ["b-job"], ["a-job", "b-job"]]


def test_scoping_is_restored_after_a_nested_repository_call(sync_db: Database) -> None:
    """A repository keeps its own scoping after calling into another one."""
    owner_a = TOwner(id=uuid4())
    setup = TJobRepository(sync_db)
    setup.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    setup.create(TJobCreate(name="b-job", created_by_user_id=uuid4()))

    names = JobsAroundItems(sync_db, owner=owner_a).names_around_an_item_read()

    assert names == [["a-job"], ["a-job"]]


def test_context_outliving_its_parent_scope_opens_its_own(sync_db: Database) -> None:
    """Work run in a context copied inside a scope, after it exited, does not join it."""
    repo = TItemRepository(sync_db)

    def late_write() -> None:
        repo.create(TItemCreate(name="late"))

    @sync_db.scoped
    def parent() -> contextvars.Context:
        repo.create(TItemCreate(name="parent"))
        return contextvars.copy_context()

    inherited = parent()
    inherited.run(late_write)

    assert CommittedRows(sync_db).items() == 2


def test_handled_statement_error_refuses_to_commit(sync_db: Database) -> None:
    """Swallowing a database error in a scope raises at exit instead of losing the writes."""
    repo = TItemRepository(sync_db)
    existing = repo.create(TItemCreate(name="existing"))

    @sync_db.scoped
    def write_and_swallow() -> None:
        repo.create(TItemCreate(name="would-be-lost"))
        with contextlib.suppress(IntegrityError):
            repo.create(TItemWithIdCreate(id=existing.id, name="duplicate"))

    with pytest.raises(ScopeAbortedError):
        write_and_swallow()

    assert CommittedRows(sync_db).items() == 1


def test_statement_error_inside_a_savepoint_still_commits(sync_db: Database) -> None:
    """A failure rolled back to a savepoint leaves the scope committable."""
    repo = TItemRepository(sync_db)
    existing = repo.create(TItemCreate(name="existing"))

    @sync_db.scoped
    def write_around_a_failed_savepoint() -> None:
        repo.create(TItemCreate(name="before"))
        with contextlib.suppress(IntegrityError), sync_db.current_session.begin_nested():
            repo.create(TItemWithIdCreate(id=existing.id, name="duplicate"))
        repo.create(TItemCreate(name="after"))

    write_around_a_failed_savepoint()

    rows = repo.get_many()
    assert sorted(row.name for row in rows) == ["after", "before", "existing"]


def test_failure_to_connect_surfaces_unchanged(postgres_sync_url: str) -> None:
    """An error raised before any connection exists passes through the scope as is."""
    url = make_url(postgres_sync_url).set(database="mint_no_such_database")
    db = Database(url.render_as_string(hide_password=False))
    try:
        with pytest.raises(DBAPIError), db.session_scope() as session:
            session.execute(text("select 1"))
    finally:
        db.engine.dispose()
