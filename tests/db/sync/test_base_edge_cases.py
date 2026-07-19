"""Coverage-closing tests for edge cases and error paths across mint.db.sync."""

from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.orm import QueryableAttribute, joinedload, selectinload
from sqlmodel import Field, Relationship, SQLModel, col, select

from mint.db.exc import (
    ConfigError,
    DBSchemaNotSetError,
    NotFoundError,
    OperationalError,
    SessionNotInitializedError,
)
from mint.db.models import Base, BaseWithUUID, IsDeletedMixin
from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
from mint.db.sync.mixins import SoftDeleteMixin
from mint.db.sync.mv import MaterializedViewRepository
from mint.db.sync.uow import UnitOfWork
from tests.db.schemas import TDoc, TItem

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture


class TItemCreate(SQLModel):
    """Create-payload for TItem."""

    name: str


class TItemUpdate(SQLModel):
    """Update-payload for TItem."""

    name: str | None = None


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


class TDocCreate(SQLModel):
    """Create-payload for TDoc."""

    name: str


class TDocRepository(SoftDeleteMixin[TDoc, UUID]):
    """Soft-deletable repository for TDoc."""

    Schema = TDoc


def test_database_requires_uri_or_engine() -> None:
    """Database() raises ConfigError when neither uri nor engine is supplied."""
    with pytest.raises(ConfigError):
        Database()


def test_session_raises_when_unbound(sync_db: Database) -> None:
    """RepositoryBase.session raises SessionNotInitializedError with no session bound."""
    repo = TItemRepository(sync_db)
    with pytest.raises(SessionNotInitializedError):
        _ = repo.session


def test_force_session_schema_raises_when_dbschema_unset(sync_db: Database) -> None:
    """force_session_schema() raises DBSchemaNotSetError when dbschema was never configured."""
    repo = TItemRepository(sync_db)
    with pytest.raises(DBSchemaNotSetError):
        repo.force_session_schema()


def test_update_with_empty_payload_and_missing_id_raises_not_found(sync_db: Database) -> None:
    """update() with no fields set still raises NotFoundError for a missing id."""
    repo = TItemRepository(sync_db)
    with pytest.raises(NotFoundError):
        repo.update(UUID(int=0), TItemUpdate())


def test_count_raises_operational_error_when_scalar_is_none(
    sync_db: Database,
    mocker: "MockerFixture",
) -> None:
    """count() raises OperationalError if the count query returns no value.

    Not reachable via normal Postgres usage (COUNT always returns a row) —
    mocked to exercise this defensive branch.
    """
    repo = TItemRepository(sync_db)
    mock_result = mocker.Mock()
    mock_result.scalar.return_value = None
    session_mock = mocker.Mock()
    session_mock.execute = mocker.Mock(return_value=mock_result)
    # ensure_session still opens (and cleans up) a real session for the
    # wrapper's own bookkeeping; only the count() body's self.session access
    # is intercepted, via a class-level property override.
    mocker.patch.object(TItemRepository, "session", new=property(lambda _self: session_mock))

    with pytest.raises(OperationalError):
        repo.count()


def test_create_raises_operational_error_when_no_row_returned(
    sync_db: Database,
    mocker: "MockerFixture",
) -> None:
    """create() raises OperationalError if the insert returns no row.

    Not reachable via normal Postgres usage — mocked to exercise this
    defensive branch. Mirrors the real production case that motivated this
    exception choice: an upsert-style statement (``ON CONFLICT DO NOTHING``)
    silently returning nothing because the row already existed — an
    infrastructure-level anomaly, not a domain "not found".
    """
    repo = TItemRepository(sync_db)
    mock_result = mocker.Mock()
    mock_result.scalar.return_value = None
    session_mock = mocker.Mock()
    session_mock.execute = mocker.Mock(return_value=mock_result)
    session_mock.commit = mocker.Mock()
    mocker.patch.object(TItemRepository, "session", new=property(lambda _self: session_mock))

    with pytest.raises(OperationalError):
        repo.create(TItemCreate(name="x"))


def test_soft_delete_remove_many(sync_db: Database) -> None:
    """SoftDeleteMixin.remove_many() soft-deletes every matching row."""
    repo = TDocRepository(sync_db)
    first = repo.create(TDocCreate(name="a"))
    second = repo.create(TDocCreate(name="b"))

    updated = repo.remove_many([first.id, second.id])

    assert {row.id for row in updated} == {first.id, second.id}
    assert all(row.is_deleted for row in updated)
    assert repo.get(first.id) is None
    assert repo.get(second.id) is None


def test_soft_delete_remove_missing_id_raises_not_found(sync_db: Database) -> None:
    """SoftDeleteMixin.remove() raises NotFoundError for an id that doesn't exist."""
    repo = TDocRepository(sync_db)
    with pytest.raises(NotFoundError):
        repo.remove(UUID(int=0))


def test_execute_many_selectinload_populates_every_object(sync_db: Database) -> None:
    """A selectinload() option on a custom statement populates every returned row."""
    parent = ParentRepository(sync_db).create(TParentCreate(name="p"))
    ChildRepository(sync_db).create(TChildCreate(name="c1", parent_id=parent.id))
    ChildRepository(sync_db).create(TChildCreate(name="c2", parent_id=parent.id))

    repo = ChildRepository(sync_db)
    parent_attr = cast("QueryableAttribute[SWithFolderList | None]", SDocForFetch.parent)
    stmt = select(SDocForFetch).options(selectinload(parent_attr))
    children = repo.execute_many(stmt)

    assert len(children) == 2
    for child in children:
        assert child.parent is not None
        assert child.parent.name == "p"


def test_database_from_uri_creates_own_engine(postgres_sync_url: str) -> None:
    """Database(uri=...) creates its own engine rather than requiring one be passed in."""
    db = Database(postgres_sync_url)
    assert db.engine is not None
    db.engine.dispose()


class SMVWithSchema(Base, table=True):
    """Materialized-view-shaped schema used only to exercise table_name."""

    __tablename__ = "smvwithschema"

    id: UUID = Field(primary_key=True)
    name: str


class MVWithSchemaRepository(MaterializedViewRepository[SMVWithSchema]):
    """Repository used only to exercise table_name with dbschema set."""

    Schema = SMVWithSchema


def test_materialized_view_table_name_with_dbschema(sync_db: Database) -> None:
    """table_name is dbschema-qualified when dbschema is configured."""
    repo = MVWithSchemaRepository(sync_db, dbschema="tenant_x")
    assert repo.table_name == "tenant_x.smvwithschema"

    unscoped_repo = MVWithSchemaRepository(sync_db)
    assert unscoped_repo.table_name == "smvwithschema"


def test_dbschema_routes_queries_to_the_configured_schema(sync_db: Database) -> None:
    """dbschema= on a repository routes unqualified table access to that Postgres schema."""
    with sync_db.engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_a"))
        conn.execute(text("DROP TABLE IF EXISTS tenant_a.titem"))
        conn.execute(
            text("CREATE TABLE tenant_a.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    repo = TItemRepository(sync_db, dbschema="tenant_a")
    created = repo.create(TItemCreate(name="tenant-scoped"))

    with sync_db.engine.begin() as conn:
        result = conn.execute(
            text("SELECT name FROM tenant_a.titem WHERE id = :id"),
            {"id": str(created.id)},
        )
        row = result.first()
    assert row is not None
    assert row[0] == "tenant-scoped"

    default_repo = TItemRepository(sync_db)
    assert default_repo.get(created.id) is None

    with sync_db.engine.begin() as conn:
        conn.execute(text("DROP SCHEMA tenant_a CASCADE"))


def test_schema_translate_map_lost_after_commit_without_reforce(sync_db: Database) -> None:
    """A mid-session commit drops ``schema_translate_map`` unless reapplied.

    Spike confirming the exact premise behind ``force_session_schema()``/
    ``refresh()``'s reapply-on-refresh logic — see the asynk mirror of this
    test for the full explanation and
    specs/002-db-repository-layer/research.md, "Schema-translate
    persistence across commits".
    """
    with sync_db.engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_spike"))
        conn.execute(text("DROP TABLE IF EXISTS tenant_spike.titem"))
        conn.execute(
            text("CREATE TABLE tenant_spike.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    item_id = uuid4()
    with sync_db.create_session(dbschema="tenant_spike") as session:
        session.execute(insert(TItem).values(id=item_id, name="first"))
        session.commit()

        # No reforce: the unqualified query resolves against public.titem
        # (empty), not tenant_spike.titem — silently wrong, not an error.
        lost = session.execute(select(TItem).where(col(TItem.id) == item_id))
        assert lost.scalar() is None
        session.commit()

        # Reapplying schema_translate_map — what force_session_schema()
        # does — restores correct routing to tenant_spike. Must happen
        # before any statement on the new transaction: once a Connection is
        # checked out, session.connection(execution_options=...) is a
        # silent no-op (SAWarning: "Connection is already established").
        session.connection(
            execution_options={"schema_translate_map": {None: "tenant_spike"}},
        )
        found = session.execute(select(TItem).where(col(TItem.id) == item_id))
        assert found.scalar() is not None

    with sync_db.engine.begin() as conn:
        conn.execute(text("DROP SCHEMA tenant_spike CASCADE"))


def test_refresh_applies_dbschema_when_configured(sync_db: Database) -> None:
    """refresh() routes through force_session_schema() when dbschema is set.

    create() and refresh() must share one session for refresh() to see the
    instance as persistent — each auto-opened top-level call gets its own
    session, so this uses UnitOfWork to keep both in the same one.
    """
    with sync_db.engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_b"))
        conn.execute(text("DROP TABLE IF EXISTS tenant_b.titem"))
        conn.execute(
            text("CREATE TABLE tenant_b.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    with UnitOfWork(sync_db, dbschema="tenant_b") as uow:
        repo = TItemRepository(
            sync_db,
            session=uow.session,
            dbschema="tenant_b",
            auto_commit=False,
        )
        created = repo.create(TItemCreate(name="x"))
        repo.refresh(created)
        assert created.name == "x"
        uow.commit()

    with sync_db.engine.begin() as conn:
        conn.execute(text("DROP SCHEMA tenant_b CASCADE"))


class SWithFolderList(BaseWithUUID, table=True):
    """A parent with a list relationship, for selectinload() eager-loading tests."""

    name: str
    docs: list["SDocForFetch"] = Relationship(back_populates="parent")


class SDocForFetch(IsDeletedMixin, BaseWithUUID, table=True):
    """Child row used to exercise selectinload() on a list relationship."""

    __tablename__ = "sdocforfetch"

    name: str
    parent_id: UUID | None = Field(default=None, foreign_key="swithfolderlist.id")
    parent: SWithFolderList | None = Relationship(back_populates="docs")


class TParentCreate(SQLModel):
    """Create-payload for SWithFolderList."""

    name: str


class TChildCreate(SQLModel):
    """Create-payload for SDocForFetch."""

    name: str
    parent_id: UUID | None = None


class ParentRepository(EntityRepository[SWithFolderList, UUID]):
    """CRUD repository for SWithFolderList."""

    Schema = SWithFolderList


class ChildRepository(EntityRepository[SDocForFetch, UUID]):
    """CRUD repository for SDocForFetch."""

    Schema = SDocForFetch


def test_selectinload_none_relationship_stays_none(sync_db: Database) -> None:
    """selectinload() on a relationship that's None for a row simply leaves it None."""
    child = ChildRepository(sync_db).create(TChildCreate(name="orphan"))

    parent_attr = cast("QueryableAttribute[SWithFolderList | None]", SDocForFetch.parent)
    stmt = (
        select(SDocForFetch)
        .where(col(SDocForFetch.id) == child.id)
        .options(selectinload(parent_attr))
    )
    fetched = ChildRepository(sync_db).execute(stmt)

    assert fetched is not None
    assert fetched.parent is None


def test_selectinload_walks_list_relationship(sync_db: Database) -> None:
    """selectinload() eager-loads a list relationship for a single-row query."""
    parent = ParentRepository(sync_db).create(TParentCreate(name="p"))
    ChildRepository(sync_db).create(TChildCreate(name="c1", parent_id=parent.id))
    ChildRepository(sync_db).create(TChildCreate(name="c2", parent_id=parent.id))

    docs_attr = cast("QueryableAttribute[list[SDocForFetch]]", SWithFolderList.docs)
    stmt = (
        select(SWithFolderList)
        .where(col(SWithFolderList.id) == parent.id)
        .options(selectinload(docs_attr))
    )
    fetched = ParentRepository(sync_db).execute(stmt)

    assert fetched is not None
    assert {d.name for d in fetched.docs} == {"c1", "c2"}


def test_execute_many_unique_deduplicates_joinedload_rows(sync_db: Database) -> None:
    """execute_many(unique=True) deduplicates rows from a joinedload on a collection."""
    parent = ParentRepository(sync_db).create(TParentCreate(name="p"))
    ChildRepository(sync_db).create(TChildCreate(name="c1", parent_id=parent.id))
    ChildRepository(sync_db).create(TChildCreate(name="c2", parent_id=parent.id))

    repo = ParentRepository(sync_db)
    docs_attr = cast("QueryableAttribute[list[SDocForFetch]]", SWithFolderList.docs)
    stmt = select(SWithFolderList).options(joinedload(docs_attr))
    rows = repo.execute_many(stmt, unique=True)

    assert len(rows) == 1
    assert len(rows[0].docs) == 2


def test_unit_of_work_rollback_and_properties(sync_db: Database) -> None:
    """UnitOfWork exposes db/session and rollback() discards the transaction."""
    with UnitOfWork(sync_db) as uow:
        assert uow.db is sync_db
        repo = TItemRepository(sync_db, session=uow.session, auto_commit=False)
        repo.create(TItemCreate(name="to-be-rolled-back"))
        uow.rollback()

    verify_repo = TItemRepository(sync_db)
    assert verify_repo.count() == 0


def test_unit_of_work_session_raises_before_enter(sync_db: Database) -> None:
    """UnitOfWork.session raises SessionNotInitializedError before __enter__."""
    uow = UnitOfWork(sync_db)
    with pytest.raises(SessionNotInitializedError):
        _ = uow.session
