"""Coverage-closing tests for edge cases and error paths across mint.db.asynk."""

from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy import insert, text
from sqlalchemy.orm import QueryableAttribute, joinedload, selectinload
from sqlmodel import Field, Relationship, SQLModel, col, select

from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mixins import SoftDeleteMixin
from mint.db.asynk.mv import MaterializedViewRepository
from mint.db.asynk.uow import UnitOfWork
from mint.db.exc import (
    ConfigError,
    DBSchemaNotSetError,
    NotFoundError,
    OperationalError,
    SessionNotInitializedError,
)
from mint.db.models import Base, BaseWithUUID, IsDeletedMixin
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


@pytest.mark.asyncio
async def test_session_raises_when_unbound(async_db: Database) -> None:
    """RepositoryBase.session raises SessionNotInitializedError with no session bound."""
    repo = TItemRepository(async_db)
    with pytest.raises(SessionNotInitializedError):
        _ = repo.session


@pytest.mark.asyncio
async def test_force_session_schema_raises_when_dbschema_unset(async_db: Database) -> None:
    """force_session_schema() raises DBSchemaNotSetError when dbschema was never configured."""
    repo = TItemRepository(async_db)
    with pytest.raises(DBSchemaNotSetError):
        await repo.force_session_schema()


@pytest.mark.asyncio
async def test_update_with_empty_payload_and_missing_id_raises_not_found(
    async_db: Database,
) -> None:
    """update() with no fields set still raises NotFoundError for a missing id."""
    repo = TItemRepository(async_db)
    with pytest.raises(NotFoundError):
        await repo.update(UUID(int=0), TItemUpdate())


@pytest.mark.asyncio
async def test_count_raises_operational_error_when_scalar_is_none(
    async_db: Database,
    mocker: "MockerFixture",
) -> None:
    """count() raises OperationalError if the count query returns no value.

    Not reachable via normal Postgres usage (COUNT always returns a row) —
    mocked to exercise this defensive branch.
    """
    repo = TItemRepository(async_db)
    mock_result = mocker.Mock()
    mock_result.scalar.return_value = None
    session_mock = mocker.AsyncMock()
    session_mock.execute = mocker.AsyncMock(return_value=mock_result)
    # ensure_session still opens (and cleans up) a real session for the
    # wrapper's own bookkeeping; only the count() body's self.session access
    # is intercepted, via a class-level property override.
    mocker.patch.object(TItemRepository, "session", new=property(lambda _self: session_mock))

    with pytest.raises(OperationalError):
        await repo.count()


@pytest.mark.asyncio
async def test_create_raises_operational_error_when_no_row_returned(
    async_db: Database,
    mocker: "MockerFixture",
) -> None:
    """create() raises OperationalError if the insert returns no row.

    Not reachable via normal Postgres usage — mocked to exercise this
    defensive branch. Mirrors the real production case that motivated this
    exception choice: an upsert-style statement (``ON CONFLICT DO NOTHING``)
    silently returning nothing because the row already existed — an
    infrastructure-level anomaly, not a domain "not found".
    """
    repo = TItemRepository(async_db)
    mock_result = mocker.Mock()
    mock_result.scalar.return_value = None
    session_mock = mocker.AsyncMock()
    session_mock.execute = mocker.AsyncMock(return_value=mock_result)
    session_mock.commit = mocker.AsyncMock()
    mocker.patch.object(TItemRepository, "session", new=property(lambda _self: session_mock))

    with pytest.raises(OperationalError):
        await repo.create(TItemCreate(name="x"))


@pytest.mark.asyncio
async def test_soft_delete_remove_many(async_db: Database) -> None:
    """SoftDeleteMixin.remove_many() soft-deletes every matching row."""
    repo = TDocRepository(async_db)
    first = await repo.create(TDocCreate(name="a"))
    second = await repo.create(TDocCreate(name="b"))

    updated = await repo.remove_many([first.id, second.id])

    assert {row.id for row in updated} == {first.id, second.id}
    assert all(row.is_deleted for row in updated)
    assert await repo.get(first.id) is None
    assert await repo.get(second.id) is None


@pytest.mark.asyncio
async def test_soft_delete_remove_missing_id_raises_not_found(async_db: Database) -> None:
    """SoftDeleteMixin.remove() raises NotFoundError for an id that doesn't exist."""
    repo = TDocRepository(async_db)
    with pytest.raises(NotFoundError):
        await repo.remove(UUID(int=0))


@pytest.mark.asyncio
async def test_execute_many_selectinload_populates_every_object(async_db: Database) -> None:
    """A selectinload() option on a custom statement populates every returned row."""
    parent = await ParentRepository(async_db).create(TParentCreate(name="p"))
    await ChildRepository(async_db).create(TChildCreate(name="c1", parent_id=parent.id))
    await ChildRepository(async_db).create(TChildCreate(name="c2", parent_id=parent.id))

    repo = ChildRepository(async_db)
    parent_attr = cast("QueryableAttribute[TWithFolderList | None]", TDocForFetch.parent)
    stmt = select(TDocForFetch).options(selectinload(parent_attr))
    children = await repo.execute_many(stmt)

    assert len(children) == 2
    for child in children:
        assert child.parent is not None
        assert child.parent.name == "p"


@pytest.mark.asyncio
async def test_database_from_uri_creates_own_engine(postgres_async_url: str) -> None:
    """Database(uri=...) creates its own engine rather than requiring one be passed in."""
    db = Database(postgres_async_url)
    assert db.engine is not None
    await db.engine.dispose()


class TMVWithSchema(Base, table=True):
    """Materialized-view-shaped schema used only to exercise table_name."""

    __tablename__ = "tmvwithschema"

    id: UUID = Field(primary_key=True)
    name: str


class MVWithSchemaRepository(MaterializedViewRepository[TMVWithSchema]):
    """Repository used only to exercise table_name with dbschema set."""

    Schema = TMVWithSchema


def test_materialized_view_table_name_with_dbschema(async_db: Database) -> None:
    """table_name is dbschema-qualified when dbschema is configured."""
    repo = MVWithSchemaRepository(async_db, dbschema="tenant_x")
    assert repo.table_name == "tenant_x.tmvwithschema"

    unscoped_repo = MVWithSchemaRepository(async_db)
    assert unscoped_repo.table_name == "tmvwithschema"


@pytest.mark.asyncio
async def test_dbschema_routes_queries_to_the_configured_schema(async_db: Database) -> None:
    """dbschema= on a repository routes unqualified table access to that Postgres schema."""
    async with async_db.engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_a"))
        await conn.execute(text("DROP TABLE IF EXISTS tenant_a.titem"))
        await conn.execute(
            text("CREATE TABLE tenant_a.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    repo = TItemRepository(async_db, dbschema="tenant_a")
    created = await repo.create(TItemCreate(name="tenant-scoped"))

    async with async_db.engine.begin() as conn:
        result = await conn.execute(
            text("SELECT name FROM tenant_a.titem WHERE id = :id"),
            {"id": str(created.id)},
        )
        row = result.first()
    assert row is not None
    assert row[0] == "tenant-scoped"

    default_repo = TItemRepository(async_db)
    assert await default_repo.get(created.id) is None

    async with async_db.engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA tenant_a CASCADE"))


@pytest.mark.asyncio
async def test_schema_translate_map_lost_after_commit_without_reforce(
    async_db: Database,
) -> None:
    """A mid-session commit drops ``schema_translate_map`` unless reapplied.

    Spike confirming the exact premise behind ``force_session_schema()``/
    ``refresh()``'s reapply-on-refresh logic: SQLAlchemy's ``Session``
    checks out a fresh ``Connection`` the next time it performs work after
    a commit ends the prior transaction, and per-connection execution
    options (including ``schema_translate_map``) do not carry over to that
    new connection. Without reapplying, a post-commit query on an
    unqualified table name silently resolves against the default
    (``public``) schema instead of erroring — verified here against real
    asyncpg, not assumed (see specs/002-db-repository-layer/research.md,
    "Schema-translate persistence across commits"). This is why
    ``force_session_schema()``/``refresh()`` keep their reapply logic.
    """
    async with async_db.engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_spike"))
        await conn.execute(text("DROP TABLE IF EXISTS tenant_spike.titem"))
        await conn.execute(
            text("CREATE TABLE tenant_spike.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    item_id = uuid4()
    async with async_db.create_session(dbschema="tenant_spike") as session:
        await session.execute(insert(TItem).values(id=item_id, name="first"))
        await session.commit()

        # No reforce: the unqualified query resolves against public.titem
        # (empty), not tenant_spike.titem — silently wrong, not an error.
        lost = await session.execute(select(TItem).where(col(TItem.id) == item_id))
        assert lost.scalar() is None
        await session.commit()

        # Reapplying schema_translate_map — what force_session_schema()
        # does — restores correct routing to tenant_spike. Must happen
        # before any statement on the new transaction: once a Connection is
        # checked out, session.connection(execution_options=...) is a
        # silent no-op (SAWarning: "Connection is already established").
        await session.connection(
            execution_options={"schema_translate_map": {None: "tenant_spike"}},
        )
        found = await session.execute(select(TItem).where(col(TItem.id) == item_id))
        assert found.scalar() is not None

    async with async_db.engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA tenant_spike CASCADE"))


@pytest.mark.asyncio
async def test_refresh_applies_dbschema_when_configured(async_db: Database) -> None:
    """refresh() routes through force_session_schema() when dbschema is set.

    create() and refresh() must share one session for refresh() to see the
    instance as persistent — each auto-opened top-level call gets its own
    session, so this uses UnitOfWork to keep both in the same one.
    """
    async with async_db.engine.begin() as conn:
        await conn.execute(text("CREATE SCHEMA IF NOT EXISTS tenant_b"))
        await conn.execute(text("DROP TABLE IF EXISTS tenant_b.titem"))
        await conn.execute(
            text("CREATE TABLE tenant_b.titem (id UUID PRIMARY KEY, name VARCHAR NOT NULL)"),
        )

    async with UnitOfWork(async_db, dbschema="tenant_b") as uow:
        repo = TItemRepository(
            async_db,
            session=uow.session,
            dbschema="tenant_b",
            auto_commit=False,
        )
        created = await repo.create(TItemCreate(name="x"))
        await repo.refresh(created)
        assert created.name == "x"
        await uow.commit()

    async with async_db.engine.begin() as conn:
        await conn.execute(text("DROP SCHEMA tenant_b CASCADE"))


class TWithFolderList(BaseWithUUID, table=True):
    """A parent with a list relationship, for selectinload() eager-loading tests."""

    name: str
    docs: list["TDocForFetch"] = Relationship(back_populates="parent")


class TDocForFetch(IsDeletedMixin, BaseWithUUID, table=True):
    """Child row used to exercise selectinload() on a list relationship."""

    __tablename__ = "tdocforfetch"

    name: str
    parent_id: UUID | None = Field(default=None, foreign_key="twithfolderlist.id")
    parent: TWithFolderList | None = Relationship(back_populates="docs")


class TParentCreate(SQLModel):
    """Create-payload for TWithFolderList."""

    name: str


class TChildCreate(SQLModel):
    """Create-payload for TDocForFetch."""

    name: str
    parent_id: UUID | None = None


class ParentRepository(EntityRepository[TWithFolderList, UUID]):
    """CRUD repository for TWithFolderList."""

    Schema = TWithFolderList


class ChildRepository(EntityRepository[TDocForFetch, UUID]):
    """CRUD repository for TDocForFetch."""

    Schema = TDocForFetch


@pytest.mark.asyncio
async def test_selectinload_none_relationship_stays_none(async_db: Database) -> None:
    """selectinload() on a relationship that's None for a row simply leaves it None."""
    child = await ChildRepository(async_db).create(TChildCreate(name="orphan"))

    parent_attr = cast("QueryableAttribute[TWithFolderList | None]", TDocForFetch.parent)
    stmt = (
        select(TDocForFetch)
        .where(col(TDocForFetch.id) == child.id)
        .options(selectinload(parent_attr))
    )
    fetched = await ChildRepository(async_db).execute(stmt)

    assert fetched is not None
    assert fetched.parent is None


@pytest.mark.asyncio
async def test_selectinload_walks_list_relationship(async_db: Database) -> None:
    """selectinload() eager-loads a list relationship for a single-row query."""
    parent = await ParentRepository(async_db).create(TParentCreate(name="p"))
    await ChildRepository(async_db).create(TChildCreate(name="c1", parent_id=parent.id))
    await ChildRepository(async_db).create(TChildCreate(name="c2", parent_id=parent.id))

    docs_attr = cast("QueryableAttribute[list[TDocForFetch]]", TWithFolderList.docs)
    stmt = (
        select(TWithFolderList)
        .where(col(TWithFolderList.id) == parent.id)
        .options(selectinload(docs_attr))
    )
    fetched = await ParentRepository(async_db).execute(stmt)

    assert fetched is not None
    assert {d.name for d in fetched.docs} == {"c1", "c2"}


@pytest.mark.asyncio
async def test_execute_many_unique_deduplicates_joinedload_rows(async_db: Database) -> None:
    """execute_many(unique=True) deduplicates rows from a joinedload on a collection."""
    parent = await ParentRepository(async_db).create(TParentCreate(name="p"))
    await ChildRepository(async_db).create(TChildCreate(name="c1", parent_id=parent.id))
    await ChildRepository(async_db).create(TChildCreate(name="c2", parent_id=parent.id))

    repo = ParentRepository(async_db)
    docs_attr = cast("QueryableAttribute[list[TDocForFetch]]", TWithFolderList.docs)
    stmt = select(TWithFolderList).options(joinedload(docs_attr))
    rows = await repo.execute_many(stmt, unique=True)

    assert len(rows) == 1
    assert len(rows[0].docs) == 2


@pytest.mark.asyncio
async def test_unit_of_work_rollback_and_properties(async_db: Database) -> None:
    """UnitOfWork exposes db/session and rollback() discards the transaction."""
    async with UnitOfWork(async_db) as uow:
        assert uow.db is async_db
        repo = TItemRepository(async_db, session=uow.session, auto_commit=False)
        await repo.create(TItemCreate(name="to-be-rolled-back"))
        await uow.rollback()

    verify_repo = TItemRepository(async_db)
    assert await verify_repo.count() == 0


@pytest.mark.asyncio
async def test_unit_of_work_session_raises_before_enter(async_db: Database) -> None:
    """UnitOfWork.session raises SessionNotInitializedError before __aenter__."""
    uow = UnitOfWork(async_db)
    with pytest.raises(SessionNotInitializedError):
        _ = uow.session
