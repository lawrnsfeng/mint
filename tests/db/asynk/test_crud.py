"""Zero-boilerplate CRUD tests (spec.md User Story 1)."""

from uuid import UUID, uuid4

import pytest
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel

from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.exc import NotFoundError
from mint.db.models import BaseWithUUID
from tests.db.schemas import TItem


class TItemCreate(SQLModel):
    """Create-payload for TItem: no id, no server-generated fields."""

    name: str


class TItemUpdate(SQLModel):
    """Update-payload for TItem: all fields optional for partial updates."""

    name: str | None = None


class TItemUpsertCreate(SQLModel):
    """Create-payload for TItem that carries an explicit id, for upsert tests."""

    id: UUID
    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem with zero method overrides."""

    Schema = TItem


class TUniqueSlug(BaseWithUUID, table=True):
    """Table with a composite unique constraint on non-primary-key columns."""

    __table_args__ = (UniqueConstraint("tenant_id", "slug"),)

    tenant_id: UUID
    slug: str
    name: str


class TUniqueSlugCreate(SQLModel):
    """Create-payload for TUniqueSlug."""

    tenant_id: UUID
    slug: str
    name: str


class TUniqueSlugRepository(EntityRepository[TUniqueSlug, UUID]):
    """CRUD repository for TUniqueSlug."""

    Schema = TUniqueSlug


@pytest.mark.asyncio
async def test_db_property_returns_shared_database(async_db: Database) -> None:
    """RepositoryBase.db returns the Database instance it was constructed with."""
    repo = TItemRepository(async_db)

    assert repo.db is async_db


@pytest.mark.asyncio
async def test_zero_boilerplate_crud(async_db: Database) -> None:
    """A bare EntityRepository subclass delivers full CRUD (SC-002)."""
    repo = TItemRepository(async_db)

    created = await repo.create(TItemCreate(name="x"))
    assert created.name == "x"

    fetched = await repo.get(created.id)
    assert fetched is not None
    assert fetched.name == "x"

    updated = await repo.update(created.id, TItemUpdate(name="y"))
    assert updated.name == "y"

    removed = await repo.remove(created.id)
    assert removed.id == created.id

    assert await repo.get(created.id) is None


@pytest.mark.asyncio
async def test_update_partial_only_changes_supplied_fields(async_db: Database) -> None:
    """Update payload with unset fields leaves them untouched (FR-014)."""
    repo = TItemRepository(async_db)
    created = await repo.create(TItemCreate(name="original"))

    updated = await repo.update(created.id, TItemUpdate())

    assert updated.name == "original"


@pytest.mark.asyncio
async def test_update_missing_id_raises_not_found(async_db: Database) -> None:
    """update() raises NotFoundError for an id that doesn't exist."""
    repo = TItemRepository(async_db)

    with pytest.raises(NotFoundError):
        await repo.update(UUID(int=0), TItemUpdate(name="y"))


@pytest.mark.asyncio
async def test_remove_missing_id_raises_not_found(async_db: Database) -> None:
    """remove() raises NotFoundError for an id that doesn't exist."""
    repo = TItemRepository(async_db)

    with pytest.raises(NotFoundError):
        await repo.remove(UUID(int=0))


@pytest.mark.asyncio
async def test_get_many_by_ids(async_db: Database) -> None:
    """get_many_by_ids returns exactly the requested rows."""
    repo = TItemRepository(async_db)
    first = await repo.create(TItemCreate(name="a"))
    second = await repo.create(TItemCreate(name="b"))
    await repo.create(TItemCreate(name="c"))

    result = await repo.get_many_by_ids([first.id, second.id])

    assert {row.id for row in result} == {first.id, second.id}


@pytest.mark.asyncio
async def test_remove_many(async_db: Database) -> None:
    """remove_many deletes exactly the requested rows."""
    repo = TItemRepository(async_db)
    first = await repo.create(TItemCreate(name="a"))
    second = await repo.create(TItemCreate(name="b"))

    removed = await repo.remove_many([first.id, second.id])

    assert {row.id for row in removed} == {first.id, second.id}
    assert await repo.get(first.id) is None
    assert await repo.get(second.id) is None


@pytest.mark.asyncio
async def test_get_many_paginates(async_db: Database) -> None:
    """get_many respects skip/limit."""
    repo = TItemRepository(async_db)
    for i in range(5):
        await repo.create(TItemCreate(name=f"item-{i}"))

    page = await repo.get_many(skip=0, limit=2)

    assert len(page) == 2


@pytest.mark.asyncio
async def test_count(async_db: Database) -> None:
    """count() returns the total row count."""
    repo = TItemRepository(async_db)
    await repo.create(TItemCreate(name="a"))
    await repo.create(TItemCreate(name="b"))

    assert await repo.count() == 2


@pytest.mark.asyncio
async def test_get_many_page_returns_page_and_total_in_one_query(async_db: Database) -> None:
    """get_many_page() returns both the requested page and the total row count."""
    repo = TItemRepository(async_db)
    for i in range(5):
        await repo.create(TItemCreate(name=f"item-{i}"))

    result = await repo.get_many_page(skip=0, limit=2)

    assert len(result.items) == 2
    assert result.total == 5


@pytest.mark.asyncio
async def test_get_many_page_past_last_page_falls_back_to_count(async_db: Database) -> None:
    """get_many_page() past the last row returns an empty page with the correct total."""
    repo = TItemRepository(async_db)
    for i in range(3):
        await repo.create(TItemCreate(name=f"item-{i}"))

    result = await repo.get_many_page(skip=10, limit=2)

    assert result.items == []
    assert result.total == 3


@pytest.mark.asyncio
async def test_create_upsert_inserts_when_no_conflict(async_db: Database) -> None:
    """create(upsert=True) behaves like a normal insert when there's no conflict."""
    repo = TItemRepository(async_db)
    item_id = uuid4()

    created = await repo.create(TItemUpsertCreate(id=item_id, name="x"), upsert=True)

    assert created.id == item_id
    assert created.name == "x"
    assert await repo.count() == 1


@pytest.mark.asyncio
async def test_create_upsert_updates_existing_row_on_conflict(async_db: Database) -> None:
    """create(upsert=True) merges new values into the existing row on a PK conflict."""
    repo = TItemRepository(async_db)
    item_id = uuid4()
    await repo.create(TItemUpsertCreate(id=item_id, name="first"), upsert=True)

    updated = await repo.create(TItemUpsertCreate(id=item_id, name="second"), upsert=True)

    assert updated.id == item_id
    assert updated.name == "second"
    assert await repo.count() == 1


@pytest.mark.asyncio
async def test_create_upsert_with_explicit_conflict_columns(async_db: Database) -> None:
    """create(upsert=True, conflict_columns=[...]) upserts against a non-PK unique constraint."""
    repo = TUniqueSlugRepository(async_db)
    tenant_id = uuid4()

    first = await repo.create(
        TUniqueSlugCreate(tenant_id=tenant_id, slug="hello", name="v1"),
        upsert=True,
        conflict_columns=["tenant_id", "slug"],
    )
    second = await repo.create(
        TUniqueSlugCreate(tenant_id=tenant_id, slug="hello", name="v2"),
        upsert=True,
        conflict_columns=["tenant_id", "slug"],
    )

    assert second.id == first.id
    assert second.name == "v2"
    assert await repo.count() == 1


@pytest.mark.asyncio
async def test_create_without_upsert_raises_integrity_error_on_conflict(
    async_db: Database,
) -> None:
    """create() without upsert=True still fails at the database level on a real conflict.

    Confirms the distinction from OperationalError: a unique-constraint
    violation is a database-level IntegrityError, not the "RETURNING
    returned nothing" anomaly OperationalError represents.
    """
    repo = TItemRepository(async_db)
    item_id = uuid4()
    await repo.create(TItemUpsertCreate(id=item_id, name="first"))

    with pytest.raises(IntegrityError):
        await repo.create(TItemUpsertCreate(id=item_id, name="second"))
