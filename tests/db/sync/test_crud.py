"""Zero-boilerplate CRUD tests (spec.md User Story 1), sync mirror."""

from uuid import UUID, uuid4

import pytest
from sqlalchemy import UniqueConstraint
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel

from mint.db.exc import NotFoundError
from mint.db.models import BaseWithUUID
from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
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


class SUniqueSlug(BaseWithUUID, table=True):
    """Table with a composite unique constraint on non-primary-key columns."""

    __table_args__ = (UniqueConstraint("tenant_id", "slug"),)

    tenant_id: UUID
    slug: str
    name: str


class SUniqueSlugCreate(SQLModel):
    """Create-payload for SUniqueSlug."""

    tenant_id: UUID
    slug: str
    name: str


class SUniqueSlugRepository(EntityRepository[SUniqueSlug, UUID]):
    """CRUD repository for SUniqueSlug."""

    Schema = SUniqueSlug


def test_db_property_returns_shared_database(sync_db: Database) -> None:
    """RepositoryBase.db returns the Database instance it was constructed with."""
    repo = TItemRepository(sync_db)

    assert repo.db is sync_db


def test_zero_boilerplate_crud(sync_db: Database) -> None:
    """A bare EntityRepository subclass delivers full CRUD (SC-002)."""
    repo = TItemRepository(sync_db)

    created = repo.create(TItemCreate(name="x"))
    assert created.name == "x"

    fetched = repo.get(created.id)
    assert fetched is not None
    assert fetched.name == "x"

    updated = repo.update(created.id, TItemUpdate(name="y"))
    assert updated.name == "y"

    removed = repo.remove(created.id)
    assert removed.id == created.id

    assert repo.get(created.id) is None


def test_update_partial_only_changes_supplied_fields(sync_db: Database) -> None:
    """Update payload with unset fields leaves them untouched (FR-014)."""
    repo = TItemRepository(sync_db)
    created = repo.create(TItemCreate(name="original"))

    updated = repo.update(created.id, TItemUpdate())

    assert updated.name == "original"


def test_update_missing_id_raises_not_found(sync_db: Database) -> None:
    """update() raises NotFoundError for an id that doesn't exist."""
    repo = TItemRepository(sync_db)

    with pytest.raises(NotFoundError):
        repo.update(UUID(int=0), TItemUpdate(name="y"))


def test_remove_missing_id_raises_not_found(sync_db: Database) -> None:
    """remove() raises NotFoundError for an id that doesn't exist."""
    repo = TItemRepository(sync_db)

    with pytest.raises(NotFoundError):
        repo.remove(UUID(int=0))


def test_get_many_by_ids(sync_db: Database) -> None:
    """get_many_by_ids returns exactly the requested rows."""
    repo = TItemRepository(sync_db)
    first = repo.create(TItemCreate(name="a"))
    second = repo.create(TItemCreate(name="b"))
    repo.create(TItemCreate(name="c"))

    result = repo.get_many_by_ids([first.id, second.id])

    assert {row.id for row in result} == {first.id, second.id}


def test_remove_many(sync_db: Database) -> None:
    """remove_many deletes exactly the requested rows."""
    repo = TItemRepository(sync_db)
    first = repo.create(TItemCreate(name="a"))
    second = repo.create(TItemCreate(name="b"))

    removed = repo.remove_many([first.id, second.id])

    assert {row.id for row in removed} == {first.id, second.id}
    assert repo.get(first.id) is None
    assert repo.get(second.id) is None


def test_get_many_paginates(sync_db: Database) -> None:
    """get_many respects skip/limit."""
    repo = TItemRepository(sync_db)
    for i in range(5):
        repo.create(TItemCreate(name=f"item-{i}"))

    page = repo.get_many(skip=0, limit=2)

    assert len(page) == 2


def test_count(sync_db: Database) -> None:
    """count() returns the total row count."""
    repo = TItemRepository(sync_db)
    repo.create(TItemCreate(name="a"))
    repo.create(TItemCreate(name="b"))

    assert repo.count() == 2


def test_get_many_page_returns_page_and_total_in_one_query(sync_db: Database) -> None:
    """get_many_page() returns both the requested page and the total row count."""
    repo = TItemRepository(sync_db)
    for i in range(5):
        repo.create(TItemCreate(name=f"item-{i}"))

    result = repo.get_many_page(skip=0, limit=2)

    assert len(result.items) == 2
    assert result.total == 5


def test_get_many_page_past_last_page_falls_back_to_count(sync_db: Database) -> None:
    """get_many_page() past the last row returns an empty page with the correct total."""
    repo = TItemRepository(sync_db)
    for i in range(3):
        repo.create(TItemCreate(name=f"item-{i}"))

    result = repo.get_many_page(skip=10, limit=2)

    assert result.items == []
    assert result.total == 3


def test_create_upsert_inserts_when_no_conflict(sync_db: Database) -> None:
    """create(upsert=True) behaves like a normal insert when there's no conflict."""
    repo = TItemRepository(sync_db)
    item_id = uuid4()

    created = repo.create(TItemUpsertCreate(id=item_id, name="x"), upsert=True)

    assert created.id == item_id
    assert created.name == "x"
    assert repo.count() == 1


def test_create_upsert_updates_existing_row_on_conflict(sync_db: Database) -> None:
    """create(upsert=True) merges new values into the existing row on a PK conflict."""
    repo = TItemRepository(sync_db)
    item_id = uuid4()
    repo.create(TItemUpsertCreate(id=item_id, name="first"), upsert=True)

    updated = repo.create(TItemUpsertCreate(id=item_id, name="second"), upsert=True)

    assert updated.id == item_id
    assert updated.name == "second"
    assert repo.count() == 1


def test_create_upsert_with_explicit_conflict_columns(sync_db: Database) -> None:
    """create(upsert=True, conflict_columns=[...]) upserts against a non-PK unique constraint."""
    repo = SUniqueSlugRepository(sync_db)
    tenant_id = uuid4()

    first = repo.create(
        SUniqueSlugCreate(tenant_id=tenant_id, slug="hello", name="v1"),
        upsert=True,
        conflict_columns=["tenant_id", "slug"],
    )
    second = repo.create(
        SUniqueSlugCreate(tenant_id=tenant_id, slug="hello", name="v2"),
        upsert=True,
        conflict_columns=["tenant_id", "slug"],
    )

    assert second.id == first.id
    assert second.name == "v2"
    assert repo.count() == 1


def test_create_without_upsert_raises_integrity_error_on_conflict(sync_db: Database) -> None:
    """create() without upsert=True still fails at the database level on a real conflict.

    Confirms the distinction from OperationalError: a unique-constraint
    violation is a database-level IntegrityError, not the "RETURNING
    returned nothing" anomaly OperationalError represents.
    """
    repo = TItemRepository(sync_db)
    item_id = uuid4()
    repo.create(TItemUpsertCreate(id=item_id, name="first"))

    with pytest.raises(IntegrityError):
        repo.create(TItemUpsertCreate(id=item_id, name="second"))
