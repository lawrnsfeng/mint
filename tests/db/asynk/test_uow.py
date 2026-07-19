"""UnitOfWork tests (spec.md User Story 4)."""

from uuid import UUID, uuid4

import pytest
from sqlmodel import SQLModel

from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mixins import ResourceOwnerMixin
from mint.db.asynk.uow import UnitOfWork
from tests.db.schemas import TItem, TJob


class TItemCreate(SQLModel):
    """Create-payload for TItem."""

    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


class TJobCreate(SQLModel):
    """Create-payload for TJob."""

    name: str
    created_by_user_id: UUID


class TJobRepository(ResourceOwnerMixin[TJob], EntityRepository[TJob, UUID]):
    """CRUD repository for TJob."""

    Schema = TJob


@pytest.mark.asyncio
async def test_rollback_discards_every_write_in_the_block(async_db: Database) -> None:
    """No writes inside a UnitOfWork block persist unless commit() is called (FR-007).

    Repositories sharing a UnitOfWork session must be constructed with
    auto_commit=False — otherwise each create()/update()/remove() call
    commits itself immediately regardless of the shared session, defeating
    the point of the unit of work. This is required, documented usage, not
    a workaround.
    """
    async with UnitOfWork(async_db) as uow:
        items = TItemRepository(async_db, session=uow.session, auto_commit=False)
        jobs = TJobRepository(async_db, session=uow.session, auto_commit=False)
        item = await items.create(TItemCreate(name="x"))
        job = await jobs.create(TJobCreate(name="y", created_by_user_id=uuid4()))
        assert item.id is not None
        assert job.id is not None
        # deliberately exit without calling uow.commit()

    verify_items = TItemRepository(async_db)
    verify_jobs = TJobRepository(async_db)
    assert await verify_items.count() == 0
    assert await verify_jobs.count() == 0


@pytest.mark.asyncio
async def test_commit_persists_every_write_in_the_block(async_db: Database) -> None:
    """All writes inside a UnitOfWork block persist together after commit()."""
    async with UnitOfWork(async_db) as uow:
        items = TItemRepository(async_db, session=uow.session, auto_commit=False)
        jobs = TJobRepository(async_db, session=uow.session, auto_commit=False)
        await items.create(TItemCreate(name="x"))
        await jobs.create(TJobCreate(name="y", created_by_user_id=uuid4()))
        await uow.commit()

    verify_items = TItemRepository(async_db)
    verify_jobs = TJobRepository(async_db)
    assert await verify_items.count() == 1
    assert await verify_jobs.count() == 1


@pytest.mark.asyncio
async def test_second_repository_on_same_session_sees_uncommitted_write(
    async_db: Database,
) -> None:
    """A second repository sharing the UoW session reads the first repo's uncommitted write."""
    async with UnitOfWork(async_db) as uow:
        writer = TItemRepository(async_db, session=uow.session, auto_commit=False)
        reader = TItemRepository(async_db, session=uow.session, auto_commit=False)
        created = await writer.create(TItemCreate(name="x"))

        fetched = await reader.get(created.id)

        assert fetched is not None
        assert fetched.id == created.id


@pytest.mark.asyncio
async def test_zero_ceremony_path_still_needs_no_unit_of_work(async_db: Database) -> None:
    """A plain repository call outside any UnitOfWork still needs no setup."""
    repo = TItemRepository(async_db)

    created = await repo.create(TItemCreate(name="solo"))

    assert created.name == "solo"
