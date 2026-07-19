"""UnitOfWork tests (spec.md User Story 4), sync mirror."""

from uuid import UUID, uuid4

from sqlmodel import SQLModel

from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
from mint.db.sync.mixins import ResourceOwnerMixin
from mint.db.sync.uow import UnitOfWork
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


def test_rollback_discards_every_write_in_the_block(sync_db: Database) -> None:
    """No writes inside a UnitOfWork block persist unless commit() is called (FR-007).

    Repositories sharing a UnitOfWork session must be constructed with
    auto_commit=False — otherwise each create()/update()/remove() call
    commits itself immediately regardless of the shared session, defeating
    the point of the unit of work. This is required, documented usage, not
    a workaround.
    """
    with UnitOfWork(sync_db) as uow:
        items = TItemRepository(sync_db, session=uow.session, auto_commit=False)
        jobs = TJobRepository(sync_db, session=uow.session, auto_commit=False)
        item = items.create(TItemCreate(name="x"))
        job = jobs.create(TJobCreate(name="y", created_by_user_id=uuid4()))
        assert item.id is not None
        assert job.id is not None
        # deliberately exit without calling uow.commit()

    verify_items = TItemRepository(sync_db)
    verify_jobs = TJobRepository(sync_db)
    assert verify_items.count() == 0
    assert verify_jobs.count() == 0


def test_commit_persists_every_write_in_the_block(sync_db: Database) -> None:
    """All writes inside a UnitOfWork block persist together after commit()."""
    with UnitOfWork(sync_db) as uow:
        items = TItemRepository(sync_db, session=uow.session, auto_commit=False)
        jobs = TJobRepository(sync_db, session=uow.session, auto_commit=False)
        items.create(TItemCreate(name="x"))
        jobs.create(TJobCreate(name="y", created_by_user_id=uuid4()))
        uow.commit()

    verify_items = TItemRepository(sync_db)
    verify_jobs = TJobRepository(sync_db)
    assert verify_items.count() == 1
    assert verify_jobs.count() == 1


def test_second_repository_on_same_session_sees_uncommitted_write(sync_db: Database) -> None:
    """A second repository sharing the UoW session reads the first repo's uncommitted write."""
    with UnitOfWork(sync_db) as uow:
        writer = TItemRepository(sync_db, session=uow.session, auto_commit=False)
        reader = TItemRepository(sync_db, session=uow.session, auto_commit=False)
        created = writer.create(TItemCreate(name="x"))

        fetched = reader.get(created.id)

        assert fetched is not None
        assert fetched.id == created.id


def test_zero_ceremony_path_still_needs_no_unit_of_work(sync_db: Database) -> None:
    """A plain repository call outside any UnitOfWork still needs no setup."""
    repo = TItemRepository(sync_db)

    created = repo.create(TItemCreate(name="solo"))

    assert created.name == "solo"
