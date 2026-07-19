"""Scoping tests (spec.md User Story 3 / SC-003).

Verifies soft-delete and owner scoping apply unconditionally — including to
hand-written custom queries with no scoping code in them, closing the exact
gap the predecessor implementation's opt-in ``restrain()`` had (found via
real production code review, see research.md, "Scoping").
"""

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from sqlmodel import SQLModel, col, select

from mint.db.asynk.base import RepositoryBase
from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from mint.db.asynk.mixins import ResourceOwnerMixin, SoftDeleteMixin
from mint.db.models import IsDeletedMixin, OwnerMixin
from tests.db.schemas import TDoc, TJob


class TDocCreate(SQLModel):
    """Create-payload for TDoc."""

    name: str


class TDocRepository(SoftDeleteMixin[TDoc, UUID]):
    """Soft-deletable repository with one hand-written custom query."""

    Schema = TDoc

    @RepositoryBase.ensure_session
    async def raw_list_all(self) -> Sequence[TDoc]:
        """List rows via a raw session.execute() with no scoping call."""
        result = await self.session.execute(select(TDoc))
        return result.scalars().all()


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
    """Owner-scoped repository with one hand-written custom query."""

    Schema = TJob

    @RepositoryBase.ensure_session
    async def raw_list_all(self) -> Sequence[TJob]:
        """List rows via a raw session.execute() with no scoping call."""
        result = await self.session.execute(select(TJob))
        return result.scalars().all()


@pytest.mark.asyncio
async def test_custom_query_excludes_soft_deleted_row(async_db: Database) -> None:
    """A hand-written query with zero scoping code still excludes soft-deleted rows."""
    repo = TDocRepository(async_db)
    kept = await repo.create(TDocCreate(name="kept"))
    deleted = await repo.create(TDocCreate(name="deleted"))
    await repo.remove(deleted.id)

    rows = await repo.raw_list_all()

    ids = {row.id for row in rows}
    assert kept.id in ids
    assert deleted.id not in ids


@pytest.mark.asyncio
async def test_get_many_excludes_soft_deleted_row(async_db: Database) -> None:
    """The built-in get_many() also excludes soft-deleted rows."""
    repo = TDocRepository(async_db)
    kept = await repo.create(TDocCreate(name="kept"))
    deleted = await repo.create(TDocCreate(name="deleted"))
    await repo.remove(deleted.id)

    rows = await repo.get_many()

    ids = {row.id for row in rows}
    assert kept.id in ids
    assert deleted.id not in ids


@pytest.mark.asyncio
async def test_soft_delete_remove_keeps_row_marked_deleted(async_db: Database) -> None:
    """SoftDeleteMixin.remove() updates is_deleted rather than hard-deleting."""
    repo = TDocRepository(async_db)
    doc = await repo.create(TDocCreate(name="x"))

    removed = await repo.remove(doc.id)
    assert removed.is_deleted is True

    stmt = (
        select(TDoc)
        .where(col(TDoc.id) == doc.id)
        .execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))
    )
    still_present = await repo.execute(stmt)
    assert still_present is not None
    assert still_present.is_deleted is True


@pytest.mark.asyncio
async def test_include_deleted_bypass_surfaces_soft_deleted_row(async_db: Database) -> None:
    """mint_scope_bypass={IsDeletedMixin} surfaces a soft-deleted row on demand."""
    repo = TDocRepository(async_db)
    doc = await repo.create(TDocCreate(name="x"))
    await repo.remove(doc.id)

    default_result = await repo.get(doc.id)
    assert default_result is None

    stmt = (
        select(TDoc)
        .where(col(TDoc.id) == doc.id)
        .execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))
    )
    bypassed = await repo.execute(stmt)
    assert bypassed is not None
    assert bypassed.id == doc.id


@pytest.mark.asyncio
async def test_owner_scoping_excludes_other_owners_row(async_db: Database) -> None:
    """An owner-scoped repository's queries exclude another owner's rows."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    repo_a = TJobRepository(async_db, owner=owner_a)
    repo_b = TJobRepository(async_db, owner=owner_b)

    job_a = await repo_a.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    job_b = await repo_b.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    visible_to_a = await repo_a.get_many()
    ids_a = {job.id for job in visible_to_a}
    assert job_a.id in ids_a
    assert job_b.id not in ids_a


@pytest.mark.asyncio
async def test_owner_scoping_applies_to_custom_query(async_db: Database) -> None:
    """A hand-written query on an owner-scoped repository is still scoped."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    repo_a = TJobRepository(async_db, owner=owner_a)
    repo_b = TJobRepository(async_db, owner=owner_b)

    job_a = await repo_a.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    job_b = await repo_b.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    rows = await repo_a.raw_list_all()

    ids = {row.id for row in rows}
    assert job_a.id in ids
    assert job_b.id not in ids


@pytest.mark.asyncio
async def test_skip_owner_scope_bypass_surfaces_other_owners_row(async_db: Database) -> None:
    """mint_scope_bypass={OwnerMixin} surfaces another owner's row on demand."""
    owner_a = TOwner(id=uuid4())
    owner_b = TOwner(id=uuid4())
    repo_a = TJobRepository(async_db, owner=owner_a)
    repo_b = TJobRepository(async_db, owner=owner_b)
    job_b = await repo_b.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    stmt = (
        select(TJob)
        .where(col(TJob.id) == job_b.id)
        .execution_options(mint_scope_bypass=frozenset({OwnerMixin}))
    )
    bypassed = await repo_a.execute(stmt)

    assert bypassed is not None
    assert bypassed.id == job_b.id


@pytest.mark.asyncio
async def test_unscoped_owner_sees_all_rows(async_db: Database) -> None:
    """An owner whose is_scoped is False sees every row, not just their own."""
    owner_a = TOwner(id=uuid4(), is_scoped=False)
    owner_b = TOwner(id=uuid4())
    repo_a = TJobRepository(async_db, owner=owner_a)
    repo_b = TJobRepository(async_db, owner=owner_b)

    job_a = await repo_a.create(TJobCreate(name="a-job", created_by_user_id=owner_a.id))
    job_b = await repo_b.create(TJobCreate(name="b-job", created_by_user_id=owner_b.id))

    visible_to_a = await repo_a.get_many()

    ids_a = {job.id for job in visible_to_a}
    assert job_a.id in ids_a
    assert job_b.id in ids_a
