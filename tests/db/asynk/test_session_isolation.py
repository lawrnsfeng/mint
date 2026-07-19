"""Concurrency regression tests (spec.md User Story 2 / SC-001).

This is the direct regression test for the bug this whole port exists to
fix: the predecessor implementation stored its active session on a plain
instance attribute, which a second concurrent task sharing the same
repository instance could silently overwrite.
"""

import asyncio
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlmodel import SQLModel

from mint.db.asynk.base import RepositoryBase
from mint.db.asynk.database import Database
from mint.db.asynk.entity import EntityRepository
from tests.db.schemas import TItem


class TItemCreate(SQLModel):
    """Create-payload for TItem."""

    name: str


class TItemRepository(EntityRepository[TItem, UUID]):
    """CRUD repository for TItem."""

    Schema = TItem


class BoomError(Exception):
    """Deliberate test-only failure, raised mid-operation."""


class FailingRepository(TItemRepository):
    """Repository whose extra method fails after touching the session."""

    @RepositoryBase.ensure_session
    async def boom(self) -> None:
        """Touch the session, then raise, to simulate a mid-operation failure."""
        await self.session.execute(select(TItem))
        raise BoomError


@pytest.mark.asyncio
async def test_concurrent_operations_share_repository_without_cross_contamination(
    async_db: Database,
) -> None:
    """Many concurrent create+get pairs on one shared repository never cross (SC-001)."""
    repo = TItemRepository(async_db)

    async def create_and_verify(name: str) -> None:
        created = await repo.create(TItemCreate(name=name))
        fetched = await repo.get(created.id)
        assert fetched is not None
        assert fetched.name == name
        assert fetched.id == created.id

    names = [f"task-{i}" for i in range(50)]
    await asyncio.gather(*(create_and_verify(name) for name in names))

    assert await repo.count() == 50


@pytest.mark.asyncio
async def test_exception_mid_operation_leaves_next_call_clean(async_db: Database) -> None:
    """A failed operation does not leave the next call holding a stale session (FR-002)."""
    repo = FailingRepository(async_db)

    with pytest.raises(BoomError):
        await repo.boom()

    result = await repo.get_many()
    assert list(result) == []

    created = await repo.create(TItemCreate(name="after-failure"))
    assert created.name == "after-failure"
