"""Concurrency regression tests, sync/thread mirror (spec.md User Story 2).

Confirms the ``ContextVar`` isolates sessions by thread as well as by
asyncio task — this is also the regression test for the predecessor
implementation's separate sync-side bug, where a
``scoped_session(scopefunc=None)`` degraded to thread-id scoping and could
leak a session across pooled threads.
"""

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlmodel import SQLModel

from mint.db.exc import NotFoundError
from mint.db.sync.base import RepositoryBase
from mint.db.sync.database import Database
from mint.db.sync.entity import EntityRepository
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
    def boom(self) -> None:
        """Touch the session, then raise, to simulate a mid-operation failure."""
        self.session.execute(select(TItem))
        raise BoomError


def test_concurrent_operations_share_repository_without_cross_contamination(
    sync_db: Database,
) -> None:
    """Many concurrent create+get pairs, across threads, never cross."""
    repo = TItemRepository(sync_db)

    def create_and_verify(name: str) -> None:
        created = repo.create(TItemCreate(name=name))
        fetched = repo.get(created.id)
        assert fetched is not None
        assert fetched.name == name
        assert fetched.id == created.id

    names = [f"task-{i}" for i in range(30)]
    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(create_and_verify, names))

    assert repo.count() == 30


def test_exception_mid_operation_leaves_next_call_clean(sync_db: Database) -> None:
    """A failed operation does not leave the next call holding a stale session."""
    repo = FailingRepository(sync_db)

    with pytest.raises(BoomError):
        repo.boom()

    result = repo.get_many()
    assert list(result) == []

    created = repo.create(TItemCreate(name="after-failure"))
    assert created.name == "after-failure"


def test_not_found_error_still_available_after_failure(sync_db: Database) -> None:
    """Confirms repo remains usable end-to-end for a not-found lookup after a failure."""
    repo = FailingRepository(sync_db)
    with pytest.raises(BoomError):
        repo.boom()

    with pytest.raises(NotFoundError):
        repo.remove(UUID(int=0))
