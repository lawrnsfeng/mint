"""The session lifecycle driven with SQLAlchemy Core only.

No SQLModel class and no repository: a hand-written query layer over a
Core ``Table``, the shape of a service that builds its statements
explicitly and only wants session and transaction handling from mint.
"""

from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, func, insert, select
from sqlalchemy.ext.asyncio import AsyncEngine

from mint.db.asynk import Database, UnitOfWork, scoped

_METADATA = MetaData()
_NOTES = Table(
    "tcore_note",
    _METADATA,
    Column("id", Integer, primary_key=True),
    Column("body", String, nullable=False),
)


class BoomError(Exception):
    """Deliberate test-only failure, raised mid-scope."""


class NoteStore:
    """A Core-only query layer holding its database."""

    def __init__(self, db: Database) -> None:
        """Keep the database this store works against."""
        self.db = db

    async def insert_one(self, body: str) -> None:
        """Insert one note on whatever session is in scope."""
        await self.db.current_session.execute(insert(_NOTES).values(body=body))

    @scoped
    async def add_pair(self, first: str, second: str) -> int:
        """Insert two notes and return the count seen inside the scope."""
        await self.insert_one(first)
        await self.insert_one(second)
        result = await self.db.current_session.execute(select(func.count()).select_from(_NOTES))
        return result.scalar_one()

    @scoped
    async def add_then_fail(self, body: str) -> None:
        """Insert one note, then raise."""
        await self.insert_one(body)
        raise BoomError

    async def committed_count(self) -> int:
        """Return the note count visible to a fresh, independent session."""
        async with self.db.create_session() as session:
            result = await session.execute(select(func.count()).select_from(_NOTES))
            return result.scalar_one()


@pytest.fixture
async def core_db(async_engine: AsyncEngine) -> AsyncGenerator[Database]:
    """Provide a ``Database`` over a freshly-created Core-only table.

    Args:
        async_engine: The per-test async engine.

    Yields:
        Database: Configured database wrapper.

    """
    async with async_engine.begin() as conn:
        await conn.run_sync(_METADATA.drop_all)
        await conn.run_sync(_METADATA.create_all)
    yield Database(engine=async_engine)
    async with async_engine.begin() as conn:
        await conn.run_sync(_METADATA.drop_all)


@pytest.mark.asyncio
async def test_scoped_core_statements_commit_together(core_db: Database) -> None:
    """Core statements in one scope share a session and commit on exit."""
    store = NoteStore(core_db)

    assert await store.add_pair("a", "b") == 2
    assert await store.committed_count() == 2


@pytest.mark.asyncio
async def test_scoped_core_statements_roll_back_on_error(core_db: Database) -> None:
    """An error in the scope discards its Core statements."""
    store = NoteStore(core_db)

    with pytest.raises(BoomError):
        await store.add_then_fail("lost")

    assert await store.committed_count() == 0


@pytest.mark.asyncio
async def test_function_decorator_with_core_statements(core_db: Database) -> None:
    """``@db.scoped`` on a plain function works for Core statements too."""
    store = NoteStore(core_db)

    @core_db.scoped
    async def add_three() -> None:
        for body in ("a", "b", "c"):
            await store.insert_one(body)

    await add_three()

    assert await store.committed_count() == 3


@pytest.mark.asyncio
async def test_unit_of_work_with_core_statements(core_db: Database) -> None:
    """UnitOfWork runs Core statements and persists only on commit()."""
    store = NoteStore(core_db)

    async with UnitOfWork(core_db) as uow:
        await uow.session.execute(insert(_NOTES).values(body="uncommitted"))
    assert await store.committed_count() == 0

    async with UnitOfWork(core_db) as uow:
        await uow.session.execute(insert(_NOTES).values(body="committed"))
        await uow.commit()
    assert await store.committed_count() == 1
