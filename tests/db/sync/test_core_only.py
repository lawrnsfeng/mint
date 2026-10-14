"""The session lifecycle driven with SQLAlchemy Core only.

No SQLModel class and no repository: a hand-written query layer over a
Core ``Table``, the shape of a service that builds its statements
explicitly and only wants session and transaction handling from mint.
"""

from collections.abc import Iterator

import pytest
from sqlalchemy import Column, Engine, Integer, MetaData, String, Table, func, insert, select

from mint.db.sync import Database, UnitOfWork, scoped

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

    def insert_one(self, body: str) -> None:
        """Insert one note on whatever session is in scope."""
        self.db.current_session.execute(insert(_NOTES).values(body=body))

    @scoped
    def add_pair(self, first: str, second: str) -> int:
        """Insert two notes and return the count seen inside the scope."""
        self.insert_one(first)
        self.insert_one(second)
        result = self.db.current_session.execute(select(func.count()).select_from(_NOTES))
        return result.scalar_one()

    @scoped
    def add_then_fail(self, body: str) -> None:
        """Insert one note, then raise."""
        self.insert_one(body)
        raise BoomError

    def committed_count(self) -> int:
        """Return the note count visible to a fresh, independent session."""
        with self.db.create_session() as session:
            result = session.execute(select(func.count()).select_from(_NOTES))
            return result.scalar_one()


@pytest.fixture
def core_db(sync_engine: Engine) -> Iterator[Database]:
    """Provide a ``Database`` over a freshly-created Core-only table.

    Args:
        sync_engine: The shared sync engine.

    Yields:
        Database: Configured database wrapper.

    """
    _METADATA.drop_all(sync_engine)
    _METADATA.create_all(sync_engine)
    yield Database(engine=sync_engine)
    _METADATA.drop_all(sync_engine)


def test_scoped_core_statements_commit_together(core_db: Database) -> None:
    """Core statements in one scope share a session and commit on exit."""
    store = NoteStore(core_db)

    assert store.add_pair("a", "b") == 2
    assert store.committed_count() == 2


def test_scoped_core_statements_roll_back_on_error(core_db: Database) -> None:
    """An error in the scope discards its Core statements."""
    store = NoteStore(core_db)

    with pytest.raises(BoomError):
        store.add_then_fail("lost")

    assert store.committed_count() == 0


def test_function_decorator_with_core_statements(core_db: Database) -> None:
    """``@db.scoped`` on a plain function works for Core statements too."""
    store = NoteStore(core_db)

    @core_db.scoped
    def add_three() -> None:
        for body in ("a", "b", "c"):
            store.insert_one(body)

    add_three()

    assert store.committed_count() == 3


def test_unit_of_work_with_core_statements(core_db: Database) -> None:
    """UnitOfWork runs Core statements and persists only on commit()."""
    store = NoteStore(core_db)

    with UnitOfWork(core_db) as uow:
        uow.session.execute(insert(_NOTES).values(body="uncommitted"))
    assert store.committed_count() == 0

    with UnitOfWork(core_db) as uow:
        uow.session.execute(insert(_NOTES).values(body="committed"))
        uow.commit()
    assert store.committed_count() == 1
