"""Unit of Work: the opt-in primitive for multi-repository atomic transactions.

The zero-ceremony auto-session path (see :mod:`mint.db.sync.base`) stays the
default for simple calls — this module exists only for the case where a
caller needs several repository operations, potentially across different
tables, to succeed or fail together as one transaction.
"""

from contextlib import ExitStack
from types import TracebackType
from typing import Self

from sqlalchemy.orm import Session

from mint.db.exc import SessionNotInitializedError

from .database import Database


class UnitOfWork:
    """Owns one session shared across multiple repositories.

    Repositories are constructed normally, passing ``session=uow.session``
    — there is no generic ``repo()`` factory: a factory forwarding
    arbitrary constructor kwargs to an arbitrary repository subclass can't
    be expressed without ``Any`` (mint's coding-style rule forbids that),
    and direct construction is no less clear.

    Example:
        ```python
        with UnitOfWork(db) as uow:
            jobs = JobRepository(db, session=uow.session)
            collections = CollectionRepository(db, session=uow.session, owner=current_user)
            job = jobs.create(...)
            collections.update(...)
            uow.commit()
        ```

    """

    def __init__(self, db: Database, *, dbschema: str | None = None) -> None:
        """Initialize the unit of work.

        Args:
            db: The shared :class:`Database` to open a session against.
            dbschema: Multi-tenant schema-translate target for the session.

        """
        self._db = db
        self._dbschema = dbschema
        self._exit_stack = ExitStack()
        self._session: Session | None = None

    def __enter__(self) -> Self:
        """Open the shared session.

        Returns:
            This unit of work.

        """
        self._session = self._exit_stack.enter_context(
            self._db.create_session(dbschema=self._dbschema),
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the shared session.

        Args:
            exc_type: Exception type if the block raised, else ``None``.
            exc_val: Exception instance if the block raised, else ``None``.
            exc_tb: Traceback if the block raised, else ``None``.

        """
        self._exit_stack.close()
        self._session = None

    @property
    def db(self) -> Database:
        """Return the shared :class:`Database`."""
        return self._db

    @property
    def session(self) -> Session:
        """Return the shared session.

        Raises:
            SessionNotInitializedError: If accessed outside ``with``.

        """
        if self._session is None:
            raise SessionNotInitializedError(repository=type(self).__name__)
        return self._session

    def commit(self) -> None:
        """Commit the shared session."""
        self.session.commit()

    def rollback(self) -> None:
        """Roll back the shared session."""
        self.session.rollback()
