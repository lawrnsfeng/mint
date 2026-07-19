"""Sync engine + session-factory owner for mint.db repositories."""

from collections.abc import Iterator
from contextlib import contextmanager
from functools import cached_property

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from mint.db.exc import ConfigError
from mint.db.settings import DatabaseSettings


class Database:
    """Owns one pooled sync engine and its session factory.

    One instance per logical database connection, shared across every
    repository in a service. Each :meth:`create_session` call yields a
    brand-new session — there is no "lazy"/scoped-session mode; a caller
    that wants one session shared across multiple repository calls uses
    :class:`mint.db.sync.uow.UnitOfWork` instead.
    """

    def __init__(
        self,
        uri: str | None = None,
        *,
        engine: Engine | None = None,
        settings: DatabaseSettings | None = None,
    ) -> None:
        """Initialize the database connection.

        Args:
            uri: SQLAlchemy sync connection URI. Required unless ``engine``
                is supplied directly.
            engine: Pre-built engine to use instead of creating one from
                ``uri``/``settings`` (primarily for tests).
            settings: Pool/echo tuning. Defaults to environment-sourced
                :class:`DatabaseSettings`.

        Raises:
            ConfigError: If neither ``uri`` nor ``engine`` is supplied.

        """
        self.settings = settings or DatabaseSettings()
        if engine is not None:
            self._engine = engine
        elif uri is not None:
            self._engine = create_engine(
                uri,
                pool_pre_ping=self.settings.POOL_PRE_PING,
                echo=self.settings.ECHO,
                pool_size=self.settings.POOL_SIZE,
                max_overflow=self.settings.MAX_OVERFLOW,
                pool_recycle=self.settings.POOL_RECYCLE,
            )
        else:
            raise ConfigError(detail="either uri or engine must be supplied")

    @property
    def engine(self) -> Engine:
        """Return the underlying sync engine."""
        return self._engine

    @cached_property
    def _sessionmaker(self) -> sessionmaker[Session]:
        return sessionmaker(
            self._engine,
            autoflush=False,
            expire_on_commit=False,
        )

    @contextmanager
    def create_session(
        self,
        *,
        dbschema: str | None = None,
    ) -> Iterator[Session]:
        """Open a new session, applying an optional multi-tenant schema.

        Args:
            dbschema: When set, applies a ``schema_translate_map`` so
                unqualified table names resolve against this schema for the
                lifetime of the session.

        Yields:
            A fresh :class:`Session`, rolled back on error and always
            closed on exit.

        """
        with self._sessionmaker() as session:
            try:
                if dbschema:
                    session.connection(
                        execution_options={"schema_translate_map": {None: dbschema}},
                    )
                yield session
            except Exception:
                session.rollback()
                raise
