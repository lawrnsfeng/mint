"""Sync engine + session-factory owner for mint.db repositories."""

from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from functools import cached_property, wraps
from typing import Final, overload

from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from mint.db.exc import (
    CommitOutcomeUnknownError,
    ConfigError,
    PoolExhaustedError,
    ScopeAbortedError,
    ScopeSchemaMismatchError,
    SessionNotInitializedError,
)
from mint.db.retry import ScopeRetry
from mint.db.settings import DatabaseSettings

# Session.info key holding the dbschema of the scope a session is bound by.
# Present only while that scope is open, which is what makes it joinable.
_SCOPE_DBSCHEMA_KEY: Final[str] = "mint_scope_dbschema"
# Connection.info key set when a statement on that connection failed.
_STATEMENT_FAILED_KEY: Final[str] = "mint_statement_failed"


class Database:
    """Owns one pooled sync engine and its session factory.

    One instance per logical database connection, shared across every
    repository in a service. It also owns the session slot: one
    ``ContextVar`` per instance, so everything running against this
    database inside one thread shares the session a :meth:`session_scope`
    bound, and concurrent threads never see each other's.

    :meth:`create_session` always yields a brand-new, unbound session.
    :meth:`session_scope` and :meth:`scoped` bind one for the duration of a
    block or a function and commit it once, when the outermost scope exits.
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
            settings: Pool, timeout and echo tuning. Defaults to
                environment-sourced :class:`DatabaseSettings`. Applied only
                when the engine is built here from ``uri``.

        Raises:
            ConfigError: If neither ``uri`` nor ``engine`` is supplied.

        """
        self.settings = settings or DatabaseSettings()
        self._session_ctx: ContextVar[Session | None] = ContextVar(
            f"_db_session_{id(self)}",
            default=None,
        )
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
                pool_timeout=self.settings.POOL_TIMEOUT,
                connect_args=self._connect_args,
            )
        else:
            raise ConfigError(detail="either uri or engine must be supplied")
        # Registered unconditionally: SQLAlchemy ignores a repeat of the same
        # target and function, whereas event.contains() looks the target up
        # by id() and answers True for a new engine at a recycled address.
        event.listen(self._engine, "handle_error", Database._flag_failed_statement)

    @property
    def engine(self) -> Engine:
        """Return the underlying sync engine."""
        return self._engine

    @property
    def _connect_args(self) -> dict[str, str]:
        """Return the psycopg2 connect arguments the settings call for."""
        timeout_ms = self.settings.STATEMENT_TIMEOUT_MS
        if timeout_ms is None:
            return {}
        return {"options": f"-c statement_timeout={timeout_ms}"}

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
    ) -> Generator[Session]:
        """Open a new session, applying an optional multi-tenant schema.

        Args:
            dbschema: When set, applies a ``schema_translate_map`` so
                unqualified table names resolve against this schema for the
                lifetime of the session.

        Yields:
            A fresh :class:`Session`, rolled back on error and always
            closed on exit.

        Raises:
            PoolExhaustedError: If no pooled connection of this database
                became free within the pool timeout.

        """
        with self._sessionmaker() as session:
            try:
                if dbschema:
                    session.connection(
                        execution_options={"schema_translate_map": {None: dbschema}},
                    )
                yield session
            except PoolTimeoutError as exc:
                exhausted = self._pool_exhaustion
                if exhausted is None:
                    raise
                raise exhausted from exc
            except Exception:
                session.rollback()
                raise

    @property
    def _pool_exhaustion(self) -> PoolExhaustedError | None:
        """Return the error describing this pool's exhaustion, if it is exhausted.

        ``None`` when this pool still has an idle connection to hand out:
        a pool timeout seen then was raised by some other engine used
        inside the block, and is left as it is.
        """
        pool = self._engine.pool
        if not isinstance(pool, QueuePool) or pool.checkedin() > 0:
            return None
        return PoolExhaustedError(
            pool_size=pool.size(),
            overflow=pool.overflow(),
            checked_out=pool.checkedout(),
            timeout=pool.timeout(),
        )

    @staticmethod
    def _flag_failed_statement(context: ExceptionContext) -> None:
        """Record on the connection that one of its statements failed.

        Postgres aborts the surrounding transaction when a statement
        fails, and then answers a later ``COMMIT`` with a silent
        ``ROLLBACK``. :meth:`_commit_scope` reads this flag to find out
        whether a commit needs checking first.
        """
        if context.connection is not None:
            context.connection.info[_STATEMENT_FAILED_KEY] = True

    @property
    def _bound_session(self) -> Session | None:
        """Return the session of the scope open in this thread, if there is one.

        A thread started inside a scope inherits the slot and keeps it after
        that scope has exited. Such a leftover session is closed and
        nobody will commit it, so it does not count as an open scope.
        """
        session = self._session_ctx.get()
        if session is None or _SCOPE_DBSCHEMA_KEY not in session.info:
            return None
        return session

    def _commit_scope(self, session: Session) -> None:
        """Commit ``session``, unless a handled error left its transaction aborted.

        Raises:
            ScopeAbortedError: If a statement failed earlier in the scope,
                the error was caught, and the transaction was not restored
                by rolling back to a savepoint.
            CommitOutcomeUnknownError: If the connection was lost while
                the commit was in flight.

        """
        if not session.in_transaction():
            return
        connection = session.connection()
        if connection.info.pop(_STATEMENT_FAILED_KEY, False):
            try:
                session.execute(text("SELECT 1"))
            except DBAPIError as exc:
                raise ScopeAbortedError from exc
        try:
            session.commit()
        except DBAPIError as exc:
            if exc.connection_invalidated:
                raise CommitOutcomeUnknownError from exc
            raise

    @property
    def current_session(self) -> Session:
        """Return the session bound by the enclosing :meth:`session_scope`.

        Raises:
            SessionNotInitializedError: If no scope is open in this thread.

        """
        session = self._bound_session
        if session is None:
            raise SessionNotInitializedError(repository=type(self).__name__)
        return session

    @contextmanager
    def session_scope(
        self,
        *,
        dbschema: str | None = None,
        commit: bool = True,
    ) -> Generator[Session]:
        """Bind one session to the current thread for the duration of the block.

        The outermost scope owns the session: it opens it, commits it on a
        clean exit and closes it. A scope entered while another is already
        open in the same thread joins that one — it yields the same session
        and neither commits nor closes it, so everything nested inside the
        outermost scope is one transaction.

        The connection is checked out on the first statement, not on entry,
        and held until the outermost scope exits.

        A thread started inside a scope and still running after it exited
        does not join the dead scope: its first call opens a scope of its
        own.

        Args:
            dbschema: Multi-tenant schema-translate target. Only the
                outermost scope applies it.
            commit: Whether the outermost scope commits on a clean exit.
                Ignored when joining.

        Yields:
            The bound :class:`Session`.

        Raises:
            ScopeSchemaMismatchError: If ``dbschema`` differs from the one
                the enclosing scope was opened for.
            ScopeAbortedError: On exit of the outermost scope, if a
                statement failed inside it and the error was handled
                without rolling back to a savepoint.

        """
        active = self._bound_session
        if active is not None:
            active_schema: str | None = active.info[_SCOPE_DBSCHEMA_KEY]
            if dbschema is not None and dbschema != active_schema:
                raise ScopeSchemaMismatchError(requested=dbschema, active=active_schema)
            yield active
            return
        with self.create_session(dbschema=dbschema) as session:
            session.info[_SCOPE_DBSCHEMA_KEY] = dbschema
            token = self._session_ctx.set(session)
            try:
                yield session
                if commit:
                    self._commit_scope(session)
            finally:
                del session.info[_SCOPE_DBSCHEMA_KEY]
                self._session_ctx.reset(token)

    def run_in_scope[T](
        self,
        call: Callable[[], T],
        *,
        retries: int = 0,
        dbschema: str | None = None,
    ) -> T:
        """Run ``call`` inside a :meth:`session_scope` and return its result.

        With ``retries`` set and no scope open yet, a scope that fails with
        a transient error — a serialization failure, a deadlock, a
        connection lost before the commit — is rolled back and ``call`` is
        run again in a fresh one, after a short backoff. A connection lost
        *while committing* is not retried: the outcome is unknown, and it
        surfaces as :class:`CommitOutcomeUnknownError`. When this call
        joins a scope that is already open it never retries either: the
        transaction belongs to that scope's owner, and only re-running the
        owner's whole function can replay it.

        Args:
            call: The zero-argument function to run.
            retries: Extra attempts after the first on a transient error.
                ``call`` runs again from the top, so everything it does
                must be safe to repeat.
            dbschema: Multi-tenant schema-translate target for the scope.

        Returns:
            Whatever ``call`` returns.

        """

        def attempt() -> T:
            with self.session_scope(dbschema=dbschema):
                return call()

        if retries <= 0 or self._bound_session is not None:
            return attempt()
        return ScopeRetry(retries=retries).as_retrying(attempt)

    @overload
    def scoped[**P, RT](self, func: Callable[P, RT], /) -> Callable[P, RT]: ...

    @overload
    def scoped[**P, RT](
        self,
        /,
        *,
        retries: int = 0,
        dbschema: str | None = None,
    ) -> Callable[[Callable[P, RT]], Callable[P, RT]]: ...

    def scoped[**P, RT](
        self,
        func: Callable[P, RT] | None = None,
        /,
        *,
        retries: int = 0,
        dbschema: str | None = None,
    ) -> Callable[P, RT] | Callable[[Callable[P, RT]], Callable[P, RT]]:
        """Wrap a function so it runs inside a :meth:`session_scope`.

        Works on any function or method, bare (``@db.scoped``) or with
        options (``@db.scoped(retries=2, dbschema="tenant_a")``). The session is reachable
        through :attr:`current_session` from the function and from
        everything it calls, decorated or not.

        Args:
            func: The function to scope, when used bare.
            retries: Extra attempts on a transient error — see
                :meth:`run_in_scope`.
            dbschema: Multi-tenant schema-translate target for the scope.
                For a schema only known per call, use
                :meth:`run_in_scope` or :meth:`session_scope` instead.

        Returns:
            The wrapped function, or a decorator producing it when called
            with options.

        """

        def decorate(target: Callable[P, RT]) -> Callable[P, RT]:
            @wraps(target)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> RT:
                return self.run_in_scope(
                    lambda: target(*args, **kwargs),
                    retries=retries,
                    dbschema=dbschema,
                )

            return wrapper

        if func is None:
            return decorate
        return decorate(func)
