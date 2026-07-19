"""Generic sync repository base: session isolation + scoping listener.

Sync mirror of :mod:`mint.db.asynk.base`. The same ``ContextVar`` mechanism
isolates sessions per thread here (a ``ContextVar`` is as valid for
thread-based isolation as it is for asyncio-task-based isolation — each OS
thread that doesn't explicitly copy a context gets its own top-level
context), which also fixes the predecessor implementation's separate
sync-side bug (a ``scoped_session(scopefunc=None)`` degrading to thread-id
scoping and risking a session leaking across pooled threads).
"""

from collections.abc import Callable, Sequence
from contextvars import ContextVar
from functools import wraps
from typing import Any, Concatenate, Protocol, TypedDict, cast, runtime_checkable

from sqlalchemy import Select, event, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import LoaderCriteriaOption, ORMExecuteState, Session, with_loader_criteria
from sqlmodel import SQLModel

from mint.db.exc import DBSchemaNotSetError, OperationalError, SessionNotInitializedError
from mint.db.models import Base, IsDeletedMixin, OwnerMixin
from mint.db.typedefs import CRUDStatement, PaginatedResult

from .database import Database

_SCOPE_BYPASS_OPTION = "mint_scope_bypass"


@runtime_checkable
class IScopedRepository(Protocol):
    """Structural contract: a repository instance capable of owner-scoping.

    Defined here (not imported from ``mixins.IOwner``) to avoid a circular
    import — ``mixins.py`` imports from ``base.py``, never the reverse.
    ``owner`` is typed ``Any`` deliberately: an owner's id can be any type
    (see ``EntityRepository``'s dropped closed ID-type union), so this
    Protocol only asserts the two attributes exist, not their shapes.

    ``issubclass()`` does not work on a ``Protocol`` with non-method (data)
    members — it raises ``TypeError`` — which is why the schema-level
    checks below use nominal ``issubclass()`` against concrete mixin
    classes instead of a Protocol. ``isinstance()`` has no such limitation,
    so it is used here for the instance-level check.
    """

    owner: Any
    is_scoped: bool


class RepositoryKwargs(TypedDict, total=False):
    """Keyword args a mixin's cooperative ``__init__`` forwards to :class:`RepositoryBase`.

    Lets a mixin adding its own constructor param (e.g. ``owner``) forward
    the rest via ``**kwargs: Unpack[RepositoryKwargs]`` instead of
    redeclaring every :class:`RepositoryBase` param itself.
    """

    session: Session | None
    dbschema: str | None
    auto_commit: bool


class RepositoryBase[T: Base]:
    """Generic data-access base for one SQLModel table.

    Session lifecycle defaults to zero-ceremony: a top-level call to any
    ``ensure_session``-decorated method opens a session, isolated per thread
    via ``ContextVar``, and closes it automatically. Passing an explicit
    ``session`` (typically via :class:`mint.db.sync.uow.UnitOfWork`) opts
    into caller-managed transaction boundaries instead.
    """

    Schema: type[T]

    def __init__(
        self,
        db: Database,
        *,
        session: Session | None = None,
        dbschema: str | None = None,
        auto_commit: bool = True,
    ) -> None:
        """Initialize the repository.

        Args:
            db: The shared :class:`Database` this repository queries.
            session: An externally-owned session (unit-of-work escape
                hatch). When set, this repository never opens its own
                session.
            dbschema: Multi-tenant schema-translate target.
            auto_commit: Whether mutating statements commit automatically.
                Reads never commit regardless of this flag.

        """
        self._db = db
        self._external_session = session
        self._session_ctx: ContextVar[Session | None] = ContextVar(
            f"_db_session_{id(self)}",
            default=None,
        )
        self._dbschema = dbschema
        self.auto_commit = auto_commit
        if session is not None:
            event.listen(session, "do_orm_execute", self._scope_listener)

    @property
    def db(self) -> Database:
        """Return the shared :class:`Database`."""
        return self._db

    @property
    def session(self) -> Session:
        """Return the currently-bound session.

        Raises:
            SessionNotInitializedError: If called outside an
                ``ensure_session``-wrapped call and with no external session
                supplied.

        """
        session = self._external_session or self._session_ctx.get()
        if session is None:
            raise SessionNotInitializedError(repository=type(self).__name__)
        return session

    def _scope_listener(self, orm_execute_state: ORMExecuteState) -> None:
        """Inject soft-delete/owner ``with_loader_criteria`` for this schema.

        Registered once per session this repository is bound to (see
        ``ensure_session`` and ``__init__``). Applies to every statement
        executed through that session, including hand-written custom
        queries that never call a helper method.

        A statement opts out of one or more scoping dimensions via
        ``.execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))``
        — a set of the mixin classes to bypass, not a per-dimension boolean
        flag; adding a future scoping dimension means adding a new mixin
        class, not a new constant.
        """
        if orm_execute_state.is_column_load or orm_execute_state.is_relationship_load:
            return
        if not (
            orm_execute_state.is_select
            or orm_execute_state.is_update
            or orm_execute_state.is_delete
        ):
            return

        bypass = orm_execute_state.execution_options.get(_SCOPE_BYPASS_OPTION, frozenset())
        criteria: list[LoaderCriteriaOption] = []

        if issubclass(self.Schema, IsDeletedMixin) and IsDeletedMixin not in bypass:
            criteria.append(
                with_loader_criteria(
                    self.Schema,
                    lambda cls: cls.is_deleted.is_(False),
                    include_aliases=True,
                ),
            )

        if (
            isinstance(self, IScopedRepository)
            and self.is_scoped
            and self.owner is not None
            and issubclass(self.Schema, OwnerMixin)
            and OwnerMixin not in bypass
        ):
            owner_id = self.owner.id
            criteria.append(
                with_loader_criteria(
                    self.Schema,
                    # Must be a true closure over owner_id (not a default-arg
                    # trick) — SQLAlchemy's lambda-SQL caching tracks closure
                    # cells to rebind the value on each call; a same-shaped
                    # lambda with a baked-in default arg gets its first-seen
                    # value reused for every later call, silently scoping
                    # every subsequent owner to whichever owner ran first.
                    lambda cls: cls.created_by_user_id == owner_id,
                    include_aliases=True,
                ),
            )

        if criteria:
            orm_execute_state.statement = orm_execute_state.statement.options(*criteria)

    @staticmethod
    def ensure_session[R: "RepositoryBase[Any]", **P, RT](
        func: Callable[Concatenate[R, P], RT],
    ) -> Callable[Concatenate[R, P], RT]:
        """Wrap a method so it always runs with a bound session.

        Nested calls within one thread reuse the session already bound in
        that thread's context (or the externally-supplied one); a
        top-level call opens a fresh, thread-isolated session and closes it
        on exit.

        Args:
            func: The method requiring a session.

        Returns:
            The wrapped method.

        """

        @wraps(func)
        def wrapper(self: R, *args: P.args, **kwargs: P.kwargs) -> RT:
            if self._external_session is not None or self._session_ctx.get() is not None:
                return func(self, *args, **kwargs)
            with self._db.create_session(dbschema=self._dbschema) as session:
                event.listen(session, "do_orm_execute", self._scope_listener)
                token = self._session_ctx.set(session)
                try:
                    return func(self, *args, **kwargs)
                finally:
                    self._session_ctx.reset(token)

        return wrapper

    @ensure_session
    def execute(self, stmt: CRUDStatement) -> T | None:
        """Execute a statement expected to return zero or one row.

        Args:
            stmt: A ``select``/``insert``/``update``/``delete`` statement
                targeting :attr:`Schema`. To eagerly load a relationship,
                add ``.options(selectinload(Schema.relationship))`` (or
                ``joinedload``) directly to the statement — SQLAlchemy
                resolves it as one extra query or JOIN total, regardless of
                result size.

        Returns:
            The matched row, or ``None``.

        """
        result = self.session.execute(stmt)
        obj = result.scalar()
        if self.auto_commit and not isinstance(stmt, Select):
            self.session.commit()
        return obj

    @ensure_session
    def execute_many(self, stmt: CRUDStatement, *, unique: bool = False) -> Sequence[T]:
        """Execute a statement expected to return any number of rows.

        Args:
            stmt: A ``select``/``update``/``delete`` statement targeting
                :attr:`Schema`. To eagerly load a relationship for every
                returned row, add ``.options(selectinload(Schema.rel))``
                (or ``joinedload``) directly to the statement — SQLAlchemy
                resolves it as one extra query or JOIN total, regardless of
                how many rows come back, never a per-row fetch loop.
            unique: Deduplicate rows before returning — required whenever
                the statement uses ``joinedload`` on a collection
                relationship.

        Returns:
            The matched rows.

        """
        result = self.session.execute(stmt)
        if unique:
            result = result.unique()
        objs = result.scalars().all()
        if self.auto_commit and not isinstance(stmt, Select):
            self.session.commit()
        return objs

    @ensure_session
    def get_many(self, *, skip: int = 0, limit: int = 10) -> Sequence[T]:
        """Return a paginated slice of rows.

        Args:
            skip: Number of rows to skip.
            limit: Maximum number of rows to return.

        Returns:
            The matched rows.

        """
        stmt = select(self.Schema).offset(skip).limit(limit)
        return self.execute_many(stmt)

    @ensure_session
    def get_many_page(self, *, skip: int = 0, limit: int = 10) -> PaginatedResult[T]:
        """Return a paginated slice of rows together with the total row count.

        A single query via a ``COUNT(*) OVER()`` window function, instead
        of the page query plus a separate :meth:`count` call — except when
        the requested page is empty (``skip`` past the last matching row),
        where the window function has no row to carry a total on and this
        falls back to one plain :meth:`count` call for that case only.

        Args:
            skip: Number of rows to skip.
            limit: Maximum number of rows to return.

        Returns:
            The page of rows and the total matching row count.

        """
        total_column = func.count().over().label("_mint_total")
        stmt = select(self.Schema, total_column).offset(skip).limit(limit)
        result = self.session.execute(stmt)
        rows = result.all()
        if not rows:
            return PaginatedResult(items=[], total=self.count())
        return PaginatedResult(items=[row[0] for row in rows], total=rows[0][1])

    @ensure_session
    def count(self) -> int:
        """Return the total row count for this schema (scoping-filtered).

        Returns:
            The row count.

        Raises:
            OperationalError: If the count query returns no value — an
                infrastructure-level anomaly (Postgres ``COUNT`` always
                returns exactly one row), not a domain error.

        """
        stmt = select(func.count()).select_from(self.Schema)
        result = self.session.execute(stmt)
        value = result.scalar()
        if value is None:
            raise OperationalError(
                RuntimeError(f"count() returned no row for {self.Schema.__name__}"),
            )
        return value

    @ensure_session
    def create(
        self,
        model: SQLModel,
        *,
        upsert: bool = False,
        conflict_columns: Sequence[str] | None = None,
    ) -> T:
        """Insert a new row from a create-payload model.

        Args:
            model: A non-table SQLModel instance (e.g. ``JobCreate``)
                holding the fields to insert.
            upsert: When ``True``, insert-or-update instead of a plain
                insert (Postgres ``ON CONFLICT ... DO UPDATE``): on a
                conflict against ``conflict_columns``, every dumped field
                that isn't part of the conflict target is merged into the
                existing row, and the final row — freshly inserted, or the
                existing row updated with the new values — is returned.
                Never raises for the "row already exists" case. ``False``
                (the default) is a plain ``INSERT ... RETURNING``, which
                fails at the database level on a conflict rather than
                resolving one.
            conflict_columns: The Postgres ``ON CONFLICT (...)`` target
                when ``upsert`` is ``True``. Defaults to :attr:`Schema`'s
                primary key columns when not given — this works whether
                :attr:`Schema` has a single ``id`` column or a composite
                primary key. Pass explicit column names to upsert against
                a different unique constraint instead (e.g. a composite
                ``UniqueConstraint`` on non-primary-key columns). Ignored
                when ``upsert`` is ``False``.

        Returns:
            The created row (or, under ``upsert=True``, the created-or-
            updated row).

        Raises:
            OperationalError: If the insert returns no row. Under
                ``upsert=False`` this is an infrastructure-level anomaly —
                a query contract violation, not a domain "already exists"
                condition; if the actual intent was insert-or-update,
                pass ``upsert=True`` instead of hitting this. Under
                ``upsert=True`` this should be unreachable (``DO UPDATE``
                always returns a row) and is defensive only.

        """
        values = model.model_dump()
        if not upsert:
            stmt = insert(self.Schema).values(**values).returning(self.Schema)
        else:
            if conflict_columns is not None:
                target = list(conflict_columns)
            else:
                # Base doesn't declare __table__ statically — only a
                # table=True subclass has one, added by SQLModel's
                # metaclass — so this needs the same cast() as
                # EntityRepository._id_column, for the same reason.
                schema = cast("type[Any]", self.Schema)
                target = [column.name for column in schema.__table__.primary_key.columns]
            pg_stmt = pg_insert(self.Schema).values(**values)
            update_columns = [key for key in values if key not in target]
            set_columns = update_columns or target
            stmt = pg_stmt.on_conflict_do_update(
                index_elements=target,
                set_={key: getattr(pg_stmt.excluded, key) for key in set_columns},
            ).returning(self.Schema)
        obj = self.execute(stmt)
        if obj is None:
            raise OperationalError(
                RuntimeError(f"create() returned no row for {self.Schema.__name__}"),
            )
        return obj

    @ensure_session
    def force_session_schema(self) -> None:
        """Apply :attr:`_dbschema` to the current session's connection.

        Raises:
            DBSchemaNotSetError: If ``dbschema`` was not configured.

        """
        if self._dbschema is None:
            raise DBSchemaNotSetError(repository=type(self).__name__)
        self.session.connection(
            execution_options={"schema_translate_map": {None: self._dbschema}},
        )

    @ensure_session
    def refresh(self, instance: T) -> None:
        """Refresh ``instance`` from the database.

        Args:
            instance: The object to refresh in place.

        """
        if self._dbschema is not None:
            self.force_session_schema()
        self.session.refresh(instance)
