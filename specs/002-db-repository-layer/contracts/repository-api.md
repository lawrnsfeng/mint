# Contract: `mint.db` public API surface

This is a library, not a network service — its "contract" is the public
Python API other mint code depends on. Reflects the actual implementation
(`mint/db/`), verified against real PostgreSQL in `tests/db/`. Async shown;
`mint.db.sync` mirrors every signature with `async`/`await` removed and
`Session` instead of `AsyncSession`.

## `mint.db.models`

```python
class Base(AsyncAttrs, SQLModel): ...

class BaseWithUUID(Base):
    id: UUID  # primary key, indexed, default_factory=uuid4

class BaseWithIntID(Base):
    id: int | None  # primary key, indexed, server/auto-increment

class BaseWithStrID(Base):
    id: str  # primary key, indexed, caller-supplied

class AuditMixin(SQLModel):
    created_at: datetime  # server_default=func.now(), sa_type=DateTime(timezone=True)
    updated_at: datetime  # + onupdate=func.now()

class OwnerMixin(SQLModel):
    created_by_user_id: UUID | None  # column only, no FK/relationship

class IsDeletedMixin(SQLModel):
    is_deleted: bool  # opts a table into soft-delete scoping
```

`HasSoftDelete`/`HasOwnerColumn` (structural `Protocol` markers) were
removed in Phase 2 — the scoping listener now checks
`issubclass(schema, IsDeletedMixin)`/`issubclass(schema, OwnerMixin)`
directly against these concrete classes; the Protocols were never usable
via `issubclass()` in the first place (see "Scoping bypass execution
options" below).

Note: `datetime` fields use an explicit `sa_type=DateTime(timezone=True)`,
not `Base.type_annotation_map` — the latter does not reliably propagate from
`Base` into mixin-declared fields (`AuditMixin` isn't a `Base` subclass)
under SQLModel's registry; confirmed via a real Postgres round-trip
(`TIMESTAMP WITHOUT TIME ZONE` came back instead of `WITH TIME ZONE` until
fixed). `type_annotation_map` is still declared on `Base` and works for
fields declared directly on `Base` subclasses.

Note (Phase 2): `AuditMixin` uses `Field(sa_type=..., sa_column_kwargs=...)`,
not `Field(sa_column=Column(...))` — a pre-built `Column` instance is
constructed once at class-body evaluation time and can only be owned by one
`Table`; a *second* table composing `AuditMixin` with the `sa_column=`
form fails with `ArgumentError: Column object 'created_at' already
assigned to Table '<the first table>'`. Confirmed directly by this port's
own 4+ mixin stress test (`tests/db/asynk/test_models.py`), which was the
first place two different tables in the same test session both composed
`AuditMixin`. `sa_type=`/`sa_column_kwargs=` let SQLModel build a fresh
`Column` per table instead.

## `mint.db.asynk.database`

```python
class Database:
    def __init__(self, uri: str | None = None, *, engine: AsyncEngine | None = None,
                 settings: DatabaseSettings | None = None) -> None: ...
                 # raises ConfigError if neither uri nor engine given

    @property
    def engine(self) -> AsyncEngine: ...

    def create_session(
        self, *, dbschema: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncSession]: ...
```

## `mint.db.asynk.base`

```python
class IScopedRepository(Protocol):
    """Instance-level structural check for owner-scoping capability.

    Defined in base.py (not imported from mixins.IOwner) to avoid a
    circular import. isinstance()-checked, not issubclass()-checked — see
    "Scoping bypass execution options" below for why.
    """
    owner: Any
    is_scoped: bool

class RepositoryBase[T: Base]:
    Schema: type[T]

    def __init__(
        self, db: Database, *, session: AsyncSession | None = None,
        dbschema: str | None = None, auto_commit: bool = True,
    ) -> None: ...

    @property
    def db(self) -> Database: ...

    @property
    def session(self) -> AsyncSession: ...  # raises SessionNotInitializedError if unbound

    @staticmethod
    def ensure_session[R, **P, RT](
        func: Callable[Concatenate[R, P], Coroutine[Any, Any, RT]],
    ) -> Callable[Concatenate[R, P], Coroutine[Any, Any, RT]]: ...

    async def execute(self, stmt: CRUDStatement) -> T | None: ...
    async def execute_many(self, stmt: CRUDStatement, *, unique: bool = False) -> Sequence[T]: ...
    async def get_many(self, *, skip: int = 0, limit: int = 10) -> Sequence[T]: ...
    async def get_many_page(self, *, skip: int = 0, limit: int = 10) -> PaginatedResult[T]: ...
    async def count(self) -> int: ...  # raises OperationalError if no value returned
    async def create(
        self, model: SQLModel, *, upsert: bool = False,
        conflict_columns: Sequence[str] | None = None,
    ) -> T: ...  # raises OperationalError if no row returned
    async def force_session_schema(self) -> None: ...  # raises DBSchemaNotSetError if dbschema unset
    async def refresh(self, instance: T) -> None: ...

class RepositoryKwargs(TypedDict, total=False):
    """Forwarding shape for mixin cooperative __init__ (Unpack[RepositoryKwargs])."""
    session: AsyncSession | None
    dbschema: str | None
    auto_commit: bool
```

`fetch_extra_relationships`/`extra_paths` (Phase 1) and
`_construction_kwargs()`/`clone()` (Phase 1) were removed in Phase 2 — see
data-model.md's "Relationship eager-loading" and research.md's "Drop
clone()/_construction_kwargs()" respectively. To eager-load a relationship,
add `.options(selectinload(Schema.relationship))` (or `joinedload(...)`)
directly to the statement passed to `execute()`/`execute_many()`.

```python
class PaginatedResult[T]:
    """dataclasses.dataclass, in mint/db/typedefs.py."""
    items: Sequence[T]
    total: int
```

`execute`/`execute_many` only commit when `auto_commit` is `True` **and**
the statement is not a plain `Select` — reads never commit regardless of
`auto_commit` (this is a deliberate fix over the ported predecessor, which
committed after every statement including pure reads).

`create(upsert=True)` (Phase 3) builds a Postgres `INSERT ... ON CONFLICT
(<conflict_columns, defaulting to Schema's primary key>) DO UPDATE SET
<every dumped field not in the conflict target> RETURNING ...` — one round
trip, the returned row is always the final state (freshly inserted, or the
existing row merged with the new values), never `None` for the
row-already-exists case. `upsert=False` (the default) is the unchanged
plain-insert path. See research.md, "Upsert support".

## `mint.db.asynk.entity`

```python
class EntityRepository[T: Base, I](RepositoryBase[T]):
    async def get(self, id_: I) -> T | None: ...
    async def update(self, id_: I, model: SQLModel) -> T: ...
        # exclude_unset=True; an empty update (nothing set) short-circuits to get()
        # rather than issuing an UPDATE ... SET <nothing> WHERE ... (invalid SQL)
        # raises NotFoundError if no row matches id_
    async def remove(self, id_: I) -> T: ...          # raises NotFoundError
    async def remove_many(self, ids: Sequence[I]) -> Sequence[T]: ...
    async def get_many_by_ids(self, ids: Sequence[I]) -> Sequence[T]: ...

    @property
    def _id_column(self) -> InstrumentedAttribute[I]: ...  # private; the one cast() this needs
```

`T` is bound only to `Base` (Phase 2; previously
`BaseWithUUID | BaseWithIntID | BaseWithStrID`), and `I` is a free,
unconstrained type parameter — not statically verified to match `T`'s
actual `id` field type (Python has no mechanism to derive one type
parameter from another's attribute; see research.md, "Generic ID type").
A schema with no `id` column uses `RepositoryBase[T]` directly, never
`EntityRepository`.

## `mint.db.asynk.mixins`

```python
class SoftDeleteMixin[T: Base, I](EntityRepository[T, I]):
    async def remove(self, id_: I) -> T: ...          # soft: UPDATE is_deleted=True
    async def remove_many(self, ids: Sequence[I]) -> Sequence[T]: ...
    # Read-side soft-delete filtering is unconditional on the schema having
    # an is_deleted column (RepositoryBase._scope_listener) — independent
    # of whether this mixin is used.

class IOwner(Protocol):
    id: Any
    @property
    def is_scoped(self) -> bool: ...

class ResourceOwnerMixin[T: Base](RepositoryBase[T]):
    def __init__(self, db: Database, *, owner: IOwner | None = None,
                 **kwargs: Unpack[RepositoryKwargs]) -> None: ...
    owner: IOwner | None
    @property
    def is_scoped(self) -> bool: ...
```

## `mint.db.asynk.mv`

```python
class MaterializedViewRepository[T: Base](RepositoryBase[T]):
    @property
    def table_name(self) -> str: ...
    async def refresh_materialized_view(self) -> None: ...
        # REFRESH MATERIALIZED VIEW CONCURRENTLY <table_name>; commits only
        # when self.auto_commit is True (Phase 2 fix — previously committed
        # unconditionally, breaking UnitOfWork atomicity for any caller
        # refreshing a view mid-transaction with auto_commit=False).
```

Extends `RepositoryBase`, not `EntityRepository` — no `get`/`update`/`remove`.

## `mint.db.asynk.uow`

```python
class UnitOfWork:
    def __init__(self, db: Database, *, dbschema: str | None = None) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, exc_type: type[BaseException] | None,
                         exc_val: BaseException | None,
                         exc_tb: TracebackType | None) -> None: ...
    @property
    def db(self) -> Database: ...
    @property
    def session(self) -> AsyncSession: ...  # raises SessionNotInitializedError if unbound
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...
```

**No `repo()` factory.** A factory forwarding arbitrary constructor kwargs
to an arbitrary repository subclass can't be typed without `Any`
(`ParamSpec`-based attempts hit `ty` errors when also injecting `session=`
alongside `**kwargs: P.kwargs`) — mint's coding-style rule forbids `Any`
parameters. Construct repositories directly:
`SomeRepository(db, session=uow.session, auto_commit=False, ...)`.

## `mint.db.exc`

```python
@dataclass
class RepositoryError(TemplatedError): ...

@dataclass
class SessionNotInitializedError(RepositoryError):
    TEMPLATE = "Session has not been initialized for {repository}"
    repository: str

@dataclass
class NotFoundError(RepositoryError):
    TEMPLATE = "{schema} not found: {id_}"
    schema: str
    id_: Any

@dataclass
class OperationalError(RepositoryError):
    TEMPLATE = "Operational uncaught error: {error}"
    error: BaseException

@dataclass
class ConfigError(RepositoryError):
    TEMPLATE = "Invalid repository configuration: {detail}"
    detail: str

@dataclass
class DBSchemaNotSetError(RepositoryError):
    TEMPLATE = "dbschema must be set on {repository} to force a session schema"
    repository: str
```

Every exception is a `@dataclass` subclass of `mint.exc.TemplatedError`
(mint's modular-design rule) — typed fields, no pre-formatted message
strings assembled at the call site.

`AbnormalResultError` (Phase 1) was removed in Phase 2 — `count()`/
`create()` raise `OperationalError` instead. Rationale: an
`INSERT ... RETURNING`/`COUNT` query returning no row is an
infrastructure-level anomaly (e.g. an `ON CONFLICT DO NOTHING` upsert
silently no-opping), correctly surfaced as an internal-server-error-flavored
exception rather than a bespoke, narrowly-scoped type — see research.md,
"Exception for anomalous query results".

## Scoping bypass execution options

Any statement executed through a repository (`repo.execute(stmt)`,
`repo.execute_many(stmt)`, or a hand-written `self.session.execute(stmt)`
inside a custom method) accepts, via `stmt.execution_options(...)`:

- `mint_scope_bypass: frozenset[type] = frozenset()` — the set of scoping
  mixin classes (`IsDeletedMixin`, `OwnerMixin`) to bypass for that
  specific statement, e.g.
  `stmt.execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))`.

(Phase 1 shipped two separate booleans, `mint_include_deleted`/
`mint_skip_owner_scope` — replaced in Phase 2 with the single option above
so a future scoping dimension is a new mixin class, not a new named
constant.)

These are the only sanctioned way to see soft-deleted/cross-owner rows
(spec.md FR-006). The listener applies to every `session.execute()` call
against a scoped session — including custom methods with no scoping code
at all — via `sqlalchemy.orm.with_loader_criteria` + a `do_orm_execute`
event registered on the specific session instance.

The listener's own checks (Phase 2) use type narrowing, not
`hasattr`/`getattr`: `issubclass(self.Schema, IsDeletedMixin)`/
`issubclass(self.Schema, OwnerMixin)` for the schema-level checks (nominal,
against the concrete mixin classes — `issubclass()` cannot be used against
a `Protocol` with non-method/data members, which is why the schema check
targets these concrete classes rather than a `HasSoftDelete`-style
Protocol), and `isinstance(self, IScopedRepository)` for the
repository-instance owner check (`IScopedRepository` is a `Protocol` with
data members `owner`/`is_scoped` — `isinstance()` has no restriction
against such Protocols, unlike `issubclass()`).

**Implementation pitfall (confirmed, fixed):** the owner criterion's lambda
must be a genuine closure over `owner_id` (`lambda cls: cls.created_by_user_id
== owner_id`), not a default-argument trick (`lambda cls, _owner_id=owner_id:
...`). SQLAlchemy's lambda-SQL caching tracks closure cells to rebind the
value on each call; a same-shaped lambda using a baked-in default argument
gets its first-seen value reused for every later call — silently scoping
every subsequent owner to whichever owner ran first. Caught by a real
multi-owner integration test against Postgres.
