# DB Repository Layer

`mint.db` is a generic, SQLModel-based repository layer for Postgres, with
a full async implementation (`mint.db.asynk`) and a sync mirror
(`mint.db.sync`) that carries the same method names, options, and
behavior — every example below is async; drop `async`/`await` and swap the
import for the sync equivalent and it's unchanged.

This page covers every public method and option. For the *why* behind
each design choice — session isolation, scoping enforcement, the ID-type
genericity trade-off, and every gotcha found while building this against
real Postgres — see the
[Implementation Notes](../db-repository-implementation-notes.md), which
this guide cross-links into rather than duplicates.

## Design philosophy

A few decisions shape everything else in this layer:

- **Session isolation is per repository *instance*, via `ContextVar`, not
  a shared attribute.** A repository instance is safe to reuse across
  concurrent tasks/threads — no per-call cloning, no locking. This is the
  single most load-bearing design choice here: the predecessor this layer
  replaced stored its active session on a plain instance attribute,
  which raced under concurrent use.
- **Scoping (soft-delete, owner) is unconditional, not opt-in.** Whether a
  query excludes soft-deleted or other-owner rows is decided by the
  *schema's own shape* (does it compose `IsDeletedMixin`/`OwnerMixin`) —
  never by whether the method that built the query remembered to call
  something. This closes a real gap found in a predecessor system: custom
  query methods that silently skipped an opt-in scoping call.
- **The zero-ceremony path needs no setup, ever.**
  `JobRepository(db).get(id)` opens a session, does the work, and closes
  it — no context manager, no explicit transaction. `UnitOfWork` exists
  purely as the *named, opt-in* escape hatch for the one case that
  genuinely needs shared state across calls (multi-table atomicity), not
  as the default ceremony every call pays.
- **One class per table, not two.** A single `SQLModel` `table=True` class
  is both the ORM schema and the domain model — no separate pydantic
  response model glued on with a converter function.
- **Generic ID types are honest about Python's limits.** `EntityRepository[T,
  I]` takes two type parameters, not one, because Python has no
  higher-kinded-types mechanism to derive `I` (the ID type) from `T`'s
  `id` field automatically — see "Generic ID type" in the implementation
  notes for the full investigation.

## Defining a table

```python
from uuid import UUID
from sqlmodel import SQLModel
from mint.db.models import BaseWithUUID
from mint.db.asynk import EntityRepository

class Job(BaseWithUUID, table=True):
    name: str

class JobCreate(SQLModel):
    name: str

class JobUpdate(SQLModel):
    name: str | None = None

class JobRepository(EntityRepository[Job, UUID]):
    Schema = Job
```

- `BaseWithUUID`/`BaseWithIntID`/`BaseWithStrID` are concrete base classes
  with an `id` field already declared with the right type and default —
  pick the one matching your table's key. For anything else (a snowflake
  ID, a `NewType`-wrapped primitive, a composite key), subclass `Base`
  directly and declare `id` yourself; `EntityRepository[T, I]` only
  requires `T: Base`, not one of the three convenience bases.
- Create/Update payloads are separate, lightweight `SQLModel` classes —
  never the table class itself. `Create` excludes system-generated fields
  (`id`, timestamps); `Update` makes every field `Optional` to support
  partial updates.
- `Schema = Job` is the one required class attribute on every repository
  subclass.

## Zero-boilerplate CRUD

```python
repo = JobRepository(db)                       # db: mint.db.asynk.Database
job = await repo.create(JobCreate(name="x"))
await repo.get(job.id)
await repo.update(job.id, JobUpdate(name="y"))
await repo.remove(job.id)
```

No session setup, no method overrides — a bare `EntityRepository[T, I]`
subclass with only `Schema` set delivers full CRUD. This is the
acceptance test for the whole layer.

### `EntityRepository[T, I]` methods

All of these require a schema with a single-column `id` — a schema
without one (a composite-key join table, a materialized view) uses
`RepositoryBase[T]` directly instead (see "Real-world schema shapes"
below).

- **`get(id_: I) -> T | None`** — fetch by ID, `None` if not found (or
  excluded by scoping — see below).
- **`update(id_: I, model: SQLModel) -> T`** — partial update via
  `model.model_dump(exclude_unset=True)`; fields not set on `model` are
  left untouched. An empty payload (nothing set) short-circuits to `get()`
  instead of issuing an invalid `UPDATE ... SET` with no columns. Raises
  `NotFoundError` if `id_` doesn't match a row.
- **`remove(id_: I) -> T`** — hard-delete, returns the deleted row. Raises
  `NotFoundError` if `id_` doesn't match a row. (`SoftDeleteMixin`
  overrides this — see "Soft-delete scoping" below.)
- **`remove_many(ids: Sequence[I]) -> Sequence[T]`** — hard-delete every
  matching row, returns whichever were actually deleted (no error for IDs
  that don't match).
- **`get_many_by_ids(ids: Sequence[I]) -> Sequence[T]`** — fetch every
  matching row.

## Reading: pagination, counting

```python
page = await repo.get_many(skip=0, limit=10)          # Sequence[Job]

result = await repo.get_many_page(skip=0, limit=10)    # PaginatedResult[Job]
result.items   # this page
result.total   # total matching rows across every page, not just this one

total = await repo.count()                             # int
```

- **`get_many(*, skip=0, limit=10) -> Sequence[T]`** — a plain paginated
  `SELECT ... OFFSET ... LIMIT ...`. There is no dict-based filter DSL
  (mini's `StatementBuilder` was deliberately dropped — stringly-typed,
  barely used in practice); anything more specific than pagination is a
  typed custom method built with `execute()`/`execute_many()` (below).
- **`get_many_page(*, skip=0, limit=10) -> PaginatedResult[T]`** — the
  page *and* the total matching-row count from a **single query**, via a
  `COUNT(*) OVER()` window function alongside the page `SELECT`, instead
  of the common two-round-trip page-query-plus-count-query pattern. Falls
  back to one plain `count()` call only when the requested page comes
  back empty (`skip` past the last row — there's no row left to carry the
  window-function total on).
- **`count() -> int`** — the total row count for this schema
  (scoping-filtered). Raises `OperationalError` if the count query somehow
  returns no value — unreachable under normal Postgres usage (`COUNT`
  always returns exactly one row); this is defensive, not something a
  caller needs to handle.

## Custom queries and eager-loading

Every built-in method is implemented in terms of two primitives available
to any subclass:

```python
from sqlmodel import col, select

stmt = select(Job).where(col(Job.name).startswith("prod-"))
jobs = await repo.execute_many(stmt)

stmt = select(Job).where(col(Job.id) == job_id)
job = await repo.execute(stmt)   # T | None
```

- **`execute(stmt) -> T | None`** — for a statement expected to return
  zero or one row (`select`/`insert`/`update`/`delete`, all with
  `.returning(Schema)` where relevant).
- **`execute_many(stmt, *, unique=False) -> Sequence[T]`** — for a
  statement expected to return any number of rows. Pass `unique=True`
  whenever the statement uses `joinedload()` on a collection relationship
  (a join fans out one row per child, and duplicates need collapsing
  before `.scalars()`).
- Both commit automatically when `self.auto_commit` is `True` **and** the
  statement isn't a plain `Select` — reads never commit, regardless of
  `auto_commit`.
- **`sqlmodel.col()`** is needed on any field used as a query expression
  (`col(Job.id) == x`, `col(Job.name).startswith(...)`) — SQLModel types
  class attributes as their plain Python field type for pydantic's sake,
  not as a comparable column, so a type checker needs the `col()` wrap. At
  runtime it's a no-op.

Because `execute`/`execute_many` accept **any** statement, eager-loading a
relationship is just `.options(...)` on the statement you already
control — there's no separate repository-level "prefetch paths"
mechanism:

```python
from sqlalchemy.orm import selectinload

stmt = (
    select(Job)
    .where(col(Job.id) == job_id)
    .options(selectinload(Job.collection))
)
job = await repo.execute(stmt)
```

`selectinload()` resolves the relationship for the *entire* result set in
one extra query; `joinedload()` resolves it in one JOIN. Either way it's
one additional query total, never a per-row fetch loop, and nothing loads
unless requested — a self-referential relationship (a folder tree) can't
cause runaway recursive loading.

## Creating: plain insert and upsert

```python
job = await repo.create(JobCreate(name="x"))
```

**`create(model, *, upsert=False, conflict_columns=None) -> T`**:

- `upsert=False` (the default): a plain `INSERT ... RETURNING`. Raises
  `OperationalError` if no row comes back (an infrastructure-level
  anomaly, not a domain condition) and fails at the database level
  (`IntegrityError`) on a real constraint conflict — it does not resolve
  conflicts.
- `upsert=True`: insert-or-update in one round trip (Postgres `INSERT ...
  ON CONFLICT (<target>) DO UPDATE SET <merged fields> RETURNING ...`).
  The returned row is always the final state — freshly inserted, or the
  existing row merged with the new values — never `None` for the
  row-already-exists case:

  ```python
  from uuid import uuid4

  job_id = uuid4()
  await repo.create(JobUpsertCreate(id=job_id, name="x"), upsert=True)
  # same id again: updates in place instead of failing on the conflict
  updated = await repo.create(JobUpsertCreate(id=job_id, name="y"), upsert=True)
  assert updated.name == "y"
  ```

- **`conflict_columns`**: the `ON CONFLICT (...)` target. Defaults to
  `Schema`'s primary key columns when omitted — this works whether that's
  a single `id` column or a composite key. Pass explicit column names to
  upsert against a different unique constraint instead:

  ```python
  await repo.create(
      SlugCreate(tenant_id=tenant_id, slug="hello", name="x"),
      upsert=True,
      conflict_columns=["tenant_id", "slug"],
  )
  ```

- **Edge case**: if every field being inserted is part of the conflict
  target (a pure composite-PK join table with no other columns), there's
  nothing left for `DO UPDATE SET` — handled internally by setting those
  columns to themselves (a no-op update) purely so `RETURNING` still
  fires and the existing row comes back.
- Postgres-specific (`sqlalchemy.dialects.postgresql.insert`), consistent
  with the rest of this layer already being Postgres-only.

## Soft-delete scoping

```python
from mint.db.models import IsDeletedMixin
from mint.db.asynk import SoftDeleteMixin

class Doc(IsDeletedMixin, BaseWithUUID, table=True):
    name: str

class DocRepository(SoftDeleteMixin[Doc, UUID]):
    Schema = Doc

doc = await DocRepository(db).create(DocCreate(name="x"))
await DocRepository(db).remove(doc.id)          # marks is_deleted=True, doesn't delete the row
await DocRepository(db).get(doc.id)             # None — excluded automatically

# any hand-written query is scoped too, with zero scoping code in it:
async def find_by_name(self, name: str) -> Doc | None:
    return await self.execute(select(Doc).where(col(Doc.name) == name))
# a soft-deleted Doc never comes back from this either
```

- Composing `IsDeletedMixin` onto the schema is what triggers scoping —
  it's read by the session-level scoping listener, not by which
  repository mixin you use. `SoftDeleteMixin` only changes what
  `remove()`/`remove_many()` *do* (soft- instead of hard-delete); the
  read-side filtering is unconditional and independent of it.
- This applies to **every** query through a scoped session, including
  hand-written custom methods with no scoping code at all — the entire
  point (see "Design philosophy" above).

## Owner scoping

```python
from mint.db.models import OwnerMixin
from mint.db.asynk import EntityRepository, ResourceOwnerMixin

class Job(OwnerMixin, BaseWithUUID, table=True):
    name: str

class JobRepository(ResourceOwnerMixin[Job], EntityRepository[Job, UUID]):
    Schema = Job

repo = JobRepository(db, owner=current_user)
await repo.get_many()   # every row scoped to current_user, incl. custom queries
```

`current_user` must satisfy `IOwner`: an `id` attribute and an
`is_scoped` property. When `is_scoped` is `False` (e.g. an admin/superuser
account), every row is visible — scoping is a property of the *owner*,
not a hardcoded repository behavior.

## Scoping bypass

```python
from mint.db.models import IsDeletedMixin, OwnerMixin

stmt = (
    select(Job)
    .where(col(Job.id) == job_id)
    .execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))
    # or frozenset({OwnerMixin}), or both together
)
job = await repo.execute(stmt)
```

`mint_scope_bypass` is a `stmt.execution_options(...)` value — a
`frozenset` of the scoping mixin classes to bypass for that one
statement — and is the *only* sanctioned way to see soft-deleted or
cross-owner rows. Every other query stays scoped by default; nothing is
silently bypassed by accident.

## Unit of Work: multi-table transactions

```python
from mint.db.asynk import UnitOfWork

async with UnitOfWork(db) as uow:
    jobs = JobRepository(db, session=uow.session, auto_commit=False)
    collections = CollectionRepository(
        db, session=uow.session, auto_commit=False, owner=current_user,
    )
    job = await jobs.create(JobCreate(name="x"))
    await collections.update(col_id, ...)
    await uow.commit()
# on any exception before commit(), or if commit() is never called:
# everything in this block rolls back together on __aexit__
```

- Repositories sharing a `UnitOfWork` session are constructed directly —
  `SomeRepository(db, session=uow.session, auto_commit=False)` — there is
  no generic `repo()` factory (one can't be typed without `Any`, which
  this codebase's coding-style rule forbids; direct construction is no
  less clear).
- **`auto_commit=False` is required** on every repository sharing the
  session — otherwise each `create()`/`update()`/`remove()` call commits
  itself immediately regardless of the shared session, defeating the
  point.
- Every simple, single-repository call outside a `UnitOfWork` block still
  needs zero setup — this is strictly an opt-in addition for the
  multi-table-atomicity case, never the default ceremony.
- **Known constraint**: don't construct two owner-scoped repositories for
  the *same schema* with *different owners* against one shared
  `UnitOfWork` session — each registers its own scoping listener and none
  are removed, so their owner criteria get silently ANDed together
  (empty results, no error). Harmless in the common case since a
  `UnitOfWork` block is short-lived and its session is discarded on exit.

## Materialized views

```python
from mint.db.models import Base
from mint.db.asynk import MaterializedViewRepository

class MVJobStats(Base, table=True):
    __tablename__ = "mv_job_stats"
    job_status: str = Field(primary_key=True)
    count: int

class JobStatsRepository(MaterializedViewRepository[MVJobStats]):
    Schema = MVJobStats

stats_repo = JobStatsRepository(db)
await stats_repo.refresh_materialized_view()   # REFRESH MATERIALIZED VIEW CONCURRENTLY
rows = await stats_repo.get_many()
```

`MaterializedViewRepository` extends `RepositoryBase` directly, not
`EntityRepository` — no `get`/`update`/`remove`, since a materialized view
can't be written to. `refresh_materialized_view()` commits only when
`self.auto_commit` is `True`, same as every other write path (so it can
participate in a `UnitOfWork` block like anything else).

## Multi-tenant schemas

```python
repo = JobRepository(db, dbschema="tenant_a")
job = await repo.create(JobCreate(name="x"))   # routed to tenant_a.job
```

`dbschema=` applies a `schema_translate_map` so every unqualified table
name resolves against that Postgres schema for the session's lifetime.
`force_session_schema()`/`refresh(instance)` exist for the case where the
map needs reapplying mid-session — **confirmed, not assumed**: a Postgres
session checks out a fresh `Connection` the first time it does work after
a commit, and per-connection options like `schema_translate_map` don't
carry over. If you commit partway through a multi-statement operation
under a `dbschema`, call `force_session_schema()` again immediately after
the commit, before any other statement (reapplying after any other
statement has already run is a silent no-op).

## Real-world schema shapes

- **No `id` column** (a composite-key join table): use `RepositoryBase[T]`
  directly instead of `EntityRepository` — `get_many()`/`count()`/
  `create()` (including `upsert=True`, which defaults its conflict target
  to the composite primary key) all still work.
- **Self-referential (tree) relationships**: use `.options(selectinload(...))`
  on a custom statement, same as any other relationship — nothing loads
  unless requested, so a folder tree or org chart can't cause runaway
  recursive loading.
- **Many-to-many**: standard SQLModel `Relationship(..., link_model=...)`
  works against the shared `Base.metadata` regardless of which module
  defines either side.

## Sync vs async

Every class and method above has a 1:1 sync mirror — `mint.db.sync`
instead of `mint.db.asynk`, `Session` instead of `AsyncSession`, no
`async`/`await`. Behavior, options, and exceptions are identical; the only
difference is the driver (`psycopg2` for sync, `asyncpg` for async).

## Designed for minimal syntax

The API surface is small on purpose:

- **One required class attribute** (`Schema`) gets a repository subclass
  full CRUD — no `__init__` override, no method stubs to fill in.
- **No context manager for the common case** — `JobRepository(db).get(id)`
  needs nothing else. `UnitOfWork` is there when multi-table atomicity is
  actually needed, not as ambient ceremony.
- **Scoping needs zero per-method code** — compose a mixin onto the
  schema once, every query (built-in or hand-written) is scoped forever
  after.
- **One bypass mechanism** (`mint_scope_bypass`), not one flag per scoping
  dimension — a future scoping dimension is a new mixin class, not a new
  named option to learn.
- **One method for insert-or-update** (`create(upsert=True)`), not a
  second method with its own name and its own mental model to remember.

## Caveats and gotchas

The condensed list — see the
[Implementation Notes](../db-repository-implementation-notes.md) for the
full write-up, including how each one was found and confirmed against
real Postgres:

- `with_loader_criteria` lambdas (used internally for scoping) must be
  true closures, not default-argument tricks — SQLAlchemy's lambda-SQL
  caching would otherwise reuse the first-seen value forever.
- `event.listen(session, "do_orm_execute", fn)` must target
  `session.sync_session` on an `AsyncSession` — it raises
  `NotImplementedError` directly on the async session.
- `Base.type_annotation_map` doesn't reach fields declared on non-`Base`
  mixins (e.g. a shared audit-timestamp mixin) — those need an explicit
  `sa_type=`/`sa_column_kwargs=` on the field itself.
- A pre-built `sa_column=Column(...)` instance on a mixin field can only
  belong to one table — a second table composing the same mixin fails
  with `Column object ... already assigned to Table ...`. Use
  `sa_type=`/`sa_column_kwargs=` instead so SQLModel builds a fresh
  `Column` per table.
- `@declared_attr`-decorated relationships raise under SQLModel's
  pydantic+SQLAlchemy metaclass — use a direct `Relationship()` on each
  table instead (one extra line per table, not one shared mixin
  declaration).
- pytest-asyncio needs a function-scoped async engine fixture, not
  session-scoped — asyncpg connections can't cross event loops.
- Prefer `isinstance()`/`issubclass()` over `hasattr()`/`getattr()` with a
  default when narrowing types in your own code that touches this layer —
  `issubclass()` doesn't work on a `Protocol` with non-method (data)
  members, so check against a concrete class there instead;
  `isinstance()` has no such restriction.
