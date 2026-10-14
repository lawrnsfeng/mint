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

- **Session isolation is per task, via a `ContextVar` on the `Database`,
  not a shared attribute.** A repository instance is safe to reuse across
  concurrent tasks/threads — no per-call cloning, no locking. This is the
  single most load-bearing design choice here: the predecessor this layer
  replaced stored its active session on a plain instance attribute,
  which raced under concurrent use. Because the slot belongs to the
  database, every repository and every scoped function on one database
  shares the session bound in the current task.
- **One transaction per outermost call.** Whatever opened the session
  commits it, once, on the way out. Nothing nested inside commits, so a
  call that fails part-way leaves nothing behind (see "Session scopes"
  below).
- **Scoping (soft-delete, owner) is unconditional, not opt-in.** Whether a
  query excludes soft-deleted or other-owner rows is decided by the
  *schema's own shape* (does it compose `IsDeletedMixin`/`OwnerMixin`) —
  never by whether the method that built the query remembered to call
  something. This closes a real gap found in a predecessor system: custom
  query methods that silently skipped an opt-in scoping call.
- **The zero-ceremony path needs no setup, ever.**
  `JobRepository(db).get(id)` opens a session, does the work, and closes
  it — no context manager, no explicit transaction. A session scope
  (`@db.scoped`) widens that to a whole function when several calls must
  succeed or fail together, and `UnitOfWork` stays available for the
  caller that wants to hold the session and commit by hand.
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
- Neither commits on its own inside a session scope: the outermost call
  commits once, on exit, when its `auto_commit` is `True` (see "Session
  scopes"). On an externally-owned session (`session=uow.session`) both
  still commit per statement when `auto_commit` is `True` and the
  statement isn't a plain `Select`.
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

## Session scopes

A session scope binds one session to the current task for the length of a
function, and everything that function calls runs on it.

```python
from sqlalchemy import insert
from mint.db.asynk import Database, scoped

db = Database("postgresql+asyncpg://user:pass@localhost/mydb")

@db.scoped                                   # any coroutine function
async def place_order(payload: OrderCreate) -> Order:
    order = await OrderRepository(db).create(payload)
    await AuditRepository(db).create(AuditCreate(order_id=order.id))
    await db.current_session.execute(insert(outbox).values(order_id=order.id))
    return order
# one session, one connection, one commit — or one rollback if anything raised
```

A class that receives its database at construction time has no `db` to
name in a decorator at class-definition time. The bare `scoped` reads it
from `self.db` instead:

```python
class OrderService:
    def __init__(self, db: Database) -> None:
        self.db = db

    @scoped
    async def place(self, payload: OrderCreate) -> Order:
        order = await OrderRepository(self.db).create(payload)
        await AuditRepository(self.db).create(AuditCreate(order_id=order.id))
        return order
```

The same thing as a block, when a decorator doesn't fit:

```python
async with db.session_scope() as session:
    ...
```

What a scope guarantees:

- **The outermost scope is the unit.** It opens the session, commits on a
  clean exit, rolls back on an exception, and closes. A scope entered
  while another is open in the same task *joins* it: same session, no
  commit, no close. Undecorated helper functions in between see the same
  session through `db.current_session`.
- **Repositories join automatically.** Every repository method already
  runs in a scope of its own; called inside yours, it joins yours. Two
  repositories in one scope share one session and one connection.
- **Nothing inside commits.** A repository's `auto_commit` only matters
  when that repository's call is the outermost one. Inside someone
  else's scope it is ignored — the scope's owner decides. Pass
  `commit=False` to `session_scope()` for a scope that never commits.
  The same goes for a repository built with `auto_commit=False` and no
  `session=`: when one of its methods is the outermost call, nothing that
  method does is committed, including writes made through other
  repositories it calls.
- **A handled database error still aborts the transaction.** When a
  statement fails, Postgres refuses every later statement in that
  transaction and turns the final `COMMIT` into a silent `ROLLBACK`.
  Catching the exception does not undo that. A scope that reaches its end
  in this state raises `ScopeAbortedError` rather than report a commit
  that did not happen. To survive an expected failure — a duplicate key,
  say — wrap that one statement in a savepoint:
  `async with db.current_session.begin_nested(): ...`.
- **Each repository keeps its own scoping.** Soft-delete and owner
  filters apply to the statements a repository's own methods run. Two
  repositories for one table with different owners can share a scope
  without seeing through each other's filter.
- **A custom repository method is atomic.** A method decorated with
  `ensure_session` that calls `self.create()` twice commits both rows
  together, or neither. (Before 0.4 each write committed separately.)
- **The connection is checked out lazily.** A scope takes a connection at
  its first statement and holds it until the outermost scope exits. Work
  that needs no database belongs before the first statement, or outside
  the scope.
- **One scope, one task.** A session is not safe for concurrent use, and
  tasks started inside a scope inherit its session. Don't `gather()` or
  open a `TaskGroup` inside a scope — fan out at the top level and let
  each task open its own. A task that is still running after the scope
  that started it has exited is the exception: the scope is gone, so the
  task's next call opens a scope of its own.
- **One scope, one `dbschema`.** `session_scope(dbschema="tenant_a")`
  routes the whole scope to that schema. A repository with a different
  `dbschema` joining it raises `ScopeSchemaMismatchError` rather than
  silently querying the wrong tenant. One with no `dbschema` joins as is,
  which means its unqualified table names resolve in the scope's schema
  too — give a table that lives outside the tenant schemas an explicit
  `schema=` in its definition.

### Keep network calls out of a scope

A scope holds a pooled connection from its first statement to its exit. A
slow remote call in the middle holds that connection, and its open
transaction, for as long as the remote takes. Nothing in mint can detect
this — it is a matter of call structure. Resolve first, then transact:

```python
# Wrong: the connection waits on the network
@db.scoped
async def assign_quota(account_id: UUID) -> None:
    account = await AccountRepository(db).get(account_id)
    quota = await quota_client.fetch(account.plan)      # connection held here
    await QuotaRepository(db).create(QuotaCreate(account_id=account_id, limit=quota))

# Right: two short scopes around an unscoped remote call
async def assign_quota(account_id: UUID) -> None:
    account = await AccountRepository(db).get(account_id)        # scope 1
    quota = await quota_client.fetch(account.plan)               # no connection held
    await QuotaRepository(db).create(QuotaCreate(account_id=account_id, limit=quota))  # scope 2
```

When the remote call must happen *because of* a write, and the two must
not drift apart, don't stretch the transaction over it. Write the intent
to an outbox table in the same scope as the business write and deliver it
from a separate worker — the
[transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html).
mint deliberately offers no primitive for suspending a transaction across
a network call.

## Timeouts, pool exhaustion, retries, savepoints

### Timeouts

```python
from mint.db.settings import DatabaseSettings

db = Database(uri, settings=DatabaseSettings(STATEMENT_TIMEOUT_MS=5_000, POOL_TIMEOUT=10))
```

- **`STATEMENT_TIMEOUT_MS`** (`SQLALCHEMY_STATEMENT_TIMEOUT_MS`) sets
  Postgres' `statement_timeout` on every connection the engine opens. A
  statement that overruns is cancelled by the server and surfaces as a
  `sqlalchemy.exc.DBAPIError` with SQLSTATE `57014`. Default `None`: no
  limit.
- **`POOL_TIMEOUT`** (`SQLALCHEMY_POOL_TIMEOUT`) is how many seconds a
  call waits for a free pooled connection. Default `30`.
- Both apply only when `Database` builds the engine itself from a URI. An
  engine passed as `engine=` keeps whatever it was created with.

### Pool exhaustion

When every pooled connection is checked out and none comes back within
`POOL_TIMEOUT`, the waiting call raises `mint.db.exc.PoolExhaustedError`
instead of SQLAlchemy's generic `TimeoutError`. It carries the pool's own
counters at that moment — `pool_size`, `overflow`, `checked_out` — and
the `timeout` that ran out. The usual cause is not a pool that is too
small but scopes that are too long — see "Keep network calls out of a
scope".

### Retrying a scope

```python
@db.scoped(retries=2)            # or @scoped(retries=2) on a method
async def transfer(source: UUID, target: UUID, amount: int) -> None:
    ...
```

`retries` re-runs the *whole function* in a fresh session when its scope
fails with a transient error: a serialization failure (`40001`), a
deadlock (`40P01`), or a connection SQLAlchemy reports as lost before the
commit. Waits between attempts back off exponentially with jitter. After
the last attempt the original database error is raised.

- **The function runs again from the top.** Everything in it must be safe
  to repeat — no counters bumped in memory, no messages sent, no remote
  calls with side effects. This is one more reason to keep network calls
  out of a scope.
- **Only the outermost scope retries.** A retrying function called inside
  another scope joins it and fails on the first error: the transaction
  belongs to the outer scope, and only re-running the outer function can
  replay it. Put `retries` on the outermost one.
- **A connection lost while committing is not retried.** The server may
  already have applied the transaction, and running the function again
  could apply it twice. The scope raises `CommitOutcomeUnknownError`
  instead; whether to retry is a decision for code that can check what
  was written.
- Anything else — a constraint violation, a cancelled statement, an
  application exception — is never retried.
- `db.run_in_scope(call, retries=2)` is the same thing without a
  decorator, for a zero-argument callable.
- Both decorators and `run_in_scope` also take `dbschema=`, to open the
  scope for a tenant schema. A decorator argument is fixed when the
  function is defined; for a tenant only known per request, use
  `db.run_in_scope(call, dbschema=tenant)` or
  `async with db.session_scope(dbschema=tenant)`.

### Savepoints

Nothing inside a scope commits, so a SQLAlchemy savepoint holds for the
length of its block. There is no mint wrapper; use the session's own:

```python
@db.scoped
async def import_rows(rows: list[RowCreate]) -> None:
    for row in rows:
        try:
            async with db.current_session.begin_nested():
                await repo.create(row)
        except IntegrityError:
            continue             # this row is rolled back, the rest stand
```

## Using mint.db without SQLModel

The session lifecycle — `Database`, session scopes, `UnitOfWork` — needs
SQLAlchemy only. A service that builds its statements with SQLAlchemy Core
and has no use for the repository classes installs `mint[db]` (not
`mint[orm]`) and never imports SQLModel:

```python
from sqlalchemy import func, insert, select
from mint.db.asynk import Database, scoped

class NoteStore:
    def __init__(self, db: Database) -> None:
        self.db = db

    @scoped
    async def add(self, body: str) -> int:
        session = self.db.current_session
        await session.execute(insert(notes).values(body=body))
        result = await session.execute(select(func.count()).select_from(notes))
        return result.scalar_one()
```

- Importing `mint.db.asynk` or `mint.db.sync` loads the lifecycle names
  only. `RepositoryBase`, `EntityRepository`, the mixins and
  `MaterializedViewRepository` are imported from the same package paths
  as before, on first access — that is the moment SQLModel is required.
- Soft-delete and owner scoping are repository features, applied through
  an ORM session event. A Core statement executed on the session is not
  filtered by them.
- There is no generic repository for Core tables, on purpose: a Core
  service's own query functions already are that layer.

## Unit of Work: multi-table transactions

A session scope covers most multi-table work with less ceremony.
`UnitOfWork` is for the caller that wants to hold the session explicitly
and decide when to commit:

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
  A session scope does not have this constraint.
- A `UnitOfWork` opened inside a session scope does not join it: it opens
  its own session on a second connection.

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
statement has already run is a silent no-op). Inside a session scope
nothing commits until the scope exits, so this only arises on a session
you commit by hand — a `UnitOfWork`, or `create_session()`.

## Not planned

Left out on purpose. Each would widen what this layer has to get right
for every consumer, for the benefit of few:

- **Read replicas, sharding, coordination across several databases.** One
  `Database` is one engine. A service that needs two constructs two and
  keeps their scopes apart.
- **A repository layer for SQLAlchemy Core tables.** See "Using mint.db
  without SQLModel".
- **Suspending a transaction across a network call.** See "Keep network
  calls out of a scope".
- **Detecting a network call inside a scope.** Socket guards and event
  hooks all leak. It stays a convention, and a review rule.

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
  needs nothing else. One decorator (`@db.scoped`) is there when several
  calls must be one transaction, not as ambient ceremony.
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
