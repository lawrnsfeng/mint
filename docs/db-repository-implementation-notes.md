# DB Repository Layer Implementation Notes

`mint.db` is a generic, SQLModel-based repository layer for Postgres,
async (`mint.db.asynk`) and sync (`mint.db.sync`), ported from an internal
predecessor (`mini.repos`) and hardened against a real concurrency bug found
during code review. This page is the permanent reference for *why* it's
built the way it is — the design decisions, the bugs found and fixed along
the way (including ones only a real Postgres integration test caught), and
the patterns other developers should follow when adding a new table.

## mini vs. mint: what changed and why

| Area | mini's approach | mint's approach | Why |
|---|---|---|---|
| Session isolation | `self._session` instance attribute, set by `ensure_session` | Per-repository-instance `ContextVar[Session \| None]` | mini's attribute could be silently overwritten by a second concurrent task/thread sharing one repository instance — a live data-integrity bug, not hypothetical. `ContextVar` gives each task/thread its own isolated slot for free, mirroring `mint/fs/asynk/s3.py`'s `_client_ctx`. |
| Exception cleanup | Session reset (`self._session = None`) sat *after* the `async with` block | Reset happens in a `finally` around the `ContextVar` token | mini left a stale/closed session reference on the instance if the wrapped call raised. `ContextVar.reset()` in `finally` always runs. |
| Scoping (soft-delete, owner) | Opt-in `self.restrain(stmt)`, called manually per method | Unconditional `with_loader_criteria` + `do_orm_execute` session event | Reading a real mini consumer's repository code found multiple custom query methods that silently skipped `restrain()` — a confirmed, live correctness gap. The new mechanism applies to every `session.execute()` call against a scoped session, including hand-written custom queries, because it isn't a per-method choice anymore. |
| Schema/model layer | Separate SQLAlchemy `DeclarativeBase` schema + pydantic domain model, glued by `modelize()` | One SQLModel `table=True` class per table | Removes the hand-maintained glue layer. Relationship eager-loading goes through `.options(selectinload(...))`/`joinedload(...)` on the caller's own statement — see "Relationship eager-loading" below. |
| Generic ID base classes | `BaseWithID[T]` (SQLAlchemy declarative generics) | Concrete `BaseWithUUID`/`BaseWithIntID`/`BaseWithStrID` at the schema layer; `EntityRepository[T: Base, I]` at the repository layer, `I` unconstrained | SQLModel's metaclass requires every base to be a pydantic model, and pydantic's own generic-model support for this shape is documented as unreliable (fastapi/sqlmodel#211, pydantic#4171). Confirmed independently: a real mini consumer's schemas already redeclared `id` concretely on every leaf table despite inheriting a generic base — the substitution was never actually load-bearing. `EntityRepository`'s `T`/`I` bound was itself widened in Phase 2 — see below. |
| Filter DSL | `StatementBuilder` — dict-based `$in`/`$like`/`$lt` query language | Dropped | Used in ~2 of 36 files in a real consumer's repository layer; stringly-typed (relationship paths as dotted strings resolved at runtime), which cuts against `ty`-checking everything for negligible real payoff. `get_many()` is now a plain paginated `select`; anything more specific is a typed custom method — which is what that codebase already does almost everywhere. |
| Multi-op transactions | Pass a `session=` into the constructor, ad hoc | `UnitOfWork` — a named, documented primitive | The zero-ceremony auto-session path stays the default (wrapping every simple call in a unit-of-work block was explicitly rejected as an indentation/boilerplate tax); `UnitOfWork` is what a caller reaches for specifically when they need multi-repository atomicity. |
| Create/Update payloads | Separate pydantic `BaseModel` layer already existed | Separate, lightweight non-table SQLModel companions (`JobCreate`, `JobUpdate`) | Even with schema+read-model collapsed, create/update need different validation than the full record — `Create` must reject server-generated fields, `Update` needs every field `Optional` for `exclude_unset` partial updates. |
| Generic ID type binding *(mint-internal, Phase 2)* | `EntityRepository[T: BaseWithUUID \| BaseWithIntID \| BaseWithStrID, I: (int, str, UUID)]` | `EntityRepository[T: Base, I]` — `I` unconstrained | A closed three-primitive union can't express a real custom ID type (snowflake ID, `NewType`-wrapped primitive). Widened after confirming Python has no mechanism (no higher-kinded types) to *infer* `I` from `T.id` instead — see research.md, "Generic ID type". One documented `cast()` in a new `_id_column` property replaces five untyped `self.Schema.id` accesses. |
| Instance/session-reconstruction *(mint-internal, Phase 2)* | `clone()`/`_construction_kwargs()` | Removed | Existed to cheaply reconstruct a repository with identical config — a workaround, in mini, for a shared instance's session racing under concurrent use. Root-caused and fixed by Phase 1's `ContextVar` session isolation; nothing in `mint/db` still needed it. |
| Anomalous-result exception *(mint-internal, Phase 2)* | `AbnormalResultError` | `OperationalError` (existing, previously-unused) | A query returning no row on `count()`/`create()` (e.g. an `ON CONFLICT DO NOTHING` upsert) is an infrastructure-level "should never happen" condition, matching `OperationalError`'s existing role in `mint.fs`'s `s3.py`/`abs.py` — not a domain error worth its own bespoke type. |
| Scoping bypass option *(mint-internal, Phase 2)* | Two booleans: `mint_include_deleted`, `mint_skip_owner_scope` | One `mint_scope_bypass: frozenset[type]` (of scoping mixin classes) | A future scoping dimension means adding a mixin class and a check, not a third named boolean constant. |
| Scoping listener type checks *(mint-internal, Phase 2)* | `hasattr(schema, "is_deleted")`, `getattr(self, "owner", None)` | `issubclass(schema, IsDeletedMixin)`, `isinstance(self, IScopedRepository)` | Real type narrowing over duck-typed attribute probing — now a project rule (`.claude/rules/coding-style.md`, "Type narrowing over hasattr/getattr"). `issubclass()` can't be used against a `Protocol` with data members (`TypeError`), which is why the schema check targets concrete mixin classes rather than a `HasSoftDelete`-style Protocol (removed). |

## Zero-boilerplate CRUD

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

repo = JobRepository(db)                       # db: mint.db.asynk.Database
job = await repo.create(JobCreate(name="x"))
await repo.get(job.id)
await repo.update(job.id, JobUpdate(name="y"))
await repo.remove(job.id)
```

No session setup, no method overrides — this is the acceptance test for the
whole layer.

## Owner scoping

```python
from mint.db.models import AuditMixin, BaseWithUUID, OwnerMixin
from mint.db.asynk import EntityRepository, ResourceOwnerMixin

class Job(OwnerMixin, AuditMixin, BaseWithUUID, table=True):
    name: str

class JobRepository(ResourceOwnerMixin[Job], EntityRepository[Job, UUID]):
    Schema = Job

repo = JobRepository(db, owner=current_user)
await repo.get_many()   # scoped to current_user, including hand-written custom queries
```

`current_user` satisfies `IOwner`: an `id` and an `is_scoped` property. When
`is_scoped` is `False` (e.g. an admin), every row is visible.

## Multi-table atomic transaction

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
# no commit() call (or an exception before it): everything rolls back together
```

`auto_commit=False` is required on every repository sharing the unit of
work's session — otherwise each `create()`/`update()`/`remove()` call
commits itself immediately regardless of the shared session, defeating the
point of the transaction. This is real, verified behavior (caught by a
failing test during implementation, not a theoretical footgun): the default
`auto_commit=True` exists for the zero-ceremony path, and a caller opting
into `UnitOfWork` must explicitly opt out of it too.

## Paginated page + total count in one query

```python
result = await repo.get_many_page(skip=0, limit=20)
result.items   # Sequence[Job] — the page
result.total   # int — total matching rows, not just this page's length
```

One query (`func.count().over()` alongside the page `SELECT`) instead of a
separate page query plus a separate `count()` call — see
specs/002-db-repository-layer/research.md, "Paginated page + total count in
one query". Falls back to one plain `count()` call only when the requested
page is empty (`skip` past the last matching row, so there's no row left
to carry the window-function total on).

## Upsert support

```python
from uuid import UUID, uuid4
from sqlmodel import SQLModel
from mint.db.models import BaseWithUUID
from mint.db.asynk import EntityRepository

class Job(BaseWithUUID, table=True):
    name: str

class JobUpsertCreate(SQLModel):
    id: UUID
    name: str

class JobRepository(EntityRepository[Job, UUID]):
    Schema = Job

repo = JobRepository(db)
job_id = uuid4()

created = await repo.create(JobUpsertCreate(id=job_id, name="x"), upsert=True)
# same id again: updates the existing row in place instead of failing
updated = await repo.create(JobUpsertCreate(id=job_id, name="y"), upsert=True)
assert updated.id == created.id
assert updated.name == "y"
```

`create(model, *, upsert=False, conflict_columns=None)`:

- `upsert=False` (the default): unchanged from Phase 1/2 — a plain
  `INSERT ... RETURNING`. Fails at the database level (`IntegrityError`)
  on a real conflict, exactly like before. Zero behavior change for
  existing callers.
- `upsert=True`: builds a single Postgres statement —
  `INSERT ... ON CONFLICT (<target>) DO UPDATE SET <merged fields>
  RETURNING ...` — via `sqlalchemy.dialects.postgresql.insert`. One round
  trip; the returned row is always the final state (freshly inserted, or
  the existing row merged with the new values from `model`), never `None`
  for the row-already-exists case.
- `conflict_columns`: the `ON CONFLICT (...)` target. Defaults to
  `Schema`'s primary key columns when omitted (works whether that's a
  single `id` column or a composite key — see
  `test_composite_key_table_upsert_with_no_extra_columns` in
  `tests/db/asynk/test_schema_shapes.py` for the composite-PK case,
  `TWidgetLabelLink`). Pass explicit column names to upsert against a
  different unique constraint instead:

  ```python
  await repo.create(
      SlugCreate(tenant_id=tenant_id, slug="hello", name="x"),
      upsert=True,
      conflict_columns=["tenant_id", "slug"],
  )
  ```

- **Edge case: nothing left to update.** If every dumped field is part of
  the conflict target (a pure composite-PK join table with no other
  columns, e.g. `TWidgetLabelLink`), there's nothing to put in `DO UPDATE
  SET` once the conflict columns are excluded — Postgres's
  `on_conflict_do_update()` rejects an empty `set_`. Handled by setting the
  conflict-target columns to themselves (a no-op update) purely so
  `RETURNING` still fires and the existing row comes back.
- Postgres-specific (`sqlalchemy.dialects.postgresql.insert`) — consistent
  with the rest of `mint.db`'s existing Postgres-only surface (`REFRESH
  MATERIALIZED VIEW CONCURRENTLY`, `schema_translate_map`), no new
  portability constraint.
- Why not a separate `upsert()` method: see
  specs/002-db-repository-layer/research.md, "Upsert support" —
  extending `create()` keeps one method and one call site for both the
  plain-insert and insert-or-update cases, opted into per call rather than
  requiring a caller to choose between two methods up front.

## Gotchas and institutional knowledge

These are the kinds of things that would otherwise live only in a docstring
buried in one call site, or get rediscovered the hard way. All of the ones
below marked **(confirmed)** were caught by writing this port's own
integration tests against real Postgres — not carried forward from mini's
docs, found fresh.

- **(confirmed) `with_loader_criteria` lambdas must be true closures, not
  default-argument tricks.** The owner-scoping criterion must be written
  `lambda cls: cls.created_by_user_id == owner_id` (closing over `owner_id`
  from the enclosing scope), not `lambda cls, _owner_id=owner_id: ...`.
  SQLAlchemy's lambda-SQL caching tracks closure cells to rebind the value
  on each call; a same-shaped lambda using a baked-in default argument gets
  its *first-seen* value reused for every later call — silently scoping
  every subsequent owner to whichever owner ran first. This shipped broken
  in an early draft of this port and was only caught by a test with two
  different owners running in sequence within one test process — a
  single-owner test does not reveal it.
- **(confirmed) `do_orm_execute` events don't attach to `AsyncSession`
  directly.** `event.listen(session, "do_orm_execute", fn)` raises
  `NotImplementedError` on an `AsyncSession`; it must target
  `session.sync_session`. Sync `Session` doesn't have this restriction.
- **(confirmed) `Base.type_annotation_map` doesn't reliably reach fields
  declared on non-`Base` mixins.** `AuditMixin`'s `created_at`/`updated_at`
  came back as `TIMESTAMP WITHOUT TIME ZONE` in Postgres despite `Base`
  declaring `type_annotation_map = {datetime: DateTime(timezone=True)}`,
  because `AuditMixin` is a plain `SQLModel` mixin, not a `Base` subclass.
  Fixed with an explicit `sa_type=DateTime(timezone=True)` on the field
  itself. `type_annotation_map` still works for fields declared directly on
  a `Base` subclass — the gap is specifically for mixin-declared fields.
- **(confirmed, Phase 2) A pre-built `sa_column=Column(...)` instance on a
  mixin field can only be used by one table — a second table composing the
  same mixin fails.** `AuditMixin` originally used
  `Field(sa_column=Column(DateTime(timezone=True), server_default=func.now()))`
  — that `Column(...)` call runs once, at `AuditMixin`'s class-body
  evaluation time (module import), producing one `Column` object shared by
  *every* table that composes the mixin. A `Column` can only be owned by
  one `Table` at a time, so the second table to compose `AuditMixin` in a
  process failed with `sqlalchemy.exc.ArgumentError: Column object
  'created_at' already assigned to Table '<the first table>'`. Not caught
  by Phase 1's own tests, which only ever had one `AuditMixin`-composing
  table (`TJob`) in the whole suite — surfaced by this port's own Phase 2
  4+ mixin stress test, the second table to compose `AuditMixin`. Fixed by
  switching to `Field(sa_type=DateTime(timezone=True),
  sa_column_kwargs={"server_default": func.now()})`, which lets SQLModel
  build a fresh `Column` per table instead of reusing one shared instance.
  `sa_type` accepts a configured `TypeEngine` instance
  (`DateTime(timezone=True)`) at runtime, matching how SQLAlchemy's own
  `Column()` (which it forwards to) has always accepted parameterized
  column types — but SQLModel's `Field()` stub declares `sa_type:
  type[Any]`, a class only. A genuine third-party typing gap (no code
  change on our side makes this typed without dropping `timezone=True`
  entirely), so `mint/db/models.py` carries the one narrowly-scoped
  `# ty: ignore[invalid-argument-type]` this port needs, on exactly the
  two `sa_type=` lines the gap forces.
- **(confirmed) `sqlmodel.col()` is required on class-attribute query
  expressions.** `Job.id == x` / `Job.id.in_(ids)` fail `ty` because
  SQLModel exposes class attributes typed as their plain pydantic field
  type (`UUID`, `str`, ...) for pydantic's sake, not as
  `InstrumentedAttribute`. `col(Job.id) == x` re-types it correctly; it's a
  runtime no-op (an `isinstance` sanity check, then returns the same
  object). `mint/db`'s own internals (`EntityRepository`) no longer use
  `col()` for the `id` column specifically — since Phase 2's ID-type
  widening, `self.Schema.id` doesn't type-check directly against `T: Base`
  at all (`Base` doesn't declare `id`), so `EntityRepository` instead
  exposes one `_id_column: InstrumentedAttribute[I]` property doing a
  single documented `cast()`, used at every internal call site instead of
  five separate `col()`-wrapped accesses. `col()` is still exactly the
  right tool for a *consuming app's own* custom query methods, and for a
  relationship attribute passed to `selectinload()`/`joinedload()`
  (`cast("QueryableAttribute[...]", Model.relationship)` is the pattern
  used in this port's own tests for that case specifically, since `col()`
  itself is typed only for column, not relationship, attributes).
- **(confirmed) `@declared_attr`-decorated relationships raise under
  SQLModel.** mini's real-world consumer apps use `declared_attr` to give a
  reusable mixin a concrete owner relationship shared across many tables.
  Under SQLModel's pydantic+SQLAlchemy metaclass, this raises at
  class-construction time — first a pydantic `PydanticUserError` ("non-annotated
  attribute"), and even after bypassing that via
  `model_config = {"ignored_types": (declared_attr,)}`, a second,
  SQLAlchemy-side `ArgumentError` ("typing annotation is required"). A
  **direct** (non-`declared_attr`) `Relationship()` on each table works
  cleanly and is the recommended pattern — one extra relationship line per
  table instead of one shared mixin declaration. `hybrid_property` hits the
  same first-stage pydantic error and needs the same
  `model_config = {"ignored_types": (hybrid_property,)}` fix, but does *not*
  hit the second-stage SQLAlchemy error — it works fully once that's added.
- **(confirmed) pytest-asyncio needs a function-scoped async engine, not
  session-scoped.** A session-scoped `AsyncEngine` fixture broke on the
  *second* test to use it (`InterfaceError: cannot perform operation:
  another operation is in progress`) — pytest-asyncio gives each test its
  own event loop by default, and asyncpg connections cannot be reused
  across event loops. The Postgres *container* stays session-scoped (slow
  to start); the *engine* is recreated (cheap) per test function.
- **(confirmed, Phase 2) `session.commit()` drops multi-tenant schema
  scoping unless reapplied.** Originally institutional knowledge from a
  real mini consumer, on an unspecified driver — now directly verified
  against this stack via a dedicated spike test
  (`test_schema_translate_map_lost_after_commit_without_reforce` in both
  `tests/db/asynk/test_base_edge_cases.py` and its sync mirror): open a
  session with `dbschema=` set, commit mid-session, then query an
  unqualified table name *without* reapplying — the query silently
  resolves against the default (`public`) schema instead of erroring.
  This is why `force_session_schema()`/`refresh()` keep their
  reapply-on-refresh logic; it's load-bearing, not defensive-but-unneeded
  code. For any multi-statement operation spanning a commit under
  `force_session_schema()`, call `force_session_schema()` again
  immediately after the commit, before any other statement.
- **(confirmed, Phase 2) Reapplying `schema_translate_map` must happen
  before any other statement on the new transaction, or it silently
  no-ops.** Once a `Session` checks out a `Connection` for a transaction —
  which happens on the *first* statement of that transaction, not only on
  `force_session_schema()` calls — a further
  `session.connection(execution_options=...)` call is a silent no-op
  (`SAWarning: Connection is already established for the given bind;
  execution_options ignored`), not an error. `refresh()`'s existing
  implementation already satisfies this (`force_session_schema()` is
  called immediately, before touching the session again), but a
  hand-written multi-statement sequence that runs even one other query
  after a commit before reapplying will silently keep using the wrong
  schema.
- **No `id` column → `RepositoryBase`, not `EntityRepository`.** Composite-key
  join tables and materialized views don't have a single-column identifier;
  `EntityRepository`'s `get`/`update`/`remove`/`get_many_by_ids` assume one.
  Use `RepositoryBase[T]` directly for these — `get_many()`/`count()`/`create()`
  still work.
- **`execute_many(unique=True)`** is needed whenever a query uses
  `joinedload` on a collection relationship — duplicate parent rows from the
  join must be deduplicated before `.scalars()`.
- **Relationship eager-loading: `.options(selectinload(...))`/
  `joinedload(...)` on the caller's own statement, not a repository-level
  mechanism.** Phase 1 shipped `extra_paths` (a constructor param, walked
  via `awaitable_attrs` with one `await` per object per relationship
  segment) — removed in Phase 2: a real N+1 round-trip/fan-out shape,
  fine at test scale but wrong at production scale (thousands to millions
  of rows), whether walked serially (slow) or `asyncio.gather`-ed across
  every object at once (unbounded concurrent fan-out). The replacement is
  strictly better and needs no `mint/db` code: add
  `.options(selectinload(Schema.relationship))` (or `joinedload(...)`)
  directly to the `select`/statement passed to `execute()`/
  `execute_many()` — SQLAlchemy resolves it as one extra query
  (`selectinload`) or one JOIN (`joinedload`) for the *entire* result set,
  regardless of row count. Still opt-in (nothing loads unless requested),
  so a self-referential relationship (a folder tree) still can't cause
  unbounded recursive loading. Prefer `lazy="selectin"` on the schema
  itself for relationships that are cheap and *always* needed — those load
  with zero call-site involvement at all. For a relationship (not column)
  attribute passed to `selectinload()`/`joinedload()`, `ty` needs
  `cast("QueryableAttribute[...]", Model.relationship)` — `col()` is typed
  only for column attributes, not relationships (see the `col()` gotcha
  above); see `tests/db/asynk/test_base_edge_cases.py`/`test_schema_shapes.py`
  for the pattern.
- **Flat bounded async fan-out (if this layer ever needs one): `Batch.seq()`
  + `asyncio.gather()`, not a raw `for` loop of `await`s.** Not currently
  needed anywhere in `mint/db` (the one place that would have needed it,
  `extra_paths`'s per-object loop, no longer exists — see above), but
  documented here since it was investigated directly: `sprout` (the
  git-dependency package `mint/fs/asynk/s3.py` uses for hierarchical
  folder-delete retry/concurrency) has no exported flat "bounded gather"
  primitive — its public API (`Executor`, `ChildRef`, `FetchResult`) is
  entirely tree-traversal-shaped, and `sprout.concurrency.ConcurrencyGate`
  exists internally but isn't exported. `mint.utils.Batch.seq()` +
  `asyncio.gather()` per batch is the established in-repo convention for
  flat bounded fan-out (exactly what `mint/fs/asynk/s3.py` already does
  for `save_many`/`copy`/`remove_many`); `sprout.Executor` is the right
  tool specifically for *hierarchical* work, not a flat list.
- **Reads never auto-commit, even with `auto_commit=True`.** `execute()`/
  `execute_many()` only commit when the statement isn't a plain `Select` —
  fixed over the ported predecessor, which committed after every statement
  including pure reads (an unnecessary round trip with no correctness
  benefit).
- **(confirmed, Phase 2) `MaterializedViewRepository.refresh_materialized_view()`
  ignored `auto_commit`.** Found via code review: it unconditionally called
  `self.session.commit()`, the only write path in the layer that didn't
  gate on `self.auto_commit` — silently broke `UnitOfWork` atomicity for
  any caller refreshing a materialized view mid-transaction with
  `auto_commit=False`. Fixed to gate the commit like every other write
  path.
- **(project rule, Phase 2) Prefer `isinstance()`/`issubclass()` over
  `hasattr()`/`getattr()` with a default.** Codified in
  `.claude/rules/coding-style.md` after the scoping listener's original
  `hasattr(schema, "is_deleted")`/`getattr(self, "owner", None)` checks
  were replaced with `issubclass(schema, IsDeletedMixin)`/
  `isinstance(self, IScopedRepository)`. One caveat worth remembering:
  `issubclass()` raises `TypeError` on a `Protocol` with any non-method
  (data) member — only a method-only `Protocol` supports `issubclass()`.
  For a data-bearing shape, check against a concrete class instead (as
  with `IsDeletedMixin`/`OwnerMixin`); `isinstance()` has no such
  restriction and works on any `@runtime_checkable` Protocol.
- **`UnitOfWork.__aexit__`/`AsyncExitStack.aclose()` doesn't forward
  exception info.** Found via code review: `aclose()` closes registered
  context managers "as if called at the end of a normal with statement"
  per its own documentation, but does not pass the triggering exception
  through to nested `__aexit__` calls the way a real `async with` would.
  `Database.create_session()`'s except-based rollback+reraise branch is
  consequently dead code specifically for the `UnitOfWork` path — harmless
  today only because `Session.close()` discards uncommitted work
  regardless. Documented, not restructured (see specs/002-db-repository-layer/data-model.md's
  "State / lifecycle notes").
- **(known constraint, Phase 2) Two repositories for the same schema with
  different owners must not share one `UnitOfWork` session.** Each
  repository constructed against a shared session registers its own
  scoping listener and none are ever removed — two owner-scoped
  repositories for the same schema on one shared session get their owner
  criteria silently ANDed together (empty results, no error). Not fixed
  (would need `event.remove()`-based listener lifecycle management on
  `UnitOfWork.__aexit__`); harmless in the common case since a
  `UnitOfWork` block is short-lived and its session is discarded on exit.

## Confirmation-spike results (SQLModel pattern coverage)

Every advanced pattern the architecture review flagged as needing
verification under SQLModel — rather than assumed to work — was exercised
against real Postgres in `tests/db/asynk/test_models.py` and
`tests/db/asynk/test_schema_shapes.py`:

| Pattern | Result |
|---|---|
| Multi-mixin composition, 3+ deep | Works — standard SQLModel `table=False` mixin composition via MRO |
| Concrete per-ID-type base classes | Works — the resolved design; see comparison table above |
| Subclass field override (redeclaring a mixin's column with a different constraint) | Works |
| Concrete owner relationship on top of `OwnerMixin` | Works via direct `Relationship()`; **not** via `declared_attr` (see gotchas) |
| Many-to-many via `secondary=`/`link_model=` | Works, resolves against the single shared `Base.metadata` |
| Composite-key join table, no `id` column | Works via `RepositoryBase[T]` |
| Self-referential (tree) relationship | Works via `.options(selectinload(...))` on the caller's own statement (Phase 2; previously `extra_paths`), no runaway recursion (nothing loads unless requested) |
| Read-only materialized view + `REFRESH ... CONCURRENTLY` | Works via `MaterializedViewRepository` |
| `PrivateAttr` + `@reconstructor` for transient non-column state | Works — but note `@reconstructor` fires on *any* ORM row hydration, including `INSERT ... RETURNING`, not only `SELECT` loads |
| `hybrid_property` with a separate SQL-expression form | Works, with the same `ignored_types` fix as `declared_attr`'s first-stage error |
| Custom column types / compiled SQL `server_default` | Not exercised in this port's own tests (no consuming table needed one yet) — expected to work unchanged, since it's SQLAlchemy Core, independent of the model metaclass |
| 4+ mixin stack (soft-delete + owner + audit + app-specific), plus a concrete owner relationship and a `hybrid_property` reading columns from two different mixins, simultaneously (Phase 2) | Works — `tests/db/asynk/test_models.py::test_four_plus_mixin_stack_with_owner_relationship_and_hybrid_property`, pushing further than the 3-mixin `TJob` spike |

## Dependencies

Added under a `db` dependency-group (`sqlmodel`, `sqlalchemy`, `asyncpg`,
`psycopg2-binary`, `pydantic-settings`) and a `postgres` extra on the `test`
group's `testcontainers`, mirroring the existing `s3`/`azure` groups. Sync
Postgres access uses `psycopg2`; async uses `asyncpg`.
