# Phase 1 Data Model: Database Repository Layer

This feature's "data model" is the shape of the infrastructure classes
themselves (schema base classes, repository classes, the unit-of-work), not
domain data — `mint/db/` ships no application tables, only the reusable
building blocks a consuming service composes to define its own.

## Schema layer (`mint/db/models.py`)

### `Base`
Abstract SQLModel base for every table in this layer. No fields of its own
beyond what `SQLModel` provides. Single shared `metadata`/registry — required
so string-based `secondary="other_table"` many-to-many resolution works
across all tables defined against this `Base`, regardless of which module
defines them (spec.md FR-012; architecture plan, "Complex real-world
patterns" table).

### `BaseWithUUID`, `BaseWithIntID`, `BaseWithStrID`
Concrete, non-generic bases, each `(Base, table=False)`, each declaring its
own `id` field with the correct Python/SQL type and default:
- `BaseWithUUID.id: UUID = Field(primary_key=True, default_factory=uuid4, index=True)`
- `BaseWithIntID.id: int | None = Field(primary_key=True, default=None, index=True)` (server/auto-increment)
- `BaseWithStrID.id: str = Field(primary_key=True, index=True)` (caller-supplied)

A concrete leaf table (e.g. `Job(BaseWithUUID, table=True)`) inherits `id`
as-is — no redeclaration needed, since (unlike mini's generic `BaseWithID[T]`)
these bases already carry a concrete, correctly-typed default. Satisfies
spec.md FR-003/User Story 1.

### `AuditMixin`
`table=False` mixin: `created_at`/`updated_at` fields with server-side
`func.now()` defaults, `onupdate` for `updated_at`. Composed via normal
Python MRO alongside an ID base and any other mixin (spec.md's "Multi-mixin
composition, 3+ deep" pattern from the architecture plan's real-world
survey).

### `OwnerMixin`
`table=False` mixin: `created_by_user_id` column only — no relationship, no
FK target. Deliberately column-only (architecture plan, "App-owned concrete
owner relationship" row): a consuming app that wants an actual `relationship()`
to its own `User` table layers its own concrete mixin on top, the same
pattern mini's real consumers already use. Satisfies spec.md FR-004 in
combination with `ResourceOwnerMixin` (repository layer, below).

### Scoping type checks (no longer separate Protocol types)
The scoping listener (see Repository layer) decides, per-schema, whether
soft-delete/owner criteria apply via nominal `issubclass(schema,
IsDeletedMixin)`/`issubclass(schema, OwnerMixin)` checks against these
concrete mixin classes directly — not a separate `HasSoftDelete`/
`HasOwnerColumn` structural `Protocol` pair (removed in Phase 2: they were
only ever checked via `hasattr()`, since `issubclass()` cannot be used on a
`Protocol` with non-method/data members — a real Python limitation, not a
style choice — and nominal `issubclass()` against the mixins themselves
sidesteps it entirely). Satisfies spec.md FR-005's "automatic, based on the
record's own shape" requirement.

## Repository layer (`mint/db/asynk/` + `mint/db/sync/`)

### `Database`
One per logical database connection. Owns the pooled `Engine` +
`(async_)sessionmaker`. `create_session(*, dbschema=None)` is an async/sync
context manager yielding a fresh `Session`. No relationship to any specific
table — shared across every repository in a service.

### `RepositoryBase[T: Base]`
Generic over the schema type. Holds:
- `_db: Database`
- `_external_session: Session | None` (unit-of-work escape hatch, constructor param)
- `_session_ctx: ContextVar[Session | None]` (auto-session isolation)
- `_dbschema: str | None` (multi-tenant schema-translate support)
- `auto_commit: bool`

Exposes `session` (property, raises if neither `_external_session` nor
`_session_ctx` is set), `ensure_session` (the session-lifecycle decorator,
public — usable on any subclass method, not just built-in CRUD), `execute`/
`execute_many` (auto-commit gated on `self.auto_commit`; a caller eager-loads
a relationship by adding `.options(selectinload(...))`/`joinedload(...)`
directly to the statement — see "Relationship eager-loading" below),
`get_many`/`get_many_page` (the latter returns `PaginatedResult[T]`, a page
plus the total matching row count from a single query — see below), `count`.
`_construction_kwargs()`/`clone()` were removed in Phase 2 (no longer
needed once per-instance `ContextVar` session isolation made a shared
repository instance safe across concurrent use — see research.md, "Drop
clone()/_construction_kwargs()"). `fetch_extra_relationships` and its
`extra_paths` constructor param were removed in the same phase — see
"Relationship eager-loading" below.

**Relationships**: Composes with `Database` (has-a). Generic parameter `T`
bound to any `Base` subclass — including schemas with no `id` column at all
(composite-key join tables, materialized views — spec.md FR-009/FR-010),
since `RepositoryBase` itself makes no assumption about identifier shape.

### `PaginatedResult[T]` (`mint/db/typedefs.py`)
`dataclasses.dataclass`: `items: Sequence[T]`, `total: int`. Returned by
`RepositoryBase.get_many_page(*, skip, limit)` — one query (a `COUNT(*)
OVER()` window function alongside the page `SELECT`) instead of the
common two-query page-plus-count pattern, with a plain `count()` fallback
only for the empty-page-past-the-end edge case (no row to carry the
window-function total on). See research.md, "Paginated page + total count
in one query".

### Relationship eager-loading (no longer a `RepositoryBase` mechanism)
Phase 1 shipped an opt-in `extra_paths: list[str]` constructor param,
walked via `fetch_extra_relationships`/`_fetch_path` with one `await` per
object per relationship-path segment. Removed in Phase 2 (a real N+1
round-trip/fan-out shape at scale — see research.md, "Relationship
eager-loading: drop extra_paths"). The replacement requires no new
`mint/db` code: a caller adds
`.options(selectinload(Schema.relationship))` (or `joinedload(...)`)
directly to the `select`/statement passed to `execute()`/`execute_many()`
— SQLAlchemy resolves it as one extra query or one JOIN total for the
entire result set, still satisfying spec.md FR-013 ("related data not
requested MUST NOT be loaded automatically").

### `EntityRepository[T: Base, I]`
Extends `RepositoryBase[T]`. Adds identifier-based operations: `get(id_)`,
`update(id_, model)`, `remove(id_)`, `get_many_by_ids(ids)`. This is the type
consuming code parameterizes for the zero-boilerplate CRUD case (spec.md
User Story 1). Requires a single-column identifier — schemas without one use
`RepositoryBase[T]` directly (spec.md FR-009's deciding rule).

`T` is bound only to `Base` (Phase 2; previously
`BaseWithUUID | BaseWithIntID | BaseWithStrID`) and `I` is a free,
unconstrained type parameter — a schema's `id` can be any type, not only
the three original primitives. Python cannot express "`I` is `T`'s `id`
field type" (no higher-kinded types — see research.md, "Generic ID type"),
so `I` is trusted, not statically verified, to match. `EntityRepository`
exposes a private `_id_column: InstrumentedAttribute[I]` property doing
the one `cast()` this requires (`Base` doesn't declare `id`); every
internal `self.Schema.id` access goes through it instead of five separate
untyped accesses.

### `SoftDeleteMixin[T: Base, I]`
Overrides `remove()`/`remove_many()` to `UPDATE is_deleted = True` instead of
hard-deleting. Does **not** control read-side filtering — that's the scoping
listener's job, driven by `issubclass(schema, IsDeletedMixin)`,
unconditionally, regardless of whether this mixin is present (architecture
plan, "Scoping listener" design — a deliberate decoupling from mini, where
`SoftDeleteMixin` controlled both).

### `ResourceOwnerMixin[T: Base]`
Holds `owner: IOwner | None`, `is_scoped: bool` (property). Read by the
scoping listener via `isinstance(self, IScopedRepository)` — a structural
`Protocol` (`owner: Any`, `is_scoped: bool`) defined in `base.py` itself
(not imported from `mixins.IOwner`, to avoid a circular import), not a
cooperative override hook.

### `MaterializedViewRepository[T: Base](RepositoryBase[T])`
`table_name` property (dbschema-qualified) + `refresh_materialized_view()`
(`REFRESH MATERIALIZED VIEW CONCURRENTLY` via `text()`). Satisfies spec.md
FR-010/User Story 5.

### `UnitOfWork`
`__init__(db, *, dbschema=None)`. `async with UnitOfWork(db) as uow:` opens
one session, exposed as `uow.session`. Repositories are constructed
directly — `SomeRepository(db, session=uow.session, auto_commit=False)` —
rather than through a generic factory method (a `repo()` factory forwarding
arbitrary kwargs to an arbitrary repository subclass can't be typed without
`Any`, which mint's coding-style rule forbids);
`uow.commit()`/`uow.rollback()`. Satisfies spec.md FR-007/FR-008/User
Story 4.

**Known constraint (not fixed, documented):** each repository constructed
against a shared `UnitOfWork` session registers its own `do_orm_execute`
scoping listener on that session and never removes it. Two repositories
for the *same* schema with different owners sharing one `UnitOfWork`
session get their owner criteria silently ANDed together by both
listeners (empty results, no error) — don't construct two
differently-scoped repositories for the same schema against one shared
session. `UnitOfWork` itself does not currently deregister listeners on
exit; this is a known, currently-harmless (each `UnitOfWork` block is
short-lived and its session is discarded on exit) limitation, not
considered worth an `event.remove()`-based fix until a real multi-repository-
same-schema use case actually needs it.

### `IRepository[T, I]`, `IEntityRepository[T, I]`
`typing.Protocol` — not `abc.ABC` — matching the convention already
established in `mint/fs/asynk/interface.py`.

## Create/Update payload shape (per consuming table, not shipped by `mint/db/`)

Not part of `mint/db/`'s own classes — a documented pattern consuming code
follows (spec.md Key Entity "Creation/Update Payload"): a thin, non-table
`SQLModel` class per table, e.g. `JobCreate(SQLModel)` (excludes
`id`/`created_at`), `JobUpdate(SQLModel)` (all fields `Optional`). Documented
in quickstart.md and the mkdocs reference page, not enforced by a base class,
since the exact fields excluded are table-specific.

## State / lifecycle notes

- A `RepositoryBase` instance is stateless with respect to which session it's
  using *between* calls under the auto-session path — each top-level
  `ensure_session`-wrapped call opens and fully closes its own session
  (`ContextVar` token set then reset), so the instance itself is safely
  shareable across concurrent tasks (spec.md User Story 2's entire point).
- Under the `UnitOfWork`/external-session path, the *session's* lifetime
  (not the repository's) is what's shared across potentially multiple
  repository instances constructed within one `uow` block.
- `UnitOfWork.__aexit__`/`__exit__` close the session via
  `AsyncExitStack.aclose()`/`ExitStack.close()`, which does not forward the
  triggering exception's info to nested `__aexit__` calls the way a real
  `async with`/`with` block would. `Database.create_session()`'s own
  except-based rollback+reraise branch is consequently dead code for the
  `UnitOfWork` path specifically — harmless today only because
  `Session.close()`/`AsyncSession.close()` also discards any uncommitted
  work regardless of which code path triggered it. Documented as a known
  limitation, not restructured, since fixing it would mean not using
  `(Async)ExitStack` at all for a currently-harmless gap; revisit if
  `create_session()`'s except block ever grows real logic beyond
  rollback+reraise.
