# Phase 0 Research: Database Repository Layer

All decisions below were resolved through two rounds of interview with the
user prior to this spec/plan (recorded in full at
`/home/lawrence/.claude/plans/ok-take-a-look-fuzzy-quail.md`). This document
restates them in research.md's Decision/Rationale/Alternatives form; it does
not reopen them.

## Session isolation

**Decision**: Per-repository-instance `ContextVar[Session | None]`, set/reset
via token around the session's lifetime, exactly mirroring
`mint/fs/asynk/s3.py`'s `_client_ctx` / `mint/fs/asynk/abs.py`'s
`_client_ctx` pattern.

**Rationale**: mini's `RepositoryBase.ensure_session` stored the open session
on a plain instance attribute (`self._session`). Two asyncio tasks sharing
one repository instance race: both see `self._session is None`, both open a
session, and whichever finishes opening last silently overwrites the other's
reference — the loser's task keeps running against an untracked session,
while a later call may reuse a session a different task already closed. A
`ContextVar` gives each task its own isolated slot for free (asyncio tasks
copy context at creation) and reset always happens in a `finally`, closing
mini's second bug (no reset on the exception path).

**Alternatives considered**:
- *Keep instance attribute, add a lock*: rejected — a lock serializes
  concurrent operations on one repository instance, defeating the purpose of
  using one repository across concurrent requests at all.
- *`contextvars` at module level, keyed by `id(self)`*: rejected — same
  effect as a `ContextVar` attribute per instance but with manual key
  bookkeeping and a leak risk if instances are garbage collected while their
  key is still referenced; a per-instance `ContextVar` attribute is simpler
  and is already mint's own established pattern.

## Schema/model collapse and where genericity lives

**Decision**: Schema and domain model collapse onto one SQLModel `table=True`
class per table (no separate pydantic response model + `modelize()` glue).
Generic *table* base classes (`BaseWithID[T]`) are not used — instead,
concrete per-ID-type bases (`BaseWithUUID`, `BaseWithIntID`, `BaseWithStrID`)
each declare their own concrete `id` field. Genericity is kept entirely at
the *repository* layer (`RepositoryBase[T]`, `EntityRepository[T, I]`), which
are plain Python classes never passed through SQLModel's metaclass.

**Rationale**: SQLModel's metaclass requires every base to be a
pydantic-model subclass, and pydantic's own generic-model support
(`Generic[T]`/`TypeVar` fields) is documented as unreliable for exactly this
shape (fastapi/sqlmodel#211, pydantic#4171; documented workaround: "subclass
and redefine the generic class with concrete types"). Independently
confirmed by reading a real mini-consumer's schemas
(`prj_easyrag_production_api/app/core/schemas/`): every leaf table already
redeclares its `id` column concretely even though it inherits a generic
`OwnedBaseWithID[UUID]` — the generic substitution was never actually load-
bearing for the column definition. Repository-layer genericity carries zero
metaclass risk and delivers the actual ergonomic payoff
(`EntityRepository[Job, UUID]`).

**Alternatives considered**:
- *Force `Generic[T]` table classes anyway*: rejected — documented unreliable
  in the SQLModel/pydantic issue trackers; this is exactly the class of
  friction the user reported hitting on a previous SQLModel attempt.
- *Drop genericity everywhere, hand-write each repository's CRUD*: rejected —
  defeats the "zero-boilerplate CRUD for a new table" design goal (spec.md
  User Story 1 / SC-002).

## Scoping (soft-delete, owner) enforcement

**Decision**: `sqlalchemy.orm.with_loader_criteria` combined with a
`do_orm_execute` session event listener, registered on the specific session
instance a repository is currently bound to (not globally on the `Session`
class), closing over the repository instance (`self.Schema`, `self.owner`,
`self.is_scoped`) so criteria are evaluated fresh at query-compile time.
Explicit, visible bypass via
`stmt.execution_options(mint_scope_bypass=frozenset({IsDeletedMixin}))` — a
single option holding a set of the scoping mixin classes to bypass, checked
first by the listener.

**Revision (Phase 2)**: The listener's own type checks were rewritten from
`hasattr(self.Schema, "is_deleted")`/`getattr(self, "owner", None)` to
`issubclass(self.Schema, IsDeletedMixin)`/
`isinstance(self, IScopedRepository)` — real type narrowing instead of
duck-typed attribute probing, per direct user review (now codified as a
project rule in `.claude/rules/coding-style.md`, "Type narrowing over
hasattr/getattr"). `issubclass()` cannot be used for the owner-instance
check because `IScopedRepository` (`owner: Any`, `is_scoped: bool`) has
non-method (data) members and `issubclass()` on such a `Protocol` raises
`TypeError` — a real, confirmed Python limitation, not a style choice.
`isinstance()` has no such restriction, so it's used for that check;
`issubclass()` is used for the schema-level checks against the concrete
`IsDeletedMixin`/`OwnerMixin` classes instead, sidestepping the limitation
entirely. `HasSoftDelete`/`HasOwnerColumn` (the original structural
`Protocol` markers) are removed from `mint/db/models.py` — they were never
actually usable via `issubclass()` for the reason above, and were only ever
checked via `hasattr()`, so they added a type with no real use.

Separately, the original two boolean execution options
(`mint_include_deleted`, `mint_skip_owner_scope`) were replaced with one
`mint_scope_bypass: frozenset[type]` option holding the mixin classes to
bypass — adding a future scoping dimension (e.g. a tenant-scoping mixin)
means adding a new mixin class and one `issubclass`/`not in bypass` check,
not a third boolean constant and a third named option.

**Rationale**: mini's `restrain()` is opt-in — `SoftDeleteMixin`/
`ResourceOwnerMixin` override it, but every method must remember to call
`self.restrain(stmt)`. Reading a real mini consumer's repository code
(`prj_easyrag_production_api/app/core/repos/`) found multiple custom query
methods (`CollectionRepository.get_by_name`, `.get_many_in_gcp`) that build a
`select()` and execute it directly, skipping `restrain()` entirely — a live,
observed correctness gap, not a hypothetical one. `with_loader_criteria` +
`do_orm_execute` applies to every `session.execute()` call against the
scoped schema regardless of which method issued it, including the dominant
real-world pattern of hand-built custom queries — closing the exact gap
`restrain()` had. Instance-scoped (not class/global) event registration
avoids introducing a new global/ambient state source, keeping the fix
consistent with the session-isolation fix above (no new implicit shared
state).

**Alternatives considered**:
- *Keep `restrain()`, document the discipline required*: rejected — the gap
  is empirically real in production code today; documentation doesn't fix a
  structurally opt-in mechanism.
- *Global `do_orm_execute` listener on the `Session` class reading an ambient
  `ContextVar` for "current owner"*: rejected — reintroduces an ambient
  mutable-state pattern (a new ContextVar for "current owner") of the same
  shape as the bug being fixed elsewhere in this feature; instance-scoped
  listener + closure over `self` achieves the same result without new global
  state.

## Session ownership / transaction pattern

**Decision**: Hybrid. Default path: `ContextVar`-based auto-session, opened
and closed per top-level call, zero setup required
(`JobRepository(db).get(id)` needs no wrapper). Additional, opt-in path: a
`UnitOfWork` context manager that opens one session, exposes repositories
bound to it via `uow.repo(SomeRepository, ...)`, and owns `commit()`/
`rollback()` — the named, reusable primitive for multi-repository atomic
transactions.

**Rationale**: A "repository never opens its own session, everything goes
through Unit of Work" design was considered and explicitly rejected by the
user: wrapping every simple, single-operation call in
`async with UnitOfWork(db) as uow:` was called out as an unacceptable
indentation/boilerplate tax for the common case, working against the
project's explicit "ease of starting simple" goal. The hybrid keeps the
zero-ceremony default and gives the complex, multi-table-atomicity case
(spec.md User Story 4) a named pattern instead of ad hoc manual
session-passing.

**Alternatives considered**:
- *Full Unit of Work only, no auto-session*: rejected per above — real
  ergonomic cost for the common case, explicitly rejected by the user.
- *Auto-session only, no formal Unit of Work, "just pass `session=`
  manually"*: rejected — leaves the complex case as an undocumented,
  ad hoc escape hatch rather than a reusable, testable primitive.

## Filter/query capability

**Decision**: Drop mini's `StatementBuilder` (dict-based `$in`/`$like`/`$lt`
filter DSL) from this port. `get_many()` becomes a plain paginated
`select(self.Schema).offset(skip).limit(limit)`; anything more specific is a
hand-written, typed custom method.

**Rationale**: Reading a real mini consumer's repository code (~36 files)
found `StatementBuilder` used in only ~2 call sites — nearly every method is
a hand-written typed query. It's also stringly-typed (relationship paths as
dotted strings resolved at runtime), which works against mint's
`ty`-check-everything rule for a capability that's barely used in practice.

**Alternatives considered**:
- *Port as-is, same role as mini (`get_many`'s default engine)*: rejected —
  low real-world payoff versus type-safety cost.
- *Port as an explicit secondary tool, not `get_many()`'s engine*: considered,
  but ultimately dropped entirely rather than carried as unused surface area;
  can be added later if a genuinely generic filterable-list endpoint need
  arises (documented as an explicit non-goal in spec.md Assumptions).

## Create/Update payload shape

**Decision**: Separate, lightweight non-table SQLModel companion classes per
table (`JobCreate`, `JobUpdate`) — not the table class itself used as the
create/update payload.

**Rationale**: Even with schema+read-model collapsed onto one table class,
create/update payloads have different validation needs than the full record:
`Create` must not accept server/client-generated fields (`id`,
`created_at`); `Update` needs all fields `Optional` to support
`model_dump(exclude_unset=True)` partial-update semantics. This matches
SQLModel's own documented Create/Update/Table pattern.

**Alternatives considered**:
- *Table class doubles as the create/update payload*: rejected — callers
  could technically set `id`/`created_at` on create, and partial-update
  semantics on a table-class instance (where most fields aren't `Optional`)
  are less natural.

## Verification approach

**Decision**: All tests requiring correctness verification (concurrency,
scoping, transactions, schema-shape support) run against a real PostgreSQL
instance via `testcontainers[postgres]` — no sqlite/in-memory substitute for
any test whose result could differ under the real engine/driver.

**Rationale**: The core bug this feature fixes (session-isolation race) is a
real-async-driver-under-real-concurrency phenomenon; an in-memory substitute
would not exercise the code path that was actually broken, defeating the
purpose of the regression test. mint already depends on `testcontainers`
(currently `[azurite]`/`[localstack]` extras only) — adding `[postgres]` is
additive, not a new tooling dependency.

**Alternatives considered**:
- *sqlite in-memory for unit tests, postgres only for a small integration
  subset*: rejected as the default — explicitly called out as
  non-negotiable in the architecture plan given what this feature exists to
  fix; a bug that only manifests under a real driver must be tested under a
  real driver.

## Generic ID type (Phase 2)

**Decision**: `EntityRepository[T: Base, I]` — `T` bound only to `Base` (no
closed union of concrete ID-typed bases), `I` a free, unconstrained,
independently-specified type parameter. A schema's `id` can now be any
type (a snowflake ID, a `NewType`-wrapped primitive, a composite value
object), not just `UUID | int | str`. `EntityRepository` exposes a single
`_id_column` property doing one documented `cast()` (`Base` doesn't
declare `id`; only convention, not the type system, guarantees `I` matches
`self.Schema`'s actual `id` field type) — every internal `self.Schema.id`
access goes through it instead of five separate untyped accesses.
`BaseWithUUID`/`BaseWithIntID`/`BaseWithStrID` are unchanged and remain the
shipped convenience bases for the common case — they're simply no longer
the only schemas `EntityRepository` accepts.

**Rationale**: A closed three-member union can't express a real
production ID shape outside those three primitives. The *ideal* design —
`EntityRepository[T]` alone, with `I` inferred from `T.id`'s declared type
— requires higher-kinded types (HKT): a way to say "the type of `T`'s `id`
attribute becomes `I`". Investigated directly: no accepted PEP introduces
this for Python 3.13/3.14, and no mechanism (`TypeVar` bounds, `Generic`
introspection, PEP 646 variadic generics) expresses a type-level
"projection" from one type parameter's attribute onto another. This is a
confirmed, current limitation of the language, not an oversight in this
design. The two-type-parameter design with an honest, narrowly-scoped
`cast()` is the accepted trade-off — the alternative (a closed union) is
strictly worse since it's both unable to express the general case *and*
unable to express the ideal case.

**Alternatives considered**:
- *Keep the closed `BaseWithUUID | BaseWithIntID | BaseWithStrID` union*:
  rejected — doesn't extend to a real custom ID type; the exact complaint
  that started this investigation.
- *`T: HasID[I]` where `HasID[I]` is a `Protocol` with `id: I`*: investigated
  — doesn't work under SQLModel's metaclass for the same reason generic
  table classes don't (see "Schema/model collapse" above): a `Protocol`
  parameterized the same way a table class would need to be hits the same
  `Generic[T]`/pydantic unreliability. Also still wouldn't solve the actual
  goal (inferring `I` from `T` automatically) — it would only rename the
  bound, not eliminate the second type parameter.
- *Wait for/adopt a hypothetical future PEP*: not viable — no such PEP
  exists or is in flight as of this investigation (2026-07-19).

## Exception for anomalous query results (Phase 2)

**Decision**: `AbnormalResultError` removed. `count()`/`create()` raise
`OperationalError` (an existing, previously-unused exception in this
module's hierarchy) when a query that should always return exactly one row
(`COUNT`) or the inserted row (`INSERT ... RETURNING`) returns none.

**Rationale**: The user has personally hit this in production: an
`INSERT ... ON CONFLICT DO NOTHING`-style upsert statement returns zero
rows via `RETURNING` when the row already existed — not a "not found"
domain condition, and not something the caller can meaningfully recover
from inline. This should surface as an internal-server-error-flavored
exception at the API boundary, which is exactly what `OperationalError`
(already used this way in `mint.fs`'s `s3.py`/`abs.py`, wrapping unexpected
caught exceptions) represents — a single, consistently-handled "this
should never happen" category, rather than a bespoke exception type.
`create()` staying strict by default (raise on no row) is correct for the
plain-insert path; the actual upsert scenario itself gets first-class
`create()` support in Phase 3 (see "Upsert support" below) rather than
requiring a hand-rolled statement via `execute()`.

**Alternatives considered**:
- *Keep `AbnormalResultError`, make it inherit from `OperationalError`*:
  rejected — an extra exception class with the exact same "should never
  happen, surfaces as infra error" semantics as an existing one adds
  nothing; the API surface is simpler with one type doing this job.
- *Return `None`/raise `NotFoundError` for the no-row case*: rejected —
  conflates a genuine infrastructure anomaly (a query contract violation)
  with a domain "the record doesn't exist" condition; a caller catching
  `NotFoundError` around `create()` would be catching the wrong thing.

## Paginated page + total count in one query (Phase 2)

**Decision**: `RepositoryBase.get_many_page(*, skip=0, limit=10) ->
PaginatedResult[T]` (`PaginatedResult`: `items: Sequence[T]`, `total: int`,
`dataclasses.dataclass`, in `mint/db/typedefs.py`). Implemented with a
single `SELECT self.Schema, func.count().over() ... OFFSET skip LIMIT
limit` — a `COUNT(*) OVER()` window function computes the total-matching-
rows count alongside every returned row, in the same query, with no
separate round trip. When the requested page is empty (`skip` past the
last matching row), there is no row to carry a window-function total on,
so this one case falls back to a plain `count()` query.

**Rationale**: The two-query pattern (one `SELECT ... LIMIT/OFFSET`, one
separate `SELECT count(*)`) is the default most ORMs push developers
toward, and the user specifically flagged repeatedly needing both a page
and a total and being unable to reduce it to one query. `COUNT(*) OVER()`
is a standard, well-supported Postgres window function purpose-built for
exactly this — same query plan cost as the base `SELECT`, no second
round trip, no second full-table scan.

**Alternatives considered**:
- *Two queries, but run concurrently (`asyncio.gather`)*: rejected — still
  two round trips and two query plans; strictly worse than one query, and
  the sync mirror can't do this at all (no concurrency to exploit there).
- *A single `func.count()` at query time, unconditionally, even on the
  empty-page path (e.g. a `LEFT JOIN` against a scalar subquery count)*:
  considered, but adds a permanent extra JOIN/subquery to *every* call for
  a case (empty page) that is comparatively rare in real usage; the plain
  fallback `count()` query only on that specific edge case is simpler and
  no slower for the common case.

## Drop clone()/_construction_kwargs() (Phase 2)

**Decision**: Removed entirely from `RepositoryBase` and
`ResourceOwnerMixin`'s override.

**Rationale**: `clone()` existed to cheaply reconstruct a repository with
identical configuration — originally a workaround, in mini, for the fact
that a *shared* repository instance's session could race under concurrent
use (a caller would `clone()` a fresh instance per operation to sidestep
the race rather than share one instance safely). The root cause is fixed
by the per-instance `ContextVar` session isolation this feature's Phase 1
delivered — instances are safely shareable across concurrent tasks/threads
without cloning. Confirmed directly by the user (who identified the
original motivation) that nothing in `mint/db` still needs it.

**Alternatives considered**:
- *Keep `clone()` as a general-purpose convenience, unrelated to the
  original race-avoidance motivation*: rejected — no call site in
  `mint/db`'s own tests needed it once `extra_paths`, its only
  configuration parameter with meaningful per-clone variation, was also
  removed (see below); an unused method sitting on every repository
  subclass is dead API surface.

## Relationship eager-loading: drop extra_paths (Phase 2)

**Decision**: `RepositoryBase.extra_paths`/`fetch_extra_relationships`/
`_fetch_path`/`_fetch_extra_relationships_many` removed entirely. A caller
eager-loads a relationship by adding
`.options(selectinload(Schema.relationship))` (or `joinedload(...)`)
directly to the `select`/statement passed to `execute()`/`execute_many()`.

**Rationale**: `extra_paths` walked each returned object's relationship
path with a Python `for` loop, one `await` per object per path segment —
a real N+1 query/round-trip shape (`selectinload`-per-object instead of
`selectinload`-per-query) that is fine at the dozens-of-rows scale this
port's own tests exercise, but the user correctly identified it as
catastrophic at real production scale (thousands to millions of rows):
serial, it's slow; naively `asyncio.gather`-ed across all objects at once,
it's unbounded concurrent fan-out (connection/resource exhaustion). Neither
shape is acceptable, and neither is necessary: SQLAlchemy's own
`selectinload()`/`joinedload()` statement options already resolve a
relationship for an *entire result set* in one extra query (`selectinload`)
or one JOIN (`joinedload`) total, regardless of how many rows come back —
strictly better on every axis (fewer round trips, no per-object Python
loop, no unbounded fan-out) than the mechanism it replaces. mini never had
`extra_paths` at all; it was introduced during this port's Phase 1 as a
replacement for mini's `fetch_necessary_relationships` (itself driven by
diffing against a separate response model that no longer exists after the
schema/domain-model collapse) — removing it is reverting an
unnecessary Phase-1 addition, not dropping a mini-inherited capability.

**Alternatives considered**:
- *Keep `extra_paths`, but implement its walk with bounded
  `asyncio.gather` (`mint.utils.Batch.seq()` + a concurrency limit,
  mirroring `mint/fs/asynk/s3.py`'s established pattern for flat fan-out)*:
  rejected — still strictly worse than `selectinload()`/`joinedload()`
  doing the same job in one query; bounding the concurrency of an
  unnecessary N+1 pattern doesn't eliminate the N+1 pattern.
- *Keep `extra_paths` for the self-referential/tree case specifically,
  where a single `selectinload()` call can't express unbounded-depth
  recursion*: rejected — `selectinload(Model.children, recursion_depth=N)`
  (a keyword `selectinload()` itself accepts) or a bounded number of
  chained `.options(selectinload(Model.children).selectinload(Model.children))`
  calls covers the same bounded-depth need without a bespoke mechanism;
  this port's own self-referential test (`test_schema_shapes.py`) was
  rewritten onto `selectinload()` directly and passes unchanged.

Checked whether `sprout` (the git-dependency package already used by
`mint/fs/asynk/s3.py` for hierarchical folder-delete retry/concurrency)
offered a ready-made flat "bounded gather" primitive that could have
replaced `extra_paths`'s loop instead of removing the mechanism entirely:
it does not — `sprout`'s exported API (`sprout.Executor`,
`sprout.ChildRef`, `sprout.FetchResult`) is entirely tree-traversal-shaped;
`sprout.concurrency.ConcurrencyGate` exists internally but isn't exported.
The already-established in-repo convention for flat bounded async fan-out
is `mint.utils.Batch.seq()` + `asyncio.gather()` per batch (exactly what
`mint/fs/asynk/s3.py` already does for `save_many`/`copy`/`remove_many`);
moot for `mint/db` itself now that the one place that would have needed it
(`extra_paths`'s per-object loop) no longer exists, but documented in
`docs/db-repository-implementation-notes.md` for the next time this
comes up in this layer.

## Schema-translate persistence across commits (Phase 2)

**Decision**: `force_session_schema()`/`refresh()` keep their existing
reapply-on-refresh logic unchanged — confirmed load-bearing, not removed.

**Rationale**: Spiked directly against real Postgres/asyncpg (not
assumed): opened a session with `dbschema=` set, inserted and committed,
then queried an unqualified table name on the *same* session *without*
reapplying `schema_translate_map` — the query silently resolved against
the default (`public`) schema instead of the configured tenant schema,
returning no matching row instead of erroring. This is documented
SQLAlchemy behavior (`schema_translate_map` is a per-`Connection`
execution option; a `Session` checks out a fresh `Connection` the next
time it does work after a commit ends the prior transaction, and
execution options don't carry over to that new `Connection` unless set at
the `Engine`/`sessionmaker` level up front), now verified against this
stack specifically rather than assumed from mini's original docstring
(which claimed this on an unspecified driver). A second finding from the
same spike, not previously documented: reapplying
`schema_translate_map` via `session.connection(execution_options=...)`
must happen *before* any other statement runs on the new transaction —
once a `Connection` is checked out (by any statement, including a probe
query), a further `session.connection(execution_options=...)` call is a
silent no-op (`SAWarning: Connection is already established for the given
bind; execution_options ignored`). `refresh()`'s existing implementation
already satisfies this (it calls `force_session_schema()` immediately,
before touching the session again), so no code change was needed — only
confirmation, now captured as a permanent regression test
(`test_schema_translate_map_lost_after_commit_without_reforce` in both
`tests/db/asynk/test_base_edge_cases.py` and its sync mirror).

**Alternatives considered**:
- *Remove `force_session_schema()`/`refresh()`'s reapply logic, since
  `create_session()` already applies `schema_translate_map` once at
  session-open*: rejected by the spike's result — session-open-time
  application does not survive a later commit within that same session.

## Upsert support (Phase 3)

**Decision**: `create()` gains two optional keyword-only parameters —
`upsert: bool = False` and `conflict_columns: Sequence[str] | None = None`
— rather than a new dedicated method. `upsert=False` (the default) is
byte-for-byte the existing plain `INSERT ... RETURNING` path from Phase 1/2
(zero behavior change for every existing caller). `upsert=True` switches to
Postgres `INSERT ... ON CONFLICT (<target>) DO UPDATE SET <merged
fields> RETURNING`, via `sqlalchemy.dialects.postgresql.insert`, in a
single round trip — the returned row is always the final state (freshly
inserted, or the existing row merged with the new values), never `None`
for the "row already exists" case. `conflict_columns`, when omitted,
defaults to `self.Schema`'s primary key columns (introspected from
`Schema.__table__.primary_key`, which works identically whether the schema
has a single `id` column or a composite key); passed explicitly, it
targets a different unique constraint instead (e.g. a composite
`UniqueConstraint` on non-primary-key columns). When the dumped values
consist *only* of conflict-target columns (a pure composite-PK join table
with no other columns), the `DO UPDATE SET` clause is built from the
conflict-target columns set to themselves (a no-op update) purely so
`RETURNING` still fires — Postgres's `on_conflict_do_update()` rejects an
empty `set_`.

**Rationale**: Phase 2 fixed *which exception* `create()` raises when a
`RETURNING` clause returns no row (`OperationalError`, replacing
`AbnormalResultError` — see "Exception for anomalous query results"
above), motivated by the user's real production upsert-returns-nothing
case, but didn't give `create()` any way to actually *perform* an upsert —
a caller wanting insert-or-update semantics still had to bypass `create()`
and hand-build a statement via `execute()`. This phase closes that gap
directly on `create()`, the method the user was already using. `DO UPDATE`
(not `DO NOTHING` + a follow-up `SELECT`) was chosen over the
two-statement alternative specifically because it's one round trip and the
returned row is unambiguous (always the final state), matching the same
one-query-not-two philosophy already applied to `get_many_page()` (see
above). Defaulting the conflict target to the schema's own primary key
means the common case (`repo.create(payload, upsert=True)`) needs no
extra ceremony, while `conflict_columns` stays available for the
non-PK-unique-constraint case a real schema can have.

**Alternatives considered**:
- *New dedicated `upsert()`/`get_or_create()` method, leaving `create()`
  untouched*: considered — keeps `create()`'s contract maximally simple,
  but splits one conceptual operation ("create a row, handling the
  already-exists case") across two methods a caller has to choose between
  up front; extending `create()` with an opt-in, default-off parameter
  keeps one method and one call site for both cases.
- *`DO NOTHING` + a follow-up `SELECT` on conflict, returning the
  pre-existing row unmodified*: rejected — two round trips instead of one,
  and "the upserted row" read literally means the caller's new values
  should be reflected, not silently discarded on a conflict.
- *Require `conflict_columns` explicitly, no default-to-primary-key
  fallback*: rejected — the common case (upserting by the schema's own
  `id`) is by far the most frequent one across real consumer usage
  patterns surveyed for this port; requiring the caller to spell out the
  primary key every time would be needless ceremony for the default case.
