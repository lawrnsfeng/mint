# Feature Specification: Database Repository Layer

**Feature Branch**: `feat/db-repository-layer`

**Created**: 2026-07-19

**Status**: Draft

**Input**: User description: "Port mini's async+sync SQLAlchemy/SQLModel repository layer into mint as a new mint/db/ package, fixing a real concurrency bug in session handling and adopting SQLModel to collapse the separate ORM-schema/domain-model layers mini has today." Full rationale and resolved architecture decisions: `/home/lawrence/.claude/plans/ok-take-a-look-fuzzy-quail.md`.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Zero-boilerplate CRUD for a new table (Priority: P1)

A developer adding a new database table to a mint-based service defines the
table's shape once and declares a repository for it, and immediately has
working create/read/update/delete operations — without writing or
reimplementing any data-access code for the common case.

**Why this priority**: This is the entire ergonomic point of having a
repository layer at all. If this doesn't hold, every consumer re-derives
boilerplate CRUD by hand, which is the exact problem this feature exists to
remove.

**Independent Test**: Define a new table (with a unique identifier and one
extra field) and a matching repository declaration only. Verify that create,
fetch-by-id, update, and delete all work correctly with zero additional code
written.

**Acceptance Scenarios**:

1. **Given** a newly defined table and a one-line repository declaration for
   it, **When** a record is created through that repository, **Then** it can
   immediately be fetched, updated, and removed through the same repository
   with no custom methods written.
2. **Given** the table also needs "owned by a specific user" scoping,
   **When** the developer composes one additional, reusable building block
   into the table and repository declarations, **Then** every operation
   through that repository is automatically limited to the current owner's
   records, with no repository method rewritten.

---

### User Story 2 - Correct behavior under concurrent use (Priority: P1)

Multiple operations run at the same time (e.g. two concurrent requests) and
happen to share the same repository object. Each operation must complete
using its own isolated database session — never reading, writing, or
committing through a session that belongs to a different, concurrently
running operation.

**Why this priority**: This is the specific, real bug this feature exists to
fix. The previous implementation stored its active session in a way that
could be silently overwritten by a second concurrent operation, corrupting
both operations' results. Any port that carries this forward reintroduces a
live data-integrity bug.

**Independent Test**: Run many concurrent create-then-fetch operations
against one shared repository instance and verify every operation's fetch
returns exactly the record it created — never another operation's record —
and that no operation ever errors due to a session closed by another
operation.

**Acceptance Scenarios**:

1. **Given** one repository instance, **When** two operations run
   concurrently against it, **Then** each completes correctly using its own
   session, with no cross-contamination of results.
2. **Given** an operation fails partway through with an error, **When** the
   next operation runs on the same repository instance, **Then** it succeeds
   normally — it does not inherit a broken or already-closed session from
   the failed operation.

---

### User Story 3 - Scoping can't be silently skipped (Priority: P2)

A developer writes a custom, hand-built query method on a repository (for a
search, a report, a join the built-in methods don't cover). That custom
query still automatically excludes soft-deleted records and records
belonging to a different owner, the same as the built-in methods — without
the developer having to remember to add that filtering themselves.

**Why this priority**: Review of a real production consumer found multiple
custom query methods that silently omitted the previous implementation's
opt-in scoping call, meaning soft-deleted or other-owner rows could leak
through those specific methods. This is a real, already-observed correctness
gap, not a hypothetical one — it directly threatens data isolation between
tenants/owners if repeated.

**Independent Test**: Write a new custom query method that talks to the
database directly (bypassing the built-in list/get helpers). Verify
soft-deleted and other-owner records are still excluded from its results
without the method containing any scoping code, and verify an explicit,
visible override can intentionally include them when needed.

**Acceptance Scenarios**:

1. **Given** a soft-deleted record and a hand-written custom query method
   with no scoping code in it, **When** that method runs, **Then** the
   soft-deleted record is excluded from its results.
2. **Given** a record owned by a different user, **When** any query — built-in
   or custom — runs under an owner-scoped repository, **Then** that record is
   excluded unless the caller explicitly opts out.
3. **Given** a caller explicitly needs to see soft-deleted or cross-owner
   records for a specific operation, **When** they use the documented,
   explicit opt-out, **Then** those records are included — with no other
   way to accidentally achieve the same result.

---

### User Story 4 - Multi-table atomic operations, only when needed (Priority: P2)

A developer needs to perform writes across two or more different tables that
must all succeed or all fail together. They use a dedicated, documented
pattern for this — while every simple, single-operation call elsewhere in
the codebase continues to need no setup at all.

**Why this priority**: Without a real answer here, developers either bolt
ad hoc session-sharing together per call site (inconsistent, easy to get
wrong) or are forced to wrap every operation — including trivial single
ones — in transaction-management boilerplate (rejected as an unacceptable
tax on the common case).

**Independent Test**: Perform two writes to two different tables inside one
explicit multi-table operation, force a failure before it completes, and
verify neither write persisted. Separately, verify a simple single-table
call still requires no such wrapping.

**Acceptance Scenarios**:

1. **Given** a multi-table write wrapped in the explicit atomic-operation
   pattern, **When** one of the writes fails, **Then** none of the writes in
   that operation persist.
2. **Given** a single, simple create-or-fetch call unrelated to any
   multi-table operation, **When** it runs, **Then** it requires no explicit
   transaction setup at all.

---

### User Story 5 - Supports real-world schema shapes, not just simple tables (Priority: P3)

The repository layer correctly supports the range of table shapes found in
actual production usage: tables without a single-column identifier (e.g.
composite-key join tables), read-only reporting views, self-referential
(tree-shaped) relationships, and many-to-many relationships.

**Why this priority**: Two real production codebases that already use the
predecessor of this layer were surveyed specifically to catalogue these
shapes. A port that only handles the simple case would break on first
contact with real usage.

**Independent Test**: Exercise one table of each shape (a composite-key join
table, a read-only view, a self-referential tree, a many-to-many
relationship) against the layer and verify each behaves correctly and safely
(e.g. a self-referential relationship does not cause unbounded data
loading).

**Acceptance Scenarios**:

1. **Given** a table with a composite key and no single `id` column,
   **When** basic data access is performed against it, **Then** it works
   without requiring identifier-based single-record operations that don't
   apply to it.
2. **Given** a read-only reporting view, **When** it is queried and
   refreshed, **Then** both operations succeed and no write operations are
   exposed for it.
3. **Given** a self-referential (tree-shaped) relationship, **When** a
   record and its relatives are loaded, **Then** loading completes without
   runaway recursive queries.

---

### Edge Cases

- What happens when two concurrent operations share one repository instance
  and one of them raises an exception mid-operation? (Covered by User
  Story 2 — the other operation must be unaffected.)
- What happens when a query needs to see soft-deleted or cross-owner rows on
  purpose? (Covered by User Story 3 — an explicit, visible opt-out exists.)
- What happens when a table has no single-column identifier? (Covered by
  User Story 5 — basic data access still works; identifier-based operations
  are not offered for it.)
- What happens when related data is not explicitly requested? It stays
  unloaded (not fetched) rather than being fetched automatically, to avoid
  unnecessary work — except for relationships the table's own definition
  marks as always-needed, which load automatically as part of normal query
  execution.
- What happens when a caller submits a creation payload that includes fields
  that should only ever be system/database-generated (e.g. an identifier)?
  The payload shape used for creation does not accept those fields at all.
- What happens when a caller wants to update only some fields of a record?
  Only the fields explicitly provided are changed; unspecified fields are
  left untouched.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST isolate each concurrent operation's database
  session such that two operations running at the same time and sharing one
  repository instance never read, write, or commit through each other's
  session.
- **FR-002**: The system MUST NOT leave a repository instance holding a
  stale or already-closed session reference after an operation fails with an
  exception; the next operation on that instance MUST obtain a fresh,
  correctly functioning session.
- **FR-003**: A developer MUST be able to define a new database table and
  obtain full create/read/update/delete operations for it without writing
  any repository method code, provided the table has a single-column
  identifier.
- **FR-004**: A developer MUST be able to add "owned by a user" scoping to a
  table's operations by composing one reusable building block, with no
  per-method reimplementation required.
- **FR-005**: The system MUST exclude soft-deleted records and records
  belonging to a different owner from query results automatically —
  including from hand-written custom query methods, not only the built-in
  list/get operations.
- **FR-006**: The system MUST provide an explicit, visible way for a
  specific operation to intentionally bypass soft-delete exclusion and/or
  owner-scoping, without weakening the default (automatic, can't-be-forgotten)
  behavior for every other operation.
- **FR-007**: The system MUST allow a developer to group multiple write
  operations, potentially across different tables, into a single
  all-or-nothing atomic operation.
- **FR-008**: The system MUST NOT require any explicit transaction setup for
  a simple, single-operation call — the multi-operation atomic pattern from
  FR-007 MUST be opt-in, not the default path.
- **FR-009**: The system MUST support tables without a single-column
  identifier (e.g. composite-key join tables) using the same underlying
  data-access capability, without exposing identifier-based single-record
  operations that don't apply to them.
- **FR-010**: The system MUST support read-only, refreshable reporting-view-
  backed tables as a supported case, distinct from writable tables, with no
  create/update/delete operations exposed for them.
- **FR-011**: The system MUST support relationships from a table back to
  itself (hierarchical/tree data) without causing unbounded or runaway
  recursive data loading.
- **FR-012**: The system MUST support many-to-many relationships between
  tables.
- **FR-013**: A developer MUST be able to request specific related data be
  loaded together with a primary record on demand; related data not
  requested MUST NOT be loaded automatically, except for relationships a
  table's own definition explicitly marks as always-needed.
- **FR-014**: The system MUST reject, at the input-validation level, fields
  that should only be system-generated (such as identifiers) when a new
  record is being created, and MUST support partial updates that change only
  the fields explicitly supplied by the caller.
- **FR-015**: Every requirement above MUST be verified against a real
  instance of the same production-grade database engine this layer targets,
  not a lightweight or in-memory substitute, before being considered
  complete.
- **FR-016**: Usage documentation covering both the zero-setup common case
  and the explicit multi-table atomic-operation case MUST be produced and
  organized so it is suitable for publishing as browsable reference
  documentation for other developers.
- **FR-017**: A developer MUST be able to retrieve a paginated page of
  records together with the total count of matching records using a single
  query, without requiring two separate round trips to the database.

### Key Entities

- **Repository**: The object a developer uses to perform data operations
  (create/read/update/delete, plus any custom queries) against one table's
  worth of data. May optionally be scoped to a specific owner.
- **Table Definition**: The developer-authored description of a database
  table's shape — its fields, its identifier (if any) and identifier type,
  and any reusable building blocks composed into it (e.g. audit timestamps,
  ownership).
- **Owner**: The acting user or tenant that an owner-scoped repository's
  operations are automatically limited to.
- **Atomic Operation Grouping**: The explicit, opt-in construct a developer
  uses to make multiple repository operations across one or more tables
  succeed or fail together as a single unit.
- **Creation/Update Payload**: The input shape accepted when creating or
  updating a record — distinct from the record's own full shape — which
  excludes system-generated fields on creation and supports partial,
  explicitly-scoped changes on update.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Under a repeated concurrent-load test (many simultaneous
  operations sharing one repository instance), zero cross-operation data
  corruption, wrong-record reads, or broken-session errors occur across
  every run.
- **SC-002**: A new table with standard create/read/update/delete needs can
  be made fully functional by a developer writing only its table definition
  and a one-line repository declaration — zero custom method code required
  to reach a working state.
- **SC-003**: 100% of automated tests exercising hand-written custom query
  methods correctly exclude soft-deleted and other-owner records, with no
  test needing to add scoping logic itself to achieve that.
- **SC-004**: 100% of the real-world usage patterns catalogued from two
  existing production consumer codebases are demonstrated working under
  test before this feature is considered complete.
- **SC-005**: 100% of verification tests pass when run against a real
  instance of the target production database engine, with no test relying
  on a lightweight/in-memory substitute for a result that would differ
  under the real engine.

## Assumptions

- The target production database engine is PostgreSQL; concurrency and
  driver-specific behavior (the bug this feature fixes) only manifests
  reliably against a real async driver talking to a real database, which is
  why FR-015/SC-005 require real-engine verification.
- This is foundational internal library infrastructure consumed by other
  developers building features within mint, not an end-user-facing product
  surface — "users" throughout this spec means developers using the
  repository layer, and their end-users (owners/tenants) whose data is
  being scoped/protected.
- The two production codebases surveyed to catalogue real-world usage
  patterns are representative of realistic future consumers; a usage shape
  not found in either is out of scope for this feature's initial
  completion and can be added later without architectural rework.
- A generic, dynamic dict-based query-filtering capability (present in the
  predecessor implementation) is not required by current real-world
  consumers and is intentionally out of scope for this feature — typed,
  hand-written query methods are the supported pattern for anything beyond
  basic paginated listing.
- The predecessor implementation being replaced is not currently in
  production use within mint itself, so there is no live-migration or
  backward-compatibility constraint against existing mint data — this is a
  fresh capability, not an in-place upgrade.
