# Tasks: Database Repository Layer

**Input**: Design documents from `specs/002-db-repository-layer/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/repository-api.md, quickstart.md

**Tests**: Explicitly required by the user ("100% covered tests on postgres testcontainers") — every phase below includes real-Postgres tests, no sqlite/in-memory substitute anywhere in this task list (spec.md FR-015/SC-005).

**Organization**: Tasks are grouped by user story (spec.md) to enable independent implementation and testing of each story. Async (`mint/db/asynk/`) is built and tested first per story; the sync mirror (`mint/db/sync/`) follows immediately within the same task where practical, or as its own `[P]` task otherwise.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: US1–US5 map to spec.md's five user stories
- File paths are exact

## Path Conventions

- Source: `mint/db/` (per plan.md's Project Structure)
- Tests: `tests/db/` (mirrors `mint/db/` 1:1)
- Docs: `docs/db-repository-implementation-notes.md`

---

## Phase 1: Setup

**Purpose**: Package skeleton and dependency wiring — no behavior yet

- [ ] T001 Create `mint/db/__init__.py`, `mint/db/asynk/__init__.py`, `mint/db/sync/__init__.py` (empty skeleton, matches `mint/fs/`'s existing convention)
- [ ] T002 Add `sqlmodel`, `asyncpg`, `psycopg2-binary` to `pyproject.toml` main dependencies, and a `postgres` extra bundling `testcontainers[postgres]` (mint currently only has `[azurite]`/`[localstack]`)
- [ ] T003 [P] Create `tests/db/__init__.py`, `tests/db/asynk/__init__.py`, `tests/db/sync/__init__.py`

**Checkpoint**: `uv sync --extra postgres` succeeds; package importable but empty.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The shared session-lifecycle mechanism every user story depends on — including the core concurrency fix (US2) and the scoping listener (US3), since both live in `RepositoryBase` itself, not in any one story's own file.

**⚠️ CRITICAL**: No user story phase can begin until this phase is complete.

- [ ] T004 Postgres testcontainer pytest fixtures in `tests/db/conftest.py`: session-scoped container, an `engine`/`async_engine` fixture per test, and a function-scoped fixture that creates+drops all tables registered on `mint.db.models.Base.metadata` around each test
- [ ] T005 [P] Shared test schema fixtures in `tests/db/schemas.py`: a plain `TItem(BaseWithUUID, table=True)`, an owner+audit-scoped `TJob(OwnerMixin, AuditMixin, BaseWithUUID, table=True)`, and a soft-deletable `TDoc(BaseWithUUID, table=True)` with `is_deleted` — reused across every story's tests instead of redefined per file
- [ ] T006 [P] `mint/db/exc.py`: `RepositoryError` hierarchy (`NotFoundError`, `OperationalError`, `ConfigError`, `AbnormalResultError`), per contracts/repository-api.md
- [ ] T007 [P] `mint/db/settings.py`: `DatabaseSettings` (pydantic-settings, ported from mini's shape — `POOL_PRE_PING`/`ECHO`/`POOL_SIZE`/`MAX_OVERFLOW`/`POOL_RECYCLE`)
- [ ] T008 [P] `mint/db/typedefs.py`: `CRUDStatement`/`PrepableStatement` type aliases
- [ ] T009 `mint/db/models.py`: `Base`, `BaseWithUUID`, `BaseWithIntID`, `BaseWithStrID`, `AuditMixin`, `OwnerMixin`, `HasSoftDelete`/`HasOwnerColumn` protocols — per data-model.md's "Schema layer" section (depends on T002)
- [ ] T010 [P] Confirmation-spike test `tests/db/asynk/test_models.py`: subclass field override (redeclare a mixin's column with a different constraint), `declared_attr` relationship on a concrete subclass, `PrivateAttr` for transient non-column instance state + `@reconstructor`, `hybrid_property` with a separate `.expression` form usable in `order_by`, and confirmation that string-based `secondary=` many-to-many resolves against the single shared `Base.metadata` — every item from the architecture plan's "Complex real-world patterns" table that concerns `models.py` specifically (depends on T009; run against real Postgres per T004)
- [ ] T011 `mint/db/asynk/database.py`: `Database` — engine + `async_sessionmaker` + `create_session()` async context manager, `dbschema`/`schema_translate_map` support (ported from `mini/repos/asynk/database.py`, per contracts/repository-api.md)
- [ ] T012 [P] `mint/db/sync/database.py`: sync mirror of T011 (`Session`/`sessionmaker`)
- [ ] T013 `mint/db/asynk/base.py`: `RepositoryBase[T]` — `ContextVar`-based `ensure_session` (the session-isolation fix), `session` property, `execute`/`execute_many` with `auto_commit` gating, `_construction_kwargs()`/`clone()`, and the `with_loader_criteria` + instance-scoped `do_orm_execute` scoping listener (soft-delete + owner criteria, `mint_include_deleted`/`mint_skip_owner_scope` bypass execution options) — the single most load-bearing file in this feature; depends on T006, T009, T011
- [ ] T014 [P] `mint/db/sync/base.py`: sync mirror of T013 (same `ContextVar` mechanism — valid for thread-based isolation, not just task-based)
- [ ] T015 [P] `mint/db/asynk/interface.py`: `IRepository`/`IEntityRepository` as `typing.Protocol` (per `mint/fs/asynk/interface.py` convention)
- [ ] T016 [P] `mint/db/sync/interface.py`: sync mirror of T015

**Checkpoint**: `RepositoryBase` is session-isolated and scoping-capable in both async and sync; every user story phase below builds on this without touching it further.

---

## Phase 3: User Story 1 - Zero-boilerplate CRUD for a new table (Priority: P1) 🎯 MVP

**Goal**: `EntityRepository[T, I]` delivers full CRUD for any single-ID-column table with zero method overrides.

**Independent Test**: Define a table + one-line repository, exercise create/get/update/remove with no custom code.

### Tests for User Story 1

- [ ] T017 [P] [US1] Zero-boilerplate CRUD test in `tests/db/asynk/test_crud.py`: create/get/update/remove against `TItem` (from T005) via a bare `EntityRepository[TItem, UUID]` subclass with only `Schema = TItem` set — asserts SC-002 directly
- [ ] T018 [P] [US1] `create()` payload-validation test in the same file: a `TItemCreate(SQLModel)` companion (no `id`) and `TItemUpdate(SQLModel)` companion (`exclude_unset` partial update) — asserts FR-014

### Implementation for User Story 1

- [ ] T019 [US1] `mint/db/asynk/entity.py`: `EntityRepository[T, I]` — `get`, `update`, `remove`, `remove_many`, `get_many_by_ids` (depends on T013; makes T017/T018 pass)
- [ ] T020 [P] [US1] `mint/db/sync/entity.py`: sync mirror of T019
- [ ] T021 [P] [US1] Sync-side CRUD test `tests/db/sync/test_crud.py` mirroring T017/T018

**Checkpoint**: Zero-boilerplate CRUD works end-to-end, async and sync, against real Postgres.

---

## Phase 4: User Story 2 - Correct behavior under concurrent use (Priority: P1)

**Goal**: Verify the `ContextVar` session-isolation fix from Phase 2 under real concurrency — this is the regression test for the bug that motivated this entire port.

**Independent Test**: Many concurrent create-then-fetch operations on one shared repository instance; every operation must see only its own record.

### Tests for User Story 2

- [ ] T022 [US2] Concurrency regression test in `tests/db/asynk/test_session_isolation.py`: `asyncio.gather` many concurrent `create()`+`get()` pairs through **one shared** `EntityRepository` instance; assert every task's fetch returns exactly the record it created, zero cross-task bleed — the direct test for spec.md SC-001 (depends on T019; MUST have failed against mini's original `self._session`-instance-attribute design, so write this to genuinely exercise concurrent scheduling, e.g. via a small artificial `await asyncio.sleep(0)` yield point between create and get to force interleaving)
- [ ] T023 [P] [US2] Exception-mid-operation test in the same file: force an exception inside one `ensure_session`-wrapped call, then verify the next call on that same repository instance succeeds cleanly (not left holding a stale/closed session) — direct test for spec.md FR-002 / Edge Case 1
- [ ] T024 [P] [US2] Thread-based concurrency test in `tests/db/sync/test_session_isolation.py`: same shape as T022 but with a `ThreadPoolExecutor` driving concurrent sync calls through one shared repository instance, confirming the `ContextVar` isolates by thread as well as by task (this is also the regression test for mini's separate sync-side `scopefunc=None` bug)

**Checkpoint**: The core bug this feature exists to fix is verifiably closed, async and sync.

---

## Phase 5: User Story 3 - Scoping can't be silently skipped (Priority: P2)

**Goal**: Soft-delete and owner scoping apply automatically to every query — including hand-written custom ones — via the Phase 2 listener; `SoftDeleteMixin`/`ResourceOwnerMixin` compose cleanly on top.

**Independent Test**: A custom query method with zero scoping code still excludes soft-deleted/other-owner rows; explicit bypass flags work.

### Tests for User Story 3

- [ ] T025 [P] [US3] Custom-query scoping-gap-closure test in `tests/db/asynk/test_scoping.py`: a hand-written method that calls `self.session.execute(select(TDoc))` directly (no `restrain()`-equivalent call anywhere in it) still excludes a soft-deleted `TDoc` row — this is the specific gap mini's `restrain()` had, per architecture plan's Interview #2, and is the primary acceptance test for User Story 3
- [ ] T026 [P] [US3] Owner-scoping test in the same file: two `TJob` rows with different `created_by_user_id`, an owner-scoped repository's `get_many()` **and** a custom hand-written query both return only the current owner's row
- [ ] T027 [P] [US3] Bypass-flag test in the same file: `stmt.execution_options(mint_include_deleted=True)` surfaces a soft-deleted row; `mint_skip_owner_scope=True` surfaces a different owner's row — asserts FR-006
- [ ] T028 [P] [US3] `SoftDeleteMixin.remove()` test: confirms `remove()` performs `UPDATE is_deleted = True` (row still present in the table, `is_deleted=True`) rather than a hard `DELETE`

### Implementation for User Story 3

- [ ] T029 [US3] `mint/db/asynk/mixins.py`: `SoftDeleteMixin[T, I]` (soft `remove`/`remove_many` only — no `restrain()` override, per architecture plan), `ResourceOwnerMixin[T]` (`owner`, `is_scoped`, read directly by the Phase 2 listener closure) (depends on T013, T019)
- [ ] T030 [P] [US3] `mint/db/sync/mixins.py`: sync mirror of T029
- [ ] T031 [P] [US3] Sync-side scoping tests `tests/db/sync/test_scoping.py` mirroring T025–T028

**Checkpoint**: Scoping is unconditional on schema shape, verified against the exact gap found in real production code.

---

## Phase 6: User Story 4 - Multi-table atomic operations, only when needed (Priority: P2)

**Goal**: `UnitOfWork` gives multi-repository atomicity as an explicit, opt-in primitive without touching the zero-ceremony default path.

**Independent Test**: Two writes across two tables inside one `UnitOfWork`, forced failure, verify neither persisted; separately verify a simple call still needs no wrapping.

### Tests for User Story 4

- [ ] T032 [P] [US4] `UnitOfWork` atomicity test in `tests/db/asynk/test_uow.py`: write a `TItem` and a `TJob` inside one `async with UnitOfWork(db) as uow:` block, raise before `commit()`, verify neither row persists after the block exits
- [ ] T033 [P] [US4] `UnitOfWork` cross-repository read-your-writes test in the same file: write via one repository constructed with session=uow.session, read the uncommitted row via a **second** repository on the same shared session, before commit()
- [ ] T034 [P] [US4] Zero-ceremony-path-untouched test in the same file: a plain `ItemRepository(db).get(id)` call outside any `UnitOfWork` still requires no setup — guards against a future regression coupling the two paths

### Implementation for User Story 4

- [ ] T035 [US4] `mint/db/asynk/uow.py`: `UnitOfWork` — opens one session via `Database.create_session()`, `repo()` factory binding repositories to it via the existing `session=` constructor param, `commit()`/`rollback()`, and scoping-listener cleanup for the shared-session path on `__aexit__` (depends on T011, T013)
- [ ] T036 [P] [US4] `mint/db/sync/uow.py`: sync mirror of T035
- [ ] T037 [P] [US4] Sync-side `UnitOfWork` tests `tests/db/sync/test_uow.py` mirroring T032–T034

**Checkpoint**: Multi-table atomicity is available, documented, and doesn't leak cost into the simple path.

---

## Phase 7: User Story 5 - Supports real-world schema shapes (Priority: P3)

**Goal**: Composite-key join tables, read-only materialized views, self-referential relationships, and many-to-many relationships all work correctly.

**Independent Test**: One table of each shape exercised against the layer.

### Tests for User Story 5

- [ ] T038 [P] [US5] Composite-key join-table test in `tests/db/asynk/test_schema_shapes.py`: a `TItemTag(Base, table=True)` with a two-column primary key and no `id`, accessed via plain `RepositoryBase[TItemTag]` (not `EntityRepository`) — `get_many()`/`create()` work, no identifier-based methods are available on it
- [ ] T039 [P] [US5] Materialized-view test in the same file: a read-only `TMVItemStats(Base, table=True)` behind `MaterializedViewRepository`, `refresh_materialized_view()` then `get_many()` returns refreshed data, no write methods exposed
- [ ] T040 [P] [US5] Self-referential relationship test in the same file: a `TFolder(BaseWithUUID, table=True)` with `parent_folder_id`/`parent_folder`/`child_folders` (`remote_side=`), load a folder with `extra_paths=["parent_folder"]` and confirm no runaway recursive query (bounded query count)
- [ ] T041 [P] [US5] Many-to-many relationship test in the same file: two tables joined via a `secondary=` association table (reusing the T038 composite-key shape as the association table), confirm the relationship resolves correctly against the shared `Base.metadata`

### Implementation for User Story 5

- [ ] T042 [US5] `mint/db/asynk/mv.py`: `MaterializedViewRepository[T: Base](RepositoryBase[T])` — `table_name` property (dbschema-qualified), `refresh_materialized_view()` via `text("REFRESH MATERIALIZED VIEW CONCURRENTLY ...")` (depends on T013)
- [ ] T043 [P] [US5] `mint/db/sync/mv.py`: sync mirror of T042
- [ ] T044 [US5] `fetch_extra_relationships` on `RepositoryBase` (`mint/db/asynk/base.py`): walk + `await` each dotted `extra_paths` entry via `awaitable_attrs`, unconditionally, for every object a repo call returns — the surviving half of mini's relationship-prefetch mechanism (depends on T013; makes T040 pass)
- [ ] T045 [P] [US5] Sync equivalent of T044 in `mint/db/sync/base.py` (plain attribute access, no `awaitable_attrs` needed under sync)
- [ ] T046 [P] [US5] Sync-side schema-shape tests `tests/db/sync/test_schema_shapes.py` mirroring T038–T041

**Checkpoint**: All catalogued real-world schema shapes (spec.md SC-004) are demonstrated working under test.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, final quality gates, full-suite verification

- [ ] T047 [P] `docs/db-repository-implementation-notes.md`: mini-vs-mint comparison table (session race fix, scoping mechanism replacement, ID-genericity resolution, dropped `StatementBuilder`), the zero-boilerplate + owner-scoped + `UnitOfWork` usage examples from quickstart.md, the gotchas table (`session.commit()` + `schema_translate_map` interaction, `lazy="selectin"` vs. `extra_paths`, "no `id` column → `RepositoryBase` not `EntityRepository`", `execute_many(unique=True)` for `joinedload`), and the confirmation-spike findings from T010 — mirrors `docs/s3-implementation-notes.md`'s structure, per architecture plan's Documentation section
- [ ] T048 [P] Google-style docstring audit across every public class/method in `mint/db/` (mint's documentation rule — non-negotiable, this becomes mkdocs reference material)
- [ ] T049 `uv run ruff check mint/db tests/db`, `uv run ruff format --check mint/db tests/db`, `uv run ty check mint/db` — fix everything to green
- [ ] T050 Full-suite run: `uv run pytest tests/db -v` — every test in Phases 2–7 passing against the real Postgres testcontainer, 100%, no skips (spec.md FR-015/SC-005 — the hard gate)
- [ ] T051 Execute quickstart.md's four code examples verbatim in a scratch script (or as a final `tests/db/test_quickstart.py`) to confirm the documented usage actually runs as written

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies.
- **Foundational (Phase 2)**: Depends on Setup. **Blocks every user story** — `RepositoryBase` (T013/T014) is shared by all five stories.
- **User Stories (Phases 3–7)**: All depend on Foundational. US1 (Phase 3) should land first since US2–US5's tests reuse `EntityRepository` (T019/T020). After that, US2–US5 are independent of each other and can proceed in any order or in parallel.
- **Polish (Phase 8)**: Depends on all five user story phases being complete (T050's full-suite gate requires everything else green).

### User Story Dependencies

- **US1**: Foundational only.
- **US2**: Foundational + US1's `EntityRepository` (T019) as the vehicle its regression test drives through.
- **US3**: Foundational + US1's `EntityRepository` (mixins extend it).
- **US4**: Foundational only (`UnitOfWork` wraps `RepositoryBase`/`Database` directly, not `EntityRepository`).
- **US5**: Foundational only (`MaterializedViewRepository` wraps `RepositoryBase` directly).

### Parallel Opportunities

- T006–T008, T010 (after T009), T012 within Foundational.
- T014–T016 within Foundational (sync mirrors, independent files).
- Every `[P]`-marked test task within a story phase (different files or independent assertions in the same file, no shared mutable fixture state across them).
- US3, US4, US5 implementation can proceed in parallel once US1 lands, by different contributors.

---

## Parallel Example: User Story 3

```bash
# Tests, once T029 (mixins.py) exists:
Task: "Custom-query scoping-gap-closure test in tests/db/asynk/test_scoping.py"
Task: "Owner-scoping test in tests/db/asynk/test_scoping.py"
Task: "Bypass-flag test in tests/db/asynk/test_scoping.py"
Task: "SoftDeleteMixin.remove() test"

# Sync mirror, independent of the above:
Task: "mint/db/sync/mixins.py sync mirror"
```

---

## Implementation Strategy

### MVP First

1. Phase 1 (Setup) → Phase 2 (Foundational — the concurrency fix and scoping
   listener both land here, even though their *dedicated regression tests*
   live in US2/US3's phases).
2. Phase 3 (US1) → **STOP, validate independently**: zero-boilerplate CRUD
   works against real Postgres. This alone is a demonstrable MVP.

### Incremental Delivery

1. Setup + Foundational + US1 → MVP: CRUD works, session-isolation fix is in
   place (even before its dedicated test in Phase 4 exists — write Phase 4's
   test immediately after, since it's the direct verification of the bug
   this whole feature fixes).
2. Add US2 → the core bug is now verifiably closed.
3. Add US3 → scoping can't be silently skipped, verified against the exact
   gap found in production code.
4. Add US4 → multi-table atomicity available.
5. Add US5 → all catalogued real-world schema shapes verified.
6. Phase 8 → documentation, lint/type gates, full-suite 100%-pass
   confirmation. This is the completion gate the user set explicitly.

### Notes

- Every `[Story]`-labeled test task must be run against the real Postgres
  container from T004 — none of them are meaningful against sqlite/in-memory,
  since several (T022/T024 concurrency, T025 custom-query scoping) test
  behavior that specifically depends on a real async driver / real session
  semantics.
- Commit after each task or logical group, per user's global git workflow
  preference (new commits, never amend unless asked).
- T050 (full-suite run) is the single hard completion gate: 100% pass, no
  skips, against real Postgres — matches the user's explicit instruction.
