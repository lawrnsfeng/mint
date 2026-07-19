# Implementation Plan: Database Repository Layer

**Branch**: `feat/db-repository-layer` | **Date**: 2026-07-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `specs/002-db-repository-layer/spec.md`, and the
approved architecture plan at `/home/lawrence/.claude/plans/ok-take-a-look-fuzzy-quail.md`
(referred to below as "the architecture plan" — the primary technical source; this
document formalizes it into spec-kit's plan/research/data-model/contracts shape rather
than re-deriving it).

## Summary

Port `mini/mini/repos/` (a generic SQLAlchemy repository layer) into mint as
`mint/db/`, async and sync, fixing a real session-isolation race condition and
adopting SQLModel to collapse mini's separate ORM-schema/domain-model split
into one class per table. Primary technical approach: per-repository-instance
`ContextVar[Session]` for session isolation (mirrors `mint/fs/asynk/s3.py`'s
`_client_ctx`), concrete per-ID-type SQLModel base classes with genericity
kept at the repository layer (`EntityRepository[T, I]`), and
`sqlalchemy.orm.with_loader_criteria` + a `do_orm_execute` session event for
soft-delete/owner scoping that can't be silently skipped by a custom query
method (replacing mini's opt-in `restrain()`).

## Technical Context

**Language/Version**: Python 3.13+ (mint's `requires-python`), PEP 695 generics only.

**Primary Dependencies**: `sqlalchemy>=2.0`, `sqlmodel`, `asyncpg` (async
driver), `psycopg2-binary` (sync driver), `pydantic-settings` — same
dependency family mini already pins, `sqlmodel` newly added.

**Storage**: PostgreSQL (production target; async via `asyncpg`, sync via
`psycopg2`).

**Testing**: `pytest`, `pytest-asyncio`, `testcontainers[postgres]` (new
extra — mint currently only has `testcontainers[azurite]`/`[localstack]`).
Verification is against a real PostgreSQL container, never sqlite/in-memory —
this is a hard project constraint (architecture plan, "Branch &
implementation"; spec.md FR-015/SC-005), since the bug this feature fixes
only manifests against a real async driver under real concurrency.

**Target Platform**: Linux server (matches mint's existing `fs/` layer;
no platform-specific concerns).

**Project Type**: Library — an internal, importable package (`mint.db`)
consumed by other mint-based services, not a standalone service or CLI.
Mirrors `mint.fs`'s existing shape (`asynk/` + `sync/` submodules under one
package).

**Performance Goals**: Not a throughput-optimization feature; the functional
bar is correctness under concurrency (spec.md SC-001), not a specific
requests/sec target. No new performance goals beyond "a session opened by one
concurrent operation is never observed by another."

**Constraints**:
- All code passes `uv run ruff check`, `uv run ruff format --check`,
  `uv run ty check` (mint coding-style rule).
- Every public class/method has a full Google-style docstring (mint
  documentation rule) — non-negotiable here since this becomes
  mkdocs-published reference documentation for other developers
  (architecture plan, "Documentation" section).
- Files target 200–400 lines, hard cap 800 (mint modular-design rule) — this
  is *why* the architecture plan splits `base.py`/`entity.py`/`mixins.py`/
  `mv.py`/`uow.py`/`interface.py` instead of one large module.
- Module-local exception hierarchy rooted at `mint.db.exc.RepositoryError`
  (mint modular-design rule) — mirrors mini's `RepositoryError`/
  `NotFoundError`/`OperationalError`/`ConfigError`/`AbnormalResultError`.
- OOP method-placement rule: any function whose first param is an owned class
  (`RepositoryBase`, `Database`, a schema class) is a method on that class or
  a service class, not a dangling function.

**Scale/Scope**: One new top-level package (`mint/db/`), ~12 source files
(async + sync mirrors of `base`, `entity`, `mixins`, `mv`, `uow`,
`interface`, plus shared `models.py`/`exc.py`/`settings.py`/`typedefs.py`),
one new doc page, one new test-dependency extra. No changes to `mint/fs/` or
`mint/utils/` beyond reuse (e.g. `mint.utils.limiter.ConcurrencyLimiter` if a
connection-limiting need surfaces the same way `fs/asynk/s3.py` uses it — not
assumed necessary here since `Database` owns a pooled `Engine`, not a
per-call client).

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

mint's `.claude/memory/constitution.md` is still the unfilled spec-kit
template (no project-specific principles have been ratified into it). The
binding, already-enforced source of truth for this repo is
`.claude/rules/*.md` (`coding-style.md`, `modular-design.md`, `oop.md`,
`documentation.md`), which this plan already treats as gates — see
Technical Context → Constraints above, each one traced to a specific rule
file. No fabricated constitution principles are introduced here; gates below
are re-statements of those existing rule files, applied to this feature:

| Gate | Source | Status |
|---|---|---|
| PEP 695 generics, no `TypeVar`-only style | `coding-style.md` | Pass — `EntityRepository[T, I]`, `RepositoryBase[T]` use `class Foo[T]` syntax throughout, per architecture plan |
| Google-style docstrings on all public API | `documentation.md` | Pass — required deliverable, tracked in tasks.md |
| 200–400 line files, 800 hard cap | `modular-design.md` | Pass — target layout already splits by responsibility (session lifecycle vs. entity CRUD vs. mixins vs. materialized views vs. unit-of-work) specifically to stay under this cap |
| Module-local exception hierarchy | `modular-design.md` | Pass — `mint/db/exc.py`, rooted at `RepositoryError` |
| OOP method placement | `oop.md` | Pass — `_construction_kwargs()`/`clone()` cooperative-override hook (architecture plan) is itself the mechanism that keeps mixin logic as methods, not dangling functions |
| No new module named after a stdlib module | `coding-style.md` | Pass — `mint/db/typedefs.py` (not `types.py`), consistent with mint's existing `mint/fs/typedefs`-style naming already avoided elsewhere |

No violations requiring the Complexity Tracking table below.

## Project Structure

### Documentation (this feature)

```text
specs/002-db-repository-layer/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md         # Phase 1 output
├── quickstart.md          # Phase 1 output
├── contracts/               # Phase 1 output — public Python API surface
│   └── repository-api.md
└── tasks.md                   # Phase 2 output (/speckit-tasks — not this command)
```

### Source Code (repository root)

```text
mint/db/
├── __init__.py
├── exc.py                    # RepositoryError hierarchy
├── settings.py                # DatabaseSettings (pydantic-settings)
├── typedefs.py                  # CRUDStatement / PrepableStatement type aliases
├── models.py                      # Base, BaseWithUUID/IntID/StrID, OwnerMixin,
│                                   # AuditMixin, HasSoftDelete/HasOwnerColumn protocols
├── asynk/
│   ├── __init__.py
│   ├── database.py                # Database: engine + async_sessionmaker + create_session()
│   ├── base.py                     # RepositoryBase: ContextVar session isolation,
│   │                                # ensure_session, with_loader_criteria scoping listener
│   ├── entity.py                    # EntityRepository[T, I]: get/update/remove/get_many_by_ids
│   ├── mixins.py                     # SoftDeleteMixin, ResourceOwnerMixin
│   ├── mv.py                          # MaterializedViewRepository[T]
│   ├── uow.py                          # UnitOfWork
│   └── interface.py                     # IRepository/IEntityRepository (Protocol)
└── sync/
    └── (mirrors asynk/, sync Session instead of AsyncSession)

tests/db/
├── conftest.py                # postgres testcontainer fixture, shared across asynk/sync
├── asynk/
│   ├── test_session_isolation.py     # the concurrency regression test (SC-001)
│   ├── test_crud.py                   # zero-boilerplate CRUD (SC-002)
│   ├── test_scoping.py                 # soft-delete/owner scoping incl. custom queries (SC-003)
│   ├── test_uow.py                      # UnitOfWork atomicity + zero-ceremony path (User Story 4)
│   ├── test_schema_shapes.py             # composite-key, materialized view, self-referential,
│   │                                      # many-to-many (User Story 5 / SC-004)
│   └── test_models.py                     # confirmation-spike items: subclass field override,
│                                           # declared_attr, PrivateAttr, hybrid_property
└── sync/
    └── (mirrors asynk/)

docs/
└── db-repository-implementation-notes.md   # mirrors docs/s3-implementation-notes.md
```

**Structure Decision**: Single project, library shape — `mint/db/` sibling to
the existing `mint/fs/` package, same `asynk/`+`sync/` submodule convention
`mint/fs/` would use if it had a sync side. Tests live under `tests/db/`
mirroring the source tree 1:1 so `pytest tests/db` runs the whole feature's
verification surface. This matches how `mint/fs/asynk/{s3,abs}.py` are
already tested (real backend via testcontainers, not mocks) — same pattern,
different backend.

## Complexity Tracking

*No entries — no Constitution Check violations.*
