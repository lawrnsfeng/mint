# Quickstart: `mint.db`

## Simple table, zero boilerplate

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

No session setup, no method overrides.

## Upsert

```python
from uuid import UUID, uuid4

class JobUpsertCreate(SQLModel):
    id: UUID
    name: str

job_id = uuid4()
await repo.create(JobUpsertCreate(id=job_id, name="x"), upsert=True)
# same id again: updates in place instead of failing on the conflict
await repo.create(JobUpsertCreate(id=job_id, name="y"), upsert=True)
```

`upsert=True` inserts, or updates the conflicting row in place, in one
query (Postgres `ON CONFLICT ... DO UPDATE`) — the returned row is always
the final state, never `None`. Defaults to conflicting on `Job`'s primary
key; pass `conflict_columns=[...]` to upsert against a different unique
constraint instead:

```python
await repo.create(
    SlugCreate(tenant_id=tenant_id, slug="hello", name="x"),
    upsert=True,
    conflict_columns=["tenant_id", "slug"],
)
```

Without `upsert=True` (the default), `create()` behaves exactly as before —
a plain insert that fails at the database level on a conflict.

## Owner-scoped table

```python
from mint.db.models import AuditMixin, BaseWithUUID, OwnerMixin
from mint.db.asynk import EntityRepository, ResourceOwnerMixin

class Job(OwnerMixin, AuditMixin, BaseWithUUID, table=True):
    name: str

class JobRepository(ResourceOwnerMixin[Job], EntityRepository[Job, UUID]):
    Schema = Job

repo = JobRepository(db, owner=current_user)
await repo.get_many()   # every row scoped to current_user, incl. custom queries
```

`current_user` must satisfy `mint.db.asynk.IOwner`: an `id` attribute and an
`is_scoped` property. When `is_scoped` is `False` (e.g. an admin/superuser),
every row is visible — scoping is opt-out per owner, not hardcoded.

## Multi-table atomic transaction

Repositories sharing a `UnitOfWork` session are constructed normally, passing
`session=uow.session` — there is no generic `repo()` factory (forwarding
arbitrary constructor kwargs to an arbitrary repository subclass can't be
expressed without `Any`, which mint's coding-style rule forbids). They must
also be constructed with `auto_commit=False`, otherwise each
`create()`/`update()`/`remove()` call commits itself immediately regardless
of the shared session, defeating the point of the unit of work:

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

## Explicit scoping bypass (soft-deleted / cross-owner)

```python
from sqlmodel import col, select
from mint.db.models import IsDeletedMixin, OwnerMixin

stmt = select(Job).where(col(Job.id) == job_id).execution_options(
    mint_scope_bypass=frozenset({IsDeletedMixin}),  # or {OwnerMixin}, or both
)
result = await repo.execute(stmt)
```

## Eager-loading a relationship

There is no repository-level "prefetch paths" mechanism — add
`.options(selectinload(...))` (or `joinedload(...)`) directly to the
statement passed to `execute()`/`execute_many()`. SQLAlchemy resolves it as
one extra query (`selectinload`) or one JOIN (`joinedload`) for the entire
result set, regardless of how many rows come back:

```python
from sqlalchemy.orm import selectinload
from sqlmodel import col, select

stmt = (
    select(Job)
    .where(col(Job.id) == job_id)
    .options(selectinload(Job.collection))
)
job = await repo.execute(stmt)
```

## Paginated page + total count in one query

```python
result = await repo.get_many_page(skip=0, limit=20)
result.items   # Sequence[Job] — the page
result.total   # int — total matching rows, not just this page's length
```

A single query (`COUNT(*) OVER()` alongside the page `SELECT`) rather than
a separate page query plus a separate `count()` call.

`sqlmodel.col()` is needed on any field access used as a query expression
(`col(Job.id) == x`, `col(Job.id).in_(ids)`, ...) — SQLModel exposes class
attributes typed as their plain Python field type for pydantic's sake, not
as `InstrumentedAttribute`, so a type checker sees `UUID`/`str`/etc., not a
comparable column, without it.

## Materialized view

```python
from uuid import UUID
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
`EntityRepository` — no `get`/`update`/`remove` are exposed, since a
materialized view can't be written to.

## Verifying locally

```bash
uv sync --all-groups        # or: uv sync --group db --group test
uv run pytest tests/db      # spins up a real Postgres container per session
uv run ruff check mint/db
uv run ruff format --check mint/db
uv run ty check mint/db
```
