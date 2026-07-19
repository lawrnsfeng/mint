# mint

Practical utility library: async file storage backends behind a common
protocol, plus the concurrency and execution primitives they're built on.

## What's in here

- **`mint.fs`** — `IFileStorage[T]` protocol (`get`, `save`, `copy`, `move`,
  `remove`, `remove_many`, `stat`, `list`, `list_detailed`, `is_folder`) with
  two implementations:
  - `mint.fs.asynk.abs.AzureBlobStorage` — Azure Blob Storage
  - `mint.fs.asynk.s3.S3Storage` — S3-compatible (AWS S3, LocalStack, MinIO)
- **[`sprout`](https://github.com/lawrnsfeng/sprout)** — bounded, retrying async
  tree executor (external dependency) used internally for folder
  traversal/copy/remove without unbounded fan-out.
- **`mint.utils`** — `ConcurrencyLimiter` (reentrant async semaphore wrapper),
  `Batch`, and `run_bounded` (bounded concurrent fan-out with retry and
  structured per-item failures).
- **`mint.db`** — generic SQLModel-based repository layer for Postgres, async
  (`mint.db.asynk`) and sync (`mint.db.sync`): `RepositoryBase[T]`/
  `EntityRepository[T, I]` for zero-boilerplate CRUD on a new table,
  `SoftDeleteMixin`/`ResourceOwnerMixin` for soft-delete and per-owner
  scoping (enforced automatically, including on hand-written custom
  queries — not opt-in), `UnitOfWork` for multi-repository atomic
  transactions, and `MaterializedViewRepository` for read-only
  materialized-view-backed tables.

See `docs/s3-implementation-notes.md` for a method-by-method comparison
between the Azure and S3 backends (including known behavioral gaps),
`docs/db-repository-implementation-notes.md` for `mint.db`'s design
decisions and confirmed SQLModel gotchas, and `specs/001-s3-storage/spec.md`
/ `specs/002-db-repository-layer/spec.md` for their feature specs.

## Install

```bash
uv sync --all-extras --all-groups --all-packages -U   # or: make sync
```

Storage backends and the DB layer are optional dependency groups — pull in
only what you need:

```bash
uv sync --group azure   # AzureBlobStorage
uv sync --group s3      # S3Storage
uv sync --group db      # mint.db (SQLModel + asyncpg + psycopg2)
```

## Usage

```python
from mint.fs.asynk.abs import AzureBlobStorage

storage = AzureBlobStorage(
    container_name="my-container",
    storage_account_name="my-account",
    connection_string="<connection-string>",
)

await storage.save("path/to/file.txt", b"hello")
await storage.get("path/to/file.txt", "local/file.txt")
await storage.copy("path/", "backup/", recursive=True)
```

```python
from mint.fs.asynk.s3 import S3Storage

storage = S3Storage(
    bucket_name="my-bucket",
    endpoint_url="http://localhost:4566",  # e.g. LocalStack; omit for AWS
    access_key="...",
    secret_key="...",
)

await storage.save("path/to/file.txt", b"hello")
await storage.get("path/to/file.txt", "local/file.txt")
await storage.copy("path/", "backup/", recursive=True)
```

Both classes accept a `max_concurrent_clients` keyword to cap concurrent
underlying client operations via `ConcurrencyLimiter`.

```python
from uuid import UUID
from sqlmodel import SQLModel
from mint.db.models import BaseWithUUID
from mint.db.asynk import Database, EntityRepository

class Job(BaseWithUUID, table=True):
    name: str

class JobCreate(SQLModel):
    name: str

class JobRepository(EntityRepository[Job, UUID]):
    Schema = Job

db = Database("postgresql+asyncpg://user:pass@localhost/mydb")
repo = JobRepository(db)
job = await repo.create(JobCreate(name="x"))
await repo.get(job.id)
```

See `docs/db-repository-implementation-notes.md` for owner-scoped tables,
`UnitOfWork`, and materialized views.

## Development

```bash
uv run pytest                # needs Docker: spins up Azurite + LocalStack + Postgres
uv run pytest --cov          # with coverage
uv run ruff check
uv run ruff format --check
uv run ty check
pre-commit run --all-files
```

Coding standards live in `.claude/rules/`.
