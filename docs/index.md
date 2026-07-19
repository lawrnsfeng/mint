# mint

Practical utility library: async file storage backends behind a common
protocol, the concurrency primitives they're built on, and a generic
SQLModel-based repository layer for Postgres.

## What's in here

- **[File Storage](fs/usage.md)** — `IFileStorage[T]` protocol (`get`,
  `save`, `copy`, `move`, `remove`, `remove_many`, `stat`, `list`,
  `list_detailed`) with two implementations: `AzureBlobStorage` (Azure Blob
  Storage) and `S3Storage` (S3-compatible — AWS S3, LocalStack, MinIO).
- **[DB Repository Layer](db/usage.md)** — generic SQLModel-based
  repository layer for Postgres, async (`mint.db.asynk`) and sync
  (`mint.db.sync`): zero-boilerplate CRUD on a new table, soft-delete and
  per-owner scoping enforced automatically (not opt-in, including on
  hand-written custom queries), single-query pagination-with-count,
  insert-or-update (`upsert`), multi-repository atomic transactions, and
  read-only materialized-view-backed tables.
- **[Utils](utils/usage.md)** — `ConcurrencyLimiter` (reentrant async
  semaphore wrapper) and `Batch` (splitting a sequence/iterator into sized
  batches for bounded concurrent fan-out).
- **[sprout](https://github.com/lawrnsfeng/sprout)** — bounded, retrying
  async tree executor (external dependency) used internally for
  hierarchical folder traversal (recursive `copy`/`remove`) without
  unbounded fan-out or recursive-`async def` stack growth.

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

## Where to go next

- **[File Storage usage](fs/usage.md)** / **[API reference](fs/api.md)**
  — construction options for both backends, recursive copy/remove via
  `sprout.Executor`, known behavioral gaps between them.
- **[DB Repository Layer usage](db/usage.md)** / **[API
  reference](db/api.md)** — the deepest guide in this site: design
  rationale, every method and option, real-world schema shapes, and
  caveats confirmed against real Postgres.
- **[Utils usage](utils/usage.md)** / **[API reference](utils/api.md)**
  — `Batch`/`ConcurrencyLimiter` for bounded concurrent fan-out.
