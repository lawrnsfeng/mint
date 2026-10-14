# mint

Practical utility library: async file storage backends behind a common
protocol, the concurrency primitives they're built on, and a Postgres
session lifecycle with a generic SQLModel-based repository layer on top.

## What's in here

- **[File Storage](fs/usage.md)** — `IFileStorage[T]` protocol (`get`,
  `save`, `copy`, `move`, `remove`, `remove_many`, `stat`, `list`,
  `list_detailed`) with two implementations: `AzureBlobStorage` (Azure Blob
  Storage) and `S3Storage` (S3-compatible — AWS S3, LocalStack, MinIO).
- **[DB Repository Layer](db/usage.md)** — Postgres session lifecycle
  (usable with SQLAlchemy Core alone, via `mint[db]`) and a generic
  SQLModel-based repository layer on top of it (`mint[orm]`), async
  (`mint.db.asynk`) and sync (`mint.db.sync`): zero-boilerplate CRUD on a
  new table, soft-delete and
  per-owner scoping enforced automatically (not opt-in, including on
  hand-written custom queries), single-query pagination-with-count,
  insert-or-update (`upsert`), session scopes (`@db.scoped`, `@scoped`)
  that give any function one session and one transaction shared with
  everything it calls, caller-committed `UnitOfWork` transactions, and
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

Storage backends are optional dependency groups and the DB layer is a pair
of optional extras — pull in only what you need:

```bash
uv sync --group azure   # AzureBlobStorage
uv sync --group s3      # S3Storage
uv sync --extra db      # mint.db session lifecycle (SQLAlchemy + asyncpg + psycopg2)
uv sync --extra orm     # the above plus the SQLModel repository layer
```

As a dependency of another project, the extras are `mint[db]` and
`mint[orm]`.

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
- **[Versioning and deprecation](versioning.md)** — what the public API
  is, how it may change, and what CI proves on every pull request.
