# mint

Practical utility library: async file storage backends behind a common
protocol, the concurrency primitives they're built on, a generic
SQLModel-based repository layer for Postgres, and an async canvas worker for
chain/chord task orchestration over a broker.

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
- **[Worker](worker/usage.md)** — `Worker[T, RT]`/`WorkerApp` for chain
  (sequence) and chord (fan-out/fan-in) task orchestration, fully async and
  by default with no orchestrator process (each worker advances the canvas
  itself, right after finishing its own task). Four broker implementations
  (RabbitMQ, Redis, NATS, Kafka) behind one `IBroker` Protocol, all
  at-least-once and tested against a shared contract suite; five executor
  strategies (inline, thread pool, process pool, gRPC, AMQP-RPC) behind one
  `ITaskExecutor` Protocol; an opt-in centralized `Coordinator` mode adding
  `cancel(canvas_id)` and a timeout sweeper on top of the same engine.
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
uv sync --group worker  # mint.worker (redis, aio-pika, nats-py, aiokafka, grpcio)
```

## Where to go next

- **[File Storage usage](fs/usage.md)** / **[API reference](fs/api.md)**
  — construction options for both backends, recursive copy/remove via
  `sprout.Executor`, known behavioral gaps between them.
- **[Worker usage](worker/usage.md)** / **[API reference](worker/api.md)**
  / **[Implementation Notes](worker-implementation-notes.md)** / **[Bugs and
  Fixes](worker-bugs-and-fixes.md)** — the `Chain`/`Chord` DSL, embedded vs.
  centralized deployment modes, every broker/executor, a migration mapping from
  an internal predecessor, and a walkthrough of every bug found while porting
  and reviewing it.
- **[DB Repository Layer usage](db/usage.md)** / **[API
  reference](db/api.md)** — the deepest guide in this site: design
  rationale, every method and option, real-world schema shapes, and
  caveats confirmed against real Postgres.
- **[Utils usage](utils/usage.md)** / **[API reference](utils/api.md)**
  — `Batch`/`ConcurrencyLimiter` for bounded concurrent fan-out.
