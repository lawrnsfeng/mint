# mint

Practical utility library: async file storage backends behind a common
protocol, the concurrency and execution primitives they're built on, and an
async canvas worker for chain/chord task orchestration over a broker.

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
- **`mint.worker`** — `Worker[T, RT]`/`WorkerApp` for chain (sequence) and
  chord (fan-out/fan-in) task orchestration, fully async and, by default,
  with no orchestrator process (each worker advances the canvas itself,
  right after finishing its own task). Four at-least-once broker
  implementations (RabbitMQ, Redis, NATS, Kafka) behind one `IBroker`
  Protocol; five executor strategies (inline, thread pool, process pool,
  gRPC, AMQP-RPC) behind one `ITaskExecutor` Protocol; an opt-in
  centralized `Coordinator` mode adding `cancel(canvas_id)` and a timeout
  sweeper on top of the same engine.

See `docs/s3-implementation-notes.md` for a method-by-method comparison
between the Azure and S3 backends (including known behavioral gaps),
`docs/db-repository-implementation-notes.md` for `mint.db`'s design
decisions and confirmed SQLModel gotchas, `docs/worker-implementation-notes.md`
for `mint.worker`'s 17-bug catalogue and migration mapping from an internal
predecessor, and `specs/001-s3-storage/spec.md` /
`specs/002-db-repository-layer/spec.md` for the file-storage/DB layers'
feature specs.

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

```python
from pydantic import BaseModel
from mint.worker.worker import Worker
from mint.worker.app import WorkerApp
from mint.worker.brokers.rabbitmq import RabbitMQBroker
from mint.worker.stores.redis import RedisCanvasStore
from mint.worker.canvas.builder import Chain, Node

class GreetIn(BaseModel):
    name: str

class GreetOut(BaseModel):
    message: str

class Greet(Worker[GreetIn, GreetOut]):
    topic = "greet"
    Input = GreetIn
    Output = GreetOut

    async def process(self, input_obj: GreetIn) -> GreetOut:
        return GreetOut(message=f"hello, {input_obj.name}")

app = WorkerApp(
    broker=RabbitMQBroker("amqp://guest:guest@localhost/"),
    store=RedisCanvasStore("redis://localhost"),
)
app.register(Greet())

await Chain([
    Node(topic="greet", input=GreetIn(name="world").model_dump_json()),
]).apply(app.store, app.broker.publish)

await app.run()   # SIGTERM/SIGINT -> drain in-flight work -> close
```

See `docs/worker/usage.md` for the `Chord` fan-out/fan-in DSL, error policies,
executors, and the opt-in centralized `Coordinator` mode.

## Development

```bash
uv run pytest                # needs Docker: spins up Azurite + LocalStack + Postgres
uv run pytest --cov          # with coverage
uv run ruff check
uv run ruff format --check
uv run ty check
pre-commit run --all-files
```

`mint.worker`'s tests are memory-capped on purpose (a nested-fan-in payload bug
once OOM-killed `pytest` on this machine — see
`docs/worker-implementation-notes.md`'s incident writeup). Use the Makefile
lanes instead of a bare `pytest tests/worker`:

```bash
make test-worker              # fast lane: engine/worker/app + all mocked broker clients, no Docker
make test-worker-containers   # container lane: one broker container at a time, each memory-capped
make test-worker-capped TARGET=<pytest target> [MEM=1G]   # ad-hoc: any single depth/size-varying test
```

Coding standards live in `.claude/rules/`.
