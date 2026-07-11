# mint

Practical utility library: async file storage backends behind a common
protocol, plus the concurrency and execution primitives they're built on.

## What's in here

- **`mint.fs`** — `IFileStorage[T]` protocol (`get`, `save`, `copy`, `move`,
  `remove`, `remove_many`, `stat`, `list`, `list_detailed`, `is_folder`) with
  two implementations:
  - `mint.fs.asynk.abs.AzureBlobStorage` — Azure Blob Storage
  - `mint.fs.asynk.s3.S3Storage` — S3-compatible (AWS S3, LocalStack, MinIO)
- **`mint.asynctree`** — bounded, retrying async tree executor used internally
  for folder traversal/copy/remove without unbounded fan-out.
- **`mint.utils`** — `ConcurrencyLimiter` (reentrant async semaphore wrapper),
  `Batch`, and `run_bounded` (bounded concurrent fan-out with retry and
  structured per-item failures).

See `docs/s3-implementation-notes.md` for a method-by-method comparison
between the Azure and S3 backends (including known behavioral gaps), and
`specs/001-s3-storage/spec.md` for the S3 backend's feature spec.

## Install

```bash
uv sync --all-extras --all-groups --all-packages -U   # or: make sync
```

Storage backends are optional dependency groups — pull in only what you need:

```bash
uv sync --extra azure   # AzureBlobStorage
uv sync --extra s3      # S3Storage
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

## Development

```bash
uv run pytest                # needs Docker: spins up Azurite + LocalStack
uv run pytest --cov          # with coverage
uv run ruff check
uv run ruff format --check
uv run ty check
pre-commit run --all-files
```

Coding standards live in `.claude/rules/`.
