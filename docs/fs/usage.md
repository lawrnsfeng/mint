# File Storage

`mint.fs` is a common async protocol (`IFileStorage[T]`) over two storage
backends — `AzureBlobStorage` and `S3Storage` — so calling code can be
written once against the protocol and swapped between backends. Both
implementations live under `mint.fs.asynk`.

## Path conventions

- A path ending in `/` is a **folder/prefix** — operations against it are
  recursive-capable (`copy`, `move`, `remove`) or list-shaped (`list`,
  `list_detailed`).
- A path without a trailing `/` is a **single object**.

## Construction

```python
from mint.fs.asynk.abs import AzureBlobStorage

storage = AzureBlobStorage(
    container_name="my-container",
    storage_account_name="my-account",
    connection_string="<connection-string>",
    max_concurrent_clients=10,
)
```

Credentials are resolved in priority order: SAS token → shared access key
→ connection string → client secret (with `tenant_id`/`client_id`) →
`AZURE_STORAGE_ACCESS_KEY` env var → `AZURE_STORAGE_CONNECTION_STRING` env
var → `DefaultAzureCredential`. Supply whichever one you have; the rest
can be left `None`.

```python
from mint.fs.asynk.s3 import S3Storage

storage = S3Storage(
    bucket_name="my-bucket",
    endpoint_url="http://localhost:4566",  # e.g. LocalStack; omit for AWS
    access_key="...",
    secret_key="...",
    max_concurrent_clients=10,
)
```

Credentials are resolved in priority order: explicit `access_key`/
`secret_key` → `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars →
`~/.aws/credentials` shared profile → IAM role/instance metadata.

`max_concurrent_clients` (both backends) caps concurrent underlying client
operations via `ConcurrencyLimiter`, and matters more for `S3Storage`
specifically — each top-level call opens a new `aiobotocore`
client/session, so unlimited concurrency can overwhelm a lightweight
single-process endpoint like LocalStack.

## Basic operations

```python
await storage.save("path/to/file.txt", b"hello")   # bytes, Path, IO, or a str path
await storage.get("path/to/file.txt", "local/file.txt")
await storage.is_folder("path/to/")
await storage.stat("path/to/file.txt")               # Stat(last_modified, size)
await storage.remove("path/to/file.txt")
```

`save()` accepts `str | Path | IO[Any] | bytes` as the content reference.
`save(path, ref, overwrite=False)` costs one extra round trip on
`S3Storage` specifically (a `head_object` existence check before
`put_object`, since S3 has no atomic conditional PUT) — Azure handles
this atomically inside the SDK.

## Recursive copy, move, remove

```python
await storage.copy("path/", "backup/", recursive=True)
await storage.move("path/", "archive/", recursive=True)
await storage.remove("path/", recursive=True)
```

Recursive folder traversal for both backends goes through
[`sprout.Executor`](https://github.com/lawrnsfeng/sprout) rather than
recursive `async def` calls — deliberately: recursive-`async`
implementations grow the call stack proportional to folder nesting depth
(a real stack-overflow risk on deeply nested trees), while `sprout`'s
non-recursive event-loop-driven traversal gets bounded concurrency, a
grand timeout, retry, partial-result recovery on cancellation, and
cycle/duplicate detection for free, at O(1) stack depth. `list`/
`list_detailed` pagination uses a plain iterative cursor loop for the same
reason — pagination is a linear sequence, not a tree, so it doesn't need
`Executor` at all.

`move()` is `copy()` then `remove()` — it inherits `copy()`'s limits (see
below).

## Listing

```python
paths = await storage.list("path/", recursive=True)          # Collection[str]
items = await storage.list_detailed(
    "path/", show_stats=True, show_info=True, recursive=True,
)                                                              # Sequence[ListItem]
```

`show_stats=True` is meaningfully more expensive on `S3Storage` than
`AzureBlobStorage`: Azure returns `content_type`/`metadata` inline from
the list call; S3's `list_objects_v2` doesn't, so each object needs a
separate `head_object` call (batched via `Batch.DEFAULT_SIZE`-sized
`asyncio.gather` chunks, but still N extra round trips for N objects).

## Presigned URLs

```python
url = await storage.gen_presigned_url(
    "path/to/file.txt", expiration_in_seconds=3600, file_name="download.txt",
)
```

Simpler on S3 (`generate_presigned_url("get_object", ...)` unconditionally)
than on Azure, which needs a user delegation key for
`DefaultAzureCredential`-based auth and an account key otherwise — S3 has
no equivalent "user delegation key" concept.

## Known behavioral gaps between backends

- **`copy()` on S3 is limited to 5 GB per object** — S3's `copy_object`
  API caps out there; objects larger than that need the multipart copy
  API (`create_multipart_upload`/`upload_part_copy`/
  `complete_multipart_upload`), not currently implemented. `S3Storage`
  raises `CopySourceTooLargeError` for an object over the limit rather
  than silently failing partway through. Azure has no equivalent limit.
- **`stat()` 404 detection** differs by SDK: `ClientError` on S3 vs
  `ResourceNotFoundError` on Azure — both are caught and translated to
  `ObjectNotFoundError` at the `mint.fs` layer, so calling code never
  needs to know the difference.
- **`remove_many()`** batches via S3's native `delete_objects` (max 1000
  per call) plus `sprout.Executor` for recursive folders; Azure has no
  equivalent bulk-delete API, so it's individual `delete_blob()` calls
  batched through `Batch`.

See the [Implementation Notes](../s3-implementation-notes.md) for the
full method-by-method comparison table and the stack-overflow
investigation behind the `sprout.Executor` design choice.
