# S3 Storage Implementation Notes

## ABS vs S3: Method Comparison

| Method | ABS Mechanism | S3 Mechanism | Status | Caveats |
|--------|---------------|--------------|--------|---------|
| `client` | `ContextVar[BlobServiceClient]` | `ContextVar[S3Client]` (aiobotocore) | Equivalent | Different type param |
| `is_folder` | `anext(list_blobs(prefix))`, checks `blob.name != path` | `list_objects_v2(Prefix, MaxKeys=2)`, checks if result differs from path | Equivalent | None |
| `get` | `download_blob()` + `aiofiles` | `get_object()["Body"]` + `aiofiles` | Equivalent | None |
| `save` | `upload_blob(overwrite=)` | `put_object()` always overwrites; `head_object` check for `overwrite=False` | Equivalent | Extra round-trip for `overwrite=False` |
| `copy` (single) | `start_copy_from_url()` | `copy_object()` | **Behavioral gap** | S3 `copy_object` limited to **5 GB**. Objects >5 GB require multipart copy. Current impl raises error for >5 GB. |
| `copy` (folder) | Concurrent `start_copy_from_url` via `asyncio.gather` | `AsyncTreeExecutor` traverses prefix, copies each file | Equivalent | See recursive analysis below |
| `move` | `copy` then `remove` | `copy` then `remove` | Equivalent | Inherits copy 5 GB limit |
| `remove` (single) | `delete_blob()` | `delete_object()` | Equivalent | None |
| `remove` (folder) | `list` then `remove_many` | `list` then `remove_many` | Equivalent | None |
| `remove_many` | Individual deletes via `_remove_many_files` in Batch | S3 `delete_objects` (max 1000/call) batched, `AsyncTreeExecutor` for recursive folders | More efficient | S3 bulk delete is atomic per object |
| `stat` | `get_blob_properties()` | `head_object()` with ClientError 404 detection | Equivalent | Different exception type (ClientError vs ResourceNotFoundError) |
| `list` | `list_blobs(name_starts_with)` auto-paginating `AsyncItemPaged` | `list_objects_v2` with manual `while` loop on `ContinuationToken` | Equivalent | ABS auto-paginates; S3 must paginate manually (max 1000 keys/call) |
| `list_detailed` | Blob properties returned inline from list call; `show_stats` adds `blob_type` + `metadata` | `list_objects_v2` returns keys + basic info; `show_stats` requires extra `head_object` per object | **Performance gap** | `show_stats=True` on 1000 objects = 1000 extra `head_object` calls |
| `gen_presigned_url` | Complex: user delegation key for `DefaultAzureCredential`, account key otherwise | Simple: `generate_presigned_url("get_object", ...)` always | Simpler in S3 | No user delegation key concept in S3 |
| `save_many` | `Batch.seq` + `asyncio.gather` per batch | Same pattern | Equivalent | None |

---

## Recursive Async Patterns

### Problem: Stack Overflow via Async Recursion

Two patterns in the legacy mini S3 implementation used **async recursion** that grows the call stack:

1. **Pagination recursion** (`list`):
   ```python
   # DANGER: Calls itself for every page of 1000 keys
   async def list(self, path, *, continuation_token=""):
       response = await self.client.list_objects_v2(...)
       if "NextContinuationToken" in response:
           obj_keys.extend(await self.list(path, continuation_token=...))
       return obj_keys
   ```
   With 100,000 objects = 100 stack frames, each awaited.

2. **Folder traversal recursion** (`remove_many`, `copy`):
   ```python
   # DANGER: Calls itself for each subfolder discovered
   async def remove_many(self, paths, *, recursive=False):
       for folderpath in folders:
           deleted_, errors_ = await self.remove_many(children, recursive=True)
   ```
   Depth proportional to folder nesting depth.

### Remedy 1: Iterative while loop (for pagination)

Pagination is not true tree traversal — it is a linear sequence. Use a cursor loop:

```python
token: str | None = None
while True:
    response = await self.client.list_objects_v2(
        Bucket=self.bucket_name,
        Prefix=path,
        **({} if token is None else {"ContinuationToken": token}),
    )
    # collect results ...
    token = response.get("NextContinuationToken")
    if token is None:
        break
```

No recursion. No stack growth. Used for `list` and `list_detailed`.

### Remedy 2: AsyncTreeExecutor (for recursive folder traversal)

`remove_many(recursive=True)` and `copy(folder/, recursive=True)` are genuine tree traversal
problems: each folder may have subfolders discovered only at runtime. This maps directly to
the `AsyncTreeExecutor` pattern from `mint.asynctree`.

Advantages over recursive async calls:
- **Bounded concurrency**: `ConcurrencyGate` with semaphore + rate limiting
- **Grand timeout**: `StaticClock` / `DynamicClock` prevents unbounded operations
- **Retry strategies**: `RetryConfig` with exponential jitter per node
- **Partial result recovery**: on cancellation, already-processed nodes contribute to result
- **Deduplication**: cycle detection prevents infinite loops on symlink-like structures
- **Non-recursive**: uses `asyncio.wait(FIRST_COMPLETED)` event loop, O(1) stack depth

#### AsyncTreeExecutor usage in S3Storage

For `remove(folder/, recursive=True)` and `remove_many(paths, recursive=True)`:
- Root ref = folder path
- Fetcher: list children with `list_objects_v2(Prefix=folder, Delimiter="/")` — collect file keys (items) and subfolder prefixes (child refs)
- Files are deleted at each node; subfolders become child refs for deeper traversal

For `copy(src/, dst/, recursive=True)`:
- Fetcher: list src children — files are copied to corresponding dst paths; subfolders become child refs

---

## Known Gaps & Limitations

### 1. copy() 5 GB Object Limit
S3 `copy_object` API only supports objects up to **5 GB**. Objects larger than 5 GB
require the multipart copy API (`create_multipart_upload` + `upload_part_copy` + `complete_multipart_upload`).

Current implementation raises `InvalidArgumentsError` for objects >5 GB with a clear message.
Multipart copy can be added as a follow-up enhancement.

### 2. list_detailed show_stats Performance
`show_stats=True` enriches each object with `content_type` and `metadata`. In S3, these fields
are not returned by `list_objects_v2` — each requires a separate `head_object` call.

For 1000 objects with `show_stats=True`:
- ABS: 0 extra API calls (returned inline from list)
- S3: 1000 extra `head_object` calls

Mitigation: `head_object` calls are batched via `asyncio.gather` in chunks of `Batch.DEFAULT_SIZE`.

### 3. overwrite=False add extra round-trip
`save(path, ref, overwrite=False)` requires a `head_object` existence check before `put_object`.
ABS handles this atomically inside the SDK. S3 has no atomic conditional PUT without S3
Object Lambda or checksums — the TOCTOU race is accepted as a known limitation.

### 4. gen_presigned_url requires active credentials
`generate_presigned_url` in aiobotocore requires credentials at the time of URL generation
(to sign the URL). IAM role credentials work, but the URL will expire per `ExpiresIn`.
Unlike ABS user delegation keys, there is no way to generate presigned URLs without credentials.
