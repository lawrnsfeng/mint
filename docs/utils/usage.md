# Utils

`mint.utils` is the concurrency/batching foundation the rest of `mint` is
built on — `mint/fs/asynk/s3.py`'s `save_many`/`copy`/`remove_many` and
`mint.db`'s scoping-listener machinery both use these primitives directly.

## `Batch`: splitting work into sized chunks

```python
from mint.utils.batch import Batch

for chunk in Batch.seq(items, size=32):
    await asyncio.gather(*[process(item) for item in chunk])
```

This is the established pattern for **flat bounded async fan-out** in this
codebase: split a sequence into batches, `asyncio.gather` each batch in
turn. Bounded because only one batch's worth of coroutines is ever
in flight at once — unlike a single `asyncio.gather(*[process(i) for i in items])`
over the *entire* input, which fans out every item at once regardless of
size.

- **`Batch.seq(items, size=DEFAULT_SIZE) -> Sequence[Sequence[T]]`** — for
  a `Sequence` you already have in memory. `DEFAULT_SIZE` is `32`.
- **`Batch.iter(iterator, size=DEFAULT_SIZE) -> Iterator[list[T]]`** — the
  same, for a plain sync `Iterator`.
- **`Batch.aiter(iterator, size=DEFAULT_SIZE) -> AsyncIterator[list[T]]`**
  — the same, for an `AsyncIterator` (e.g. a paginated API response
  stream) — batches are yielded as they fill, without materializing the
  whole source first.

For **hierarchical** work instead of a flat list (a tree, a folder
structure, anything with runtime-discovered children), reach for
[`sprout.Executor`](https://github.com/lawrnsfeng/sprout) instead — it has
no exported flat "bounded gather" primitive of its own, so `Batch` +
`asyncio.gather` and `sprout.Executor` are complementary, not
overlapping: pick based on whether the input is a flat collection or a
tree.

## `ConcurrencyLimiter`: reentrant async semaphore

```python
from mint.utils.limiter import ConcurrencyLimiter

limiter = ConcurrencyLimiter(max_concurrent=10)

@limiter.limit
async def fetch_data(url: str) -> bytes:
    async with aiohttp.get(url) as resp:
        return await resp.read()

# or as a context manager directly:
async with limiter:
    await some_operation()
```

A wrapper around `asyncio.Semaphore` with one addition: **reentrancy**
within the same async context, via `ContextVar`. A nested call inside an
already-limited call shares the outer acquisition instead of deadlocking
against its own semaphore:

```python
@limiter.limit
async def outer() -> None:
    await inner()   # inner() is also @limiter.limit-decorated

@limiter.limit
async def inner() -> None:
    ...
```

Without reentrancy, `outer()` would acquire the semaphore, then `inner()`
would block forever trying to acquire the same semaphore from within
`outer()`'s own execution. `ConcurrencyLimiter` tracks acquisition depth
per async context and only actually acquires/releases the underlying
`asyncio.Semaphore` at depth 0 → 1 and 1 → 0.

- **`ConcurrencyLimiter(max_concurrent: int | None = None)`** — `None`
  uses `DEFAULT_MAX_CONCURRENT` (`10`). Raises
  `InvalidConcurrencyLimitError` if `max_concurrent` is given and `<= 0`.
- **`.limit`** — a decorator for an async method; every call through it
  competes for the same `max_concurrent` slots.
- **`async with limiter:`** — the same acquisition, usable directly around
  an arbitrary block instead of an entire method.

This is the mechanism behind `AzureBlobStorage`/`S3Storage`'s
`max_concurrent_ops` constructor option (see
[File Storage usage](../fs/usage.md#concurrency-knobs)). It bounds concurrent
*operations*; the SDK clients themselves are cached per (configuration, event
loop), and their HTTP pool size is set by `max_pool_connections` instead.
