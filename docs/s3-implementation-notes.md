# S3 Storage Implementation Notes

## ABS vs S3: Method Comparison

| Method | ABS Mechanism | S3 Mechanism | Status | Caveats |
|--------|---------------|--------------|--------|---------|
| `client` | `ContextVar[BlobServiceClient]` | `ContextVar[S3Client]` (aiobotocore) | Equivalent | Different type param |
| `is_folder` | `anext(list_blobs(prefix))`, checks `blob.name != path` | `list_objects_v2(Prefix, MaxKeys=2)`, checks if result differs from path | Equivalent | None |
| `get` | `download_blob()` + `aiofiles` | `get_object()["Body"]` + `aiofiles` | Equivalent | None |
| `save` | `upload_blob(overwrite=)` | `put_object()` always overwrites; `head_object` check for `overwrite=False` | Equivalent | Extra round-trip for `overwrite=False` |
| `copy` (single) | `start_copy_from_url()` | `copy_object()` | **Behavioral gap** | S3 `copy_object` limited to **5 GB**. Objects >5 GB require multipart copy. Current impl raises error for >5 GB. |
| `copy` (folder) | Concurrent `start_copy_from_url` via `asyncio.gather` | `sprout.Executor` traverses prefix, copies each file | Equivalent | See recursive analysis below |
| `move` | `copy` then `remove` | `copy` then `remove` | Equivalent | Inherits copy 5 GB limit |
| `remove` (single) | `delete_blob()` | `delete_object()` | Equivalent | None |
| `remove` (folder) | `list` then `remove_many` | `list` then `remove_many` | Equivalent | None |
| `remove_many` | Individual deletes via `_remove_many_files` in Batch | S3 `delete_objects` (max 1000/call) batched, `sprout.Executor` for recursive folders | More efficient | S3 bulk delete is atomic per object |
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

### Remedy 2: sprout.Executor (for recursive folder traversal)

`remove_many(recursive=True)` and `copy(folder/, recursive=True)` are genuine tree traversal
problems: each folder may have subfolders discovered only at runtime. This maps directly to
the `Executor` pattern from the [`sprout`](https://github.com/lawrnsfeng/sprout) package.

Advantages over recursive async calls:
- **Bounded concurrency**: `ConcurrencyGate` with semaphore + rate limiting
- **Grand timeout**: `StaticClock` / `DynamicClock` prevents unbounded operations
- **Retry strategies**: `RetryConfig` with exponential jitter per node
- **Partial result recovery**: on cancellation, already-processed nodes contribute to result
- **Deduplication**: cycle detection prevents infinite loops on symlink-like structures
- **Non-recursive**: uses `asyncio.wait(FIRST_COMPLETED)` event loop, O(1) stack depth

#### sprout.Executor usage in S3Storage

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

---

## Client caching: gotchas and institutional knowledge

Added by `003-storage-client-cache`. Everything marked **(confirmed)** was
verified against the installed SDKs on this machine (aiobotocore 3.9.0,
botocore 1.43.56, aiohttp 3.14.1, azure-storage-blob 12.27.1, azure-core 1.41.0,
azure-identity 1.25.3, Python 3.14), not carried forward from memory.

- **(confirmed) `aiobotocore.session.get_session()` caches nothing.** It is
  literally `return AioSession(env_vars)`. Both the resolved credentials
  (`botocore/session.py` `get_credentials`, guarded by `self._credentials is
  None`) and the parsed ~1 MB service-model JSON (`botocore/loaders.py`
  `instance_cache`) are memoized **per session**, and `create_loader` builds a
  cold `Loader` each time with no process-global fallback. A session per call
  therefore re-runs the whole credential provider chain, terminating at
  `InstanceMetadataProvider` — a live request to `http://169.254.169.254/`
  (`botocore/utils.py` `METADATA_BASE_URL`) on any host without env credentials.
  Measured: creating a client from a warm session is ~4.1 ms; creating one from
  a fresh session is ~94 ms, a 23x difference before any network I/O.
- **(confirmed) End-to-end, this was roughly a 28x penalty.** 100 concurrent
  `head_object` calls against LocalStack, five runs on one machine: a median
  12 313 ms with a session per call (range 11 600-13 218, 100 clients built)
  versus a median 458 ms with a cached client (range 418-791, 1 built). The
  per-run ratio ranged 14.7x-29.5x, median 28.0x -- the spread is machine load,
  so treat the order of magnitude as the result, not the exact figure.
- **(confirmed) A client's TCP pool is strictly per-client.** Each
  `AioBaseClient` gets its own `AIOHTTPSession` and `TCPConnector` with
  `limit=max_pool_connections` (`aiobotocore/endpoint.py`), so two clients from
  one session share no connections. The real cost of client churn is not the
  ~4 ms of CPU but discarding warm TLS connections.
- **(confirmed) `TCPConnector` binds the running event loop.** The source says
  so outright: `aiobotocore/httpsession.py`, `# TCPConnector binds the running
  loop, so build it here.` Any client cache **must** key on loop identity. This
  is the same lesson `docs/db-repository-implementation-notes.md` records for
  asyncpg, and `pyproject.toml` sets
  `asyncio_default_fixture_loop_scope = "function"`, so a loop-blind cache
  breaks the project's own test suite immediately.
- **(confirmed) A closed Azure client can never be reopened.**
  `AioHttpTransport.open()` raises
  `ValueError("HTTP transport has already been closed…")` once `close()` has
  nulled the session (`azure/core/pipeline/transport/_aiohttp.py`). Eviction
  must therefore be terminal: remove the entry, *then* close, and never hand the
  client out again. aiobotocore happens to tolerate re-entry after `__aexit__`
  (it resets `_sessions = None`), but do not rely on that asymmetry.
- **(confirmed) Azure child clients share everything and cost nothing.**
  `BlobServiceClient.get_container_client` wraps the parent transport in
  `AsyncTransportWrapper` — whose `open()`/`close()` are deliberate no-ops — and
  passes the parent's `_impl_policies` through, so the child shares the
  connection pool *and* the cached bearer token. Measured at 0.139 ms. This is
  why `abs.py`'s per-access `container` property is left alone: caching it would
  buy nothing and add a lifetime to reason about. The inverse is the trap —
  building a `ContainerClient` directly from a URL gets a brand-new
  `AioHttpTransport` and a cold token cache.
- **(confirmed) The expensive Azure object is the credential, not the client.**
  A `BlobServiceClient` constructor is ~0.17 ms warm, ~25x cheaper than an
  aiobotocore client. But `DefaultAzureCredential` / `ClientSecretCredential`
  each own a `TokenCache` (`azure/identity/_internal/managed_identity_client.py`)
  and their own `AioHttpTransport` (`azure/identity/_internal/pipeline.py`). The
  old `_create_client` built one per operation and **never closed it**, so every
  call both leaked an aiohttp session and discarded the token cache, forcing a
  fresh IMDS token fetch — which retries with `retry_total=5` and up to 60 s of
  backoff (`azure/identity/_credentials/imds.py`). Cache and close the
  credential, or caching the client alone achieves little.
- **(confirmed) Azure's bearer token is cached at the pipeline policy too.**
  `AsyncBearerTokenCredentialPolicy` holds the token behind double-checked
  locking (`azure/core/pipeline/policies/_authentication_async.py`), so
  `get_token` is not called per request — only when the policy's own copy goes
  stale. Sharing one client therefore shares that cache as well.
- **(confirmed) `isinstance` against a Protocol uses `inspect.getattr_static`.**
  On Python 3.12+ it does not fire `__getattr__`, so a **bare `MagicMock`
  satisfies no runtime-checkable Protocol** — it grows attributes only on
  access. A spec'd mock (`MagicMock(spec=IS3Client)`) passes for a different
  reason: it reports the spec as its `__class__`. Both facts matter: doubles
  must be spec'd, and a conformance test asserting real member presence needs a
  genuine class, not a spec'd mock, or it is vacuous.
- **(confirmed) A `ContextVar` token removes its entry; `set(None)` does not.**
  1000 set+reset cycles leave 0 entries in the context; 1000 set-only cycles
  leave 1000. `_ensure_client`'s `finally: reset(token)` is load-bearing.
  Conversely, `ConcurrencyLimiter.__aenter__`/`__aexit__` restoring depth by
  arithmetic rather than by token is **not** a leak — the entry it leaves lives
  in the borrowing task's own context and dies with it (verified: 1 entry inside
  the finished task, 0 in the parent). A token cannot be used there anyway: a
  `Token` may only be reset in the Context that created it, so an
  instance-level token stack desynchronises the moment two tasks overlap.
- **A cached client must be pinned for the length of an operation.** The
  provider counts borrows (`_CacheEntry.inflight`) and the idle sweep skips any
  entry in flight. Without that, a long `copy()` over many objects could have
  its client closed underneath it by another coroutine's sweep — holding a
  reference is not enough when the resource can be closed out from under you.
- **Route `client_factory` in the base class, not in each `_build`.** The first
  implementation checked the factory inside `S3ClientProvider._build` and
  `BlobClientProvider._build`; a stub provider in the tests that did not repeat
  the check silently ignored the caller's factory. `ClientProviderBase` now owns
  the branch so a backend cannot forget it.

### Shutting cached clients down

- **(confirmed) There is no running event loop at `atexit`.** `asyncio.run` and
  `asyncio.Runner` close their loop inside `Runner.close()`, long before exit
  handlers run. On Python 3.14 `asyncio.get_event_loop()` no longer mints a loop
  either — `_BaseDefaultEventLoopPolicy.get_event_loop` raises `RuntimeError`
  when none is set, and `Runner.close()` calls `set_event_loop(None)`. So an
  exit handler can do synchronous work only; it can never `await` a close.
- **(confirmed) Async-generator finalization is the one real hook.**
  `Runner.close()` calls `loop.run_until_complete(loop.shutdown_asyncgens())`
  *before* `loop.close()`, so a generator suspended at a `yield` gets
  `GeneratorExit` thrown in while the loop is still running — and its `finally`
  may await. Verified firing on normal completion, on `KeyboardInterrupt`, and
  under `asyncio.Runner`; `pytest_asyncio/plugin.py` uses `asyncio.Runner`, so it
  fires in the test suite too. The gap: a hand-rolled
  `loop.run_until_complete(...)` + `loop.close()` never calls
  `shutdown_asyncgens()`, so the hook silently does not run (the loop instead
  prints `Task was destroyed but it is pending`).
- **(confirmed) `asyncio` offers no loop-close callback.** `BaseEventLoop.close()`
  has no hook list and clears `self._ready`, discarding anything queued with
  `call_soon`. `weakref.finalize` fires on loop *garbage collection*, not on
  close — and the cached clients hold references to the loop, delaying it.
  Polling `loop.is_closed()` is the only non-intrusive alternative, which is what
  `_sweep` already does.
- **(confirmed) `BaseConnector.close()` is a plain `def`, not a coroutine.** All
  the socket work happens synchronously inside `_close()`; the returned awaitable
  only *waits* for the close handshakes. That is what makes an exit-time teardown
  possible at all. Call `connector._close()` rather than `close()`: the public
  method wraps the result in a `_DeprecationWaiter` whose `__del__` warns
  "Connector.close() is a coroutine" when it is never awaited — which is exactly
  the situation at exit.
- **(confirmed) A dead loop's connector must be left alone.** `_close()` bails at
  `if self._loop.is_closed(): return waiters`, but its `finally` still clears
  `_conns` and sets `_closed = True`. Calling it there therefore *silences*
  aiohttp's `Unclosed connector` warning without ever sending a FIN — it hides
  the leak instead of fixing it. FDs are still reclaimed later by
  `_SelectorTransport.__del__`, so the honest move is to report and stop.
- **(confirmed) A client can never be closed from a foreign loop.** With loop A
  open but not running, `proto.close()` queues `_call_connection_lost` on A via
  `call_soon` — a callback that never runs — and `BaseConnector.close()` builds
  its Task on A, so awaiting it from B gives futures attached to the wrong loop.
  Eviction therefore skips entries owned by another live loop and leaves them
  cached, so their own loop's teardown can close them properly.
- **(confirmed) The two SDKs disagree about double-close.** azure-core guards it
  (`if self._session_owner and self.session:`) and is safely idempotent;
  aiobotocore's `AIOHTTPSession.__aexit__` opens with
  `assert self._sessions is not None, 'Session was never entered'` and **raises**.
  Neither exposes a public "is it closed" flag, so mint probes
  `client._endpoint.http_session._sessions` for S3 and the transport's `session`
  for Azure. The probe is load-bearing, not defensive: without it, evicting a
  client the caller had already closed logs an `AssertionError` traceback.
- **(confirmed) Azure's transport is reached by three different paths.** A
  `BlobServiceClient` nests its generated client one level deeper
  (`_client._client._pipeline._transport`) than a credential does
  (`_client._pipeline._transport`), and a child client is handed the pipeline
  directly (`_pipeline._transport`). An implementation that only knew the client
  shape silently failed to detect a credential's state. Child clients also wrap
  the transport in `AsyncTransportWrapper`, and the wrapping nests
  (service → container → blob), so unwrapping needs a bounded loop that stops at
  the real `AioHttpTransport`.
- **(confirmed) A never-opened Azure client correctly reports closed.**
  `AioHttpTransport` creates its session lazily in `open()`, so `session is None`
  before the first request. That is the right answer for shutdown purposes:
  there is nothing to release. Supplying an explicit pool-limited transport
  creates the session eagerly, and then it reports open.
- **(confirmed) No library in site-packages registers an `atexit` hook** — not
  aiohttp, aiobotocore, botocore, azure-core, azure-identity or
  azure-storage-blob. Their entire exit story is `__del__` plus `ResourceWarning`.
  Whatever mint does here is novel, which is why the exit layer stays best-effort
  and quiet, and reports rather than pretending it fixed something.
- **A concurrency test whose stub never awaits is weaker than it looks.** The
  original "50 racing borrows build one client" test passed trivially: the stub's
  `_build` had no `await`, so the first borrower ran to completion before the
  second started and the post-lock re-check was never reached. Forcing a yield
  inside the stub build is what actually exercises the per-key lock.

- **(confirmed) `id(loop)` is not a safe cache key.** CPython reuses the address
  of a collected event loop, so a map keyed on `id(loop)` can hand a new loop a
  resource wired to a dead one. Reproduced across three sequential
  `asyncio.run()` calls: the third got the session built on the first. This bites
  hardest on a refreshable credential, which carries an `asyncio.Lock` bound to
  whichever loop first awaited it. Both the aiobotocore session cache and the
  Azure credential cache are `WeakKeyDictionary` maps keyed on the loop object,
  which cannot collide and cannot outlive the loop.
- **(confirmed) Supplying an Azure transport opts out of the SDK's timeouts.**
  `azure/storage/blob/_shared/base_client_async.py` applies
  `connection_timeout=20` / `read_timeout=60` with `setdefault`, and only to a
  transport it builds itself. Passing a pre-built `AioHttpTransport` — which is
  the only way to bound the connection pool — silently falls back to azure-core's
  300s/300s, turning a fail-fast request into a five-minute hang. The same
  applies to `trust_env`: omit it and `HTTPS_PROXY`/`NO_PROXY`/netrc are ignored.
  A custom transport must re-supply everything `_create_pipeline` would have.
- **A provider shared through a process registry must survive being closed.**
  `aclose_shared()` is the documented shutdown call, but a storage held at module
  scope captures the provider object; marking it permanently closed made every
  later operation raise for the rest of the process, while a *newly constructed*
  storage worked fine. Storages that own their provider now re-resolve a closed
  one; a caller-supplied provider deliberately stays closed, since its lifetime
  belongs to the caller and `ProviderClosedError` is the honest signal.
- **Shutdown is loop-local, for credentials as much as clients.** The first
  implementation refused to close another live loop's *client* but closed every
  cached *credential* regardless — which would have queued teardown on a loop
  that may never run it, and left that loop's still-open client holding a dead
  credential. Both caches now use the same rule: close what the running loop
  owns, drop what belongs to a dead loop, leave what another live loop owns.
- **A per-key `asyncio.Lock` must not be dropped while it is held.** Eviction
  originally popped the lock unconditionally. Because eviction awaits
  (`stack.aclose()`), another coroutine can start rebuilding the same key in
  that window; dropping its lock lets a second builder run concurrently, and
  the loser's client is overwritten in the cache and never closed — a leaked
  session and a double-counted build. Only an unheld lock is safe to remove.
- **Check "closed" again after acquiring the build lock.** `borrow()` tests
  `_closed` once and then awaits. `aclose()` is itself async, so it can finish
  iterating the cache while a builder waits on the lock; without a re-check the
  new client lands in a closed provider and escapes shutdown entirely.
- **A client built eagerly before its owner exists must be released on failure.**
  Azure's pool-limited path constructs an `aiohttp.ClientSession` *before* the
  `BlobServiceClient` that will own it. A malformed connection string raises in
  between, so each retried operation stranded one session and connector until
  the build was wrapped.
- **A cloned storage must not be handed the provider object.** `clone()`
  originally passed `self._provider`, which made the clone treat it as
  caller-owned — so after `aclose_shared()` the clone raised
  `ProviderClosedError` forever while its parent silently re-resolved. Passing
  no provider lets the clone resolve the same shared instance and keep that
  recovery.
- **Every one of these was a consistency failure, not a novel bug.** The
  loop-object keying, the "drop a dead loop's resource rather than fake-closing
  it" rule, and the "leave another live loop's resource alone" rule each had to
  be applied to the session cache, the credential cache *and* the sentinel
  cache. Fixing one and not the others is the failure mode this design invites.
- **`asyncio.Semaphore` binds to a loop too.** `ConcurrencyLimiter` built its
  semaphore eagerly in `__init__`, and asyncio's `_LoopBoundMixin` binds it to
  the first loop that contends on it — so a limiter held at module scope raised
  `RuntimeError: ... is bound to a different event loop` on a second loop, even
  though the provider beside it recovered cleanly. The semaphore is now created
  per running loop. Note the weak-key trick alone is not enough here: a bound
  semaphore holds a reference back to its own loop, so the `WeakKeyDictionary`
  never fires and closed loops must be pruned explicitly.
- **A `WeakKeyDictionary` whose values reference their keys is not weak.** This
  bit the limiter (semaphore → loop) and is worth checking for any per-loop
  cache: if the value can reach the key, add an explicit prune on closed loops.
- **Re-check "closed" *after* the build, not just before it.** Building is where
  the awaiting happens, so `aclose()` can start and finish entirely within that
  window. Publishing the finished client then leaves it in a closed provider
  where nothing can reach it — `borrow()` refuses on `_closed`, so no sweep ever
  runs, and a second `aclose()` is a no-op. The connector leaks for the life of
  the process.
- **The shutdown sentinels are the only strong reference to their generators.**
  asyncio tracks async generators in a `WeakSet`, so clearing `_sentinels`
  wholesale in `aclose()` let GC finalize a foreign loop's sentinel — which
  schedules `aclose_loop()` on that loop at an arbitrary point, closing clients
  `aclose()` had just promised to leave open, potentially mid-operation. Only
  sentinels for loops that were actually cleaned up may be dropped.
- **The process-global registry needs a process-global lock.** `_SHARED` is
  documented as giving two threads the same provider for one configuration, but
  `shared()` did an unguarded get-then-set: racing threads could both miss and
  both write, and the loser's provider — still used by its storage — would be
  absent from the registry, so `aclose_shared()` could never close it.
- **(confirmed) `asyncio.Lock.locked()` is False while waiters are still queued.**
  `release()` clears `_locked` and only *schedules* the next waiter, so
  `locked()` is not a safe signal for "nobody needs this lock". Pruning a
  per-key build lock on that signal let a later caller create a second lock for
  the same key and build concurrently; both published into the cache and the
  loser's client was orphaned with its connector open. Build locks are now
  dropped only during that loop's shutdown, where no builder can be waiting --
  there is at most one per (configuration, loop), so holding them costs nothing.
  This one was self-inflicted: it was introduced *while fixing* the opposite
  complaint, that a failed build leaked its lock.
- **Caching a client changes what the connection-pool default means.** botocore
  defaults to `max_pool_connections=10`. That was harmless when every operation
  built its own client -- effective concurrency scaled with the fan-out -- but
  one shared client turns it into a process-wide ceiling of 10, and aiobotocore
  sets only socket timeouts, which do not cover connector-queue wait. It would
  surface as unbounded latency, not an error. `S3ClientProvider` therefore
  defaults to 64 rather than inheriting botocore's value. Azure is unaffected:
  its default transport uses aiohttp's own limit of 100.
- **Register in a process-global registry only after validation.** Building the
  provider before constructing the `ConcurrencyLimiter` meant a rejected
  `max_concurrent_ops=0` still left its provider in `SHARED`, where the next
  storage with that configuration would inherit it. Validate first, register
  second.
- **`isinstance` against a Protocol also rejects a callable member whose value
  is None.** The diagnostic originally checked only the `getattr_static`
  AttributeError half, so a client with `close = None` produced the useless
  message `missing: []` -- from the very helper that exists to explain the
  refusal. The two checks have to agree.
- **Guard *every* cross-thread map, not just the ones you thought of.** `_sweep`
  snapshots under the lock, but `aclose()` and the exit sweep iterated
  `_entries` and `_sentinels` unguarded, and the backend overrides did the same
  for their session and credential caches. With two threads sharing one
  process-default provider, a publish during shutdown could raise
  `RuntimeError: dictionary changed size during iteration` and abort `aclose()`
  half-way, leaving the provider partly closed and still registered.
- **A provider holding an injected client is not shareable.** `config_digest`
  describes configuration, not injection, so registering such a provider let an
  unrelated storage with matching credentials silently borrow someone else's
  client. `shared()` now returns those candidates unregistered. The storages
  never hit this — they skip `shared()` when an injection is present — but
  `shared()` is public.
- **"Nothing owns it yet" extends past construction.** The Azure build released
  its eagerly-created session when `_create_client` raised, but not when
  `stack.enter_async_context(client)` did — and until the client is on the
  stack, the exit stack has nothing to unwind. A failure inside
  `AioHttpTransport.open()` stranded the session just as a failed construction
  would.
- **Changing a default means changing every place that documents it.** Raising
  the S3 pool default to 64 left the constructor docstring and two doc pages
  still telling readers that botocore's 10 was the ceiling — a number that no
  longer applied anywhere, in exactly the docs someone would consult when
  sizing fan-out.
