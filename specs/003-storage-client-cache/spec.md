# Cached, Injectable Storage Clients

**Feature ID**: 003-storage-client-cache
**Status**: Specified
**Created**: 2026-08-31
**Supersedes**: `001-s3-storage` FR-04 (Client Lifecycle)

---

## Overview

`S3Storage` and `AzureBlobStorage` built a brand-new SDK client for every
top-level operation. Introduce a provider that owns and reuses one client per
(configuration, event loop), and let callers inject a client or a client factory
of their own.

### The problem

`S3Storage._create_client` called `aiobotocore.session.get_session()` per client,
which is `return AioSession(env_vars)` — no caching of any kind. botocore
memoizes both the resolved credentials (`botocore/session.py` `get_credentials`,
guarded by `self._credentials is None`) and the parsed service-model JSON
(`botocore/loaders.py` `instance_cache`) **on the session**, and `create_loader`
builds a cold `Loader` per session with no process-global fallback.

A new session per call therefore re-ran the entire credential provider chain,
terminating at `InstanceMetadataProvider` — a live request to
`http://169.254.169.254/` (`botocore/utils.py` `METADATA_BASE_URL`) on any host
without environment credentials. The aiohttp `TCPConnector` is also strictly
per-client, so each teardown discarded a warm TLS pool.

Azure had the same shape plus a resource leak: `_create_client` built a fresh
`DefaultAzureCredential` / `ClientSecretCredential` per operation and never
closed it. Each owns its own `TokenCache` (discarded per call, forcing a fresh
instance-metadata token fetch with `retry_total=5` and up to 60 s of backoff)
and its own `AioHttpTransport` (leaked per call).

### Measured effect

100 concurrent `head_object` calls against LocalStack:

| | wall clock | clients built |
|---|---|---|
| Per-call session (before) | 12 313 ms | 100 |
| Cached client (after) | 458 ms | 1 |

Medians over five runs on one machine; per-run ratios spanned 14.7x-29.5x
(median 28.0x). Roughly 28x, with `AioSession` constructions dropping from 100 to 1 and IMDS
DNS lookups from N to 0.

---

## User Scenarios & Testing

### Scenario 1: A service issues many operations

1. A long-lived service constructs `S3Storage` once.
2. It issues hundreds of concurrent operations across the process lifetime.
3. One client, one session, one credential resolution serve all of them.

**Acceptance**: `provider.created_count == 1` after a 50-way `asyncio.gather`.

### Scenario 2: A caller supplies their own client

1. A caller already owns a configured, instrumented S3 client.
2. They pass it as `S3Storage(..., client=their_client)`.
3. mint drives it for every operation and never closes it.

**Acceptance**: the client is still usable after `provider.aclose()`.

### Scenario 3: Tests run under function-scoped event loops

1. pytest-asyncio gives each test its own loop.
2. Each test's operations get a client bound to that test's loop.
3. No client is ever carried across loops.

**Acceptance**: the pre-existing storage suite passes unchanged, with no
`Unclosed connector` warnings.

---

## Functional Requirements

### FR-01: Provider owns the client

A `ClientProviderBase[T]` owns a `dict[ClientCacheKey, _CacheEntry[T]]`.
`S3ClientProvider` and `BlobClientProvider` supply the backend specifics. This
mirrors `mint.db.asynk.database.Database`, which owns one pooled engine and
accepts a pre-built one.

### FR-02: Cache key includes the running event loop

`ClientCacheKey` is `(backend, endpoint, credential_digest, extra, loop_id)`.
aiohttp's `TCPConnector` binds the loop that built it
(`aiobotocore/httpsession.py`: "TCPConnector binds the running loop"), so a
client must never cross loops. An entry whose loop has closed is dropped —
without closing, since closing requires that loop — on the next sweep.

### FR-03: Credentials never appear in the key

`credential_digest` is a sha256 over the canonical credential tuple. A key is
`repr`'d into logs and error messages, so it must carry no secret.

### FR-04: A shared AioSession per credential identity and loop

`S3ClientProvider` caches the `AioSession` separately from the client, so two
clients differing only by endpoint still share one credential resolution. Keyed
by loop as well: refreshable credentials carry an `asyncio.Lock`.

### FR-05: Azure caches and owns the credential

`BlobClientProvider` caches the token credential per loop and closes it in
`aclose()`, after the clients built from it. `ContainerClient` is **not** cached:
`get_container_client` costs ~0.14 ms and shares the parent's transport and
policies via `AsyncTransportWrapper`, whose `close()` is a no-op.

### FR-06: Eviction is terminal

An evicted client is removed from the cache *before* it is closed, and is never
handed out again. Azure's `AioHttpTransport.open()` raises
`ValueError("HTTP transport has already been closed")` on reuse after close.

### FR-07: Idle TTL, swept lazily, never mid-operation

`idle_ttl_seconds` (default 900, `None` disables) closes a client left unused
that long. Sweeping happens on borrow — no background task. A borrow is counted
(`_CacheEntry.inflight`), and an entry in flight is never swept, so a long
operation cannot have its client closed underneath it.

### FR-08: ContextVar binding retained

`_ensure_client` still binds the client to a per-instance `ContextVar` for the
duration of a call, and still `reset`s it with the token in a `finally`. Only
the source changes: it borrows from the provider instead of constructing, and
does not close on exit. This keeps `client` a sync property, leaves
`IFileStorage[T]` untouched, and preserves nested-call reuse.

### FR-09: Injection is validated at wiring time

`IS3Client` and `IBlobServiceClient` are `runtime_checkable` Protocols naming
only the members mint calls. An injected client is `isinstance`-checked at
construction; a mismatch raises `IncompatibleClientError` naming the protocol,
the actual type, and the missing members. Test doubles must be spec'd.

### FR-10: A factory is honoured by the base class

`client_factory` is routed by `ClientProviderBase` itself, not by each
subclass's `_build`, so a backend cannot forget to honour it. The factory is
called once per cache miss — hence once per loop, not once per operation — and
may return a client or an async context manager, whose teardown mint adopts.

### FR-11: Process-default providers

When no `provider=` is given, configuration selects a shared provider from a
process registry keyed by `(provider class, config digest)`, so sibling storages
reuse one client. `aclose_shared()` closes them all. A provider wrapping an
injected client or factory is caller-specific and never shared.

### FR-12: Concurrency knobs renamed

`max_pool_connections` is new and now matters: with one shared client,
botocore's default of 10 is the effective concurrency ceiling.
`max_concurrent_clients` is renamed `max_concurrent_ops` — it bounds concurrent
operations, not client creation — with the old name kept as a deprecated alias
that emits `DeprecationWarning`.

### FR-13: Shutdown fallback

Cached clients are long-lived, so something must close them. Three layers:

1. **Explicit** — `await provider.aclose()` / `aclose_shared()`, called from
   inside the event loop. What applications should do.
2. **Loop teardown** — each provider arms a sentinel async generator the first
   time it builds a client on a loop. `loop.shutdown_asyncgens()` finalizes it
   *before* `loop.close()` and while the loop still runs, so the `finally` can
   genuinely await a close. Fires under `asyncio.run`, `asyncio.Runner`, uvicorn
   and pytest-asyncio, including on `KeyboardInterrupt`. It calls `aclose_loop()`,
   not `aclose()`, so the provider stays usable for a subsequent loop. Opt out
   with `auto_shutdown=False`, accepted by both the storage classes and the
   providers -- it also participates in the shared-provider key, so storages
   that disagree about it do not share one.
3. **Interpreter exit** — no loop remains, so nothing can be awaited. The sweep
   reports every client still genuinely open and, where the owning loop is still
   alive, releases its connectors synchronously via `BaseConnector._close()`.
   Where the loop is already closed it reports and stops: a sync close there
   would flip the connector's `_closed` flag and suppress aiohttp's warning
   without sending a FIN, hiding the leak.

Every layer gates on `_is_client_open()`. This is mandatory rather than
defensive: aiobotocore's `AIOHTTPSession.__aexit__` asserts `_sessions is not
None` and raises on a second close, while azure-core's is guarded and idempotent.
Neither exposes a public flag, so each backend probes its own internals and
reports "open" when the shape is unreadable — leaking is the worse failure.

Eviction never closes a client owned by another live loop, and leaves it cached
so that loop's own teardown can close it gracefully.

---

## Success Criteria

- 50 concurrent top-level operations build exactly one client.
- Consecutive top-level calls reuse the client (the case the ContextVar binding
  alone always missed).
- The pre-existing storage suite passes unchanged.
- Two `asyncio.run()` calls against one provider yield two distinct clients and
  no `Event loop is closed`.
- An injected client is used verbatim and outlives `provider.aclose()`.
- The Azure token credential is built once and closed on shutdown.
- A program that never calls `aclose_shared()` still closes its clients at loop
  teardown, and `auto_shutdown=False` demonstrably leaves them open.
- A client already closed by the caller is never closed a second time.
- 100% line coverage of `mint/fs/asynk/provider.py`.

---

## Out of Scope

Recorded so these are a deliberate deferral, not an oversight:

- `AzureBlobStorage` has no `endpoint_url` / `account_url` override, so Azurite
  tests must route through `connection_string`.
- `abs.py` `gen_presigned_url` still uses `hasattr(credential, "get_token")`,
  which the repo's coding-style rule forbids. The provider now knows the
  credential mode, making an `isinstance` or mode comparison a natural follow-up.
- ABS still lacks `clone()`, `ensure_container()`, `get_fileobj()`,
  `calculate_etag()`.
