# Client Caching

Both storage backends borrow their SDK client from a **provider** that keeps one
client per (configuration, event loop). This page explains the problem that
motivated it, what it cost, how the remedy works, how the two backends differ,
and where the sharp edges are.

For day-to-day construction and operations see [Usage](usage.md); for the
verified SDK-internals notes behind all of this, see the
[Implementation Notes](../s3-implementation-notes.md).

## The phenomenon

Every top-level storage operation used to build a brand-new SDK client and throw
it away on the way out. On S3 that turned out to be far more expensive than it
looks, for three compounding reasons.

**`get_session()` caches nothing.** `aiobotocore.session.get_session()` is
literally `return AioSession(env_vars)`. A client per call therefore meant a
session per call.

**botocore memoises the expensive work on the *session*, not globally.** Both the
resolved credentials and the parsed service-model JSON — roughly a megabyte for
S3 — are cached on the session object, and the loader is built cold each time
with no process-wide fallback. A fresh session discards both.

**So every operation re-ran the whole credential provider chain.** That chain
terminates at the EC2 instance-metadata provider, which means a real HTTP request
to `http://169.254.169.254/` on any host without environment credentials — a
link-local address that, off EC2, is answered by nothing and has to time out.
This is the failure people usually notice first: an application that seems fine
in development starts hanging in production, and the endpoint being hammered is
not the storage endpoint at all.

**And each teardown discarded a warm TLS pool.** A client owns its own
`TCPConnector`, so closing one throws away every pooled keep-alive connection.
Two clients built from the same session share no sockets.

## What it cost

100 concurrent `head_object` calls against LocalStack, five runs on one
machine, before and after:

| | wall clock (median) | clients built | sessions built | IMDS lookups |
|---|---|---|---|---|
| A client per operation | 12 313 ms | 100 | 100 | one per call |
| One cached client | 458 ms | 1 | 1 | 0 |

**About 28x.** Per-run ratios spanned 14.7x-29.5x (median 28.0x) — the spread is
machine load, so take the order of magnitude rather than the exact figure.
Isolating the two effects: creating a client from a warm session costs
about 4.1 ms, while creating one from a fresh session costs about 94 ms — a 23x
gap before any network I/O happens at all. The remaining difference is the TLS
pool staying warm.

In one observed run the project's own S3 test suite dropped from 33.1 s to 23.8 s,
and its `Unclosed connector` warnings disappeared.

## The remedy

A provider owns the client and the storage borrows it:

```python
from mint.fs.asynk.s3 import S3Storage

storage = S3Storage(
    bucket_name="my-bucket",
    endpoint_url="http://localhost:4566",
    access_key="...",
    secret_key="...",
)
# no opt-in required; the configuration selects a shared provider
```

The cache key is `(backend, endpoint, credential digest, region/pool, event
loop)`. Two points about that key are load-bearing.

**Credentials appear only as a digest.** A key is interpolated into logs and error
messages, so it carries a SHA-256 of the credential material rather than the
material itself.

**The event loop is part of the key.** aiohttp's `TCPConnector` binds the loop it
was built on — aiobotocore's own source says so — so a client may never be used
from, or closed by, a different loop. Each loop gets its own client.

Sibling storages configured alike share one client:

```python
a = S3Storage(bucket_name="one", endpoint_url=..., access_key=..., secret_key=...)
b = S3Storage(bucket_name="two", endpoint_url=..., access_key=..., secret_key=...)
assert a.provider is b.provider
```

See [Choosing how the client is supplied](#choosing-how-the-client-is-supplied)
for the explicit-provider, bring-your-own-client and factory forms, on both
backends.

An idle client is closed and dropped after `idle_ttl_seconds`, swept lazily on
borrow rather than by a background task. A client with an operation in flight is
never swept, so a long `copy()` cannot have its transport closed underneath it.

## Was Azure affected?

Yes — but almost none of the cost sat where it does on S3, so the fix is aimed
somewhere else entirely.

| Concern | S3 | Azure Blob Storage |
|---|---|---|
| Client construction | 94 ms with a cold session | 0.17 ms — negligible |
| Credential / token cache discarded per call | yes | **yes — the real cost here** |
| Metadata round trip per call | `169.254.169.254` | yes, retried 5x with up to 60 s of backoff |
| Warm TLS pool discarded per call | yes | yes |
| Resource leaked per call | no | **yes — the credential was never closed** |
| Child clients need caching | n/a | no |

Constructing a `BlobServiceClient` is about 25x cheaper than constructing an
aiobotocore client, so caching the client alone would have bought Azure very
little. What actually hurt was the **credential**. `DefaultAzureCredential` and
`ClientSecretCredential` each own an in-memory token cache *and* their own HTTP
transport. Building one per operation meant discarding the token cache every
call — forcing a fresh instance-metadata token fetch, which retries aggressively
— and, because the old code never closed them, leaking an aiohttp session every
single call. The provider now caches the credential per loop and closes it on
shutdown.

**Container and blob clients are deliberately *not* cached.**
`get_container_client()` costs about 0.14 ms and returns a child that shares the
parent's transport *and* its policy objects, including the cached bearer token,
through a wrapper whose `close()` is an intentional no-op. Caching those would
buy nothing and add a second lifetime to reason about, so `AzureBlobStorage`
still derives its container client per access. The inverse is the trap worth
knowing: building a `ContainerClient` directly from a URL gets a brand-new
transport and a cold token cache.

## Choosing how the client is supplied

Four ways, in increasing order of how much you take on. The first is the default
and needs no code.

### 1. No provider — configuration only

Pass credentials to the storage and let it resolve a shared, process-default
provider. Two storages configured alike get the same client.

```python
from mint.fs.asynk.abs import AzureBlobStorage
from mint.fs.asynk.s3 import S3Storage

s3 = S3Storage(
    bucket_name="my-bucket",
    endpoint_url="http://localhost:4566",   # omit for AWS
    access_key="...",
    secret_key="...",
    region_name="us-east-1",
)

blob = AzureBlobStorage(
    container_name="my-container",
    storage_account_name="my-account",
    connection_string="<connection-string>",
)
```

Shut down with the class method, since the provider is shared:

```python
from mint.fs.asynk.abs_provider import BlobClientProvider
from mint.fs.asynk.s3_provider import S3ClientProvider

await S3ClientProvider.aclose_shared()
await BlobClientProvider.aclose_shared()
```

### 2. An explicit provider — when you want to own the lifetime or tune it

Only a provider you built yourself should be closed with `provider.aclose()`.

```python
provider = S3ClientProvider(
    endpoint_url="http://localhost:4566",
    access_key="...",
    secret_key="...",
    region_name="us-east-1",
    max_pool_connections=64,     # default; raise for wider fan-out
    idle_ttl_seconds=900,        # None to never evict
)
storage = S3Storage(bucket_name="my-bucket", provider=provider)
...
await provider.aclose()
```

The Azure equivalent takes the account name positionally and the same credential
arguments as `AzureBlobStorage`:

```python
provider = BlobClientProvider(
    "my-account",
    connection_string="<connection-string>",
    max_pool_connections=64,
)
storage = AzureBlobStorage(
    container_name="my-container",
    storage_account_name="my-account",
    provider=provider,
)
...
await provider.aclose()
```

### 3. Your own client — mint drives it and never closes it

Use this when something else already owns the client: an instrumented or
retry-wrapped wrapper, or a client shared with code outside mint. Its lifetime
stays entirely yours — no eviction, no shutdown hook, no exit sweep touches it.

```python
from aiobotocore.session import AioSession

session = AioSession()
async with session.create_client("s3", endpoint_url=..., region_name=...) as client:
    storage = S3Storage(bucket_name="my-bucket", client=client)
    await storage.save("path/to/file.txt", b"hello")
    # `client` is still open here, and still yours to close
```

```python
from azure.storage.blob.aio import BlobServiceClient

client = BlobServiceClient.from_connection_string("<connection-string>")
try:
    storage = AzureBlobStorage(
        container_name="my-container",
        storage_account_name="my-account",
        client=client,
    )
    await storage.save("path/to/file.txt", b"hello")
finally:
    await client.close()
```

The client is checked at construction against `IS3Client` /
`IBlobServiceClient` — protocols naming only the members mint calls — so a wrong
shape raises `IncompatibleClientError` naming what is missing, rather than
failing deep inside an operation later.

### 4. A factory — your construction, mint's lifetime

A single client you build yourself is bound to one event loop. A factory lets
mint call you once per cache miss, so it still gets one client *per loop*, and it
adopts the teardown. Use this to apply configuration mint does not expose.

```python
from aiobotocore.config import AioConfig
from aiobotocore.session import AioSession

def make_client():
    # Returned either bare or as an async context manager; mint owns whichever.
    return AioSession().create_client(
        "s3",
        endpoint_url="http://localhost:4566",
        region_name="us-east-1",
        config=AioConfig(retries={"max_attempts": 10, "mode": "adaptive"}),
    )

storage = S3Storage(bucket_name="my-bucket", client_factory=make_client)
```

Unlike an injected client, a factory's result **is** owned by mint: it is closed
on eviction and at shutdown. Its result is checked against the protocol the first
time the factory runs, not at construction.

### Which to pick

| | Who closes it | Cached per loop | Checked when |
|---|---|---|---|
| Configuration only | `aclose_shared()` | yes | n/a |
| Explicit provider | `provider.aclose()` | yes | n/a |
| `client=` | **you** | no — used verbatim | at construction |
| `client_factory=` | mint | yes | first call |

`provider=`, `client=` and `client_factory=` are mutually exclusive; passing more
than one raises `ConflictingClientSourceError` rather than silently picking.

## Shutting down

A cached client is deliberately long-lived, so something has to close it.

**The explicit way, and the one to prefer:**

```python
await S3ClientProvider.aclose_shared()   # process-default providers
await provider.aclose()                  # one you built yourself
```

Do this from inside the event loop, during your application's shutdown — a
FastAPI lifespan, a CLI's `finally`, a worker's teardown — and once traffic has
quiesced. Neither call waits for in-flight operations, so a request still using
a client when it closes will fail; drain first, exactly as you would before
disposing a database engine.

Use `aclose_shared()` unless you built the provider yourself. A storage
configured with plain credential kwargs shares its provider with every sibling
configured alike, so `storage.provider.aclose()` would close theirs too.

**The fallback, if you forget.** Each provider arms a hook the first time it
builds a client on a loop. When that loop tears down, the hook closes the loop's
clients — while the loop is still running, so it is a real, graceful close, not a
best-effort one. It fires on normal completion, on `KeyboardInterrupt`, and under
`asyncio.run`, `asyncio.Runner`, uvicorn and pytest-asyncio.

It closes only that loop's clients and leaves the provider usable, so a process
that runs several loops in sequence gets a fresh client each time rather than an
error. Opt out when something outside the provider owns the clients' lifetime:

```python
storage = S3Storage(..., auto_shutdown=False)          # via the storage
provider = S3ClientProvider(..., auto_shutdown=False)  # or on the provider
```

Two storages that disagree about `auto_shutdown` get separate providers, so
whichever was constructed first cannot silently decide the behaviour of the
other.

**The last resort.** At interpreter exit there is no event loop left — nothing can
be awaited — so a final sweep reports any client still genuinely open and names
the call you should have made. Where the owning loop happens to still be alive it
also releases the sockets synchronously. Where the loop is already gone it reports
and stops, deliberately: silently flipping a connector's closed flag would
suppress the warnings without ever sending a FIN, hiding the leak rather than
fixing it.

Every layer probes whether a client is *genuinely* still open before touching it,
because the two SDKs disagree — azure-core guards its `close()` and is safely
idempotent, while aiobotocore asserts and **raises** on a second close.

## Caveats

- **A client belongs to one event loop.** Calling `asyncio.run()` twice produces
  two clients. This is aiohttp's constraint, not a choice.
- **Close from inside the loop.** Once a loop closes, its clients can only be
  dropped, never closed gracefully.
- **The teardown hook needs `loop.shutdown_asyncgens()`.** `asyncio.run`,
  `asyncio.Runner`, uvicorn and pytest-asyncio all call it. A hand-rolled
  `loop.run_until_complete(...)` followed by `loop.close()` does **not**, and the
  hook will silently not fire — the exit-time sweep will report those clients
  instead. Prefer `asyncio.run` or `asyncio.Runner`.
- **`os._exit()`, `SIGKILL`, and hard crashes run nothing at all.** No shutdown
  mechanism in any library survives those.
- **Eviction is terminal on Azure.** A closed transport raises if reopened, so an
  evicted client is dropped from the cache and never handed out again.
- **`max_pool_connections` is now the real concurrency ceiling.** With one
  shared client it bounds the whole process, so mint defaults it to 64 rather
  than inheriting botocore's 10, which would have silently capped every
  fan-out. Raise it further if you need more, and note it is part of the cache
  key: two storages asking for different pool sizes get different clients.
- **`max_concurrent_clients` is deprecated.** It once bounded client creation;
  clients are cached now, so nothing does. It is an alias for
  `max_concurrent_ops`, which bounds concurrent *operations*.
- **Injected clients are never closed by mint** — not on eviction, not by the
  teardown hook, not at exit. If you pass `client=`, its lifetime stays yours.
- **Test doubles must be spec'd.** Since Python 3.12 an `isinstance` check against
  a runtime-checkable Protocol uses `inspect.getattr_static`, which does not fire
  `MagicMock.__getattr__` — so a bare `MagicMock` satisfies no protocol. Use
  `MagicMock(spec=IS3Client)`.
- **The open-probe reads private SDK attributes**, because neither SDK exposes a
  public "is this closed" flag. If an upgrade moves them, the probe reports the
  client as open and mint attempts a close anyway — leaking is the worse failure.
