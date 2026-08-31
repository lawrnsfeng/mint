# S3 File Storage Implementation

**Feature ID**: 001-s3-storage
**Status**: Specified
**Created**: 2026-06-30

---

## Overview

Add async S3-compatible file storage (`S3Storage`) to the `mint.fs` module,
conforming to the existing `IFileStorage` Protocol and matching the structural
depth of the existing `AzureBlobStorage` implementation. The new class enables
developers to use S3, LocalStack, or MinIO as a drop-in storage backend.

---

## User Scenarios & Testing

### Scenario 1: Developer uploads and retrieves files

1. Developer instantiates `S3Storage` with bucket name and credentials.
2. Developer calls `save(path, content)` to upload an object.
3. Developer calls `get(path, local_path)` to download it.
4. File content on disk matches what was uploaded.

**Acceptance**: Upload + download round-trip preserves byte-exact content.

### Scenario 2: Developer copies a folder structure

1. Developer calls `copy("source/", "backup/", recursive=True)`.
2. All objects under `source/` appear under `backup/`.
3. Original objects remain intact.
4. `CopyResult.failure` is empty on success.

**Acceptance**: All objects copied, source unchanged, result contains correct paths.

### Scenario 3: Developer removes stale data recursively

1. Developer calls `remove("archive/", recursive=True)`.
2. All nested objects and sub-prefix contents are deleted.
3. Subsequent `list("archive/", recursive=True)` returns empty.

**Acceptance**: Post-remove list is empty; no exceptions thrown.

### Scenario 4: Developer generates a time-limited download link

1. Developer calls `gen_presigned_url("report.pdf", expiration_in_seconds=3600)`.
2. Returned URL is accessible for the expiration window.
3. URL contains correct Content-Disposition header.

**Acceptance**: URL is a non-empty string containing the object key.

### Scenario 5: Developer runs 300 concurrent operations

1. Developer calls 300 concurrent `stat` or `save` operations.
2. All complete without exception.
3. With `max_concurrent_clients=5`, all still complete (just serialized).

**Acceptance**: `asyncio.gather` of 300 operations returns 300 results.

---

## Functional Requirements

### FR-01: IFileStorage Protocol Conformance
All methods defined in `IFileStorage[S3Client]` must be implemented:
`is_folder`, `get`, `save`, `copy`, `move`, `remove`, `remove_many`, `stat`,
`list`, `list_detailed`.

### FR-02: Extra Methods
Implement `gen_presigned_url`, `save_many`, `calculate_etag`, `get_fileobj`,
`ensure_bucket`, and `clone` matching the capability set of `AzureBlobStorage`.

### FR-03: Credential Resolution
Support four credential modes in priority order:
1. Explicit key pair (`access_key` + `secret_key`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS shared credentials profile file (`~/.aws/credentials`)
4. IAM role / instance metadata fallback

### FR-04: Client Lifecycle
**Superseded by `003-storage-client-cache`.**

Originally: open a client per top-level call, using `ContextVar` for
per-coroutine isolation, with optional `max_concurrent_clients` via
`ConcurrencyLimiter` to bound concurrent client creation.

That per-call construction was the defect 003 fixes — a fresh `AioSession` per
operation re-ran the whole credential chain, including the EC2 instance-metadata
probe, and discarded a warm TLS pool every time (measured: a median 12 313 ms
vs 458 ms for 100 concurrent operations, roughly 28x).

Now: a provider owns one client per (configuration, event loop) and the
`ContextVar` binds a *borrowed* client for the duration of a call rather than a
freshly constructed one. `max_concurrent_clients` is deprecated in favour of
`max_concurrent_ops`; see `specs/003-storage-client-cache/spec.md`.

### FR-05: No Async Recursion
Pagination (`list`, `list_detailed`) uses iterative `while` loops with
`ContinuationToken`. Recursive folder traversal (`remove`, `copy`) uses
`Executor` from the `sprout` package.

### FR-06: Error Mapping
Map S3 `ClientError` 404 codes to `ObjectNotFoundError`. Map `ValueError`
to `InvalidArgumentsError`. Map all other unexpected exceptions to
`OperationalError`.

### FR-07: Batch Deletion
`remove_many` uses S3 `delete_objects` API in batches of at most 1000 keys.

### FR-08: copy() 5 GB Limit
Single-object copy raises `InvalidArgumentsError` for objects exceeding
5 GB (S3 `copy_object` API limit). Message clearly states the limit.

### FR-09: save() Overwrite Control
`save(..., overwrite=False)` raises `FileAlreadyExistsError` if the object
already exists. Default behavior (`overwrite=True`) always writes.

---

## Success Criteria

- All `IFileStorage` Protocol methods pass static type checking with `ty`.
- 100% test coverage on `mint/fs/asynk/s3.py`.
- Integration tests use a real LocalStack container (no mocks).
- 300 concurrent operations complete without error in under 30 seconds.
- Tests with `max_concurrent_clients=5` complete all operations.
- `ruff check` and `ruff format --check` pass with zero violations.

---

## Key Entities

| Entity | Description |
|--------|-------------|
| `S3Storage` | Main async S3 storage class implementing `IFileStorage[S3Client]` |
| `S3CredentialMode` | Enum for credential resolution strategy |
| `S3SessionParams` | TypedDict for aiobotocore session parameters |
| `Executor` | Async tree traversal engine from `sprout`, used for recursive folder operations |
| `LocalStackContainer` | Testcontainer providing S3-compatible backend for tests |

---

## Assumptions

- S3-compatible endpoints (LocalStack, MinIO) behave identically to AWS S3
  for all operations used in this implementation.
- Object keys with trailing `/` are treated as folder prefixes; this is a
  convention, not enforced by S3 itself.
- The 5 GB single-copy limit is a known S3 API constraint; multipart copy
  is out of scope for this feature.
- `aiobotocore>=2.15.2` and `types-aiobotocore[s3]>=2.15.2` are added as
  optional dependency group `s3` in `pyproject.toml`.

---

## Dependencies & Notes

- Depends on: `sprout` (external git dependency providing `Executor`)
- Related: `mint/fs/asynk/abs.py` (ABS implementation, structural reference)
- Documentation: `docs/s3-implementation-notes.md` (implementation decisions)
