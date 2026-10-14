# Session Scopes and an ORM-Free Lifecycle Core

**Feature ID**: 004-db-session-scope
**Status**: Specified
**Created**: 2026-10-14
**Amends**: `002-db-repository-layer` (session isolation, auto-commit)

---

## Overview

Scope a database session to any function or method, share it with everything
that function calls, and commit once when the outermost scope exits. Make the
session lifecycle usable by a service that writes SQLAlchemy Core and never
imports SQLModel. Close the hardening gaps a consumer review listed, and put
the test and versioning contract where a consumer can see it.

### The problem

A review of `mint.db` from the point of view of a service built on SQLAlchemy
Core found four things.

1. **A session could only be scoped to a repository method.** The session slot
   was one `ContextVar` per repository instance, set by
   `RepositoryBase.ensure_session`. A plain function had no way to get a scoped
   session, and a call from one repository into another found the second
   repository's slot empty and opened a second session on a second pooled
   connection.
2. **Nested calls shared a session but not a transaction.** Every mutating
   statement committed when `auto_commit` was set, nested or not. An inner
   `create()` therefore committed the outer call's earlier writes mid-flight,
   and a method that failed after two writes left the first two behind.
   Atomicity needed `UnitOfWork` and `auto_commit=False` on every participant.
3. **The lifecycle code could not be imported without SQLModel.** `Database`
   and `UnitOfWork` use SQLAlchemy only, but the package `__init__` imported
   the repository base eagerly, and with it `sqlmodel`. The dependencies were
   also declared as a dependency group, which is not published with the wheel,
   so a consumer could not install them as an extra.
4. **Missing guard rails.** No statement timeout, no typed error for an
   exhausted pool, no retry on a transient failure, no guidance on savepoints.
   CI ran linters only, and nothing stated what counts as a breaking change.

## User Scenarios & Testing

### Scenario 1: A function needs one transaction

A developer decorates a coroutine function with `@db.scoped`. Inside it,
`db.current_session` returns a session; so does every function it calls. All
statements commit together when the function returns, and none of them persist
if it raises.

### Scenario 2: A service method coordinates two repositories

A service class holds its database as `self.db`. Its method is decorated with
the bare `@scoped`. Two repositories called inside it run on one session and
one connection, and their writes commit or roll back together, with no
`UnitOfWork` and no `auto_commit=False`.

### Scenario 3: A Core-only service adopts the lifecycle

A service with no SQLModel installed imports `Database` and `UnitOfWork`,
decorates its own query functions and executes Core statements through
`db.current_session`.

### Scenario 4: A network call sits next to a transaction

A developer resolves a value from a remote service, then writes it. The remote
call happens before the scoped function is entered, so no connection is held
while waiting on the network.

## Functional Requirements

### FR-01: The Database owns the session slot

One `ContextVar` per `Database` instance holds the session bound in the current
task (thread, in the sync mirror). Repositories no longer hold a slot of their
own.

### FR-02: `session_scope` binds and releases

`Database.session_scope()` is a context manager. With no scope open it opens a
session, binds it, and on exit commits (unless `commit=False`), unbinds and
closes it. An exception rolls back instead. With a scope already open in the
same task it yields the bound session and does nothing else.

### FR-03: Decorator forms

`Database.scoped` wraps any coroutine function in a scope. The module-level
`scoped` wraps a method and takes the database from `self.db`.

### FR-04: The outermost scope decides the commit

Only the outermost scope commits. A repository call that opens the scope
passes its `auto_commit` as the scope's `commit`. A repository call that joins
a scope never commits. A repository bound to an externally-owned session keeps
committing per mutating statement when `auto_commit` is set.

### FR-05: A joining repository keeps its scoping, and only its own

A repository's soft-delete and owner scoping applies to the statements run by
its own methods, on whatever session they join. It never applies to another
repository's statements on the same session.

### FR-05a: A scope does not report a commit that did not happen

When a statement failed inside a scope, the error was handled, and the
transaction was not restored by rolling back to a savepoint, the outermost
scope raises `ScopeAbortedError` on exit instead of committing.

### FR-05b: A dead scope is not joinable

A task or context that inherited a scope's session and is still running after
that scope exited opens a scope of its own on its next call.

### FR-06: A schema mismatch is an error

A scope is opened for at most one `dbschema`. A joiner naming a different one
raises `ScopeSchemaMismatchError`. A joiner naming none shares the session as
it is.

### FR-07: Lifecycle imports without SQLModel

Importing `mint.db.asynk` or `mint.db.sync` and using `Database`, `UnitOfWork`
and `scoped` does not import `sqlmodel`. The repository classes stay importable
from the same paths and load on first access.

### FR-08: Installable extras

`mint[db]` installs the lifecycle dependencies. `mint[orm]` adds SQLModel.

### FR-09: Timeouts

`DatabaseSettings` carries a statement timeout and a pool checkout timeout,
both applied when `Database` builds its own engine.

### FR-10: Pool exhaustion has its own error

A pool checkout that times out raises `PoolExhaustedError` carrying the pool
size, the overflow and the timeout.

### FR-11: Opt-in retry of the outermost scope

`@db.scoped(retries=n)` re-runs the function when its scope fails with a
serialization failure, a deadlock or an invalidated connection. A scope that
joined another does not retry.

### FR-12: Savepoints work inside a scope

`db.current_session.begin_nested()` holds for the length of its block, since
nothing inside a scope commits.

### FR-13: Visible test and release contract

CI runs the db tests across a Postgres version matrix and against the lowest
declared dependency versions, with a named concurrency regression suite and a
coverage gate. A versioning page states what the public API is and how it may
change.

## Success Criteria

- A scoped function's writes are invisible to another session until it
  returns, and absent after it raises.
- Two repositories in one scope report the same session object.
- Fifty concurrent scoped tasks on one database get fifty distinct sessions
  and never read each other's rows.
- After the outermost scope exits, no session is bound and no connection is
  checked out.
- `mint.db.asynk` and `mint.db.sync` import with `sqlmodel` blocked.
- Every pre-existing db test passes without modification.

## Behaviour Change

A repository method that performs several writes is now atomic. Before, each
write committed separately, so a failure part-way left the earlier writes in
place. Code that relied on a partial write surviving a later failure has to
commit explicitly, by splitting the work into separate top-level calls.

## Out of Scope

- A repository layer for SQLAlchemy Core. A Core service's own query builders
  already are that layer.
- A primitive that suspends a transaction across a network call. That is the
  transactional outbox problem; the documentation points at the pattern.
- Read replicas, sharding and coordination across several databases.
- Binding `UnitOfWork` into the session slot. It stays the explicit,
  caller-committed alternative.
