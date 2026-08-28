# `mint.worker`: review-and-fix report

A summary of the work on the `feat/worker` branch: what was reviewed, what was
fixed, what kept going wrong, and what is still open. The per-bug detail lives in
[`worker-bugs-and-fixes.md`](worker-bugs-and-fixes.md) (117 entries); this page is
the shape of it.

## Outcome

| | |
|---|---|
| Commits | 35 — 1 `feat`, 22 `fix`, 12 `docs` |
| Diff vs `master` | 71 files, +20,903 / −675 |
| Source | 4,964 lines across 30 modules |
| Tests | 9,708 lines — 388 fast, 27 container |
| Bugs catalogued | 117 |
| Gates | `ruff`, `ruff format`, `ty`, `mypy`, `mkdocs --strict` all clean |

Bug provenance:

| Where they came from | Count |
|---|---|
| Inherited from the `mini.worker` predecessor | 14 |
| Found live while building the replacement test-first | 8 |
| Found by twelve review rounds after the package was complete | 95 |

Of those 95, **19 are regressions from an earlier round's own fix.** That number
is the most useful thing in this report, and the reason every round reviewed the
fixes rather than only the original code.

## Method

Each round followed the same loop:

1. Run `/code-review` over the branch (rounds 2–4 and 13 at `high` effort, 5–12 at
   `medium`).
2. **Verify every finding against the source before acting.** Several turned out
   to need a repro before they could be believed — and one turned out to
   contradict a claim in my own commit message.
3. Fix, with a regression test that **fails without the fix**. Every test was
   checked by reverting the fix and watching it fail; three tests passed against
   broken code until that check exposed them.
4. Run the full gate plus the memory-capped container lane.
5. Write the bug up, then re-review.

Three habits earned their place the hard way:

- **Guard every `MemoryBroker` read with a timeout.** The failure mode in this
  package is usually a message that never arrives, which hangs a suite instead of
  failing it.
- **Test a claim the fix depends on.** Two fixes rested on beliefs about asyncio;
  one was false and cost five rounds.
- **Assert on the state after the damage would show**, not the first observable
  step.

## What was fixed, by subsystem

### Canvas engine, builder and models — 24 issues

The fan-in rollback protocol accounts for most of them. A chord's callback
dispatch burns a one-shot guard *before* the caller publishes it, so every way
that publish can fail needs a matching release. Closing that took five rounds: the
guard was released for publish failures, then for guards burned deeper in the
walk, then for `CancelledError`, then for driver errors escaping the walk
unwrapped. Also: `ErrorPolicy.PROPAGATE` behaved identically to `CONTINUE` for a
chord with a callback; `ABORT` never recorded its own container's outcome and
never applied to a callback's own failure; cancelled subtrees kept running and
dispatching; and a nested container could steal its parent's id and produce a
group listed as its own child.

### Brokers — 33 issues

Kafka kept one consumer slot for every worker in an app, so one worker's ack
committed another's offsets. Its per-record commit then went too far the other
way, committing past records still in flight; only the contiguous settled prefix
is safe. The Redis broker declared at-least-once while never reclaiming a crashed
consumer's pending entries — `XREADGROUP >` returns only never-delivered entries,
and the docstring's cited fallback did not exist. RabbitMQ deadlocked every
publish once enough workers held pooled channels. Four separate bugs were the
dead-letter chain failing to terminate, one broker at a time.

### Stores — 14 issues

`RedisCanvasStore` promised, in `ICanvasStore`'s own words, that a cancelled node
could not be resurrected — a guarantee that held only inside one process, while
`MemoryCanvasStore`'s lock made it look solved. Three separate status writes moved
to a Lua compare-and-set, one round apart each. A reused `canvas_id` inherited the
previous attempt's status, then its fan-in guards, then its key-registry TTL.

### Worker, app, coordinator and executors — 45 issues

Deliveries could be stranded unacked and unlogged, settled twice, or retried
forever with no cap. Shutdown tore the transport down under in-flight handlers —
twice, because the first fix was built on a false premise. A dead consume loop was
never noticed. The coordinator's mutual exclusion against its own timeout sweeper
took four rounds to close.

### Toolchain — 1 issue

The package did not pass the project's own `make check`: 65 mypy errors, and a
`ruff` 0.15→0.16 bump in its lock file that broke lint repo-wide. Both are fixed;
the mypy blocker turned out to be test models named `Input`/`Output` shadowing the
class attributes, not a flaw in `Worker`'s design.

## The patterns worth keeping

Distilled from the 71-item checklist at the end of the catalogue — these are the
mistakes that recurred, not one-offs.

**A rule applied to one half of a pair — 8 occurrences, the most repeated error
here.** Declare/publish, ABORT/PROPAGATE, track/settle, Redis store/memory store,
and four separate instances of the dead-letter-chain rule. When a fix lands on one
implementation of a Protocol, the others belong in the same commit.

**A half-satisfied contract, which is worse than an unsupported one** — because
the caller is told it worked. Restarting an app cleared the worker's stop flag but
not the closed broker. Retrying a canvas reset the status but not the fan-in
guards. Both were fixes to earlier fixes.

**A test double that hides the thing under test.** `MemoryBroker` didn't implement
the dead-letter rule, so the tests couldn't see it. `MemoryCanvasStore` has no real
suspension points, so a race test written against it drove two paths in sequence
and called that concurrency. Where the property under test is the double's
behaviour, the double is part of the contract.

**A guard placed after the `await` it protects.** The sweeper marked a node
timed-out after advancing it; settlement was recorded after the ack rather than
before. Both left exactly the window they were written to close.

**A docstring asserting behaviour the code never implemented.** Four instances,
including one — `NodeStatus.RUNNING` — where an earlier bug's fix explicitly
promised "every transition writes status explicitly" and the RUNNING transition
was never written at all.

## What I got wrong

Worth recording, because these are the failures the method was supposed to catch.

**A fix built on a false belief about the language.** I claimed that holding a
reference to an async generator would stop its `finally` running when the
consuming task was cancelled. It does not — that prevents *garbage-collection*
finalisation only; cancellation raises inside the generator and unwinds it. The
shutdown bug I reported as fixed in round 7 was still live in round 12, and the
`close_consumer()` method I added was dead code from the moment I wrote it. It
survived because the regression test asserted the *order* of two calls while the
real cleanup ran through a different mechanism.

**A test that asserted the wrong thing.** The app-restart fix's test asserted
`app._running is True` — a flag `run()` sets before the consume loops hit the
closed transport and die. The checklist rule "assert on the state after the damage
would show" had been added one round earlier. Writing a rule down did not stop me
breaking it; reverting the fix and watching the test fail would have.

**Numbers I reported to the user that were wrong.** I quoted "17 inherited from
`mini.worker`" (it is 14) and "29 regressions" (19 by the write-ups' own
attributions). Both were hand tallies I never checked against the catalogue.
Corrected here and in the two documents that carried them.

**A fix that overshot.** Replacing a too-wide Kafka commit with a per-record one
made it commit too far *ahead* instead. Two rounds later the contiguous-prefix
version was correct.

## State and what is open

The branch is **not merged and its commits are unsigned** — GPG signing could not
prompt for a passphrase in this environment, and unsigned commits were an explicit
decision. Re-sign before merging if the remote enforces verification.

**The rounds have not converged.** Round 13, at `high` effort, still returned two
HIGH findings. The trend is downward — 18 issues in round 2, 6 in round 13 — and
the last four rounds cleared areas they had previously flagged, but stopping was a
decision rather than a signal from the code.

## Where to look

- [`worker-bugs-and-fixes.md`](worker-bugs-and-fixes.md) — all 117 entries, each
  with a concrete failure scenario, the root cause, and the fix; plus the 71-item
  checklist for anyone extending the package.
- [`worker-implementation-notes.md`](worker-implementation-notes.md) — why the
  package is built the way it is, and the migration mapping from `mini.worker`.
- [`worker/usage.md`](worker/usage.md) — the `Chain`/`Chord` DSL, deployment
  modes, brokers, executors, and the constraints callers need to know about.
