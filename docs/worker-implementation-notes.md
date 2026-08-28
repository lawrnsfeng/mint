# Worker Implementation Notes

`mint.worker` is a decentralized async canvas-orchestration library — chains and
chords (fan-out/fan-in) of tasks dispatched over a broker, similar in spirit to
Celery's `chain`/`chord` but fully asynchronous, ported from an internal
predecessor (`mini.worker`) and hardened against 101 confirmed bugs — 17 inherited
from `mini.worker`, 5 found live while building this package test-first against
real containers, and 79 across ten review rounds afterwards. Twenty-four of those were
introduced by an earlier round's own fix, which is why every round reviewed the
fixes and not just the original code. This
page is the permanent reference for *why* it's built the way it is, a summary of
the inherited bugs, and the mapping for migrating an existing `mini.worker`
consumer. [`worker-bugs-and-fixes.md`](worker-bugs-and-fixes.md) is the expanded
version, with a concrete failure scenario and the exact fix for every one.

## The core idea, unchanged from mini

There is no orchestrator process by default: a task graph (chain/chord) is
written to a store, then each worker — after finishing its own task — records
its result, looks up its parent in the graph, and publishes whatever comes next
itself. That decentralization is the library's differentiator and the one
design choice this port never questioned. Everything else was fixed.

## mini vs. mint: bugs found and fixed

The 17 bugs inherited from `mini.worker`. Bugs #18-22 (found live while building
this package) and #23-101 (found by the ten review rounds) are catalogued in
[`worker-bugs-and-fixes.md`](worker-bugs-and-fixes.md).

| # | Bug | mini location | Fix |
|---|---|---|---|
| 1 | A `Chain` used as a chord leg dispatched to child 1's topic but stamped the **chain's own** id on the message — steps 2..n never ran | `workers/base.py:194-203` | `Envelope.node_id` is now always the id of the node actually being targeted; a chain's `publish_entries()` always resolves down to its first real task. |
| 2 | A group with `callback=None` never propagated its completion to its own parent — a chord nested inside another chord's leg hung forever | `workers/base.py:167-168` | `CanvasEngine._complete()` treats a no-callback group's completion as a normal outcome that still walks up to `parent_id`, same as any other node. |
| 3 | Fan-in used `INCR` + `!= num_children` — one redelivery of the same leg's result double-counted, firing the callback early or never | `workers/base.py:152-155` | Replaced with an atomic Lua script (`SADD` + `SCARD` + `SETNX`) — `added == 1 AND count == num_children` is the only way the callback fires, so a redelivered leg is idempotent by construction. |
| 4 | A failed leg's error was swallowed — the message acked, the fan-in counter never incremented, the chord hung. The mini consumer this was ported from worked around it by calling the **private** `_check_next_step` directly from `on_failure` | `base.py:88-94`; consumer's `app.py:549-556` | `ErrorPolicy` (`CONTINUE`/`PROPAGATE`/`ABORT`) makes failure a first-class, engine-handled outcome — `CONTINUE` (a group's default) still counts the leg and fires the callback with `ok=False` present. No private-method workaround is possible or needed anymore. |
| 5 | A root task's result (no parent at all) was never stored — an early `parent_id is None` return skipped the write | `workers/base.py:105-106` | `CanvasEngine._complete()` always writes the just-finished node's own outcome before walking to a parent, so a bare root task's result is recorded even when there's nothing above it. |
| 6 | `Chord.start()` published every leg's first message in a bare loop — a failure partway through left the group's graph half-published with no cleanup | `workers/canvas.py:185-196` | `Chain.apply()`/`Chord.apply()` write every node to the store **before** publishing any leg; a publish failure marks the whole canvas `ERROR` instead of leaving it stranded. |
| 7 | The Kafka broker's consumer never started (a `started` property checked with `is None`, but it's a bool that's never actually `None`); `create_topics(...)` was called without `await`, silently discarding the coroutine; `AIOKafkaProducer(group_id=...)` isn't a valid producer argument at all | `brokers/kafka.py:69,91,133` | Every client (producer/consumer/admin) is started explicitly via an `if self._x is None:` lazy-construction check — no property-based "is it started" heuristic. |
| 8 | The Redis broker was at-most-once (`BRPOP`, no ack) | `brokers/redis.py:49-65` | Rebuilt on Streams + consumer groups (`XADD`/`XREADGROUP`/`XACK`) — genuinely at-least-once, matching every other broker's `DeliveryGuarantee`. |
| 9 | RabbitMQ's `reject()` defaulted `requeue=False` with no dead-letter exchange declared — a rejected message vanished with nowhere to go | `brokers/rabbitmq.py:118` | Every queue is declared with `x-dead-letter-exchange` pointing at its own per-topic DLX; `nack(requeue=False)` reliably lands on `{topic}.dlq`. Confirmed against a real broker, not just asserted against a mock — see "Bugs only a container test could catch" below. |
| 10 | NATS used one hardcoded durable consumer name for every topic — two different topics' consumers fought over the same JetStream delivery cursor | `brokers/nats.py:7,83-87` | The durable name is derived per topic (`f"{group}-{stream_name(topic)}"`), so each topic gets its own cursor. |
| 11 | `AIOPikaAsyncExecutor.__del__` called `asyncio.run()` — which raises immediately when called from a running loop, exactly when `__del__` actually fires during normal operation; a lost RPC reply left its future (and its correlation-id map entry) leaking forever, with no timeout at all | `executors/aiopika.py:106-107,90` | `aclose()` is explicit everywhere in `mint.worker` — no executor, broker, or store does cleanup from `__del__`. `AMQPRPCExecutor` wraps every call in a configurable timeout that cancels the future and removes its map entry on expiry. |
| 12 | `GRPCExecutor` raised `TypeError` when a stub method **was** a coroutine function — the check was inverted, rejecting exactly the case that should have worked | `executors/grpc.py:36-39` | The check is removed rather than fixed: awaiting whatever the method returns works uniformly whether it's a coroutine or one of grpc.aio's actual generated stub calls (an awaitable `Call` object, never itself a coroutine function) — the check was never load-bearing. |
| 13 | `NodeStatus` was defined but never written anywhere; no TTL on canvas keys; Redis keys were unnamespaced (two canvases could collide) | `enums.py:4`; `result_backends/redis.py` | Every transition writes `NodeStatus` explicitly; `RedisCanvasStore` namespaces every key under `{namespace}:canvas:{cid}:...` and expires every tracked key once a canvas reaches a terminal status. |
| 14 | `RedisBackend` didn't structurally satisfy `IResultBackend`, forcing `# type: ignore[arg-type]` on every consumer's container wiring | `result_backends/redis.py:9` | `ICanvasStore`/`IBroker`/`ITaskExecutor` are Protocols every implementation satisfies structurally, checked by `ty` — no suppression needed anywhere in the wiring. |
| 15 | **Nested-group fan-in re-embedded an already-serialized result as a string field, at every level of nesting — O(2^depth) payload growth, an unbounded-memory latent bug** | `mint/worker/canvas/engine.py::_advance_group` (found live during this port's own TDD — see the incident below) | A no-callback group's bubbled outcome carries `result=None`, never the encoded fan-in payload; `CanvasEngine._record` also enforces a `max_result_bytes` guard (default 256KB) as defense in depth. |
| 16 | **Kafka's consumer defaults `auto_offset_reset="latest"`** — a brand-new consumer group (a worker restart with a fresh `group_id`, or any consumer attaching after a topic already has a backlog) silently skips everything already sitting in the topic: an at-least-once violation | `mint/worker/brokers/kafka.py::consume` (found live via a real-container test: publish-then-consume timed out) | Every consumer is built with `auto_offset_reset="earliest"` — a group's first-ever attach to a topic sees the backlog instead of skipping past it. |
| 17 | **`KafkaBroker.deadletter` passed a real `ConsumerRecord.headers` tuple straight to the producer**, which requires a `list` (its Cython record-batch builder rejects a tuple with `TypeError`) | `mint/worker/brokers/kafka.py::deadletter` (found live via a real-container test: `nack(requeue=False)` raised) | Headers are copied into a `list` before publishing. A mocked producer never enforces this type distinction — this bug was only reachable against the real broker. |

## Bugs only a container test could catch

Bugs #9, #16, #17 and #21 all passed their mocked-client test suite and only
surfaced against a real broker — the reason a broker gets two test files
(`test_<name>_mocked.py`, then `test_<name>_container.py`), run in that order,
never combined into one file. Mocking catches *our own* logic bugs (a wrong
method call, a missing `await`, a hardcoded name — bugs #7, #8, #10, #12 above
all fall in this category); a handful of real-protocol behaviors (exact
wire-level type requirements, a client's default offset-reset policy, a queue
redeclaration conflict, whether two consumer groups really commit independently)
are only observable against the real thing.

Container coverage is deliberately uneven rather than uniform, and worth being
precise about: RabbitMQ and Kafka have container suites (`test_rabbitmq_container.py`,
`test_kafka_container.py`), and so does `RedisCanvasStore`
(`test_redis_container.py`, for the Lua fan-in's atomicity under real
concurrency). The Redis *broker* and NATS have mocked suites only — a gap, not a
claim of equivalence.

## The idempotent fan-in, in detail

Bug #3's fix is the keystone of the whole redesign, because it's what makes
acking *after* the canvas advances (rather than before, or racing it) safe
under at-least-once redelivery — which in turn is what lets every broker be
honest about its actual delivery guarantee instead of papering over it.

```
SADD  {ns}:canvas:{cid}:group:{gid}:done  {child_id}   -> added (1|0)
SCARD {ns}:canvas:{cid}:group:{gid}:done               -> n
fire callback  iff  added == 1 AND n == num_children
```

`added == 1` alone kills redelivery double-counting on its own; the `SETNX` on
a `callback_fired` key is belt-and-braces against the store itself restarting
mid-script. `MemoryCanvasStore` (used for the entire mocked test suite) mirrors
this exact contract without Redis, via a plain Python `set` guarded the same
way — the atomicity property under real concurrency is what needs a container
to prove, not the logic itself.

## Incident: bug #15 was found the hard way

While writing the "deep nesting resolves without recursion" regression test for
this port, `pytest` was OOM-killed twice on the development machine. Two
separate causes stacked:

1. **The first version of that test wasn't safe to run at all**: it called
   `sys.setrecursionlimit(80)` process-wide inside an async test to "prove" the
   engine doesn't recurse. CPython's own machinery (asyncio's task stepping,
   pytest-asyncio, assertion rewriting, traceback formatting) needs far more
   than 80 stack frames just to keep running — this doesn't test the engine, it
   wedges the interpreter into cascading `RecursionError`s during its own
   cleanup paths. **Fix**: the limit mutation was removed entirely; depth alone
   (past `sys.getrecursionlimit()`, never by mutating the limit itself) is
   sufficient proof — a truly-recursive `_complete` would raise a natural
   `RecursionError` at the default limit, an iterative one won't.

2. **That fix still hung and grew to 8.5GB RSS**, because the test's graph
   alternated `ChainNode`/`GroupNode` at every level. Verified in isolation,
   under a memory cap so it couldn't repeat the OOM, with a bounded simulation
   of exactly what `_advance_group` does:

   | level | `len(result)` | growth |
   |---|---|---|
   | 0 | 90 | — |
   | 9 | 17,090 | 1.94x |
   | 15 | 1,049,720 | 2.00x |
   | 21 | 67,110,446 | 2.00x |

   Exactly **2x per level, forever**. `_advance_group` built a `ChildResult`
   from each child's already-serialized `.result` string, wrapped them into a
   `FanIn`, and re-serialized that whole object to JSON as the *group's own*
   `.result`. When that group's result became one more child's value inside an
   **outer** group's fan-in, the JSON encoder had to escape every `"` and `\`
   the inner blob already contained — escaping an already-escaped string
   roughly doubles its character count. Nest groups N levels deep and the
   payload is O(2^N); the test's target depth would never have finished, just
   allocated ever-larger strings until the kernel OOM-killed it — exactly what
   was observed both times.

   This was not only a bad test — it was a **real latent bug**, reachable any
   time a `callback=None` group is nested more than a couple of levels inside
   another group. The callback-*present* path was never at risk: a real
   callback runs actual business logic and returns its own fresh, normal-sized
   result, which is what naturally resets payload size at every level in
   practice.

**Fix, two parts:**

- **Structural**: a `callback=None` group's bubbled outcome carries
  `result=None`, not the encoded `FanIn`. Nothing consumes that value in the
  no-callback path — it existed purely to be re-embedded by whatever contained
  it, which was the exact mechanism that compounded. A caller that genuinely
  needs a nested leg's own children's outcomes can query the store directly
  (`get_results`), which already exists for this.
- **Defense in depth**: `CanvasEngine._record` enforces a `max_result_bytes`
  guard (default 256KB, constructor-configurable) — any `NodeOutcome.result`
  over the limit raises `ResultTooLargeError` immediately instead of silently
  continuing to allocate. This also anticipates a constraint real brokers
  already impose (RabbitMQ and Kafka both cap message frame size).

**Process fix, going forward**: any test in this suite that varies nesting
*depth* as its independent variable is run once, standalone, under a memory
cap before it ever joins the full suite — a standing rule for this package,
documented in the README's Development section, not a one-off.

## Two deployment modes, one engine

`CanvasEngine` holds every transition rule and is completely I/O-shaped around
`ICanvasStore` — it has no idea whether it's being driven by many embedded
workers or one centralized process. That's what makes centralized mode an
additive deployment choice rather than a second implementation to keep in sync:

- **Embedded** (the default): each `Worker` calls `engine.complete()` itself,
  right after finishing its own task, and publishes whatever dispatches come
  back. This is `mini.worker`'s original decentralized model, fixed.
- **Centralized** (opt-in, `WorkerApp(..., results_topic=...)`): every worker
  instead reports its `NodeOutcome` to one shared results topic and never
  touches the engine or dispatch at all; a single `Coordinator` process
  consumes that topic and is the only thing that ever calls
  `engine.complete()`. This is what makes `Coordinator.cancel(canvas_id)` and
  the timeout sweeper possible without any store schema change — since every
  non-entry dispatch in the whole canvas passes through one process, it can
  track "what am I still waiting on" purely in memory. Pairing
  `Coordinator.track_and_publish` with `Chain.apply()`/`Chord.apply()`'s
  `publish` argument (in place of a bare `broker.publish`) is what brings a
  canvas's very first dispatch(es) under the same tracking, since otherwise the
  coordinator only ever learns about dispatches it derives itself.

Both modes produce identical dispatch sequences for the same input — verified
by construction (the same engine, the same store contract), not by a separate
parity test suite.

## Migrating from `mini.worker`

This is a clean break, not a compatibility shim — there is no `mini.worker`
import path preserved in `mint.worker`. Every symbol below has a 1:1 conceptual
replacement; most call sites change shape slightly along with the bug fixes
above.

| `mini.worker` | `mint.worker` | Notes |
|---|---|---|
| `workers.base.AsyncWorker` | `worker.Worker[T, RT]` | Same class-based shape (`Input`/`Output`/`process`/hooks); `topic` is now a required class attribute, validated at `WorkerApp.register()` instead of failing on first message (bug #14's `# type: ignore` is gone along with it). |
| Container-specific wiring (`app.py` boilerplate per service) | `app.WorkerApp` | `register()` validates `Input`/`Output`/`topic` and rejects duplicate topics up front; `run()` owns SIGTERM/SIGINT-triggered graceful drain. |
| `workers.canvas.Chain`/`Chord` | `canvas.builder.Chain`/`Chord` | Same DSL shape. `apply()` now takes an explicit `store`/`publish` pair (or `Coordinator.track_and_publish` for centralized mode) instead of assuming one `WorkerApp`. |
| `on_failure` reaching into `_check_next_step` (bug #4's workaround) | `error_policy=ErrorPolicy.CONTINUE` (a group's default) | Delete the workaround entirely — a failed leg is counted and reported (`ok=False`) by the engine itself. |
| `BoxAutoSyncInput.input: str` + `input_obj` re-parsing property | `FanIn` | A real pydantic model (`children: list[ChildResult]`, each with `node_id`/`ok`/`value`/`error`) instead of an untyped dict — `ok`/`error` are now first-class instead of inferred from a missing key. `ChildResult.value`/`FanIn.input` are still JSON-encoded strings (a leg's own `Output` type isn't known generically at the fan-in point), so a callback still calls `Output.model_validate_json(child.value)` per child — the shape is fixed, the string-parsing step at the leaf isn't. |
| `brokers.redis.RedisBroker` (BRPOP) | `brokers.redis.RedisBroker` (Streams) | Same import path shape, same `IBroker` Protocol — now genuinely at-least-once (bug #8). Redeploy needs a fresh consumer group name; there is no wire-format compatibility with the old BRPOP-based queues. |
| `brokers.rabbitmq.RabbitMQBroker` | `brokers.rabbitmq.RabbitMQBroker` | Queues now declare a per-topic DLX (bug #9) — existing queues declared by the old broker will conflict (`PRECONDITION_FAILED`) on redeploy; delete and let them redeclare, or migrate under a new topic name. |
| `result_backends.redis.RedisBackend` | `stores.redis.RedisCanvasStore` | Namespaced keys (`{namespace}:canvas:{cid}:...`, bug #13) — not wire-compatible with the old backend's key layout. |
| `executors.aiopika.AIOPikaAsyncExecutor` | `executors.amqp_rpc.AMQPRPCExecutor` | Explicit `aclose()` instead of a crashing `__del__` (bug #11); every call has a configurable timeout. |
| `executors.grpc.GRPCExecutor` | `executors.grpc.GRPCExecutor` | Same shape; the inverted coroutine-function check (bug #12) is gone. |
| `executors.thread_pool`/`process_pool` | `executors.thread_pool`/`process_pool` | Constructor no longer binds one `fn` — `execute(fn, input_)` takes the callable per call, matching `ITaskExecutor`'s shared shape with every other executor (including the remote ones, which ignore `fn` entirely and dispatch `input_` remotely instead). |
| No centralized mode | `coordinator.Coordinator` | New, opt-in — see "Two deployment modes" above. Nothing to migrate; adopt when a service needs `cancel()`/timeout-sweeping and doesn't already have another way to get it. |

A concrete migration for a chord-shaped service (N leg-sync tasks with an
auto-include callback, some legs expected to fail) looks like:

```python
# mini.worker
class BoxReferenceSyncHandler(AsyncWorker):
    async def on_failure(self, msg, exc):
        # reaching into a private method — bug #4's workaround
        await self._check_next_step(msg)

# mint.worker
class BoxReferenceSync(Worker[RefIn, RefOut]):
    topic = "box.sync_reference"
    Input = RefIn
    Output = RefOut
    # error_policy=ErrorPolicy.CONTINUE is the group's own default —
    # nothing to override here; a failed leg is just counted and reported.
```
