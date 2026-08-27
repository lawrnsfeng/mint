# Async Canvas Worker

`mint.worker` orchestrates a graph of tasks — a `Chain` (sequence) or `Chord`
(fan-out with an optional fan-in callback) — dispatched asynchronously over a
message broker, similar in spirit to Celery's `chain`/`chord` but fully async
and, by default, with no orchestrator process: each worker advances the canvas
itself, right after finishing its own task.

For the *why* behind every design choice here — the bugs found and fixed
porting this from an internal predecessor, the idempotent fan-in algorithm, and
the migration mapping — see the
[Implementation Notes](../worker-implementation-notes.md), which this guide
cross-links into rather than duplicates.

## Design philosophy

- **Decentralized by default, centralized when you actually need it.** A
  `CanvasEngine` holds every transition rule and knows nothing about who calls
  it — embedded mode has each `Worker` call it directly; centralized mode has
  one `Coordinator` process call it instead, off a shared results topic. Same
  engine, same dispatch sequences, two deployment shapes.
- **At-least-once, made safe by an idempotent engine, not by hoping.** Every
  broker's `nack(requeue=True)` genuinely redelivers; the canvas engine's
  fan-in is atomic (`SADD`+`SCARD`+`SETNX` in one round trip) specifically so a
  redelivered leg never double-fires a callback. A `Worker` acks only *after*
  the store write and every resulting dispatch publish succeed — never before.
- **Explicit delivery guarantees, not one Protocol hiding five different
  behaviors.** Every `IBroker` implementation declares its actual
  `DeliveryGuarantee` and is tested against the same set of properties
  (round-trip, redelivery, DLQ routing) — a mocked-client suite first, then a
  small real-broker suite for what mocking can't prove.
- **A real fan-in model, not an untyped dict.** A chord callback receives
  `FanIn` — `children: list[ChildResult]`, each with `node_id`/`ok`/`value`/
  `error` as first-class fields — not a `{"children_result": [...], "input":
  "<json string>"}` shape inferring success from a missing key.
- **Class-based workers keep per-worker dependencies.** `Worker[T, RT]`
  carries its own repos/connectors/storage naturally as instance attributes;
  `WorkerApp` owns only the shared wiring (broker, store, topic registry,
  graceful shutdown) — not a redesign away from what already worked well.

## Defining a worker

```python
from pydantic import BaseModel
from mint.worker.worker import Worker

class SyncRefIn(BaseModel):
    reference_id: str

class SyncRefOut(BaseModel):
    synced: bool

class SyncReference(Worker[SyncRefIn, SyncRefOut]):
    topic = "box.sync_reference"
    Input = SyncRefIn
    Output = SyncRefOut

    def __init__(self, connector: BoxConnector) -> None:
        super().__init__()
        self.connector = connector          # ordinary per-worker dependency

    async def process(self, input_obj: SyncRefIn) -> SyncRefOut:
        await self.connector.sync(input_obj.reference_id)
        return SyncRefOut(synced=True)

    async def on_failure(self, input_obj: SyncRefIn, exc: Exception) -> None:
        logger.warning("sync failed", reference_id=input_obj.reference_id, error=str(exc))
```

- `topic`/`Input`/`Output` are required class attributes, validated at
  `WorkerApp.register()` — a missing one is a startup-time error, never an
  `AttributeError` on the first message.
- `before_start`/`on_success`/`on_failure` are optional hooks; a hook that
  raises is logged and swallowed — it never un-advances the canvas or leaves a
  message unacked.
- `process` returning a value `Output` can't validate is treated exactly like
  a raised exception: `on_failure` fires, the node completes `ERROR`.

## Wiring an app

```python
from mint.worker.app import WorkerApp
from mint.worker.brokers.rabbitmq import RabbitMQBroker
from mint.worker.stores.redis import RedisCanvasStore

app = WorkerApp(
    broker=RabbitMQBroker("amqp://guest:guest@localhost/"),
    store=RedisCanvasStore("redis://localhost", namespace="easyrag"),
)
app.register(SyncReference(connector=BoxConnector(settings)))
app.register(AutoInclude(db=db))

await app.run()   # consumes every registered topic; SIGTERM/SIGINT -> drain -> close
```

Each worker handles at most `Worker.max_concurrency` deliveries at once (32 by
default; set it as a class attribute). The consume loop stops pulling while that
many are in flight — the only backpressure Kafka and `MemoryBroker` get, since
neither has a prefetch of its own.

`register()` rejects two workers claiming the same topic, and validates
`Input`/`Output`/`topic` exist before the app ever starts consuming.
`app.stop()` triggers the same graceful shutdown programmatically. A worker
still in flight when the drain timeout (`WorkerApp(..., drain_timeout=5.0)`)
expires is nacked for redelivery, not dropped.

## Building a canvas: Chain and Chord

```python
from mint.worker.canvas.builder import Chain, Chord, Node

# a linear sequence
canvas_id = await Chain([
    Node(topic=SyncReference.topic, input=SyncRefIn(reference_id=ref_id).model_dump_json()),
    Node(topic=Announce.topic, input=AnnounceIn(...).model_dump_json()),
]).apply(store, app.broker.publish)

# a fan-out with a callback that only fires once every leg finishes
canvas_id = await Chord(
    [
        Node(topic=SyncReference.topic, input=SyncRefIn(reference_id=r).model_dump_json())
        for r in reference_ids
    ],
    callback=Node(topic=AutoInclude.topic),  # no input: it receives the FanIn instead
).apply(store, app.broker.publish)
```

- Every node in a `Chain`/`Chord` is persisted **before** any leg is
  published — a publish failure partway through marks the whole canvas
  `ERROR` instead of stranding a half-dispatched group.
- Nested `Chain`s flatten at construction time; a `Chord` leg can itself be a
  `Chain` or another `Chord` — nesting composes because every compound node's
  `parent_id` points at whatever directly contains it, so "a chain finished"
  and "a chord's callback finished" are the same kind of event bubbling up.
- `apply()` takes an explicit `(store, publish)` pair rather than a whole
  `WorkerApp` — in centralized mode, pass `coordinator.track_and_publish` as
  `publish` instead of `app.broker.publish` (see "Centralized mode" below).
- `error_policy` (`ErrorPolicy.CONTINUE` / `PROPAGATE` / `ABORT`) is settable
  per `Chain`/`Chord`: a chain defaults to `PROPAGATE` (stop and cancel the
  remainder), a chord defaults to `CONTINUE` (fire the callback regardless,
  with failed legs present as `ok=False`).

## Reading a chord callback's fan-in

```python
from mint.worker.canvas.models import FanIn

class AutoInclude(Worker[FanIn, AutoIncludeOut]):
    topic = "box.auto_include"
    Input = FanIn
    Output = AutoIncludeOut

    async def process(self, input_obj: FanIn) -> AutoIncludeOut:
        failed = [c.node_id for c in input_obj.children if not c.ok]
        succeeded = [
            SyncRefOut.model_validate_json(c.value)
            for c in input_obj.children
            if c.ok and c.value is not None
        ]
        ...
```

`FanIn`/`ChildResult` aren't generic over each leg's `Output` type — a chord's
legs can be heterogeneous, so the fan-in point has no single type to be generic
over. `ok`/`error` are first-class fields (no more inferring success from a
missing key), but a succeeded leg's `.value` is still the JSON string its own
`Output.model_dump_json()` produced — decode it with that leg's own `Output`
type. `FanIn.input` carries whatever `Chord(..., input=...)` was given, for
context the aggregation step needs that isn't any single leg's output (the
originating request, a tenant id); it's `None` if you didn't pass one.

## Brokers

| Broker | `DeliveryGuarantee` | Notes |
|---|---|---|
| `brokers.memory.MemoryBroker` | at-least-once | In-process `asyncio.Queue`; for tests and single-process use. |
| `brokers.rabbitmq.RabbitMQBroker` | at-least-once | Declares a per-topic dead-letter exchange; `nack(requeue=False)` reliably lands on `{topic}.dlq`. `nack(requeue=True)` republishes with `attempt` bumped rather than using AMQP's native requeue, so the counter is observable — at the cost of ordering on that path. |
| `brokers.redis.RedisBroker` | at-least-once | Streams + consumer groups (`XADD`/`XREADGROUP`/`XACK`). |
| `brokers.nats.NatsBroker` | at-least-once | JetStream pull consumers with a per-topic durable name. |
| `brokers.kafka.KafkaBroker` | at-least-once | `auto_offset_reset="earliest"` — a new consumer group sees a topic's backlog, never skips it. |

Every implementation satisfies the same `IBroker` Protocol
(`publish`/`consume`/`close`), and every one is tested against the same
properties — publish/consume round-trip, `nack(requeue=True)` redelivers with an
incremented `attempt`, `nack(requeue=False)` reaches the DLQ — against a mocked
client (`test_<broker>_mocked.py`).

Container suites exist for RabbitMQ and Kafka (`test_<broker>_container.py`),
and for `RedisCanvasStore`; the Redis *broker* and NATS have mocked coverage
only. That unevenness is a real gap rather than a claim of equivalence — the
bugs a container catches (exact wire-level types, a client's default policies, a
redeclaration conflict) are structurally invisible to a mock. See the
[Implementation Notes](../worker-implementation-notes.md); container suites run
standalone and memory-capped.

Construction never requires a running event loop — every broker builds its
connection pool lazily, on first actual use, so ordinary synchronous
DI/container setup works.

## Executors

`Worker.executor` (or `WorkerApp(..., executor=...)` for the app-wide default)
controls how `process` actually runs:

```python
from mint.worker.executors.thread_pool import ThreadPoolExecutor
from mint.worker.executors.process_pool import ProcessPoolExecutor
from mint.worker.executors.grpc import GRPCExecutor
from mint.worker.executors.amqp_rpc import AMQPRPCExecutor

class BlockingWorker(Worker[In, Out]):
    topic = "blocking"
    Input = In
    Output = Out
    executor = ThreadPoolExecutor()          # overrides the app's default

class RemoteWorker(Worker[In, Out]):
    topic = "remote"
    Input = In
    Output = Out
    # replaces process() entirely — process() is never called
    executor = GRPCExecutor("localhost:50051", MyServiceStub, "Handle")

class QueueBackedWorker(Worker[In, Out]):
    topic = "rpc-backed"
    Input = In
    Output = Out
    executor = AMQPRPCExecutor("remote.queue", "amqp://guest:guest@localhost/", Out)
```

- `InlineExecutor` (the default) just awaits `process` directly — correct for
  any non-blocking task.
- `ThreadPoolExecutor`/`ProcessPoolExecutor` offload a blocking or CPU-bound
  `process` onto a worker thread/process; `ProcessPoolExecutor` checks
  `fn`/`input_` are picklable up front and raises `UnpicklableTaskError`
  immediately rather than hanging the pool.
- `GRPCExecutor`/`AMQPRPCExecutor` are remote executors: they **replace**
  `process` rather than wrap it, dispatching `input_` to a remote service or
  queue instead. A worker using one never needs to implement `process` at all.
- Every executor holding a resource (a pool, a connection) exposes an explicit
  `aclose()` — never a `__del__` side effect. `WorkerApp` closes every distinct
  closable executor in use exactly once on shutdown, whether it's the app's
  shared default or a per-worker override.

## Centralized mode

By default (embedded mode), a worker advances the canvas itself. Set
`results_topic` on `WorkerApp` to switch every registered worker into
centralized mode instead:

```python
from mint.worker.coordinator import Coordinator

app = WorkerApp(broker, store, results_topic="canvas.results")
app.register(SyncReference(connector=...))

coordinator = Coordinator(broker, store, results_topic="canvas.results")
await coordinator.run()   # the only process that ever calls engine.complete()
```

Workers now only report their `NodeOutcome` to `results_topic` and never touch
the engine or dispatch — a single `Coordinator` process does. This unlocks two
things embedded mode can't offer on its own, without any store schema change:

```python
await coordinator.cancel(canvas_id)   # mark every in-flight node CANCELLED, error the canvas
```

- **`cancel(canvas_id)`** — cancels every node the coordinator is still
  waiting a result for and marks the canvas `ERROR`.
- **A timeout sweeper** — a node dispatched more than `max_age` seconds ago
  with no reply is reported as a synthetic `ERROR` outcome (the exact same
  path a real task failure takes), so a canvas can't hang forever on a lost
  message.

Since the coordinator can only track dispatches it sees, pass
`coordinator.track_and_publish` as `Chain.apply()`'s/`Chord.apply()`'s
`publish` argument (instead of a bare `broker.publish`) so a canvas's entry
dispatch(es) are covered by the sweeper too — otherwise the coordinator only
ever learns about dispatches it derives itself from `engine.complete()`.

## Error policies

```python
from mint.worker.enums import ErrorPolicy

Chain([...], error_policy=ErrorPolicy.ABORT)      # cancel remaining siblings, error the canvas
Chord([...], callback=..., error_policy=ErrorPolicy.CONTINUE)  # fire callback with failures present
```

| Policy | Chain default? | Chord default? | Behavior |
|---|---|---|---|
| `CONTINUE` | no | **yes** | Record the error, keep going — a chain proceeds to its next step; a chord still counts the leg toward fan-in. |
| `PROPAGATE` | **yes** | no | Stop (a chain cancels its remaining steps), mark the immediate parent `ERROR`, still bubble up. |
| `ABORT` | no | no | Cancel every pending sibling that hasn't run yet and fail the whole canvas — nothing further dispatches. Legs that already finished keep their outcomes. |

A chain step that fails under `CONTINUE` has no result to hand its successor, so
the next step is dispatched with `"{}"` as its body. Unless that worker's `Input`
has all-optional fields, it will fail validation — which now fails that node
explicitly and reaches a terminal canvas status, rather than stalling. If a
chain's later steps need to run after an earlier failure, give their `Input`
models usable defaults.

## Testing against `mint.worker`

`MemoryBroker`/`MemoryCanvasStore` mirror every real implementation's contract
(including the atomic fan-in guarantee) with no Docker and no sleeps — this
package's own test suite is built entirely on them for anything that isn't
specifically testing a real broker's wire behavior. See the memory-capped
Makefile lanes in the root `README.md`'s Development section before running
`tests/worker` directly — a nesting-depth test once OOM-killed the machine (see
the [Implementation Notes](../worker-implementation-notes.md) incident writeup).
