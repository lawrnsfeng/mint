# `mint.worker`: Every Issue Found, and How It Was Fixed

This is a practical, detailed walkthrough of every bug found while porting
`mini.worker` to `mint.worker` — both the bugs found by design review before
writing any code, and the ones found live while building the replacement
test-first. Each entry gives: where the bug lived, a concrete scenario where it
bites, why it happens, and exactly what the fix looks like. For the condensed
reference table and the migration mapping, see
[`worker-implementation-notes.md`](worker-implementation-notes.md) — this file
is the expanded, "how do I actually fix this" version.

## Table of contents

**Part 1 — Bugs in the original `mini.worker`, found by review and fixed while porting**

- [1. A `Chain` used as a chord leg dispatches with the wrong id](#1-a-chain-used-as-a-chord-leg-dispatches-with-the-wrong-id)
- [2. A callback-less group never tells its own parent it finished](#2-a-callback-less-group-never-tells-its-own-parent-it-finished)
- [3. Fan-in double-counts a redelivered leg](#3-fan-in-double-counts-a-redelivered-leg)
- [4. A failed leg silently stalls the whole chord](#4-a-failed-leg-silently-stalls-the-whole-chord)
- [5. A root task's result is never stored](#5-a-root-tasks-result-is-never-stored)
- [6. A publish failure mid-fan-out strands the group](#6-a-publish-failure-mid-fan-out-strands-the-group)
- [7. The Kafka consumer never actually starts](#7-the-kafka-consumer-never-actually-starts)
- [8. The Redis broker silently drops messages on crash](#8-the-redis-broker-silently-drops-messages-on-crash)
- [9. A rejected RabbitMQ message vanishes instead of dead-lettering](#9-a-rejected-rabbitmq-message-vanishes-instead-of-dead-lettering)
- [10. Two NATS topics fight over the same delivery cursor](#10-two-nats-topics-fight-over-the-same-delivery-cursor)
- [11. The AMQP-RPC executor crashes on GC and leaks futures forever](#11-the-amqp-rpc-executor-crashes-on-gc-and-leaks-futures-forever)
- [12. The gRPC executor rejects the one case that should work](#12-the-grpc-executor-rejects-the-one-case-that-should-work)
- [13. Node status is invisible; canvases never expire; keys collide](#13-node-status-is-invisible-canvases-never-expire-keys-collide)
- [14. The Redis result backend doesn't actually satisfy its own interface](#14-the-redis-result-backend-doesnt-actually-satisfy-its-own-interface)

**Part 2 — New bugs found live while building `mint.worker` itself (test-first)**

- [15. Nested groups double their payload size at every level (the OOM bug)](#15-nested-groups-double-their-payload-size-at-every-level-the-oom-bug)
- [16. Kafka silently skips a topic's backlog for a new consumer group](#16-kafka-silently-skips-a-topics-backlog-for-a-new-consumer-group)
- [17. Kafka's dead-letter publish crashes on a real broker](#17-kafkas-dead-letter-publish-crashes-on-a-real-broker)
- [18. `Worker.run()` spins the CPU to 100% on shutdown](#18-workerrun-spins-the-cpu-to-100-on-shutdown)
- [19. Graceful shutdown can double-nack a message](#19-graceful-shutdown-can-double-nack-a-message)
- [20. Constructing a broker outside an event loop crashes](#20-constructing-a-broker-outside-an-event-loop-crashes)
- [21. RabbitMQ rejects redeclaring its own dead-letter queue](#21-rabbitmq-rejects-redeclaring-its-own-dead-letter-queue)
- [22. A test helper's hidden hang (process hygiene, not a `mint.worker` bug)](#22-a-test-helpers-hidden-hang-process-hygiene-not-a-mintworker-bug)

---

## Part 1 — Bugs in the original `mini.worker`

### 1. A `Chain` used as a chord leg dispatches with the wrong id

**Where:** `mini/mini/worker/workers/base.py:194-203`

**Symptom:** Build a chord whose legs are themselves chains
(`Chord([Chain([step1, step2, step3]), ...])`). The first leg's `step1` runs —
and then nothing. `step2`/`step3` never fire; the chord hangs waiting for a
leg that will never report.

**Root cause:** The original `Message` model has one `id` field doing two
jobs: identifying the delivery *and* identifying which canvas node it targets.
When a `Chain` gets picked as a chord leg, its `publish_entries()`-equivalent
correctly resolves the message body down to the chain's first real task — but
it stamps the **chain's own id** on the envelope, not the first step's id.
When that first step's worker finishes and looks up "who am I, and who's my
parent", it resolves against the chain's id instead of its own — the engine
can never find the actual `step1` node to advance from.

**The practical fix:** Split "which delivery is this" from "which node does
it target" into two separate fields, and never let a builder conflate them:

```python
# mint/worker/envelope.py
class Envelope(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))   # this delivery attempt
    node_id: str                                             # the canvas node it targets
    canvas_id: str
    ...
```

```python
# mint/worker/canvas/builder.py
class Chain:
    async def publish_entries(self, canvas_id: str, publish: PublishFn) -> None:
        """Publish the single message that starts this chain: its first step."""
        await self.steps[0].publish_entries(canvas_id, publish)   # delegates to the step, not self
```

A `Chain`'s `publish_entries()` always delegates to its **first step's own**
`publish_entries()`, which stamps `node_id=step1.id` — never the chain's id.
Regression test: `test_engine.py::TestGroup::test_chain_leg_counts_only_after_the_whole_chain_finishes`.

---

### 2. A callback-less group never tells its own parent it finished

**Where:** `mini/mini/worker/workers/base.py:167-168`

**Symptom:** Nest a chord inside another chord's leg, and make the inner one
`callback=None` (fan-out with no aggregation step). The outer chord hangs
forever — it never sees the inner one as "done".

**Root cause:** The original code only walks up to a parent when a group *has*
a callback (since that's the code path that publishes something). A
callback-less group's completion has nowhere to "go" in that design — it just
stops. But a callback-less group can still be someone else's leg, and that
outer group is waiting to be told this leg finished.

**The practical fix:** Make "this node finished" always try to walk to its
parent, regardless of whether it had a callback. The callback is what
determines *what gets dispatched*, not *whether the parent gets notified*:

```python
# mint/worker/canvas/engine.py, in _advance_group
if group.callback is not None:
    entry = await self._entry_task(canvas_id, group, group.callback)
    return Dispatch(topic=entry.topic, ...), None   # dispatch the callback

# no callback: nothing to dispatch, but still bubble a real outcome up to the parent
any_error = any(not child.ok for child in children)
final_status = NodeStatus.ERROR if any_error and group.error_policy != ErrorPolicy.CONTINUE else NodeStatus.FINISHED
return None, NodeOutcome(node_id=group.id, status=final_status, result=None)
```

The caller (`_complete`'s walk loop) always processes the returned
`NodeOutcome` by recording it and continuing to the grandparent — a
callback-less group's completion is just as real an event as any other node's.
Regression test: `test_engine.py::TestGroup::test_chord_nested_in_a_chord_increments_the_outer_group`.

---

### 3. Fan-in double-counts a redelivered leg

**Where:** `mini/mini/worker/workers/base.py:152-155`

**Symptom:** Under normal at-least-once delivery, a broker occasionally
redelivers a message the consumer already handled (a network blip before the
ack landed, a consumer restart). When that happens to a chord leg's
completion, the callback fires **early** (with fewer legs actually done than
`num_children`) or **twice**.

**Root cause:**

```python
# original approach, paraphrased
count = await redis.incr(f"group:{group_id}:count")
if count == num_children:
    fire_callback()
```

`INCR` has no idea whether this particular child already counted itself. A
redelivered "leg 3 finished" message increments the counter a second time —
now the counter reaches `num_children` one message early (skipping a real
leg), or overshoots it entirely (the `== num_children` check never true
again, so the callback *never* fires).

**The practical fix:** Track *which* children are done, not *how many* — a
`SADD` on a real child id is naturally idempotent (adding the same member
twice is a no-op), and doing the add-and-check in one atomic round trip closes
the race between two legs finishing concurrently:

```lua
-- mint/worker/stores/redis.py, FAN_IN_SCRIPT
local added = redis.call('SADD', KEYS[1], ARGV[1])
local count = redis.call('SCARD', KEYS[1])
if added == 1 and count == tonumber(ARGV[2]) then
    if redis.call('SETNX', KEYS[2], '1') == 1 then
        return 1   -- this call is the one that fires the callback
    end
end
return 0
```

```python
# mint/worker/stores/redis.py
async def mark_child_done(self, canvas_id, group_id, child_id, num_children) -> GroupProgress:
    added, count, fired = await self.client.eval(
        FAN_IN_SCRIPT, 2, done_key, fired_key, child_id, num_children,
    )
    return GroupProgress(added=bool(added), done_count=count, fired=bool(fired))
```

`added == 1` alone already kills double-counting; the `SETNX` on a
`callback_fired` key is belt-and-braces against the store itself restarting
mid-script. `MemoryCanvasStore` mirrors the exact same contract with a plain
Python `set`, so the whole mocked test suite exercises the same idempotency
guarantee with no Redis at all. Regression tests:
`test_engine.py::TestGroup::test_duplicate_delivery_of_a_leg_is_not_double_counted`,
`test_duplicate_delivery_of_the_final_leg_fires_callback_only_once`,
`test_concurrent_completion_of_the_last_two_legs_fires_exactly_once`; plus a
container test hammering this with 50 concurrent legs against real Redis
(`test_redis_container.py`).

---

### 4. A failed leg silently stalls the whole chord

**Where:** `mini/mini/worker/base.py:88-94`; the consumer's own
`app.py:549-556` shows the actual damage.

**Symptom:** One leg of a chord raises an exception. The message still gets
acked (so it's not stuck retrying forever), but the fan-in counter is never
incremented for that leg — so the chord's callback never fires, even though
every *other* leg succeeded. The service this was ported from worked around
it by having `on_failure` reach into a private method:

```python
# the workaround this bug forced
async def on_failure(self, msg, exc):
    await self._check_next_step(msg)   # private method, never meant to be called from here
```

**Root cause:** Failure and fan-in progress were two unrelated code paths.
`on_failure` logs the error and acks the message; nothing tells the canvas
engine "this leg is done, it just didn't succeed." The counter genuinely never
moves for that child.

**The practical fix:** Make failure a first-class *outcome* the engine
processes exactly like success — same `_complete()` call, same fan-in
counting, just a different `status`:

```python
# mint/worker/canvas/models.py
class NodeOutcome(BaseModel):
    status: Literal[NodeStatus.FINISHED, NodeStatus.ERROR]
    result: str | None = None
    error: ErrorInfo | None = None
```

```python
# mint/worker/worker.py, _run_task — a raised exception becomes a real outcome,
# not a swallowed log line
except Exception as exc:
    await self._safe_on_failure(input_obj, exc)
    outcome = NodeOutcome(node_id=node_id, status=NodeStatus.ERROR, error=ErrorInfo(...))
    return outcome, None
# ...this outcome still goes through binding.engine.complete() exactly like a success would
```

An `ErrorPolicy` (`CONTINUE`/`PROPAGATE`/`ABORT`) then decides what a failure
*means* for the group, but the counting itself is unconditional — a failed leg
always counts:

```python
# mint/worker/canvas/engine.py, _advance_group — runs regardless of ok/error
progress = await self.store.mark_child_done(canvas_id, group.id, finished_child_id, group.num_children)
if not progress.fired:
    return None, None
# ...proceeds to build the FanIn and dispatch the callback, with ok=False children present
```

There is no private method to reach into anymore — `CONTINUE` (a group's
default) is exactly "count it, report it as `ok=False`, keep going." Delete
the workaround entirely when migrating. Regression tests:
`test_engine.py::TestGroup::test_continue_policy_fires_callback_with_failed_children_present`,
`test_all_children_erroring_still_fires_the_callback`.

---

### 5. A root task's result is never stored

**Where:** `mini/mini/worker/workers/base.py:105-106`

**Symptom:** A single standalone task with no parent at all (not part of any
chain/chord) finishes — and its result is nowhere. Anything that later tries
to read it (a status check, a debugging tool) finds nothing.

**Root cause:**

```python
# original approach, paraphrased
if message.parent_id is None:
    return   # "nothing to advance to" — but this also skips writing the result at all
```

The early return meant to skip the *parent-walking* logic accidentally also
skipped the *"record my own result"* logic, because both lived in the same
early-exit branch.

**The practical fix:** Record the just-finished node's own outcome
unconditionally, before ever checking whether it has a parent to walk to:

```python
# mint/worker/canvas/engine.py, _complete
node = await self._require_node(canvas_id, node_id)
await self._record(canvas_id, node_id, outcome)   # always happens first

visited = {node_id}
current_id, current_outcome, parent_id = node_id, outcome, node.parent_id
while parent_id is not None:
    ...   # only the walking is conditional on having a parent
```

Regression test: `test_engine.py::TestChain::test_bare_root_task_still_records_its_result`.

---

### 6. A publish failure mid-fan-out strands the group

**Where:** `mini/mini/worker/workers/canvas.py:185-196`

**Symptom:** A chord with 20 legs starts publishing. Leg 12's publish call
throws (broker hiccup). Legs 1-11 are dispatched and will run; legs 13-20 were
never even attempted and never will be — but the group's fan-in counter still
expects all 20 to report. The canvas hangs forever, silently short a few legs.

**Root cause:** A bare loop with no atomicity boundary:

```python
# original approach, paraphrased
for leg in legs:
    await broker.publish(leg.topic, leg.body)   # if this throws on leg 12, legs 13-20 are just... gone
```

Nothing marks the canvas as broken; nothing retries the remaining legs;
nothing tells the caller anything went wrong beyond the raised exception at
the call site (which the caller may or may not even be watching).

**The practical fix:** Two changes, together. First, persist the *entire*
graph before publishing *any* leg, so a publish failure never leaves a
half-known group:

```python
# mint/worker/canvas/builder.py, Chord.apply
async def apply(self, store, publish, *, canvas_id=None) -> str:
    canvas_id = canvas_id or str(uuid4())
    nodes: dict[str, AnyNode] = {}
    self.build(canvas_id, None, nodes)
    await store.create_canvas(canvas_id, nodes)   # every node written first
    try:
        await self.publish_entries(canvas_id, publish)   # then legs are published
    except Exception:
        await store.set_canvas_status(canvas_id, CanvasStatus.ERROR)   # and a failure is explicit
        raise
    return canvas_id
```

The graph existing in full means a retry of the whole `apply()` call (or a
manual re-publish of just the missing legs, since every node's id is stable
and known) is possible in a way it never was when the graph itself was only
half-built. The canvas is also explicitly marked `ERROR` instead of silently
hanging — a monitoring/alerting system has something to actually observe.

---

### 7. The Kafka consumer never actually starts

**Where:** `mini/mini/worker/brokers/kafka.py:69,91,133`

**Symptom:** A Kafka-backed worker deploys, looks healthy, and never
processes a single message. No error in the logs.

**Root cause:** Three separate bugs stacked in one file:

```python
# 1. `started` is checked with `is None`, but it's a bool that's never actually None
if self.consumer.started is None:   # always False — .started is a bool property, e.g. False, not None
    await self.consumer.start()      # this line never runs

# 2. create_topics() is called without awaiting the coroutine
self.admin_client.create_topics([new_topic])   # returns a coroutine that's immediately discarded

# 3. group_id passed to the producer, which doesn't accept it
AIOKafkaProducer(bootstrap_servers=..., group_id=self.group_id)   # TypeError at construction
```

**The practical fix:** Replace every "is it started" *property check* with an
explicit `if self._x is None:` guard around lazy construction — no guessing
based on a value that was never designed to answer that question:

```python
# mint/worker/brokers/kafka.py
async def _ensure_producer(self) -> AIOKafkaProducer:
    if self._producer is None:
        producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)  # no group_id here
        await producer.start()
        self._producer = producer
    return self._producer

async def _ensure_topic(self, topic: str) -> None:
    admin = await self._ensure_admin()
    new_topic = NewTopic(name=topic, num_partitions=..., replication_factor=...)
    with suppress(TopicAlreadyExistsError):
        await admin.create_topics([new_topic])   # awaited
```

`group_id` only ever goes to the *consumer* (`AIOKafkaConsumer(..., group_id=self.group_id)`),
never the producer. Regression tests:
`test_kafka_mocked.py::TestConsume::test_consume_starts_and_stops_the_consumer`,
`TestAdminStartupAndTopicCreation::test_ensure_topic_awaits_create_topics`,
`TestProducerStartup::test_publish_never_passes_group_id_to_the_producer`.

---

### 8. The Redis broker silently drops messages on crash

**Where:** `mini/mini/worker/brokers/redis.py:49-65`

**Symptom:** A worker pulls a message via the Redis broker, starts processing
it, and crashes (OOM-killed, deployment restart, whatever) before finishing.
The message is gone — nobody else ever gets it.

**Root cause:** `BRPOP` pops a message off the list the instant it's read;
there is no separate "acknowledge" step. The moment a consumer reads a
message, it's gone from Redis whether or not the consumer ever finishes with
it — this is at-most-once delivery by construction, not a configuration
choice.

**The practical fix:** Redis Streams + consumer groups give you exactly the
ack/nack semantics `mint.worker`'s `Delivery` Protocol needs — a message stays
"pending" for a consumer until explicitly acked, and can be reclaimed if never
acked:

```python
# mint/worker/brokers/redis.py
async def consume(self, topic: str) -> AsyncIterator[RedisStreamDelivery]:
    await self._ensure_group(topic)
    while True:
        response = await self.client.xreadgroup(
            groupname=self.group, consumername=self.consumer_name,
            streams={topic: ">"}, count=1, block=self.block_ms,
        )
        ...
        yield RedisStreamDelivery(...)

class RedisStreamDelivery:
    async def ack(self) -> None:
        await self._broker.client.xack(self._entry.topic, self._broker.group, self._entry.message_id)

    async def nack(self, *, requeue: bool) -> None:
        if requeue:
            await self._broker.client.xadd(self._entry.topic, {...attempt+1...})
        else:
            await self._broker.client.xadd(f"{self._entry.topic}.dlq", {...})
        await self._broker.client.xack(...)   # remove from the pending list either way
```

A message is only removed from the stream's pending-entries list by an
explicit `XACK` — a crashed consumer just leaves it there for reclaim, instead
of it having vanished the instant it was read. Confirmed genuinely
at-least-once by a real-Redis test that kills the delivery without acking and
checks it's still recoverable.

---

### 9. A rejected RabbitMQ message vanishes instead of dead-lettering

**Where:** `mini/mini/worker/brokers/rabbitmq.py:118`

**Symptom:** A message fails permanently (bad data, a business rule that says
"never retry this") and gets rejected with `requeue=False` — expecting it to
land somewhere inspectable. Instead it's just gone. No dead-letter queue has
anything in it, because none was ever declared.

**Root cause:**

```python
# original approach, paraphrased
await message.reject()   # requeue defaults to False, and no DLX exists to catch it
```

RabbitMQ's behavior here is exactly correct and exactly the problem: a
rejected, non-requeued message is dropped **unless** the queue it came from
has `x-dead-letter-exchange` configured. Nothing in the original setup ever
declared one.

**The practical fix:** Declare a per-topic dead-letter exchange and queue
alongside the topic's own queue, and point the topic queue at it:

```python
# mint/worker/brokers/rabbitmq.py, _declare_topic
dlx_name = f"{topic}{self.DLX_SUFFIX}"
dlq_name = f"{topic}{self.DLQ_SUFFIX}"
dlx = await channel.declare_exchange(dlx_name, durable=True)
dlq = await channel.declare_queue(dlq_name, durable=True)
await dlq.bind(dlx, dlq_name)

exchange = await channel.declare_exchange(topic, durable=True)
queue = await channel.declare_queue(
    topic, durable=True,
    arguments={"x-dead-letter-exchange": dlx_name, "x-dead-letter-routing-key": dlq_name},
)
await queue.bind(exchange, topic)
```

`nack(requeue=False)` now reliably lands the message on `{topic}.dlq`,
verified against a **real broker** (not just a mock — see [issue
21](#21-rabbitmq-rejects-redeclaring-its-own-dead-letter-queue) for a second,
related bug this same code path surfaced live). Regression test:
`test_rabbitmq_container.py::TestDeadLetterRegression::test_nack_requeue_false_lands_in_the_dead_letter_queue`.

---

### 10. Two NATS topics fight over the same delivery cursor

**Where:** `mini/mini/worker/brokers/nats.py:7,83-87`

**Symptom:** Two different workers, consuming two different topics, start
missing or double-receiving messages depending on which one subscribed first.

**Root cause:** JetStream scopes a *durable* consumer's delivery cursor to the
pair `(stream, durable name)` — reuse the same durable name across two
different topics and they share one cursor between them, even though they're
logically unrelated consumers:

```python
# original approach, paraphrased
DEFAULT_CONSUMER_NAME = "mint-worker"   # one constant, used for every topic
await jetstream.pull_subscribe(subject=topic, durable=DEFAULT_CONSUMER_NAME)
```

**The practical fix:** Derive the durable name from the topic itself, so every
topic gets its own cursor:

```python
# mint/worker/brokers/nats.py
def _stream_name(self, topic: str) -> str:
    return topic.replace(".", "-")

def _durable_name(self, topic: str) -> str:
    return f"{self.group}-{self._stream_name(topic)}"

async def consume(self, topic: str) -> AsyncIterator[NatsDelivery]:
    ...
    subscription = await jetstream.pull_subscribe(subject=topic, durable=self._durable_name(topic))
```

Regression test: `test_nats_mocked.py::TestDurableNaming::test_durable_name_differs_between_topics`.

---

### 11. The AMQP-RPC executor crashes on GC and leaks futures forever

**Where:** `mini/mini/worker/executors/aiopika.py:106-107,90`

**Symptom 1 — the crash:** Sporadic, unexplained `RuntimeError: asyncio.run()
cannot be called from a running event loop`, with no obvious call site in the
traceback pointing at application code.

**Root cause 1:**

```python
# original approach, paraphrased
def __del__(self) -> None:
    asyncio.run(self.shutdown())
```

`__del__` fires whenever the garbage collector reclaims the object — which,
for an object referenced from inside a running application, is almost always
*while the event loop is running*. `asyncio.run()` refuses to run inside an
already-running loop, so this raises exactly when it fires — the crash isn't
rare because the bug is rare, it's rare because this executor doesn't get
garbage-collected very often.

**Symptom 2 — the leak:** A reply that never arrives (the remote service is
down, or the reply message gets lost) leaves that call's `asyncio.Future`
sitting in a dict forever, keyed by `correlation_id`. Over enough lost
replies, this is a slow, silent memory leak with no way to observe it short of
inspecting the process's heap.

**Root cause 2:** There was no timeout on a reply at all — the code just
`await`ed the future indefinitely, and the correlation-id map entry it created
was only ever removed by a reply actually arriving.

**The practical fix, both parts:**

```python
# mint/worker/executors/amqp_rpc.py — no __del__ at all; aclose() is explicit
async def aclose(self) -> None:
    """Close both pools, if they were ever built. Never called from __del__."""
    if self._channel_pool is not None:
        await self._channel_pool.close()
    if self._connection_pool is not None:
        await self._connection_pool.close()
```

```python
# every call has an explicit timeout that both cancels the future and removes the map entry
async def _await_reply(self, correlation_id: str, future: asyncio.Future[RT]) -> RT:
    try:
        async with asyncio.timeout(self.timeout):
            return await future
    except TimeoutError as exc:
        self._pending.pop(correlation_id, None)   # the map entry is gone either way
        future.cancel()
        raise RemoteCallTimeoutError(queue=self.queue, timeout=self.timeout) from exc
```

The caller (`WorkerApp`/`Worker`) is responsible for calling `aclose()`
explicitly during its own shutdown sequence — `mint.worker` never does cleanup
from `__del__` anywhere in the codebase, for exactly this reason. Regression
tests: `test_amqp_rpc.py::TestTimeout::test_a_timed_out_call_raises_and_cleans_up_its_pending_entry`,
`TestUnknownCorrelationId::test_an_unknown_correlation_id_does_not_resolve_or_pop_any_pending_future`.

---

### 12. The gRPC executor rejects the one case that should work

**Where:** `mini/mini/worker/executors/grpc.py:36-39`

**Symptom:** Configure `GRPCExecutor` against a normal, working gRPC stub
method, and every call raises `TypeError: Method X is not a coroutine
function` — for a method that genuinely *is* async and works fine when called
directly.

**Root cause:** The check was inverted:

```python
# original approach, paraphrased
if iscoroutinefunction(func):
    raise TypeError(f"Method {func_name} is not a coroutine function")   # backwards
return await func(input_)
```

This raises precisely when `func` **is** a coroutine function — the one case
that should just work. And it says nothing useful, either: the message claims
the method "is not" a coroutine function while the condition it's guarding
against is literally the opposite.

**The practical fix:** Delete the check entirely rather than flip its
polarity. It was never load-bearing: `await func(input_)` works uniformly
whether `func` is a genuine `async def` (returns a coroutine, which `await`
handles) or one of grpc.aio's actual generated stub methods (a plain callable
whose *return value* is an awaitable `Call` object, not itself a coroutine
function — `await` handles that too, via `__await__`):

```python
# mint/worker/executors/grpc.py
async def execute(self, fn: object, input_: T) -> RT:
    del fn
    async with insecure_channel(self.uri) as channel:
        stub = self._stub_cls(channel)
        try:
            method = getattr(stub, self._method_name)
        except AttributeError as exc:
            raise RemoteMethodNotFoundError(stub=self._stub_cls.__name__, method=self._method_name) from exc
        return await method(input_)
```

The thing actually worth checking — does this method exist on the stub at all
— gets its own explicit, correctly-typed error instead. Regression tests:
`test_grpc.py::TestExecute::test_a_coroutine_function_stub_method_is_accepted`,
`test_a_sync_multicallable_shaped_stub_method_is_accepted`.

---

### 13. Node status is invisible; canvases never expire; keys collide

**Where:** `enums.py:4`; `result_backends/redis.py`

**Symptom (three separate, related gaps):**

1. `NodeStatus` exists as an enum but is never actually written anywhere — a
   status-check tool has nothing to read.
2. Nothing ever expires — every canvas that ever ran leaves its keys in Redis
   forever, an unbounded, silent memory leak on the Redis side.
3. Keys aren't namespaced by canvas — two services (or two canvases with a
   colliding node id, which is entirely possible with any non-UUID id scheme)
   can stomp on each other's data.

**The practical fix, all three together:**

```python
# mint/worker/canvas/engine.py — every transition writes status explicitly
async def _record(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
    ...
    await self.store.set_result(canvas_id, node_id, outcome)
    await self.store.set_node_status(canvas_id, node_id, outcome.status)   # always written
```

```python
# mint/worker/stores/redis.py — every key namespaced under {namespace}:canvas:{cid}:...
def _node_key(self, canvas_id: str, node_id: str) -> str:
    return f"{self.namespace}:canvas:{canvas_id}:node:{node_id}"

async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
    ...
    if status in TERMINAL_STATUSES:
        for key in self._tracked_keys(canvas_id):   # every key touched by this canvas
            await self.client.expire(key, self.terminal_ttl_seconds)
```

A small internal key-registry (`_track()`, called every time a key is
written) is what lets `set_canvas_status()` expire *every* key that canvas
ever touched, in one pass, once it reaches a terminal status — without having
to enumerate the graph again from scratch.

---

### 14. The Redis result backend doesn't actually satisfy its own interface

**Where:** `result_backends/redis.py:9`

**Symptom:** Every place that wires `RedisBackend` into a DI container needs a
`# type: ignore[arg-type]` comment, because the type checker (correctly)
doesn't believe `RedisBackend` satisfies `IResultBackend`.

**Root cause:** `RedisBackend`'s methods don't structurally match
`IResultBackend`'s Protocol (different parameter names, a slightly different
return shape somewhere) — nobody had gone back and reconciled them after the
Protocol was defined, so every consumer just suppressed the error at each call
site instead of fixing the mismatch once.

**The practical fix:** `ICanvasStore`/`IBroker`/`ITaskExecutor` in
`mint.worker` are all `Protocol`s that every real implementation is written
*against* from the start — `ty check` verifies structural conformance for
every implementation as part of the normal test/lint pass, so a drift like
this gets caught immediately instead of accumulating suppressions:

```python
# mint/worker/stores/interface.py
class ICanvasStore(Protocol):
    async def get_node(self, canvas_id: str, node_id: str) -> AnyNode | None: ...
    async def set_node_status(self, canvas_id: str, node_id: str, status: NodeStatus) -> None: ...
    ...

# mint/worker/stores/redis.py — matches every method signature exactly, checked by ty
class RedisCanvasStore:
    async def get_node(self, canvas_id: str, node_id: str) -> AnyNode | None: ...
```

There is no `# type: ignore` anywhere in `mint.worker`'s wiring — if a new
implementation drifts from the Protocol, `ty check` fails the build, not a
silent runtime `AttributeError` months later.

---

## Part 2 — New bugs found live while building `mint.worker` itself

These weren't in `mini.worker` — they were introduced (or exposed) while
building the replacement, and caught specifically *because* this was built
test-first with real containers in the loop, not just mocks.

### 15. Nested groups double their payload size at every level (the OOM bug)

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** Writing the "deep nesting resolves without recursion" regression
test, `pytest` was OOM-killed **twice** on the development machine.

**Root cause, found by bounded simulation, not guessing:** A `callback=None`
group's no-callback branch carried its *entire* fan-in payload forward as its
own `.result`:

```python
# the bug, paraphrased
fan_in = FanIn(children=children, input=group.input)
return None, NodeOutcome(node_id=group.id, status=final_status, result=fan_in.model_dump_json())
```

When that group's result becomes one more child's `.value` inside an
**outer** group's own fan-in, the JSON encoder has to escape every `"` and
`\` the inner blob already contains. Escaping an already-escaped string
roughly **doubles** its character count. Nest groups N levels deep and the
payload is O(2^N):

| level | `len(result)` | growth |
|---|---|---|
| 0 | 90 | — |
| 9 | 17,090 | 1.94x |
| 15 | 1,049,720 | 2.00x |
| 21 | 67,110,446 | 2.00x |

At the test's target depth this would never have finished — just allocated
ever-larger strings until the kernel killed the process, exactly as observed.

**The practical fix, two parts:**

```python
# mint/worker/canvas/engine.py — the no-callback branch's bubbled result is None,
# never the encoded FanIn. Nothing reads it in this path anyway — there is no
# callback to consume it.
return None, NodeOutcome(node_id=group.id, status=final_status, result=None)
```

```python
# defense in depth: a hard cap on any stored result, regardless of cause
async def _record(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
    if outcome.result is not None:
        size = len(outcome.result.encode())
        if size > self.max_result_bytes:   # default 256KB
            raise ResultTooLargeError(node_id=node_id, size=size, limit=self.max_result_bytes)
    await self.store.set_result(canvas_id, node_id, outcome)
    ...
```

A caller that genuinely needs a nested leg's own children's outcomes can query
the store directly (`get_results(canvas_id, group.children)`), which already
exists — it just shouldn't be smuggled through every level of nesting as a
string field. **Process fix, going forward:** any test in this suite that
varies nesting *depth* runs once, standalone, under a memory cap
(`systemd-run --user --scope -p MemoryMax=1G --`) before it's ever allowed to
join the full suite — a standing rule now documented in the README's
Development section, not a one-off. Regression tests:
`test_engine.py::TestResultSizeGuard::*` (three tests: the no-callback bubble
carries `result=None`, nested groups stay O(1) per level under a memory cap,
and an oversized result raises immediately).

---

### 16. Kafka silently skips a topic's backlog for a new consumer group

**Where:** `mint/worker/brokers/kafka.py::consume`

**Symptom:** Found live, writing the Kafka container test: publish a message,
then immediately try to consume it with a brand-new consumer group — the
consume call times out. The message is sitting right there in the topic.

**Root cause:** `AIOKafkaConsumer` defaults `auto_offset_reset` to
`"latest"`. A consumer group that has never attached to a topic before has no
committed offset — with `"latest"`, its starting position becomes "whatever
is at the end of the log right now," which is *after* the message that was
already published. This isn't just a test artifact: the identical situation
happens for real whenever a worker restarts with a fresh `group_id`, or any
consumer attaches to a topic that already has a backlog — an at-least-once
violation, silently skipping real work.

**The practical fix:**

```python
# mint/worker/brokers/kafka.py
consumer = AIOKafkaConsumer(
    topic,
    bootstrap_servers=self.bootstrap_servers,
    group_id=self.group_id,
    enable_auto_commit=False,
    auto_offset_reset="earliest",   # a new group must see the backlog, never skip past it
)
```

With manual commit already off (this broker always commits explicitly, on
ack/nack), `"earliest"` only ever affects a group's *first-ever* attach to a
topic — from then on its committed offset governs where it resumes, same as
any other broker here. Regression tests:
`test_kafka_mocked.py::TestConsume::test_consumer_resets_to_earliest_not_latest`
(asserts the kwarg is actually passed) and
`test_kafka_container.py::TestOffsetResetRegression::test_a_message_published_before_the_first_ever_consume_is_still_delivered`
(the real scenario, against a real broker).

---

### 17. Kafka's dead-letter publish crashes on a real broker

**Where:** `mint/worker/brokers/kafka.py::deadletter`

**Symptom:** Also found live, in the same container test session: calling
`nack(requeue=False)` against a real Kafka broker raised
`TypeError: Expected list, got tuple` — deep inside aiokafka's own Cython
record-batch builder, nowhere near application code in the traceback.

**Root cause:**

```python
# the bug
async def deadletter(self, record: "ConsumerRecord") -> None:
    producer = await self._ensure_producer()
    await producer.send_and_wait(
        f"{record.topic}{self.DLQ_SUFFIX}", value=record.value, headers=record.headers,
    )
```

A real `ConsumerRecord.headers` comes back from aiokafka as a `tuple` of
pairs. aiokafka's producer requires a `list` — its internal record-batch
builder indexes into `headers` in a way that only works on a list. A mocked
producer (used everywhere in the fast test lane) never enforces this type
distinction at all — `MagicMock`/`AsyncMock` happily accept a tuple, a list,
anything — so this bug was **only ever reachable against the real broker**,
which is exactly why it stayed hidden through the entire mocked test suite
passing 100%.

**The practical fix:**

```python
# mint/worker/brokers/kafka.py
async def deadletter(self, record: "ConsumerRecord") -> None:
    producer = await self._ensure_producer()
    await producer.send_and_wait(
        f"{record.topic}{self.DLQ_SUFFIX}",
        value=record.value,
        headers=list(record.headers or ()),   # copy into a list before it crosses the boundary
    )
```

The general lesson this (and #16) reinforces: mocking catches *our own*
logic bugs (wrong method call, missing `await`, hardcoded name) reliably, but
a handful of real-protocol behaviors — exact wire-level type requirements, a
client's default offset policy, a queue redeclaration conflict (see #21) —
are only observable against the real thing. This is why every broker in
`mint.worker` gets both a `test_<name>_mocked.py` (fast, every run, catches
our bugs) *and* a small `test_<name>_container.py` (slower, run standalone,
catches theirs). Regression test:
`test_kafka_mocked.py::TestAckNack::test_nack_requeue_false_converts_the_records_header_tuple_to_a_list`
(asserts the type explicitly, since equality alone wouldn't catch a silent
tuple-passthrough) plus the real-broker round-trip in
`test_kafka_container.py::TestDeadLetterRegression`.

---

### 18. `Worker.run()` spins the CPU to 100% on shutdown

**Where:** `mint/worker/worker.py::Worker.run`

**Symptom:** Found while testing graceful shutdown against `MemoryBroker`: the
process pegged one CPU core at 99.8% and stopped responding to `SIGTERM` —
confirmed via `ps aux`, and non-responsive to the signal specifically because
this same code installs its own `SIGTERM` handler, which never got a chance
to run while the loop was spinning.

**Root cause:**

```python
# the bug
async for delivery in binding.broker.consume(self.topic):
    if self._stopped.is_set():
        await delivery.nack(requeue=True)
        continue   # <- goes back to `async for`, immediately re-polls
```

`MemoryBroker`'s `nack(requeue=True)` redelivers **synchronously**, onto the
same in-process queue the same consumer is still polling. `continue` sends
control straight back to the top of the `async for` loop, which immediately
receives that exact same message right back, sees `_stopped` still set, nacks
it again — a self-sustaining loop with no `await` boundary the event loop
could ever use to run anything else, including the signal handler.

**The practical fix:** Stop the loop outright instead of looping back for
more once a stop has been requested:

```python
# mint/worker/worker.py
async for delivery in binding.broker.consume(self.topic):
    if self._stopped.is_set():
        await delivery.nack(requeue=True)
        return   # exits the consume loop entirely — does NOT loop back for more
    ...
```

The comment in the code spells out why this matters specifically for
`MemoryBroker`'s synchronous-redelivery semantics — a real broker's redelivery
usually has some latency, which would have hidden this as an occasional
extra-nack rather than an infinite spin, making it much harder to catch.
Regression test: `test_worker.py::TestRunStoppedBranch::test_run_nacks_and_returns_without_dispatching_when_already_stopped`.

---

### 19. Graceful shutdown can double-nack a message

**Where:** `mint/worker/app.py::WorkerApp._shutdown`

**Symptom:** A drain-timeout test expected a redelivered message's `attempt`
to be `2`; it came back `3` instead — one extra nack happened somewhere during
shutdown.

**Root cause:** The original shutdown order drained in-flight work *before*
cancelling the run loop tasks:

```python
# the bug, paraphrased
await asyncio.gather(*(w.drain() for w in self._workers.values()))   # drain first
for task in self._tasks.values():
    task.cancel()   # cancel the run loops second
```

While `drain()` was busy timing out a stuck handler and nacking it for
redelivery, the *run loop itself was still alive* — on `MemoryBroker`, that
redelivery lands right back on the same queue the still-running consume loop
is still polling, gets picked up again immediately, and (depending on timing)
gets nacked a second time before shutdown finally gets around to cancelling
the loop.

**The practical fix:** Cancel every run-loop task **first** — so nothing is
left alive to race a drain-timeout's own requeue — then drain whatever
handlers were already in flight:

```python
# mint/worker/app.py
async def _shutdown(self) -> None:
    for worker in self._workers.values():
        worker.stop_consuming()
    for task in self._tasks.values():
        task.cancel()
    await asyncio.gather(*self._tasks.values(), return_exceptions=True)   # run loops fully stopped
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(self.drain_timeout):
            await asyncio.gather(*(w.drain() for w in self._workers.values()))   # then drain
    await self.broker.close()
    await self.store.close()
```

Regression test: `test_app.py::TestRunStopLifecycle::test_in_flight_work_exceeding_the_drain_timeout_is_nacked_not_dropped`
asserts `attempt == 2` exactly.

---

### 20. Constructing a broker outside an event loop crashes

**Where:** `mint/worker/brokers/rabbitmq.py::RabbitMQBroker.__init__` (same
pattern in `executors/amqp_rpc.py::AMQPRPCExecutor.__init__`)

**Symptom:** `RabbitMQBroker("amqp://...")` raises `RuntimeError: no current
event loop` when constructed in an ordinary synchronous context — exactly
what a typical DI/container setup does (`container = Container(broker=RabbitMQBroker(url), ...)`,
called before `asyncio.run()` is ever invoked).

**Root cause:** `aio_pika.pool.Pool.__init__` calls
`asyncio.get_event_loop()` internally, synchronously, at construction time.
Building the connection/channel pools **eagerly**, inside `RabbitMQBroker.__init__`,
means the broker itself inherits that requirement — even though nothing about
constructing a broker object should need a running loop.

**The practical fix:** Defer pool construction until the pools are actually
needed — the first real `publish()`/`consume()` call, which by definition
only ever happens inside a running loop:

```python
# mint/worker/brokers/rabbitmq.py
def __init__(self, uri: str, *, qos=..., connection_pool_size=..., channel_pool_size=...) -> None:
    self.uri = uri
    self.qos = qos
    self._connection_pool_size = connection_pool_size
    self._channel_pool_size = channel_pool_size
    self._connection_pool: ConnectionPool | None = None   # not built yet
    self._channel_pool: ChannelPool | None = None          # not built yet

def _ensure_connection_pool(self) -> ConnectionPool:
    if self._connection_pool is None:
        self._connection_pool = Pool(self._get_connection, max_size=self._connection_pool_size)
    return self._connection_pool
```

Every method that needs a pool calls `_ensure_connection_pool()`/
`_ensure_channel_pool()` instead of touching `self._connection_pool` directly.
This also required a structural (Protocol-based) type for the pool, rather
than the concrete `aio_pika.pool.Pool` class, so lightweight test doubles
could satisfy it without needing a real event loop either:

```python
class _AcquirablePool[T](Protocol):
    def acquire(self) -> AbstractAsyncContextManager[T]: ...
    async def close(self) -> None: ...
```

Regression test: `test_rabbitmq_mocked.py::TestGuaranteeAndClose::test_construction_outside_a_running_event_loop_does_not_raise`
— a deliberately non-`async def` test function, matching exactly how a real
synchronous DI container would construct one.

---

### 21. RabbitMQ rejects redeclaring its own dead-letter queue

**Where:** `mint/worker/brokers/rabbitmq.py::_declare_topic`

**Symptom:** Found live, against a real RabbitMQ container: consuming from a
topic's own `.dlq` (e.g. to inspect what landed there, matching issue #9's
fix) raised `PRECONDITION_FAILED - inequivalent arg 'x-dead-letter-exchange'`.

**Root cause:** `_declare_topic()` treated *every* topic name identically —
including a `.dlq` topic itself. That means consuming from `{topic}.dlq` tried
to declare **it** with its own recursive dead-letter exchange pointing at yet
another `.dlq.dlq` — but that queue already exists, declared the *first* time
(by issue #9's fix) as a plain queue with no DLX arguments at all. RabbitMQ
refuses to redeclare an existing queue with different arguments than it was
first created with — exactly what asking a `.dlq` queue to have its own DLX
does.

**The practical fix:** Special-case a topic ending in the DLQ suffix to
declare it identically to how it was first created — plain, no DLX of its
own:

```python
# mint/worker/brokers/rabbitmq.py
async def _declare_topic(self, channel, topic) -> tuple[AbstractExchange, AbstractQueue]:
    if topic.endswith(self.DLQ_SUFFIX):
        exchange = await channel.declare_exchange(topic, durable=True)
        queue = await channel.declare_queue(topic, durable=True)   # plain — matches first declaration
        await queue.bind(exchange, topic)
        return exchange, queue

    # a real topic still gets its own DLX, as in issue #9
    dlx_name = f"{topic}{self.DLX_SUFFIX}"
    ...
```

A DLQ terminates the dead-letter chain rather than extending it — this bug
would never have been caught by a mocked test, since a mock happily accepts
redeclaring anything with any arguments; only a real broker enforces
argument-equivalence on redeclaration. Regression test:
`test_rabbitmq_mocked.py` has a wiring-assertion regression test for this
(`test_consuming_from_a_dlq_topic_declares_it_without_its_own_dlx`); the
container test that originally caught it is
`test_rabbitmq_container.py::TestDeadLetterRegression::test_nack_requeue_false_lands_in_the_dead_letter_queue`,
which consumes from the DLQ as part of asserting the message actually landed
there.

---

### 22. A test helper's hidden hang (process hygiene, not a `mint.worker` bug)

**Where:** `tests/worker/test_worker.py::envelope_delivery` (a test fixture
helper, not production code)

**Symptom:** A specific test hung indefinitely — not a `mint.worker` bug, but
worth documenting because it's exactly the kind of test-authoring mistake
that produces a false sense of security ("the test passed, so the code must
be safe") right up until it silently never finishes.

**Root cause:**

```python
# the bug, in the test helper itself
def envelope_delivery(node_id: str, canvas_id: str, body: str) -> MemoryDelivery:
    envelope = Envelope(node_id=node_id, canvas_id=canvas_id, body=body)
    return MemoryDelivery(MemoryBroker(), TOPIC, envelope.to_bytes(), attempt=1)   # a THROWAWAY broker
```

The delivery was built against a **brand-new, disposable** `MemoryBroker()`
instead of the test's actual broker under test. When the worker under test
nacked the delivery, the dead-letter/redelivery went to that throwaway broker
— which nothing was ever listening to. The test's own `await anext(...)` on
the *real* broker then waited forever for a message that was never going to
arrive there.

**The practical fix:** Require the broker as an explicit parameter, so the
helper can never silently construct its own:

```python
# tests/worker/test_worker.py
def envelope_delivery(broker: MemoryBroker, node_id: str, canvas_id: str, body: str) -> MemoryDelivery:
    envelope = Envelope(node_id=node_id, canvas_id=canvas_id, body=body)
    return MemoryDelivery(broker, TOPIC, envelope.to_bytes(), attempt=1)   # the caller's own broker
```

Every one of the nine call sites was updated to thread the correct broker
through explicitly. **The general practice this established for the rest of
the session:** trace async control flow for hang/infinite-loop risk by hand
before ever running a new or modified test file, and run any new/risky test
file under a hard `timeout -k <grace> <limit>` wrapper as the first pass,
before trusting it enough to fold into the regular suite.

---

## Practical checklist for anyone extending `mint.worker`

Distilled from every bug above — the recurring patterns to watch for when
adding a new broker, executor, or engine transition:

- **Fan-in/counting logic must be redelivery-safe.** Ask "what happens if
  this exact message arrives twice?" before merging — `SADD`-then-check, not
  `INCR`-then-compare (issue #3).
- **A node's completion must always try to notify its parent**, regardless of
  whether that completion also does something else (dispatches a callback,
  etc.) — don't gate "tell the parent" behind an unrelated conditional
  (issue #2).
- **Never carry an already-serialized string forward as a value that might
  itself get serialized again one level up** — that's an O(2^depth) trap
  every time (issue #15). If a nested aggregate's own payload isn't consumed
  by anything, don't produce it at all.
- **No cleanup in `__del__`, ever.** Every resource-holding class gets an
  explicit `aclose()`; the caller (usually `WorkerApp`/`Coordinator`) is
  responsible for calling it during its own shutdown (issues #11, #19).
- **Never construct a connection pool eagerly in `__init__`** if the
  underlying library's pool needs a running event loop — build it lazily, on
  first real use (issue #20).
- **A `continue` inside a consume loop that might receive the exact same
  message right back (synchronous redelivery) needs to become a `return`**
  once a stop condition is detected — check whether looping back can starve
  the event loop of any `await` boundary at all (issue #18).
- **Mock what you can, but budget one small real-broker test per broker for
  what mocking structurally cannot prove**: exact wire-level type
  requirements, a client's default policy choices (offset reset, ack mode),
  and redeclaration/argument-equivalence conflicts (issues #17, #21). A 100%
  passing mocked suite is not proof of correctness against the real thing.
- **Any test that varies nesting depth or payload size** — the two shapes
  that can blow up memory silently — runs once, standalone, under a memory
  cap before it's trusted to join the full suite (issue #15's process fix).
