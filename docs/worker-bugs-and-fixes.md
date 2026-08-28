# `mint.worker`: Every Issue Found, and How It Was Fixed

This is a practical, detailed walkthrough of every bug found while porting
`mini.worker` to `mint.worker` — the bugs found by design review before writing
any code, the ones found live while building the replacement test-first, and the
ones a second review round found in `mint.worker` itself once it was complete.
Each entry gives: where the bug lived, a concrete scenario where it bites, why it
happens, and exactly what the fix looks like. For the condensed reference table
and the migration mapping, see
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

**Part 3 — Bugs found in `mint.worker` itself by a second review round**

- [23. A chord's callback is lost for good if its dispatch fails to publish](#23-a-chords-callback-is-lost-for-good-if-its-dispatch-fails-to-publish)
- [24. The fan-in script never reloads after a Redis script-cache flush](#24-the-fan-in-script-never-reloads-after-a-redis-script-cache-flush)
- [25. One Kafka consumer slot, shared by every worker, commits the wrong offsets](#25-one-kafka-consumer-slot-shared-by-every-worker-commits-the-wrong-offsets)
- [26. A Kafka ack commits past records still being processed](#26-a-kafka-ack-commits-past-records-still-being-processed)
- [27. A message the worker's `Input` can't parse stalls its canvas forever](#27-a-message-the-workers-input-cant-parse-stalls-its-canvas-forever)
- [28. A backlog spawns one handler task per message, without limit](#28-a-backlog-spawns-one-handler-task-per-message-without-limit)
- [29. The coordinator tracks in-flight nodes by node id alone](#29-the-coordinator-tracks-in-flight-nodes-by-node-id-alone)
- [30. The coordinator's shutdown leaks its store's connections](#30-the-coordinators-shutdown-leaks-its-stores-connections)
- [31. A `stop()` that arrives before the run loop starts is discarded](#31-a-stop-that-arrives-before-the-run-loop-starts-is-discarded)
- [32. Every AMQP RPC call leaks a reply queue and a consumer](#32-every-amqp-rpc-call-leaks-a-reply-queue-and-a-consumer)
- [33. A malformed RPC reply is reported as a timeout](#33-a-malformed-rpc-reply-is-reported-as-a-timeout)
- [34. The Redis broker can lose a message while requeueing it](#34-the-redis-broker-can-lose-a-message-while-requeueing-it)
- [35. RabbitMQ's attempt counter never increments](#35-rabbitmqs-attempt-counter-never-increments)
- [36. Aborting a group overwrites the legs that already succeeded](#36-aborting-a-group-overwrites-the-legs-that-already-succeeded)
- [37. A chord's own input never reaches its callback](#37-a-chords-own-input-never-reaches-its-callback)
- [38. Flattening a nested chain silently discards its error policy](#38-flattening-a-nested-chain-silently-discards-its-error-policy)
- [39. Two different NATS topics can still share one stream and cursor](#39-two-different-nats-topics-can-still-share-one-stream-and-cursor)
- [40. Consuming a NATS dead-letter subject is rejected outright](#40-consuming-a-nats-dead-letter-subject-is-rejected-outright)

**Part 4 — Bugs found by a third review round, after Part 3's fixes landed**

- [41. An unexpected exception strands its delivery, unacked and unlogged](#41-an-unexpected-exception-strands-its-delivery-unacked-and-unlogged)
- [42. Shutdown can nack a delivery that was already acked](#42-shutdown-can-nack-a-delivery-that-was-already-acked)
- [43. Nothing reads `Delivery.attempt`, so failures retry forever](#43-nothing-reads-deliveryattempt-so-failures-retry-forever)
- [44. A dead run loop is never noticed](#44-a-dead-run-loop-is-never-noticed)
- [45. One bad result kills the coordinator's only result loop](#45-one-bad-result-kills-the-coordinators-only-result-loop)
- [46. The drain timeout tears the broker down mid-nack](#46-the-drain-timeout-tears-the-broker-down-mid-nack)
- [47. A result arriving mid-sweep completes its node twice](#47-a-result-arriving-mid-sweep-completes-its-node-twice)
- [48. A nested container can steal its parent's id and corrupt the graph](#48-a-nested-container-can-steal-its-parents-id-and-corrupt-the-graph)
- [49. Twenty RabbitMQ consumers deadlock every publish](#49-twenty-rabbitmq-consumers-deadlock-every-publish)
- [50. The fix for #39 was itself not injective](#50-the-fix-for-39-was-itself-not-injective)
- [51. A cancelled RPC call leaks its pending entry](#51-a-cancelled-rpc-call-leaks-its-pending-entry)
- [52. Trace ids die at the first hop](#52-trace-ids-die-at-the-first-hop)
- [53. `MemoryBroker.close()` wakes only one consumer per topic](#53-memorybrokerclose-wakes-only-one-consumer-per-topic)
- [54. The package never passed the project's own `make check`](#54-the-package-never-passed-the-projects-own-make-check)

**Part 5 — Bugs found by a fourth review round**

- [55. Every AMQP RPC reply raises a `TypeError`](#55-every-amqp-rpc-reply-raises-a-typeerror)
- [56. The coordinator retries forever with no poison-message escape](#56-the-coordinator-retries-forever-with-no-poison-message-escape)
- [57. A group's `PROPAGATE` policy behaves exactly like `CONTINUE`](#57-a-groups-propagate-policy-behaves-exactly-like-continue)
- [58. The Redis broker is at-most-once across a consumer crash](#58-the-redis-broker-is-at-most-once-across-a-consumer-crash)
- [59. A finished canvas reads as RUNNING once its status expires](#59-a-finished-canvas-reads-as-running-once-its-status-expires)
- [60. A node its parent doesn't list raises a bare `ValueError`](#60-a-node-its-parent-doesnt-list-raises-a-bare-valueerror)
- [61. The idempotency guarantee is narrower than documented](#61-the-idempotency-guarantee-is-narrower-than-documented)

**Part 6 — Bugs found by a fifth review round, four of them in earlier fixes**

- [62. Concurrent handlers commit past in-flight Kafka records](#62-concurrent-handlers-commit-past-in-flight-kafka-records)
- [63. `ProcessPoolExecutor` can never run a real `Worker`](#63-processpoolexecutor-can-never-run-a-real-worker)
- [64. A group's terminal branches skip its only de-duplication](#64-a-groups-terminal-branches-skip-its-only-de-duplication)
- [65. The RabbitMQ consumer connection races its own lazy init](#65-the-rabbitmq-consumer-connection-races-its-own-lazy-init)
- [66. The handler's catch-all bypasses `max_attempts`](#66-the-handlers-catch-all-bypasses-max_attempts)
- [67. Untracking before advancing strands a dead-lettered result](#67-untracking-before-advancing-strands-a-dead-lettered-result)
- [68. A reused `canvas_id` is dead on arrival](#68-a-reused-canvas_id-is-dead-on-arrival)
- [69. `ABORT` leaves the group's callback `PENDING`](#69-abort-leaves-the-groups-callback-pending)
- [70. `XAUTOCLAIM` reclaims this consumer's own in-flight work](#70-xautoclaim-reclaims-this-consumers-own-in-flight-work)

**Part 7 — Bugs found by a sixth review round**

- [71. `rollback` releases only one of the guards a call burned](#71-rollback-releases-only-one-of-the-guards-a-call-burned)
- [72. The sweeper and a live result can complete the same node](#72-the-sweeper-and-a-live-result-can-complete-the-same-node)
- [73. A failed sweeper advance strands its node](#73-a-failed-sweeper-advance-strands-its-node)
- [74. The Redis reclaim guard is keyed by stream id alone](#74-the-redis-reclaim-guard-is-keyed-by-stream-id-alone)
- [75. Kafka offset tracking wedges on a rebalance](#75-kafka-offset-tracking-wedges-on-a-rebalance)

**Part 8 — Bugs found by a seventh review round**

- [76. Shutdown tears down the transport under in-flight handlers](#76-shutdown-tears-down-the-transport-under-in-flight-handlers)
- [77. A `WorkerApp` cannot be restarted](#77-a-workerapp-cannot-be-restarted)
- [78. Signal handlers are installed and never removed](#78-signal-handlers-are-installed-and-never-removed)
- [79. A failed request publish leaks its pending entry](#79-a-failed-request-publish-leaks-its-pending-entry)
- [80. Kafka records offsets that were never queued](#80-kafka-records-offsets-that-were-never-queued)
- [81. Dropping offset bookkeeping loses in-flight commits silently](#81-dropping-offset-bookkeeping-loses-in-flight-commits-silently)
- [82. An empty `Chain`/`Chord` escapes the `WorkerError` hierarchy](#82-an-empty-chainchord-escapes-the-workererror-hierarchy)
- [83. The Redis key registry inherits a TTL across a `canvas_id` reuse](#83-the-redis-key-registry-inherits-a-ttl-across-a-canvas_id-reuse)
- [84. One slow handler blocks reclaiming abandoned entries](#84-one-slow-handler-blocks-reclaiming-abandoned-entries)

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

## Part 3 — Bugs found in `mint.worker` itself by a second review round

Everything above was found while *writing* `mint.worker`. These were found by
reviewing the finished package — the class of bug a passing test suite doesn't
surface, because the tests were written against the same assumptions the code
was.

### 23. A chord's callback is lost for good if its dispatch fails to publish

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** A chord's last leg finishes, the broker has a connection blip, and
the chord's callback simply never runs. The canvas sits in `RUNNING` forever
with every leg `FINISHED` and nothing left that will ever advance it.

**Root cause:** The one-shot fan-in guard is burned *before* the caller gets a
chance to publish what it authorised:

```python
progress = await self.store.mark_child_done(...)   # SETNX burns the guard here
if not progress.fired:
    return None, None
...
return Dispatch(topic=entry.topic, ...), None      # caller publishes AFTER this returns
```

`Worker._advance_canvas` then publishes, fails, and returns `False`, which nacks
for redelivery. On redelivery `mark_child_done` finds the guard already burned,
returns `fired=False`, `_advance_group` returns `(None, None)`, `complete()`
returns `[]` — and `_advance_canvas` reports success, so the worker **acks**.
This directly defeats the module's own stated contract: ack only after both the
store write and the dispatch publish have succeeded.

**The practical fix:** Make the guard releasable, and release it on exactly the
path that burned it for nothing:

```python
# mint/worker/canvas/engine.py
async def rollback(self, dispatches: Sequence[Dispatch]) -> None:
    for dispatch in dispatches:
        if dispatch.group_id is not None:
            await self.store.reset_group_fired(dispatch.canvas_id, dispatch.group_id)
```

```python
# mint/worker/worker.py, _advance_canvas
except Exception:
    logger.exception("Failed to publish dispatch", node_id=envelope.node_id)
    await binding.engine.rollback(dispatches)   # the retry can now re-fire
    return False
```

`Dispatch` gained a `group_id`, set only on a callback dispatch, so a caller
rolls back exactly the bookkeeping the engine did on its behalf and nothing else.

The subtle half of this fix is in the Lua script. `FAN_IN_SCRIPT` required
`added == 1 AND count == num_children` to fire, and `added == 1` is false for
*any* redelivery — so even with the guard released, a redelivered leg could
never re-fire. The `added` clause turns out to be redundant: `SETNX` on the
fired key is the entire exactly-once guarantee, and `SADD`'s idempotency is what
keeps `count` honest. Dropping it is what makes firing re-derivable:

```lua
-- was: if added == 1 and tonumber(done_count) == tonumber(ARGV[2]) then
if tonumber(done_count) == tonumber(ARGV[2]) then
    if redis.call('SETNX', KEYS[2], '1') == 1 then
        fired = 1
    end
end
```

Bug #3's double-counting guarantee is untouched, and still has its own tests.
Regression tests: `test_engine.py::TestGroup::test_rollback_lets_a_redelivered_leg_re_fire_a_callback_that_never_published`
and `test_without_a_rollback_a_redelivered_leg_still_fires_only_once`, plus
`test_worker.py::TestRedeliverySafety::test_a_failed_callback_publish_leaves_the_chord_callback_dispatchable`
end to end. **Note on the test itself:** the failure mode is a message that
never arrives, so an unguarded `anext()` would *hang* the suite rather than fail
it — exactly bug #22's trap. Every `MemoryBroker` read in those tests goes
through a timeout-guarded helper.

---

### 24. The fan-in script never reloads after a Redis script-cache flush

**Where:** `mint/worker/stores/redis.py::mark_child_done`

**Symptom:** Chords work fine for weeks, then after a Redis restart (or a
failover, or an ops `SCRIPT FLUSH`) every chord in the system stops firing its
callback. Restarting the workers fixes it. Nothing in the workers' own logs
explains why.

**Root cause:** The script's SHA was loaded once and cached for the process
lifetime, then invoked with a bare `EVALSHA`:

```python
async def _ensure_fan_in_script(self) -> str:
    if self._fan_in_sha is None:
        self._fan_in_sha = await self.client.script_load(FAN_IN_SCRIPT)
    return self._fan_in_sha
```

Redis's script cache is not persistent and is not replicated. Once it's gone,
`EVALSHA` raises `NOSCRIPT` — and nothing here ever reloads, so every subsequent
fan-in call raises too, permanently.

**The practical fix:** Use redis-py's `Script` object, which handles exactly
this:

```python
def _ensure_fan_in_script(self) -> AsyncScript:
    if self._fan_in_script is None:
        self._fan_in_script = self.client.register_script(FAN_IN_SCRIPT)
    return self._fan_in_script

# ...
added, done_count, fired = await script(keys=[done_key, fired_key], args=[child_id, num_children])
```

`AsyncScript.__call__` catches `NoScriptError` and re-loads the body before
retrying. Regression test:
`test_redis_mocked.py::TestFanIn::test_the_script_is_invoked_not_a_process_cached_sha`
asserts `evalsha`/`script_load` are never called directly, since a passing
happy-path test can't distinguish the two.

---

### 25. One Kafka consumer slot, shared by every worker, commits the wrong offsets

**Where:** `mint/worker/brokers/kafka.py::consume`, `::commit`

**Symptom:** A `WorkerApp` with two Kafka-backed workers. After a restart, one
worker reprocesses everything it already did, and the other silently skips work
it never finished.

**Root cause:** `consume()` builds a consumer per call but stores it in one slot:

```python
consumer = AIOKafkaConsumer(topic, ...)
await consumer.start()
self._consumer = consumer      # overwritten by the next consume() call
```

`WorkerApp` deliberately shares one broker across every registered worker, so
worker B's `consume()` overwrites worker A's consumer. `commit()` reads that one
slot, so **A's ack commits B's consumer**: A's offsets never move (full replay on
restart) while B's advance past records still in flight (loss on restart).
`close()` had the same shape and stopped only the last consumer, leaking the rest.

**The practical fix:** Key consumers by topic, and let each delivery carry the
consumer it actually came from rather than looking one up:

```python
self._consumers: dict[str, AIOKafkaConsumer] = {}
# ...
self._consumers[topic] = consumer
try:
    async for record in consumer:
        yield KafkaDelivery(self, record, consumer)
finally:
    self._consumers.pop(topic, None)
    await consumer.stop()
```

Regression tests: `test_kafka_mocked.py::TestAckNack::test_each_delivery_commits_the_consumer_it_came_from`,
`TestGuaranteeAndClose::test_close_stops_every_consumer_not_just_the_last`, plus
a real-broker test asserting the *committed offsets themselves*
(`test_kafka_container.py::TestSharedBrokerAcrossTopics`) — a mock can only prove
which object was called, not that two groups genuinely commit independently.

---

### 26. A Kafka ack commits past records still being processed

**Where:** `mint/worker/brokers/kafka.py::commit`

**Symptom:** Under load, a Kafka worker restart loses a handful of messages that
were mid-flight — but only when several are being handled at once.

**Root cause:**

```python
await self._consumer.commit()   # no offsets argument
```

A bare `commit()` commits every partition's *current fetch position*, not the
offset of the record being acked. `Worker.run()` spawns a task per delivery, so
several records are in flight simultaneously; acking record N commits past
N+1..N+k too. If the process dies before those finish, they are gone.

**The practical fix:** Name the offset being acknowledged:

```python
async def _commit(self) -> None:
    partition = TopicPartition(self._record.topic, self._record.partition)
    await self._consumer.commit({partition: self._record.offset + 1})
```

Out-of-order acks can now move the committed offset *backwards*, which replays —
at-least-once, and exactly what the rest of this package is built to tolerate.
The old behavior moved it forwards, which drops. Regression test:
`test_kafka_mocked.py::TestAckNack::test_ack_commits_this_records_own_partition_offset`.

---

### 27. A message the worker's `Input` can't parse stalls its canvas forever

**Where:** `mint/worker/worker.py::_process_delivery`

**Symptom:** One producer ships a message with a stale schema. That task's node
sits `PENDING` forever, its canvas sits `RUNNING` forever, and every downstream
step of the chain never runs. Nothing errors; nothing retries; nothing alerts.

**Root cause:**

```python
input_obj = self._decode_input(envelope)
if input_obj is None:
    await delivery.nack(requeue=False)   # dead-lettered, and that's all
    return
```

The message is correctly dead-lettered — retrying can't fix a schema mismatch —
but no `NodeOutcome` is ever recorded, so nothing tells the canvas this node is
finished-with-error. Embedded mode (the default) has no sweeper that would ever
notice. Note the asymmetry that hid this: the *malformed envelope* branch above
it genuinely has nowhere to record anything, because `canvas_id`/`node_id` are
unreadable. Here they're both known.

**The practical fix:** Fail the node explicitly, through the same path a raised
exception already takes:

```python
if input_obj is None:
    await self._fail_node(envelope, MALFORMED_INPUT_ERROR, binding)
    await delivery.nack(requeue=False)
    return
```

`_fail_node` routes by deployment mode exactly like a real failure does (store
in embedded mode, `results_topic` in centralized mode) and is deliberately
best-effort: the delivery is being dropped either way, so a store hiccup must
not turn an unretryable message into a redelivery loop. Regression tests:
`test_worker.py::TestUndeliverableInputFailsItsNode` (four tests: the outcome is
recorded, the canvas reaches a terminal status, the dead-letter still happens,
and centralized mode reports rather than stores).

---

### 28. A backlog spawns one handler task per message, without limit

**Where:** `mint/worker/worker.py::run`

**Symptom:** A worker restarts against a topic holding a large backlog and its
memory climbs until the process is killed.

**Root cause:**

```python
async for delivery in binding.broker.consume(self.topic):
    task = asyncio.create_task(self._handle(delivery, binding))   # no bound at all
```

Two of the five brokers happen to provide their own backpressure —
RabbitMQ via `prefetch_count`, the Redis broker by reading one stream entry at a
time — which is why this never showed up in normal use. `KafkaBroker` and
`MemoryBroker` yield as fast as the topic supplies, so the loop spawns one live
handler per backlogged message. It also compounds bug #26: more concurrent
in-flight records means a wider offset commit.

**The practical fix:**

```python
max_concurrency: int = DEFAULT_MAX_CONCURRENCY   # 32

# in run(), before spawning:
await self._slots.acquire()
task = asyncio.create_task(self._handle(delivery, binding))

# in _handle:
finally:
    self._slots.release()
```

Holding the semaphore across the yield point is what actually stops the loop
pulling more. A plain `asyncio.Semaphore` rather than
`mint.utils.ConcurrencyLimiter`, deliberately: the limiter's `ContextVar`
reentrancy assumes acquire and release happen in the same task, and this pattern
splits them across two. Regression test:
`test_worker.py::TestConcurrencyBound::test_run_never_exceeds_max_concurrency_in_flight`
— with 8 queued messages and every handler parked, the unfixed code shows all 8
in flight (`assert 8 == 2`).

---

### 29. The coordinator tracks in-flight nodes by node id alone

**Where:** `mint/worker/coordinator.py::_track`, `::_untrack`

**Symptom:** Two canvases built from the same template run concurrently. One of
them stops being swept for timeouts entirely, and a later `cancel()` marks a node
`CANCELLED` that actually finished successfully.

**Root cause:**

```python
self._in_flight[node_id] = InFlightNode(canvas_id, node_id, datetime.now(UTC))
```

`Node(topic, input, id=...)`, `Chain(..., id=...)` and `apply(canvas_id=...)`
all exist precisely so a caller can pin ids for idempotent retries — so the same
node id running in two canvases at once is supported usage, not a corner case.
With a bare `node_id` key, canvas B's `_track` evicts canvas A's entry (A drops
out of the sweeper), and `_untrack(B, node_id)` pops the shared entry while
leaving `_by_canvas["A"]` holding it forever — an unbounded leak, and a stale
entry `cancel("A")` will later act on.

**The practical fix:** Key by the pair that actually identifies a node.

```python
self._in_flight: dict[tuple[str, str], InFlightNode] = {}
# ...
self._in_flight[canvas_id, node_id] = InFlightNode(canvas_id, node_id, datetime.now(UTC))
```

Regression tests: `test_coordinator.py::TestCrossCanvasTracking` (three tests:
separate tracking, untracking isolation, and a stale node in one canvas not
timing out the other).

---

### 30. The coordinator's shutdown leaks its store's connections

**Where:** `mint/worker/coordinator.py::_shutdown`

**Symptom:** Redis connections accumulate across coordinator restarts.

**Root cause:** `WorkerApp._shutdown` closes both the broker and the store;
`Coordinator._shutdown` only ever closed the broker. A `RedisCanvasStore`'s
connection pool was simply never released.

**The practical fix:**

```python
await self.broker.close()
await self.store.close()   # matching WorkerApp
```

Regression test: `test_coordinator.py::TestShutdownReleasesResources::test_shutdown_closes_the_store_as_well_as_the_broker`,
asserting on close *order* via recording subclasses rather than monkeypatched
methods (this repo forbids `# type: ignore`, and reassigning a bound method
can't be typed).

---

### 31. A `stop()` that arrives before the run loop starts is discarded

**Where:** `mint/worker/coordinator.py::run`, `mint/worker/app.py::run`

**Symptom:** A supervisor that starts a coordinator and immediately decides to
shut down (a failed health check, a fast SIGTERM) hangs forever. `stop()`
returns cleanly and does nothing.

**Root cause:**

```python
async def run(self) -> None:
    self._running = True
    self._stop_event = asyncio.Event()   # replaces whatever stop() may have already set
```

`asyncio.create_task(coordinator.run())` doesn't run the coroutine immediately.
A `stop()` on the next line sets the event created in `__init__` — which `run()`
then throws away when it finally gets scheduled. Nothing is left that can stop it.

**The practical fix:** Create the event once, and clear it only once a shutdown
has actually been serviced:

```python
# __init__ creates it; run() no longer touches it
# _shutdown(), at the very end:
self._running = False
self._stop_event.clear()   # a serviced stop, so the instance stays reusable
```

A stop that lands early is now honored rather than lost, and re-running a
cleanly stopped instance still works. Regression tests:
`test_coordinator.py::TestShutdownReleasesResources::test_a_stop_that_lands_before_the_loop_starts_still_stops_it`
and `test_an_instance_can_run_again_after_a_clean_shutdown`.

---

### 32. Every AMQP RPC call leaks a reply queue and a consumer

**Where:** `mint/worker/executors/amqp_rpc.py::execute`

**Symptom:** A worker using `AMQPRPCExecutor` runs fine for hours, then starts
failing every call. RabbitMQ's management UI shows thousands of
`amq.gen-*` queues.

**Root cause:**

```python
async with self._ensure_channel_pool().acquire() as channel:
    reply_queue = await self._declare_reply_queue(channel)   # declares + consumes
    ...
    return await self._await_reply(correlation_id, future)
    # channel returns to the pool, still carrying this consumer
```

An exclusive queue is deleted when its **connection** closes, not its channel —
and the channel here is pooled, so the connection stays open indefinitely. Every
call therefore adds one queue and one consumer, spread across ~20 shared
channels, until a per-channel consumer limit or a queue limit is reached.

**The practical fix:** Capture the consumer tag and tear both down in a
`finally`:

```python
reply_queue, consumer_tag = await self._declare_reply_queue(channel)
try:
    return await self._call(channel, reply_queue, input_)
finally:
    await self._release_reply_queue(reply_queue, consumer_tag)
```

`_release_reply_queue` cancels the consumer, deletes the queue with
`if_unused=False, if_empty=False`, and logs rather than raises — teardown must
never mask the call's own result or error. Regression tests:
`test_amqp_rpc.py::TestReplyQueueLifecycle` (three tests, including the timeout
path, where the leak matters most, and a failing teardown not masking the real
error).

---

### 33. A malformed RPC reply is reported as a timeout

**Where:** `mint/worker/executors/amqp_rpc.py::_on_reply`

**Symptom:** A remote service is upgraded and starts returning a slightly
different shape. Callers hang for the full 30-second timeout and then raise
`RemoteCallTimeoutError: No reply on queue rpc.echo within 30.0s` — for replies
that arrived promptly.

**Root cause:**

```python
future = self._pending.pop(correlation_id, None)   # popped first
if future is None or future.done():
    return
future.set_result(self.output_type.model_validate_json(message.body))   # may raise
```

The `ValidationError` escapes into aio-pika's consumer callback with the future
already removed from `_pending` and never resolved. The caller has nothing left
that can complete it, so it waits out the timeout and reports the wrong cause —
sending you to investigate the network instead of the schema.

**The practical fix:**

```python
try:
    future.set_result(self.output_type.model_validate_json(message.body))
except ValidationError as exc:
    future.set_exception(exc)
```

Regression test: `test_amqp_rpc.py::TestReplyValidation::test_an_unparseable_reply_raises_a_validation_error_not_a_timeout`.

---

### 34. The Redis broker can lose a message while requeueing it

**Where:** `mint/worker/brokers/redis.py::redeliver`, `::deadletter`

**Symptom:** Rare, unreproducible message loss on a worker that was killed
mid-nack.

**Root cause:**

```python
await self.client.xack(entry.topic, self.group, entry.message_id)
await self.client.xdel(entry.topic, entry.message_id)      # message no longer exists
fields = {...}
await self.client.xadd(entry.topic, fields)                 # replacement written here
```

Between the `XDEL` and the `XADD` the message exists nowhere: not in the stream,
not in the pending-entries list. A crash in that window loses it outright —
which is precisely the at-most-once behavior bug #8's whole Streams rewrite
exists to eliminate.

**The practical fix:** Write first, retire second.

```python
await self.client.xadd(entry.topic, fields)   # replacement exists before...
await self._retire(entry)                     # ...the original is acked and deleted
```

A crash mid-sequence now duplicates rather than drops, and the canvas engine's
idempotent fan-in already absorbs duplicates by design. Regression tests:
`test_redis_mocked.py::TestRedeliveryOrdering` — asserting the *call order*, since
both orderings produce an identical end state on the happy path and only a crash
distinguishes them.

---

### 35. RabbitMQ's attempt counter never increments

**Where:** `mint/worker/brokers/rabbitmq.py`

**Symptom:** `delivery.attempt` is always `1` on RabbitMQ, so any retry/poison
policy written against it never triggers. `docs/worker/usage.md` documented every
broker as incrementing it.

**Root cause:** Two halves. `publish()` never wrote `ATTEMPT_HEADER` at all, so
the constant was read-only in practice — `RabbitMQDelivery` always fell back to
its default. And `nack(requeue=True)` used AMQP's native requeue:

```python
await self._message.reject(requeue=requeue)
```

which redelivers the *original frame*. Its headers are on the wire already and
cannot be rewritten, so there is no way to bump a counter through that path at
all. Redis, Kafka and `MemoryBroker` all republish and therefore all increment.

**The practical fix:** Stamp the header on publish, and make requeue a
republish-then-ack like the other brokers:

```python
async def nack(self, *, requeue: bool) -> None:
    if not requeue:
        await self._message.reject(requeue=False)   # native reject: this is what engages the DLX
        return
    await self._broker.redeliver(self._topic, self._message, self.attempt + 1)
    await self._message.ack()
```

Publish-before-ack, same ordering rule as issue #34. `requeue=False` stays a
native reject deliberately — that is what routes through the queue's dead-letter
exchange (bug #9).

**The tradeoff, taken knowingly:** a requeued message goes to the back of the
queue instead of being redelivered in place, so requeue no longer preserves
ordering. Uniform, observable retry counting was judged worth more than
per-message ordering on a nack path. It's called out in the broker's docstring
so the next reader doesn't have to rediscover it. Regression tests:
`test_rabbitmq_mocked.py::TestAttemptHeaderIsPublished` and
`test_rabbitmq_container.py::TestDeadLetterRegression::test_nack_requeue_true_redelivers_with_an_incremented_attempt`
— a mock accepts either shape without complaint, so the real broker is what
proves it.

---

### 36. Aborting a group overwrites the legs that already succeeded

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** A chord under `ErrorPolicy.ABORT` has legs 1-5 finish successfully
and leg 6 fail. Afterwards all of legs 1-5 read as `CANCELLED` — no record
survives that they ran, even though their side effects really happened.

**Root cause:**

```python
pending = [child_id for child_id in group.children if child_id != finished_child_id]
await self._abort_canvas(canvas_id, pending)   # cancel_nodes overwrites unconditionally
```

"Everything except the leg that just failed" is not the same set as "everything
that hasn't run". Compare `_chain_remaining`, which gets the equivalent right by
slicing the not-yet-run tail — a chain has an ordering to slice, and a group
doesn't, so the group version quietly cancelled completed work.

**The practical fix:** Ask the store what actually finished.

```python
async def _unfinished(self, canvas_id: str, group: GroupNode) -> list[str]:
    results = await self.store.get_results(canvas_id, group.children)
    return [child_id for child_id in group.children if child_id not in results]
```

Regression tests: `test_engine.py::TestGroupAbortPreservesFinishedLegs` (three
tests: a finished sibling stays FINISHED, genuinely pending legs are still
cancelled, and the failed leg keeps its own ERROR outcome).

---

### 37. A chord's own input never reaches its callback

**Where:** `mint/worker/canvas/builder.py::Chord`

**Symptom:** `FanIn.input` is always `None`, for every chord ever built with the
DSL.

**Root cause:** `GroupNode.input` existed, `models.py` documented `FanIn` as
"every leg's result plus the group input", and the engine faithfully passed
`input=group.input` through — but `Chord.__init__` had no `input` parameter and
`Chord.build()` never set the field. The whole path was wired except its source.

**The practical fix:** Give the DSL the argument the model was already expecting:

```python
def __init__(self, legs, callback=None, *, input=None, error_policy=..., id=None):
    ...
    self.input = input

# in build():
nodes[self.id] = GroupNode(..., input=self.input, ...)
```

This is for context an aggregation step needs that isn't any single leg's output
— the originating request, a tenant id. Regression tests:
`test_builder.py::TestChordInput`.

---

### 38. Flattening a nested chain silently discards its error policy

**Where:** `mint/worker/canvas/builder.py::Chain.__init__`

**Symptom:**

```python
Chain([a, Chain([b, c], error_policy=ErrorPolicy.CONTINUE)], error_policy=ErrorPolicy.PROPAGATE)
```

`b` and `c` run under `PROPAGATE`. The caller's explicit `CONTINUE` is reversed
with no error, no warning, and no way to notice short of watching a failure
cancel steps it was told not to.

**Root cause:**

```python
if isinstance(step, Chain):
    self.steps.extend(step.steps)   # takes the steps, drops the policy and the id
```

Flattening genuinely dissolves the nested chain — it stops existing as a node, so
it *cannot* keep a policy of its own. The bug is doing that silently.

**The practical fix:** Reject the conflict instead of resolving it invisibly.

```python
if step.error_policy != error_policy:
    raise ConflictingErrorPolicyError(
        chain_id=self.id, nested_id=step.id,
        policy=error_policy, nested_policy=step.error_policy,
    )
```

Matching and defaulted policies flatten exactly as before, so no working code
changes. Regression tests: `test_builder.py::TestNestedChainErrorPolicy`.

---

### 39. Two different NATS topics can still share one stream and cursor

**Where:** `mint/worker/brokers/nats.py::_stream_name`

**Symptom:** Topics `a.b` and `a-b` interfere with each other exactly the way
bug #10 described — in the module written to fix bug #10.

**Root cause:**

```python
def _stream_name(self, topic: str) -> str:
    return topic.replace(".", "-")
```

JetStream stream names can't contain `.`, so collapsing it is necessary. But a
plain replace is not injective: `a.b` and `a-b` both map to `a-b`. Since
`_durable_name` derives from `_stream_name`, they also share one durable
consumer — one cursor between two unrelated topics.

**The practical fix:** Escape before collapsing, so the mapping is reversible:

```python
return topic.replace("-", "--").replace(".", "-")
```

Regression tests: `test_nats_mocked.py::TestStreamNamingIsInjective`.

---

### 40. Consuming a NATS dead-letter subject is rejected outright

**Where:** `mint/worker/brokers/nats.py::_ensure_stream`

**Symptom:** Consuming `{topic}.dlq` to inspect what landed there fails —
JetStream refuses to create the stream because its subject overlaps an existing
one.

**Root cause:**

```python
await jetstream.add_stream(
    name=self._stream_name(topic),
    subjects=[topic, f"{topic}{self.DLQ_SUFFIX}"],
)
```

Applied to `foo`, this stream claims `foo` and `foo.dlq`. Applied to `foo.dlq`,
it tries to create stream `foo-dlq` claiming `foo.dlq` and `foo.dlq.dlq` — and
`foo.dlq` is already owned by `foo`'s stream. JetStream rejects overlapping
subjects across streams.

This is the same shape as bug #21 (RabbitMQ refusing to redeclare its own DLQ
with different arguments), and the fix is the same rule: **a dead-letter
destination terminates the chain rather than extending it.** Worth noting that
`RabbitMQBroker` had already learned this and NATS hadn't — a fix applied to one
broker didn't get carried across to its siblings.

**The practical fix:** Resolve a `.dlq` topic back to the stream that already
owns it:

```python
def _owning_topic(self, topic: str) -> str:
    if topic.endswith(self.DLQ_SUFFIX):
        return topic[: -len(self.DLQ_SUFFIX)]
    return topic

async def _ensure_stream(self, jetstream, topic: str) -> None:
    owner = self._owning_topic(topic)
    await jetstream.add_stream(
        name=self._stream_name(owner),
        subjects=[owner, f"{owner}{self.DLQ_SUFFIX}"],
    )
```

Regression tests: `test_nats_mocked.py::TestDeadLetterStreamOwnership`.

---

## Part 4 — Bugs found by a third review round, after Part 3's fixes landed

Part 3 fixed everything the second review found. This round reviewed the result
— and found fourteen more, including one case where Part 3's own fix was wrong.
That is the point of reviewing the fixes and not just the original code.

### 41. An unexpected exception strands its delivery, unacked and unlogged

**Where:** `mint/worker/worker.py::_handle`

**Symptom:** A message vanishes. It is not acked, not nacked, not dead-lettered,
and nothing appears in the logs — occasionally accompanied, much later, by a
bare `Task exception was never retrieved` at GC time.

**Root cause:**

```python
async def _handle(self, delivery, binding) -> None:
    try:
        await self._process_delivery(delivery, binding)
    except asyncio.CancelledError:      # the ONLY thing caught
        await delivery.nack(requeue=True)
        raise
```

Anything else — a failing `ack()` on a channel blip, a Kafka commit error, a
driver exception outside the `WorkerError` hierarchy — escapes. And `_handle`
runs as a fire-and-forget `asyncio.Task` whose only done-callback is
`self._inflight.discard`, so nothing ever retrieves the exception. The delivery
is left in limbo: invisible to the broker, to the canvas, and to the operator.

**The practical fix:** Catch it, log it, and settle the delivery.

```python
except Exception:
    logger.exception("Unhandled error handling a delivery", topic=self.topic)
    if not settled:
        await self._safe_nack(delivery, requeue=True)
```

`_safe_nack` logs rather than raises, because at that point there is genuinely
nothing above it left to catch anything.

---

### 42. Shutdown can nack a delivery that was already acked

**Where:** `mint/worker/worker.py::_handle` / `::_process_delivery`

**Symptom:** After a clean shutdown, a node that had already finished runs again
on restart. On RabbitMQ, the shutdown also logs a channel error.

**Root cause:** The cancellation guard covered more than the unsettled window:

```python
await delivery.ack()            # settled here
if result is not None:
    await self._safe_on_success(input_obj, result)   # ...but still inside the try
```

A drain-timeout cancellation landing in a slow `on_success` hook — the ordinary
shutdown path — was caught by `except CancelledError` and nacked a delivery that
had already been acked. Since issue #35 made RabbitMQ's requeue a
republish-then-ack, that double-settles: the message is republished *and* acked a
second time, which the broker rejects.

**The practical fix:** Record settlement before the hook can be interrupted, by
moving the hook out of the settled region entirely:

```python
@dataclass(frozen=True)
class DeliveryOutcome[T: BaseModel, RT: BaseModel]:
    settled: bool
    input_obj: T | None = None
    result: RT | None = None
```

```python
handled = await self._process_delivery(delivery, binding)
settled = handled.settled                       # read before the hook runs
if handled.input_obj is not None and handled.result is not None:
    await self._safe_on_success(handled.input_obj, handled.result)
```

A returned flag rather than one assigned inside the `try`: the first attempt at
this fix set `settled = await self._process_delivery(...)`, which is only
assigned *on return* — so a cancellation inside the method still saw `False`.
The test caught it immediately.

---

### 43. Nothing reads `Delivery.attempt`, so failures retry forever

**Where:** `mint/worker/worker.py::_process_delivery`

**Symptom:** With the store down, one worker pegs a CPU core and the topic makes
no progress.

**Root cause:** Every failure path returned `False` and nacked for redelivery,
unconditionally. All five brokers carefully maintain `Delivery.attempt` — and
grepping the package showed nothing outside the broker modules ever read it.
There was no poison-message escape at all. On `MemoryBroker`, whose requeue
redelivers synchronously onto the queue the same consumer is polling, that is a
tight loop with no `await` boundary long enough to make progress.

**The practical fix:**

```python
async def _retry_or_drop(self, delivery: Delivery, envelope: Envelope) -> None:
    if delivery.attempt >= self.max_attempts:      # default 5
        logger.error("Giving up on a delivery after repeated failures", ...)
        await delivery.nack(requeue=False)         # dead-letter
        return
    await delivery.nack(requeue=True)
```

This also gave issue #27's fix somewhere sane to fall back to: a node whose
*failure* can't even be recorded is now retried under the same cap rather than
dead-lettered into a permanently stalled canvas.

---

### 44. A dead run loop is never noticed

**Where:** `mint/worker/app.py::run`, `mint/worker/coordinator.py::run`

**Symptom:** A deployment stops processing one topic. The process is up, the
health check passes, and nothing is logged.

**Root cause:** Both `run()` methods create their background tasks and then only
await `_stop_event`. Nothing ever observes those tasks. A consume loop that died
— a dropped broker connection propagating out of `consume()`, or issue #41's
escaping exception — leaves the process alive and healthy-looking while doing
none of its work, forever.

**The practical fix:** Observe them, and fail loudly.

```python
for topic, task in self._tasks.items():
    task.add_done_callback(partial(self._on_worker_exit, topic))
```

```python
def _on_worker_exit(self, topic: str, task: asyncio.Task[None]) -> None:
    if task.cancelled() or self._stop_event.is_set():
        return                                   # an ordinary shutdown
    logger.error("Worker consume loop failed", topic=topic, error=repr(task.exception()))
    self._stop_event.set()
```

A dead loop isn't recoverable in place, so the app comes down and the supervisor
restarts it — rather than degrading silently.

---

### 45. One bad result kills the coordinator's only result loop

**Where:** `mint/worker/coordinator.py::_consume_results`

**Symptom:** Every canvas in a centralized-mode deployment stalls at once. The
coordinator process is up and its sweeper keeps timing nodes out on schedule.

**Root cause:** Same shape as issue #41, in the one place with no redundancy:
the loop guarded only `CancelledError`, so a single unexpected exception — most
plausibly a failing `ack()` — terminated it permanently. Combined with issue #44,
nothing noticed.

**The practical fix:** Log it, requeue that one delivery, keep consuming.

```python
except Exception:
    logger.exception("Unhandled error handling a result", topic=self.results_topic)
    await self._safe_nack(delivery)
```

---

### 46. The drain timeout tears the broker down mid-nack

**Where:** `mint/worker/app.py::_shutdown`

**Symptom:** Work in flight past the drain timeout is lost rather than
redelivered — the exact outcome `Worker.drain`'s docstring says the design
prevents.

**Root cause:**

```python
with contextlib.suppress(TimeoutError):
    async with asyncio.timeout(self.drain_timeout):
        await asyncio.gather(*(w.drain() for w in self._workers.values()))
await self.broker.close()      # immediately
```

The timeout *requests* cancellation of the handler tasks; it does not wait for
it to complete. Those handlers are still inside `await delivery.nack(requeue=True)`
when `broker.close()` tears the connection down underneath them, so the nack
raises and the delivery is stranded.

**The practical fix:** Drain twice — once with the deadline, then again without
one, so the cancellations actually land before anything closes.

```python
drains = [worker.drain() for worker in self._workers.values()]
with contextlib.suppress(TimeoutError):
    async with asyncio.timeout(self.drain_timeout):
        await asyncio.gather(*drains)
        return
await asyncio.gather(*(w.drain() for w in self._workers.values()), return_exceptions=True)
```

---

### 47. A result arriving mid-sweep completes its node twice

**Where:** `mint/worker/coordinator.py::_sweep_once` / `::_timeout_node`

**Symptom:** Occasionally a chain's next step is dispatched twice, or a group
counts one leg twice — only ever on a canvas that was near its `max_age`.

**Root cause:** `_sweep_once` snapshots the stale entries and then `await`s per
entry:

```python
stale = [entry for entry in list(self._in_flight.values()) if ...]
for entry in stale:
    await self._timeout_node(entry)    # each await is a window
```

A genuine result landing on `results_topic` during that sequence is handled
concurrently by `_handle_result`, which completes the node — and then
`_timeout_node`, holding a now-stale snapshot, completes it *again* with a
synthetic `TimeoutError` outcome.

**The practical fix:** Make untracking the claim. Whichever path removes the
entry first is the one that gets to complete the node.

```python
if (entry.canvas_id, entry.node_id) not in self._in_flight:
    return
self._untrack(entry.canvas_id, entry.node_id)
```

---

### 48. A nested container can steal its parent's id and corrupt the graph

**Where:** `mint/worker/canvas/builder.py::Chord.build`, `::Chain.build`

**Symptom:** No error at all — and a persisted graph in which a group is listed
as its own child.

**Root cause:** A container checks its id *before* building children but writes
its own node *after* (it needs its children's ids first):

```python
if self.id in nodes:            # inner container doesn't exist yet
    raise DuplicateNodeIdError(node_id=self.id)
for leg in self.legs:
    leg.build(canvas_id, self.id, nodes)     # inner writes nodes["x"] here
...
nodes[self.id] = GroupNode(...)              # and this silently overwrites it
```

Verified concretely: `Chord([Chord([...], id="x"), Node(...)], id="x")` builds
without raising and yields `GroupNode(id="x", children=["x", <t3>])`. The inner
group is gone entirely, its legs' `parent_id` points at a group that no longer
holds them, `mark_child_done` is called with ids absent from `children`, and
`get_results(children)` finds nothing for them. Every *other* duplicate-id shape
raised properly; this one corrupted the graph in silence.

**The practical fix:** Check again after building children, before writing.

```python
def _reject_duplicate(nodes: dict[str, AnyNode], node_id: str) -> None:
    if node_id in nodes:
        raise DuplicateNodeIdError(node_id=node_id)
```

---

### 49. Twenty RabbitMQ consumers deadlock every publish

**Where:** `mint/worker/brokers/rabbitmq.py::consume`

**Symptom:** An app with enough registered workers starts up, consumes nothing,
publishes nothing, and reports no error whatsoever.

**Root cause:** `consume()` took its channel from the shared pool and held it for
the consumer's entire lifetime:

```python
async with self._ensure_channel_pool().acquire() as channel:   # held until the generator ends
    ...
    async for message in iterator:
        yield RabbitMQDelivery(self, topic, message)
```

`WorkerApp` shares one broker across every registered worker, and `_publish`
(used by both `publish` and `redeliver`) draws from that same pool of
`DEFAULT_CHANNEL_POOL_SIZE = 20`. With 20 workers consuming, every pooled channel
is permanently held and the first dispatch publish blocks forever on
`pool.acquire()`. `AMQPRPCExecutor` shares the acquire-a-pooled-channel pattern
but releases per call, so only this broker deadlocks.

**The practical fix:** Consumers get their own channels on one dedicated
connection, outside the pool.

```python
connection = await self._ensure_consumer_connection()
channel = await connection.channel()
try:
    ...
finally:
    await channel.close()
```

**This changed what's safe in tests, and the container lane caught it.** A
consumer now owns its channel and closes it on finalization, so the
fire-and-forget `anext(broker.consume(topic))` shape closes the channel out from
under a delivery still waiting to be acked — a real `ChannelInvalidStateError`.
The Kafka container tests already documented holding the generator for the test's
lifetime; the RabbitMQ ones now do the same.

---

### 50. The fix for #39 was itself not injective

**Where:** `mint/worker/brokers/nats.py::_stream_name`

**Symptom:** Same as #39 — two topics sharing a stream and a durable cursor —
just reached through a less obvious pair of inputs.

**Root cause:** #39's fix escaped `-` as `--` before mapping `.` to `-`:

```python
return topic.replace("-", "--").replace(".", "-")
```

That makes *runs of dashes* ambiguous. `a-.b` encodes to `a--` + `-b` = `a---b`;
`a.-b` encodes to `a-` + `--b` = `a---b`. Identical. Escaping into the same
character you are escaping *to* cannot be injective, and the docstring claiming
injectivity made it read as settled.

**The practical fix:** Give each source character its own distinct escape, so
every `-` in the output is unambiguously a marker followed by exactly one tag:

```python
DASH_ESCAPE: Final[str] = "-h"
DOT_ESCAPE: Final[str] = "-d"

return topic.replace("-", self.DASH_ESCAPE).replace(".", self.DOT_ESCAPE)
```

Regression test: rather than a couple of hand-picked pairs, an exhaustive check
over *every* topic up to length 5 in the alphabet that actually interacts
(`.`, `-`, `h`, `d`, and one ordinary letter). Hand-picked examples are what let
the first fix look correct.

---

### 51. A cancelled RPC call leaks its pending entry

**Where:** `mint/worker/executors/amqp_rpc.py::_await_reply`

**Symptom:** Bug #11's memory leak, back again, in a long-running worker that
restarts often.

**Root cause:** Cleanup lived only in the timeout branch:

```python
except TimeoutError as exc:
    self._pending.pop(correlation_id, None)
    ...
```

A *cancelled* call — the ordinary shutdown path, via `Worker._handle` — never
touches that branch, so its correlation id and future stayed in `_pending` for
the executor's lifetime.

**The practical fix:** A `finally`, which covers every exit including
cancellation. Note that `execute()`'s own `finally` already handled the queue
side correctly — only the map side was missed.

---

### 52. Trace ids die at the first hop

**Where:** `mint/worker/canvas/dispatch.py::Dispatch.to_envelope`

**Symptom:** A canvas started with a trace id is impossible to correlate in logs
past its entry node.

**Root cause:** Every non-entry message in a canvas is built here, and the field
simply wasn't carried:

```python
return Envelope(node_id=self.node_id, canvas_id=self.canvas_id, body=self.body)
```

`Envelope.trace_id` existed and `Worker._report_result` propagated it, so the
field looked wired — it just died on the one path every dispatched message takes.

**The practical fix:** `to_envelope(trace_id)`, threaded through by both
`Worker._advance_canvas` and `Coordinator.dispatch`.

---

### 53. `MemoryBroker.close()` wakes only one consumer per topic

**Where:** `mint/worker/brokers/memory.py::close`

**Symptom:** A test or single-process deployment with two consumers on one topic
hangs on shutdown.

**Root cause:** `close()` pushes exactly one `None` sentinel per queue. The first
consumer takes it and returns; every other consumer stays blocked on
`queue.get()` forever. A `Coordinator` alongside a worker on the same topic is
exactly that shape.

**The practical fix:** Put the sentinel back on the way out.

```python
if item is None:
    await queue.put(None)
    return
```

This also keeps a queue drainable after close, which is what makes "was this
nacked rather than dropped?" checkable at all — a property two existing tests
depended on, and which a first attempt at this fix (short-circuiting `consume`
on a `_closed` flag) broke immediately.

---

### 54. The package never passed the project's own `make check`

**Where:** the whole package, plus `pyproject.toml`

**Symptom:** `make check` fails on a repo that is otherwise clean.

**Root cause, two independent halves.**

`make tc` runs `mypy .`, and this package contributed all 65 of the repo's mypy
errors. The apparent blocker looked structural — `Variable "Input" is not valid
as a type`, pointing straight at `Worker`'s central `Input`/`Output` class
attributes. It wasn't. The *tests* named their models `Input`/`Output` at module
scope and then wrote `Input = Input`, so every later annotation in those class
bodies resolved to the attribute instead of the model. The usage docs already
used distinct names (`SyncRefIn`/`SyncRefOut`); the tests just didn't. Renaming
them removed 31 errors and confirmed the design was never the problem.

`make lint` failed on all 116 files with `CPY001` (missing copyright notice).
This looked pre-existing — it fires repo-wide, on files this change never touched
— but master lints clean. The cause was this branch's own lock file, which bumps
ruff 0.15.22 to 0.16.3, where `CPY001` joins `ALL`. The same bump splits
`PLR0913` into a second `PLR0917` and adds a `None`-not-last union check.

**The practical fix:** `CPY001` ignored in config (this project carries no
copyright headers on any file, deliberately); the two storage constructors that
already carried a `PLR0913` noqa name `PLR0917` alongside it; the one flagged
union reordered. On the mypy side: a named `TerminalStatus` alias so the engine's
status ternaries can be annotated, parameterized `ITaskExecutor`/`Worker`,
`Redis[bytes]`, and stream fields built key by key rather than by `**mapping`
unpacking (`dict` is invariant in both parameters, so a `Mapping[str, str]` never
satisfied the widened field type). `RedisBroker.ack` turned out to be
byte-identical to `_retire` and now delegates, which removed one of the two
`types-redis` suppressions along with the duplication.

**The lesson worth keeping:** "this failure is pre-existing" is a claim to verify
against the base branch, not to infer from where the errors appear.

---

## Part 5 — Bugs found by a fourth review round

Three of these (#55, #58, #59) are the same shape: a claim made in a docstring
that the code did not actually implement. Worth noting, because a confident
comment is the easiest thing in a codebase to stop re-reading.

### 55. Every AMQP RPC reply raises a `TypeError`

**Where:** `mint/worker/executors/amqp_rpc.py::_on_reply`

**Symptom:** Calls work. Every one of them also logs a "Task exception was never
retrieved" traceback, with no obvious connection to application code.

**Root cause:** Two aio-pika features that cannot be combined:

```python
await queue.consume(self._on_reply, no_ack=True)      # in _declare_reply_queue
...
async def _on_reply(self, message):
    async with message.process():                      # incompatible with no_ack
```

Verified against the installed package rather than reasoned about:
`IncomingMessage.__init__` sets `__processed = True` when `no_ack` is passed;
`ProcessContext.__aexit__` evaluates `if not self.ignore_processed or not
self.message.processed:` — with the default `ignore_processed=False` that first
clause is always true, so it calls `ack()` on every clean exit; and `ack()` opens
with `if self.__no_ack: raise TypeError`. Since aiormq dispatches consumer
callbacks with a bare `create_task` and never retrieves the result, the exception
surfaces only as GC-time noise. The call still returns, because the future is
resolved before `__aexit__` runs — which is exactly why this survived a passing
test suite and a container run.

**The practical fix:** Delete the context manager. With `no_ack=True` there is
nothing to acknowledge; `message.process()` was never doing anything but
throwing. Regression tests assert both halves — that handling a reply never
touches `process()`/`ack()`, *and* that the consumer is still registered
`no_ack=True`, since the first assertion is only meaningful while the second
holds.

---

### 56. The coordinator retries forever with no poison-message escape

**Where:** `mint/worker/coordinator.py::_handle_result`

**Symptom:** In centralized mode with the dispatch broker down, the coordinator
pegs a core and makes no progress — the exact failure `Worker._retry_or_drop`
(issue #43) was written to prevent, in the component that didn't get the fix.

**Root cause:** Identical shape, no cap:

```python
if not await self._advance(...):
    await delivery.nack(requeue=True)     # unconditional, forever
```

`delivery.attempt` appears nowhere in the file. A `WorkerError` happens to
self-heal — `engine.complete` marks the canvas ERROR, so the replay
short-circuits at the status guard — but a dispatch-publish failure or a failing
`ack()` does not, and those are the ones that recur.

**The practical fix:** The same `_retry_or_drop` the worker has, applied to both
the explicit failure path and the loop's catch-all. Adding `max_attempts` pushed
the constructor past the argument limit, so the three tunables moved into a
`CoordinatorConfig` dataclass — following `AMQPRPCConfig`, which already
establishes that pattern in this package, rather than suppressing the lint.

---

### 57. A group's `PROPAGATE` policy behaves exactly like `CONTINUE`

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** `Chord([a, b], callback=cb, error_policy=ErrorPolicy.PROPAGATE)`
nested in a chain. Leg `a` fails. `b` runs anyway, `cb` runs and succeeds, and
the enclosing chain advances to its next step as though nothing went wrong.

**Root cause:** `_advance_group` read `error_policy` in exactly two places — the
`ABORT` pre-check, and the `final_status` computation on the *callback-less*
path. A group with a callback therefore never consulted it: the callback was
dispatched with the failed leg present as `ok=False`, and because a callback's
completion becomes the group's own outcome (the `current_id == parent.callback`
branch in `_complete`), the group recorded the callback's **success**. The
failure evaporated.

This contradicts `ErrorPolicy`'s own docstring ("stop this container, mark it
errored, cancel what has not run yet, but still let its own parent decide") and
the policy table in `docs/worker/usage.md`. No test covered it — the existing
PROPAGATE coverage was all chains.

**The practical fix:** Mirror `_advance_chain`'s PROPAGATE branch.

```python
if outcome.status == NodeStatus.ERROR and group.error_policy == ErrorPolicy.PROPAGATE:
    await self._cancel_group_remainder(canvas_id, group, finished_child_id)
    return None, NodeOutcome(node_id=group.id, status=NodeStatus.ERROR, error=outcome.error)
```

The callback is cancelled along with the unfinished legs — a propagating group
never dispatches it, and leaving it PENDING would misreport it as still expected.

---

### 58. The Redis broker is at-most-once across a consumer crash

**Where:** `mint/worker/brokers/redis.py::consume`

**Symptom:** A worker is OOM-killed mid-handler. That message is never
redelivered — not to that consumer, not to any other. The broker declares
`guarantee = AT_LEAST_ONCE`, and `Worker`/`CanvasEngine` are built on that
promise.

**Root cause:** `XREADGROUP` with `>` returns *only* never-delivered entries. A
consumer's own pending-entries list is reachable only via an explicit id (`0`) or
`XAUTOCLAIM`, and the module had neither. So bug #8's whole point — "a crash
before ack leaves it recoverable rather than gone" — held only for the *storage*
of the message, not its *redelivery*. Nobody ever went back for it.

The docstring made this hard to spot, because it acknowledged the gap and then
misdescribed the mitigation:

> today a message survives a crash but needs another consumer to eventually
> re-read it via `XREADGROUP`'s own-pending-first semantics

`>` has no such semantics. The stated fallback did not exist.

**The practical fix:** Reclaim before reading, every poll.

```python
async def _reclaim(self, topic: str) -> StreamEntry | None:
    response = await self.client.xautoclaim(
        topic, self.group, self.consumer_name, self.reclaim_idle_ms,
        start_id="0-0", count=1,
    )
    entries = response[1] if len(response) > 1 else []   # Redis 6 replies with 2 elements
    ...
```

`reclaim_idle_ms` (default 60s) must comfortably exceed the slowest expected
handler, or a live consumer's work gets stolen mid-flight.

**This is also where the Redis broker finally got a container suite.** It had
none — and this is precisely a property mocking cannot express, since a mock
returns whatever the test tells it to. The new test has one consumer read a
message and "crash" without acking, then asserts another consumer recovers it;
it fails outright with the reclaim disabled.

---

### 59. A finished canvas reads as RUNNING once its status expires

**Where:** `mint/worker/stores/redis.py::set_canvas_status`

**Symptom:** A message dead-lettered from a long-finished canvas, replayed a day
later, marks that canvas `ERROR`.

**Root cause:** Reaching a terminal status expires every key tracked for the
canvas — *including the status key itself*. And `get_canvas_status` returns
`RUNNING` when the key is absent, because it cannot distinguish "expired" from
"never existed". So after the TTL a completed canvas reads as live again,
`CanvasEngine.complete`'s short-circuit no longer fires, `_require_node` raises
on the long-deleted nodes, and the `except WorkerError` handler writes a fresh
`ERROR` status for a canvas that finished successfully.

**The practical fix:** The status key is the canvas's tombstone, so it outlives
the data it describes — a separate, longer TTL (7× the data TTL by default)
rather than the same one:

```python
await self.client.expire(registry_key, self.terminal_ttl_seconds)
await self.client.expire(key, self.status_ttl_seconds)     # the status, not the data
```

**A fix that was tried and rejected:** short-circuiting `complete()` to a no-op
when the completing node is missing. It made the existing
`test_unknown_node_id_raises_and_errors_the_canvas` fail, and rightly — a
genuinely corrupt live canvas would then stay `RUNNING` forever with no signal at
all. That trades a bounded, self-expiring cosmetic problem for a permanent silent
stall. The test caught it immediately; the store was the right place to fix it.

---

### 60. A node its parent doesn't list raises a bare `ValueError`

**Where:** `mint/worker/canvas/models.py::ChainNode.next_id`,
`mint/worker/canvas/engine.py::_chain_remaining`

**Symptom:** A canvas stops advancing, its status stays `RUNNING` forever, and
its message dead-letters after burning every retry.

**Root cause:** Both call `list.index(...)`, which raises `ValueError` when the
finished child isn't among the chain's children. `ValueError` is not a
`WorkerError`, so it escaped `complete()`'s handler, `Worker._advance_canvas`'s,
and `Coordinator._advance`'s alike — landing in the generic
`except Exception` added for issue #41. That nacks and retries, so the delivery
eventually dead-letters, but nothing ever records an outcome or a terminal
status: exactly the stall `_fail_node` exists to prevent, reached by a route that
bypasses it.

Reachable in supported usage: re-running `apply(canvas_id=...)` with a different
graph under an existing canvas id, which the coordinator's own docstring cites as
a reason node ids may repeat.

**The practical fix:** A typed `ChildNotInParentError(WorkerError)`, raised from a
single `ChainNode.index_of` that both call sites now use. Routing through
`WorkerError` is what gets the canvas its `ERROR` status.

---

### 61. The idempotency guarantee is narrower than documented

**Where:** `mint/worker/canvas/engine.py::complete` (docstring)

**Symptom:** None directly — this is a documentation defect, but the kind that
causes bugs downstream, because a caller reading it will not make their handler
idempotent.

**Root cause:** `complete()` claimed idempotency "under at-least-once redelivery
of the same outcome" without qualification. Only *group fan-in* is actually
deduplicated, by the fired guard. Chain sequencing is not: replaying a chain
step's outcome calls `chain.next_id()` again, which unconditionally returns the
next step and dispatches it a second time — cascading down the rest of the chain.
The window is real (an `ack()` that fails after the canvas has advanced) and is
exactly what issue #41's generic handler now retries into.

**The practical fix:** Say so. There is no engine-side fix without an idempotency
key on the work itself; what there was, was a docstring that discouraged callers
from adding one.

---

## Part 6 — Bugs found by a fifth review round, four of them in earlier fixes

Issues #62, #64, #67 and #69 were all introduced by fixes from Parts 4 and 5.
None of them existed in the original package. That ratio is the argument for
re-reviewing after every round rather than declaring the code clean once the
first list is closed.

### 62. Concurrent handlers commit past in-flight Kafka records

**Where:** `mint/worker/brokers/kafka.py::KafkaDelivery._commit`

**Symptom:** Under load, a Kafka worker restart loses a scattering of messages —
the same symptom as issue #26, which this code was written to fix.

**Root cause:** Issue #26 replaced a bare `consumer.commit()` with this record's
own offset, and its docstring said:

> Committing `offset + 1` for this record's partition only can at worst move the
> offset backwards under out-of-order acks, which replays (at-least-once) rather
> than drops.

Only the *backwards* direction is safe. `Worker.run()` settles up to
`max_concurrency` (32 by default) records concurrently on a single partition, so
they finish out of order in the other direction too. Settling offset 7 while 5
and 6 are still running commits the group to 8; a restart resumes there and 5 and
6 are never redelivered. The fix for a too-wide commit introduced a too-far-ahead
one, and the docstring's reassurance is what made it look settled.

**The practical fix:** Commit only the contiguous settled prefix.

```python
def settle(self, record: "ConsumerRecord") -> int | None:
    key = (record.topic, record.partition)
    self._settled[key].add(record.offset)
    commit_offset = None
    while inflight and inflight[0] in self._settled[key]:
        offset = inflight.pop(0)
        self._settled[key].discard(offset)
        commit_offset = offset + 1
    return commit_offset          # None while an earlier offset is still running
```

Tracking happens in `KafkaDelivery.__init__` rather than the consume loop — a
delivery existing *is* what "this offset is in flight" means, so nothing can hand
one out untracked, including the tests that construct deliveries directly.

---

### 63. `ProcessPoolExecutor` can never run a real `Worker`

**Where:** `mint/worker/executors/process_pool.py::execute`

**Symptom:** Every message on a worker configured with
`executor = ProcessPoolExecutor()` fails with `UnpicklableTaskError` and errors
its node. The executor has never worked for its actual purpose.

**Root cause:** `Worker._run_task` calls `executor.execute(self.process, input_obj)`,
so `_ensure_picklable` pickles a **bound method** — which drags the whole `Worker`
instance along. That instance holds `_inflight`, which always contains the
currently-running `asyncio.Task` while a message is being handled, plus a
semaphore, an event, and `_binding`'s live broker/store handles.

```
>>> pickle.dumps((worker.process, payload))
TypeError: cannot pickle '_asyncio.Task' object
```

The executor's own tests pass module-level functions (`double`, `boom`), which is
the one shape that *isn't* how `Worker` uses it — so a green suite proved nothing
about the real call path.

**The practical fix:** Teach `Worker` what not to carry across the boundary.

```python
def __getstate__(self) -> dict[str, object]:
    return {k: v for k, v in self.__dict__.items() if k not in _UNPICKLABLE_RUNTIME_STATE}

def __setstate__(self, state: dict[str, object]) -> None:
    self.__dict__.update(state)
    self._binding = None
    self._inflight = set()
    self._stopped = asyncio.Event()
    self._slots = asyncio.Semaphore(self.max_concurrency)
```

The child process only ever calls `process`; it has no use for any of it. A
worker's *own* dependencies must still be picklable — that constraint is real and
is exactly what `UnpicklableTaskError` reports, so there is a test that keeps the
guard honest by hanging an unpicklable attribute on a worker and expecting it to
fire.

---

### 64. A group's terminal branches skip its only de-duplication

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** Two legs of the same chord fail at once, under `PROPAGATE`, and the
enclosing chain dispatches its next step twice.

**Root cause:** Issue #57 added a PROPAGATE branch modelled on the existing ABORT
one — and inherited its structure, including returning *before*
`mark_child_done`. That call is the only de-duplication a group has. With two
workers failing two legs concurrently in embedded mode, both take the branch and
both return a group-level `ERROR`, so `_complete` records the group twice and
walks to the parent twice.

ABORT was accidentally protected: it sets the canvas to `ERROR`, and the second
completion short-circuits on the status guard at the top of `complete()`.
PROPAGATE deliberately does not touch canvas status — it bubbles — so nothing
stopped it.

**The practical fix:** Claim the group's single terminal slot, reusing the
callback-fired guard:

```python
if not await self.store.claim_group_terminal(canvas_id, group.id):
    return None, None
```

Sharing that key is not a shortcut, it's the correct semantics: a group emits
exactly one terminal event — it fires its callback, or it aborts/propagates,
never both. Both branches now claim it, so ABORT no longer depends on a side
effect of the status guard to be safe.

---

### 65. The RabbitMQ consumer connection races its own lazy init

**Where:** `mint/worker/brokers/rabbitmq.py::_ensure_consumer_connection`

**Symptom:** A multi-worker app leaks connections that keep consuming after
`close()`.

**Root cause:** Issue #49 moved consumers off the shared channel pool onto a
dedicated connection, built lazily:

```python
if self._consumer_connection is None:
    self._consumer_connection = await connect_robust(self.uri)
```

Check, then `await`. `WorkerApp.run()` starts every worker's `run()` as a
concurrent task and each calls `consume()`, so with N workers all N see `None`,
all N connect, and the last assignment orphans the rest. `close()` closes only
the surviving reference; the orphans keep their consumers alive.

**The practical fix:** An `asyncio.Lock` around the check-and-build. The lock is
constructed in `__init__`, which is safe without a running loop on modern Python
— the same constraint that made the pools lazy in the first place (bug #20).

---

### 66. The handler's catch-all bypasses `max_attempts`

**Where:** `mint/worker/worker.py::_handle`

**Symptom:** The unbounded retry storm issue #43 exists to prevent, reachable
through the handler added by issue #41.

**Root cause:** The generic `except Exception` settled with
`_safe_nack(delivery, requeue=True)` — unconditional, ignoring the cap sitting
right beside it in `_retry_or_drop`. A persistently failing `delivery.ack()`
(closed channel, broker hiccup) therefore ran the work, failed to ack, requeued,
and ran the work again, forever.

**The practical fix:** Route it through `_retry_or_drop` like every other failure
path. `node_id` becomes optional, since the catch-all may fire before an envelope
was ever decoded.

---

### 67. Untracking before advancing strands a dead-lettered result

**Where:** `mint/worker/coordinator.py::_handle_result`

**Symptom:** A canvas stays `RUNNING` forever with nothing able to advance it —
and the timeout sweeper, which exists for exactly this, never fires.

**Root cause:** `_untrack` ran before `_advance`. Once issue #56 added a retry
cap, a result that failed to advance at `max_attempts` was dead-lettered — and by
then the node was neither completed nor tracked, so the sweeper had nothing left
to time out. The two fixes were individually correct and jointly wrong.

**The practical fix:** Untrack only after the advance succeeds. A failed advance
leaves the node tracked, so the sweeper can still fail it.

---

### 68. A reused `canvas_id` is dead on arrival

**Where:** `mint/worker/stores/redis.py::create_canvas`,
`mint/worker/stores/memory.py::create_canvas`

**Symptom:** Retrying `apply(canvas_id=...)` after a failed first attempt does
nothing at all. The nodes are written, the entry message is published, and the
canvas never advances.

**Root cause:** `Chain.apply`/`Chord.apply` accept a caller-supplied `canvas_id`
explicitly for idempotent retries, and a publish failure marks the canvas
`ERROR`. But neither store reset the status on creation — Redis never wrote it,
and `MemoryCanvasStore` used `setdefault`. Issue #59 then made a terminal status
*outlive* its data by design, which turned a narrow window into a permanent one:
the retry inherits `ERROR`, every `complete()` short-circuits on the non-RUNNING
guard and acks, and nothing ever runs.

**The practical fix:** `create_canvas` sets the status to `RUNNING`
unconditionally. Creating a canvas is a statement that it is live.

---

### 69. `ABORT` leaves the group's callback `PENDING`

**Where:** `mint/worker/canvas/engine.py::_advance_group`

**Symptom:** After an abort, the chord's callback still reads `PENDING`, as
though it were still expected.

**Root cause:** Issue #57's PROPAGATE branch cancels the callback along with the
unfinished legs; the older ABORT branch cancelled only `_unfinished(group)`,
which is legs. The new fix was more correct than the code it was modelled on, and
the asymmetry stayed. `ErrorPolicy.ABORT`'s docstring also promised to "cancel
every pending sibling", which `_abort_canvas` never did — it only ever touched
the immediate container.

**The practical fix:** ABORT uses `_cancel_group_remainder` too, and the enum
docstring now describes what actually happens: the container's remainder is
cancelled and the canvas is marked `ERROR`, which is what stops everything else —
not a per-node sweep.

---

### 70. `XAUTOCLAIM` reclaims this consumer's own in-flight work

**Where:** `mint/worker/brokers/redis.py::_reclaim`

**Symptom:** A handler that takes longer than `reclaim_idle_ms` finds its own
message being processed a second time, concurrently with the first.

**Root cause:** Issue #58 added reclaim to make the at-least-once claim true.
`XAUTOCLAIM` matches purely on idle time and has no notion of "someone else's" —
an entry this very consumer is still working on is idle by that definition too.
With the 60s default and `max_concurrency=32`, any slow handler self-duplicates.
The constructor docstring noted that `reclaim_idle_ms` should exceed the slowest
handler, which describes the tuning but not the failure it guards against.

**The practical fix:** Track the ids this broker currently holds and skip them:

```python
if message_id in self._inflight_ids:
    return None
```

Added when an entry is turned into a `StreamEntry`, discarded in `_retire`, so
the set is exactly "delivered and not yet settled".

---

## Part 7 — Bugs found by a sixth review round

Three of these five came from Part 6's own fixes, and two of them are the same
mistake in opposite directions: a guard that has to be taken *before* some I/O,
and released *after* it fails.

### 71. `rollback` releases only one of the guards a call burned

**Where:** `mint/worker/canvas/engine.py::rollback`

**Symptom:** A chord's callback is lost and its canvas hangs — the exact failure
issue #23 introduced `rollback` to prevent, now reachable through a different
door.

**Root cause:** Issue #64 gave the terminal branches their own one-shot claim
(`claim_group_terminal`), which burns the same key `mark_child_done` uses. A
single `complete()` walk can therefore burn *several* guards: an inner group
claiming its terminal slot, bubbling an ERROR outward, and an outer group then
fan-in-firing its callback. But `Dispatch.group_id` named only the group that
produced the dispatch.

Reproduced: an outer chord (CONTINUE, with a callback) whose legs are an inner
chord (PROPAGATE) and a plain task. The task finishes. The inner chord's leg then
errors — inner guard burned, group ERROR bubbles, outer fan-in completes and
returns the callback dispatch. If that publish fails, `rollback` resets only the
*outer* guard; on redelivery the inner group's `claim_group_terminal` returns
False, the walk stops there, `complete()` returns `[]`, and the worker acks.

**The practical fix:** Carry the whole set.

```python
claimed: list[str] = []            # accumulated across the walk
...
if dispatch is not None:
    return [replace(dispatch, claimed_groups=tuple(claimed))]
```

`Dispatch.group_id` is gone; `claimed_groups` replaces it, and `rollback` releases
every entry. Releasing only the last one leaves the inner guards burned, so the
redelivery stops at the first of them and the dispatch is lost anyway.

---

### 72. The sweeper and a live result can complete the same node

**Where:** `mint/worker/coordinator.py::_handle_result`

**Symptom:** A chain's next step is dispatched twice, on a canvas near its
`max_age`.

**Root cause:** `_timeout_node`'s own docstring states the invariant —
"untracking *is* the check: whichever of the two removes the entry first is the
one that gets to complete the node". Issue #67 then moved `_untrack` to *after*
`_advance`, to stop a dead-lettered result escaping the sweeper. That fix was
correct for what it addressed and silently broke the invariant it sat next to:
while `_handle_result` is inside `engine.complete()` doing store I/O, the entry
is still in `_in_flight`, so a concurrent sweep claims it and completes the same
node with a synthetic timeout.

**The practical fix:** Claim first, restore on failure — which satisfies both
requirements at once instead of trading one for the other.

```python
claim = self._claim(envelope.canvas_id, envelope.node_id)
if not await self._advance(...):
    self._restore(claim)          # the sweeper can still fail it later
    await self._retry_or_drop(delivery, envelope.node_id)
    return
await delivery.ack()
```

`_restore` puts back the *original* `InFlightNode`, keeping its `dispatched_at`.
Re-tracking with a fresh timestamp would silently grant the node another full
`max_age` before the sweeper looked at it again.

---

### 73. A failed sweeper advance strands its node

**Where:** `mint/worker/coordinator.py::_timeout_node`

**Symptom:** A canvas stays RUNNING with no error recorded, and no further sweep
ever revisits it.

**Root cause:** `_timeout_node` untracks and then advances. Unlike every other
failure path in the package, there is no delivery behind a synthetic timeout — so
when `_advance` returns False (a dispatch publish failed), nothing retries. The
node is untracked, so the sweeper won't fire again, and no broker redelivery
exists. Every other path has a retry cap precisely so failures end somewhere
observable; this one ended nowhere.

**The practical fix:** Put it back, using the same `_restore` as issue #72.

---

### 74. The Redis reclaim guard is keyed by stream id alone

**Where:** `mint/worker/brokers/redis.py::_reclaim`

**Symptom:** A genuinely abandoned entry on one topic is never reclaimed; worse,
an entry still being processed can be reclaimed and run concurrently with itself
— the very thing issue #70 added the guard to stop.

**Root cause:** `_inflight_ids` was a `set[bytes]` of stream ids. A Redis stream
id (`<ms>-<seq>`) is unique only *within its own stream*, and one `RedisBroker` is
shared across every worker in a `WorkerApp`. Two topics can hand out
`1700000000000-0` in the same millisecond, so topic A's in-flight entry masks
topic B's — and when A's entry is retired, `_retire` discards the shared id,
stripping the protection from B's entry while it is still live.

**The practical fix:** `set[tuple[str, bytes]]`. The same lesson as issue #29's
`(canvas_id, node_id)`: an identifier is only as unique as its scope.

---

### 75. Kafka offset tracking wedges on a rebalance

**Where:** `mint/worker/brokers/kafka.py::track`

**Symptom:** After a consumer-group rebalance, a partition stops committing
entirely. Everything replays on every restart, forever.

**Root cause:** Issue #62's tracking assumed each offset is delivered once. A
rebalance redelivers offsets that were fetched but never committed, so `track`
appended a duplicate: `[5, 5]`. `settle(5)` pops one instance *and* discards 5
from the settled set, leaving the twin at the head of the queue with nothing that
could ever clear it. From then on `settle` returns None for that partition
forever.

**The practical fix:** Make `track` idempotent per offset, and drop a stopped
consumer's bookkeeping so a re-consumed topic starts clean.

**Worth noting how nearly this was missed.** The first regression test asserted
only that the duplicate ack still committed offset 6 — which it does, even with
the bug. The damage shows on the *next* offset, which is blocked forever. The
test passed against the broken code until it was extended to settle a following
offset; a revert-and-watch-it-fail check is what caught that.

---

## Part 8 — Bugs found by a seventh review round

The headline one, #76, is a case of two correct requirements pulling in opposite
directions — and an earlier fix satisfying one of them by breaking the other
without anyone noticing, because the code that made it a problem arrived two
rounds later.

### 76. Shutdown tears down the transport under in-flight handlers

**Where:** `mint/worker/app.py::_shutdown`

**Symptom:** After a graceful shutdown, a node that had already completed runs
again on restart — sometimes twice.

**Root cause:** Two requirements that the ordering has to satisfy at once:

- Bug #19 established **cancel the consume loops before draining**, or a loop
  left alive picks up whatever a drain timeout just requeued and re-nacks it.
- Issue #49 later gave RabbitMQ consumers their own channel, closed in
  `consume()`'s `finally`. Kafka's `finally` stops its consumer and forgets its
  offsets.

Cancelling `run()` unwinds its `async for`, which finalises that generator and
runs the cleanup. So by the time `_drain_workers()` ran, the channel a handler
needs in order to `ack()` was already closed. The ack raises, `_handle`'s
catch-all nacks, `redeliver()` republishes a duplicate through the still-open
*publish* pool, and RabbitMQ redelivers the unacked original too. The canvas has
already advanced, so the node re-runs — twice.

This is exactly what `_drain_workers`' own docstring says the two-phase drain
prevents. The protection was defeated one step earlier, at the cancel, by a fix
that landed two rounds after the docstring was written.

**The practical fix:** Separate *stopping the loop* from *releasing the
transport*, so both orderings can hold.

```python
# Worker keeps its own generator rather than iterating an anonymous one
self._consumer = binding.broker.consume(self.topic)
async for delivery in self._consumer:
    ...
```

```python
# app._shutdown
for task in self._tasks.values():
    task.cancel()
await asyncio.gather(*self._tasks.values(), return_exceptions=True)
await self._drain_workers()                     # transport still alive
for worker in self._workers.values():
    await worker.close_consumer()               # released only now
```

---

### 77. A `WorkerApp` cannot be restarted

**Where:** `mint/worker/app.py::_shutdown`

**Symptom:** A second `app.run()` consumes nothing, exits immediately, and
increments one topic's attempt counter on the way out.

**Root cause:** `_shutdown` resets `_running` and clears `_stop_event` — work that
is only meaningful if `run()` may be called again, which the
`AppAlreadyRunningError` guard implies it may. But `Worker._stopped`, set by
`stop_consuming()`, was never cleared anywhere. On the second run every worker
takes the `if self._stopped.is_set()` branch on its first delivery, nacks it with
`attempt + 1`, and returns; `_on_worker_exit` (issue #44) then sees a loop that
exited on its own and stops the app again.

Two fixes agreeing on half a contract each: the app reset its own state, the
worker never reset its.

**The practical fix:** `Worker.resume_consuming()`, called for every worker at the
end of `_shutdown`.

---

### 78. Signal handlers are installed and never removed

**Where:** `mint/worker/app.py::_install_signal_handlers`,
`mint/worker/coordinator.py` (same shape)

**Symptom:** After `run()` returns, Ctrl-C does nothing. The process is
uninterruptible for whatever runs next.

**Root cause:** `loop.add_signal_handler(sig, self._stop_event.set)` is
loop-global and outlives the app. Once `run()` completes, SIGINT/SIGTERM still
route to an event nothing is awaiting — and the default handler, which would have
raised `KeyboardInterrupt` or terminated the process, has been displaced.

**The practical fix:** `loop.remove_signal_handler(sig)` in `_shutdown`,
suppressing both `NotImplementedError` (platforms without signal support, matching
the install side) and `ValueError`.

---

### 79. A failed request publish leaks its pending entry

**Where:** `mint/worker/executors/amqp_rpc.py::_call`

**Symptom:** `_pending` grows by one entry — and one future nothing will ever
resolve — for every RPC attempted while the broker is down.

**Root cause:** The correlation id is registered *before* publishing, correctly:
a reply can arrive the instant the request lands. But only `_await_reply`'s
`finally` pops the map, and a raising publish never reaches it. This is the same
unbounded leak bug #11 fixed for lost replies and issue #51 fixed for cancelled
calls, arriving from the third direction.

**The practical fix:** A `try/except` around the publish that pops and re-raises.
Keeping the registration before the publish (rather than moving it after) is
deliberate — the race it guards against is real.

---

### 80. Kafka records offsets that were never queued

**Where:** `mint/worker/brokers/kafka.py::settle`

**Symptom:** A commit advances past a record whose handler is still running —
silent loss on a later crash.

**Root cause:** `settle` added the offset to `_settled` before checking it was
actually in flight. That is reachable precisely *because* issue #75 made `track`
deduplicate: after a rebalance two `KafkaDelivery` objects exist for one offset
but only one queue entry does. The first settle pops it; the second adds the
offset to `_settled` with nothing to pop, and it stays there. If that offset is
tracked again after a later rebalance, it counts as settled the moment it reaches
the head of the queue.

**The practical fix:** `if inflight is None or record.offset not in inflight:
return None`. The fix for a duplicate in one structure has to be mirrored in the
other structure that indexes it.

---

### 81. Dropping offset bookkeeping loses in-flight commits silently

**Where:** `mint/worker/brokers/kafka.py::_forget_offsets`

**Symptom:** Messages in flight when a consumer stops are reprocessed on restart,
with nothing in the logs to say so.

**Root cause:** `_forget_offsets` runs in `consume()`'s `finally` — on shutdown
cancellation, or on a broker error propagating out of the iterator — while
handlers may still be running. Their later `ack()` reaches `settle()`, finds no
in-flight list, and returns `None`, so `_commit()` commits nothing at all.

With issue #76's fix the generator now closes *after* the drain, so the common
path is clean. The error path is not, and reprocessing is at-least-once behaviour
rather than a correctness break — but it should be visible.

**The practical fix:** Log a warning naming the topic, partition, and how many
offsets were dropped unsettled. Not every fix is a behaviour change; some are
just refusing to be silent.

---

### 82. An empty `Chain`/`Chord` escapes the `WorkerError` hierarchy

**Where:** `mint/worker/canvas/builder.py`

**Symptom:** `Chain([])` raises a raw pydantic `ValidationError` from `build()`
(or `IndexError` from `publish_entries` first), neither of which is a
`WorkerError`.

**Root cause:** Nothing validated the constructor argument; the failure was left
to `ChainNode.children`'s `min_length=1`, several calls later. `exc.py` establishes
that every DSL failure is a typed `WorkerError` — `ChildNotInParentError`'s
docstring spells out that exact reasoning for `list.index` — and this slipped
through it.

**The practical fix:** `EmptyContainerError`, raised in both constructors. Note
there is deliberately *no* second check after flattening: an inner chain raises at
its own construction, so flattening can never produce an empty one. That check was
written, found unreachable, and removed rather than left as reassuring dead code.

---

### 83. The Redis key registry inherits a TTL across a `canvas_id` reuse

**Where:** `mint/worker/stores/redis.py::create_canvas`

**Symptom:** A retried canvas leaks its node and result keys permanently.

**Root cause:** `set_canvas_status` expires the key-registry set along with the
data. `create_canvas` for a reused `canvas_id` — the retry path issue #68 exists
to support — rewrites the node keys via `MSET`, which clears *their* TTL, but
`_track`'s `SADD` does not clear the registry's. If the retry outlives that
expiry the registry vanishes mid-run, and every key tracked before that point is
invisible to the final expire sweep.

**The practical fix:** `PERSIST` the registry key in `create_canvas`. Same class
of bug as #68 — a reused canvas inheriting the previous attempt's expiry state —
found one key deeper.

---

### 84. One slow handler blocks reclaiming abandoned entries

**Where:** `mint/worker/brokers/redis.py::_reclaim`

**Symptom:** A genuinely dead consumer's messages are never recovered while this
consumer has one slow handler running.

**Root cause:** Issue #70 made `_reclaim` skip entries this consumer already
holds. But `XAUTOCLAIM` scans in id order from `0-0` with `count=1`, so a
self-owned entry past `reclaim_idle_ms` is the one returned *every* poll — and
returning `None` there means nothing behind it is ever examined. The guard that
stopped self-duplication introduced a head-of-line block.

**The practical fix:** Advance the cursor past self-owned entries and keep
looking, bounded by `RECLAIM_SCAN_LIMIT` per poll so a large pending list cannot
stall the consume loop.

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
- **A one-shot guard burned before an I/O call must be releasable if that call
  fails.** Anything that records "this already happened" before the thing has
  actually happened needs a rollback on the failure path, or the retry finds the
  work already claimed and silently does nothing (issue #23).
- **A shared client object needs per-topic (or per-key) state, not one slot.**
  `WorkerApp` shares one broker across every worker, so any `self._x = ...` in a
  per-topic method is a collision waiting to happen (issue #25).
- **Every branch that drops a message must still record an outcome for its
  node**, whenever the node is identifiable. Dropping the message and saying
  nothing leaves the canvas waiting on it forever (issue #27).
- **A consume loop needs a concurrency bound of its own.** Some brokers provide
  backpressure and some don't; the loop can't tell which it has (issue #28).
- **Anything keyed by node id must be keyed by `(canvas_id, node_id)`.** Node
  ids are caller-suppliable, so they are only unique within a canvas (issue #29).
- **A string-mangling function used to derive a key must be injective.** Ask
  "can two different inputs produce this same output?" before it becomes a
  cursor, a lock name, or a stream name (issue #39).
- **When a fix lands on one broker, check its siblings for the same shape.**
  Bug #21's DLQ-terminates-the-chain rule was fixed in RabbitMQ and left broken
  in NATS for exactly as long as nobody looked (issue #40).
- **Every path out of a handler must settle its delivery exactly once.** Catch
  more than `CancelledError`, and make sure the "not settled yet" window ends
  where the ack happens, not where the function returns (issues #41, #42).
- **A background task nobody awaits needs a done-callback.** Otherwise its
  exception is never retrieved and the process keeps looking healthy while doing
  none of its work (issues #44, #45).
- **A retry path needs a cap.** `Delivery.attempt` is maintained by every broker
  precisely so something can read it (issue #43).
- **A cancellation deadline only *requests* cancellation.** Wait for it to land
  before tearing down anything those tasks are still using (issue #46).
- **A snapshot plus an `await` per item is a race.** Re-check the claim inside
  the loop; whoever removes the entry first owns the work (issue #47).
- **Prove an encoding injective exhaustively, not with examples.** Hand-picked
  pairs are exactly what let a non-injective fix look correct twice in a row
  (issues #39, #50).
- **"Pre-existing" is a claim to verify against the base branch.** A repo-wide
  lint failure introduced by a lock-file bump looks identical to one that was
  always there (issue #54).
- **Check a docstring's claim against the code, not the other way round.** Three
  separate bugs here were a comment describing behaviour that was never
  implemented — an incompatible context manager, a non-existent redelivery
  fallback, and an over-broad idempotency promise (issues #55, #58, #61).
- **Read the third-party source when a contract matters.** `no_ack=True` versus
  `message.process()` is settled in twenty lines of aio-pika; no amount of
  reasoning about the API would have found it (issue #55).
- **A policy enum must be honoured on every branch that can observe it.** Reading
  `error_policy` on two of three paths made PROPAGATE silently equal CONTINUE
  (issue #57).
- **A fix that makes an existing test fail deserves a second look before the test
  is changed.** Issue #59's first attempt did, and the test was right (issue #59).
- **Anything a Protocol advertises has to be true.** Declaring
  `AT_LEAST_ONCE` while never reclaiming an abandoned pending entry is a promise
  the rest of the package is built on (issue #58).
- **Review the fixes, not just the original code.** Nine of the bugs in this
  catalogue were introduced by an earlier round's fix, and four of those came
  from a single round (issues #62, #64, #67, #69).
- **A narrowing fix can overshoot in the other direction.** Replacing a too-wide
  Kafka commit with a per-record one made it commit too far *ahead* instead
  (issue #62).
- **Test the call path the production code actually uses.** `ProcessPoolExecutor`
  was only ever tested with module-level functions — the one shape `Worker` never
  passes it (issue #63).
- **Copying a branch copies its bugs.** The PROPAGATE branch inherited ABORT's
  missing de-duplication; ABORT then inherited PROPAGATE's missing callback
  cancellation in the other direction (issues #64, #69).
- **Two individually correct fixes can be jointly wrong.** A retry cap plus an
  early untrack meant a dead-lettered result escaped the sweeper entirely
  (issue #67).
- **A fix that moves a guard can break an invariant documented right beside it.**
  Moving `_untrack` after the advance silently violated the mutual exclusion
  `_timeout_node`'s own docstring described (issue #72). Claim-then-restore
  satisfied both requirements; trading one for the other never does.
- **An identifier is only as unique as its scope.** Node ids are unique per
  canvas, Redis stream ids per stream — key by the pair, always (issues #29, #74).
- **Bookkeeping that assumes exactly-once delivery breaks under redelivery.**
  Offset tracking, in a broker whose whole point is at-least-once (issue #75).
- **Assert on the state *after* the damage would show.** The rebalance test
  passed against the broken code, because the duplicate only blocks the *next*
  offset. Reverting the fix and watching the test fail is what exposed it
  (issue #75).
- **A docstring describing an ordering is a constraint on every later change.**
  Bug #19 fixed the shutdown order; issue #49 later made that order unsafe, and
  nothing connected the two for two rounds (issue #76).
- **When two requirements conflict, look for the separation that satisfies both.**
  Cancel-before-drain and release-transport-after-drain only conflicted while the
  loop and the transport were the same lifetime (issue #76).
- **Lifecycle state lives in more than one object.** The app reset its own and
  left the workers', so restarting silently did nothing (issue #77).
- **Anything installed process-wide has to be uninstalled** — signal handlers
  outlive the object that added them (issue #78).
- **De-duplicating one structure invalidates every structure that indexes it.**
  Making `track` skip duplicates left `settle` recording offsets with nothing to
  pop (issue #80).
- **A guard that returns early can become a head-of-line block.** Skipping
  self-owned entries stopped self-duplication and stopped everything behind them
  too (issue #84).
- **Delete a check you find unreachable** rather than leaving it as reassuring
  dead code (issue #82).
