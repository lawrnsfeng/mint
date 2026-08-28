"""Redis-backed ICanvasStore: namespaced keys, atomic Lua fan-in, TTL on terminal canvases.

The fan-in decision (§2 of the design: "am I the last child, and am I the one who
gets to fire the callback") has to be a single atomic round trip — SADD then SCARD
as two separate calls would race under real concurrency. ``FAN_IN_SCRIPT`` does both,
plus the SETNX callback-fired guard, in one Lua script.

The SETNX on the fired key is the *whole* exactly-once guarantee: SADD makes the
done-set idempotent under redelivery, and SETNX makes exactly one caller the one
that fires. The script deliberately does **not** also require ``added == 1``.
Requiring it would mean a redelivered child can never fire the callback — which is
exactly the state a canvas gets stuck in when a dispatch is authorised, the fired
guard is burned, and the publish then fails. ``reset_group_fired`` releases the
guard in that case, and the redelivery is what re-fires it.

The script is registered via ``Redis.register_script`` rather than a
process-cached ``EVALSHA``: redis-py's ``Script`` reloads itself on ``NOSCRIPT``,
so a ``SCRIPT FLUSH``, a restart, or a failover to a replica that never saw the
load doesn't permanently break every chord's fan-in.
"""

from collections.abc import Mapping, Sequence
from typing import Final

from redis.asyncio import Redis
from redis.commands.core import AsyncScript

from mint.worker.canvas.models import AnyNode, NodeAdapter, NodeOutcome
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.stores.interface import GroupProgress

FAN_IN_SCRIPT: Final[str] = """
local added = redis.call('SADD', KEYS[1], ARGV[1])
local done_count = redis.call('SCARD', KEYS[1])
local fired = 0
if tonumber(done_count) == tonumber(ARGV[2]) then
    if redis.call('SETNX', KEYS[2], '1') == 1 then
        fired = 1
    end
end
return {added, done_count, fired}
"""


class RedisCanvasStore:
    """Canvas graph store over Redis: namespaced keys, atomic fan-in, TTL on terminal canvases."""

    DEFAULT_TERMINAL_TTL_SECONDS: Final[int] = 86_400  # 24h
    # The status key is a canvas's tombstone, and outlives the data it describes.
    # Expiring it *with* the data destroyed the one fact CanvasEngine.complete()'s
    # short-circuit depends on: once gone, get_canvas_status defaults back to
    # RUNNING, so a finished canvas reads as live and a late replay walks into it.
    STATUS_TTL_MULTIPLIER: Final[int] = 7

    def __init__(
        self,
        uri: str,
        *,
        namespace: str = "mint-worker",
        terminal_ttl_seconds: int = DEFAULT_TERMINAL_TTL_SECONDS,
        status_ttl_seconds: int | None = None,
    ) -> None:
        """Configure a store over ``uri``; keys are namespaced under ``namespace``.

        ``status_ttl_seconds`` defaults to ``STATUS_TTL_MULTIPLIER`` times
        ``terminal_ttl_seconds`` — a canvas's status must outlive its data so that
        "this already finished" stays answerable after the graph itself is gone.
        """
        self.uri = uri
        self.namespace = namespace
        self.terminal_ttl_seconds = terminal_ttl_seconds
        self.status_ttl_seconds = status_ttl_seconds or (
            terminal_ttl_seconds * self.STATUS_TTL_MULTIPLIER
        )
        self._client: Redis[bytes] | None = None
        self._fan_in_script: AsyncScript | None = None

    @property
    def client(self) -> "Redis[bytes]":
        """Return the lazily-connected Redis client."""
        if self._client is None:
            self._client = Redis.from_url(self.uri)
        return self._client

    def _node_key(self, canvas_id: str, node_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:node:{node_id}"

    def _result_key(self, canvas_id: str, node_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:result:{node_id}"

    def _group_done_key(self, canvas_id: str, group_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:group:{group_id}:done"

    def _group_fired_key(self, canvas_id: str, group_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:group:{group_id}:fired"

    def _status_key(self, canvas_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:status"

    def _key_registry_key(self, canvas_id: str) -> str:
        return f"{self.namespace}:canvas:{canvas_id}:keys"

    async def _track(self, canvas_id: str, *keys: str) -> None:
        """Record ``keys`` under ``canvas_id`` so a terminal status can expire them all."""
        await self.client.sadd(self._key_registry_key(canvas_id), *keys)

    async def create_canvas(self, canvas_id: str, nodes: Mapping[str, AnyNode]) -> None:
        """Persist every node of a freshly built canvas in one call."""
        if not nodes:
            return
        mp_str_bytes: dict[str | bytes, bytes | float | str] = {
            self._node_key(canvas_id, node_id): node.model_dump_json().encode()
            for node_id, node in nodes.items()
        }
        await self.client.mset(mp_str_bytes)
        await self._track(canvas_id, *(str(key) for key in mp_str_bytes))
        # SADD does not clear an existing TTL, so a reused canvas_id would inherit
        # the previous run's expiry on the registry. If the retry outlived it the
        # registry would vanish mid-run and every key tracked before that point
        # would be invisible to the final expire sweep — leaked permanently.
        await self.client.persist(self._key_registry_key(canvas_id))
        # Explicitly RUNNING, not merely absent. apply() accepts a caller-supplied
        # canvas_id for idempotent retries, and a terminal status from a previous
        # attempt outlives the data by design (see set_canvas_status) — so without
        # this a retry would short-circuit every completion and never advance.
        await self.set_canvas_status(canvas_id, CanvasStatus.RUNNING)

    async def get_node(self, canvas_id: str, node_id: str) -> AnyNode | None:
        """Look up a single node, or None if it does not exist."""
        data = await self.client.get(self._node_key(canvas_id, node_id))
        if data is None:
            return None
        return NodeAdapter.validate_json(data)

    async def set_node_status(self, canvas_id: str, node_id: str, status: NodeStatus) -> None:
        """Update a node's status. A no-op if the node does not exist."""
        node = await self.get_node(canvas_id, node_id)
        if node is None:
            return
        updated = node.model_copy(update={"status": status})
        await self.client.set(
            self._node_key(canvas_id, node_id),
            updated.model_dump_json().encode(),
        )

    async def mark_node_running(self, canvas_id: str, node_id: str) -> None:
        """Move a node from PENDING to RUNNING. A no-op from any other status."""
        node = await self.get_node(canvas_id, node_id)
        if node is None or node.status != NodeStatus.PENDING:
            return
        await self.set_node_status(canvas_id, node_id, NodeStatus.RUNNING)

    async def cancel_nodes(self, canvas_id: str, node_ids: Sequence[str]) -> None:
        """Mark every listed node CANCELLED."""
        for node_id in node_ids:
            await self.set_node_status(canvas_id, node_id, NodeStatus.CANCELLED)

    async def set_result(self, canvas_id: str, node_id: str, outcome: NodeOutcome) -> None:
        """Persist a node's terminal outcome."""
        key = self._result_key(canvas_id, node_id)
        await self.client.set(key, outcome.model_dump_json().encode())
        await self._track(canvas_id, key)

    async def get_result(self, canvas_id: str, node_id: str) -> NodeOutcome | None:
        """Look up a single node's outcome, or None if it has not finished."""
        data = await self.client.get(self._result_key(canvas_id, node_id))
        if data is None:
            return None
        return NodeOutcome.model_validate_json(data)

    async def get_results(
        self,
        canvas_id: str,
        node_ids: Sequence[str],
    ) -> dict[str, NodeOutcome]:
        """Look up outcomes for every listed node that has one recorded."""
        if not node_ids:
            return {}
        keys = [self._result_key(canvas_id, node_id) for node_id in node_ids]
        values = await self.client.mget(keys)
        mp_str_outcome: dict[str, NodeOutcome] = {}
        for node_id, data in zip(node_ids, values, strict=True):
            if data is not None:
                mp_str_outcome[node_id] = NodeOutcome.model_validate_json(data)
        return mp_str_outcome

    async def mark_child_done(
        self,
        canvas_id: str,
        group_id: str,
        child_id: str,
        num_children: int,
    ) -> GroupProgress:
        """Atomically record one group child as done and report fan-in progress."""
        script = self._ensure_fan_in_script()
        done_key = self._group_done_key(canvas_id, group_id)
        fired_key = self._group_fired_key(canvas_id, group_id)
        await self._track(canvas_id, done_key, fired_key)
        added, done_count, fired = await script(
            keys=[done_key, fired_key],
            args=[child_id, num_children],
        )
        return GroupProgress(added=bool(added), done_count=int(done_count), fired=bool(fired))

    async def claim_group_terminal(self, canvas_id: str, group_id: str) -> bool:
        """Claim the right to emit this group's single terminal outcome. True if won."""
        fired_key = self._group_fired_key(canvas_id, group_id)
        await self._track(canvas_id, fired_key)
        return bool(await self.client.setnx(fired_key, b"1"))

    async def reset_group_fired(self, canvas_id: str, group_id: str) -> None:
        """Release this group's callback-fired guard so a redelivery can re-fire it."""
        await self.client.delete(self._group_fired_key(canvas_id, group_id))

    def _ensure_fan_in_script(self) -> AsyncScript:
        if self._fan_in_script is None:
            self._fan_in_script = self.client.register_script(FAN_IN_SCRIPT)
        return self._fan_in_script

    async def get_canvas_status(self, canvas_id: str) -> CanvasStatus:
        """Return a canvas's status, defaulting to RUNNING if never set."""
        data = await self.client.get(self._status_key(canvas_id))
        if data is None:
            return CanvasStatus.RUNNING
        return CanvasStatus(data.decode())

    async def set_canvas_status(self, canvas_id: str, status: CanvasStatus) -> None:
        """Update a canvas's overall status.

        Reaching a terminal status (FINISHED/ERROR) expires every key tracked for
        this canvas — nodes, results, and fan-in bookkeeping alike — so a completed
        canvas doesn't linger in Redis forever. A RUNNING canvas gets no TTL: its
        data must survive for as long as the canvas is actually in flight.

        The status key itself gets a longer TTL than the data. It is the canvas's
        tombstone: ``CanvasEngine.complete`` short-circuits on a non-RUNNING status,
        and ``get_canvas_status`` reports RUNNING for a key that isn't there — so
        expiring the status alongside the data made a finished canvas read as live
        again, and a late replay would then fail on its long-deleted nodes and mark
        the finished canvas ERROR. Outliving the data closes that window.
        """
        key = self._status_key(canvas_id)
        await self.client.set(key, status.value.encode())
        if status == CanvasStatus.RUNNING:
            return
        registry_key = self._key_registry_key(canvas_id)
        tracked = await self.client.smembers(registry_key)
        for tracked_key in tracked:
            await self.client.expire(tracked_key, self.terminal_ttl_seconds)
        await self.client.expire(registry_key, self.terminal_ttl_seconds)
        await self.client.expire(key, self.status_ttl_seconds)

    async def close(self) -> None:
        """Release the underlying Redis connection."""
        if self._client is not None:
            # redis-py's own .close() is deprecated in favor of .aclose() since
            # 5.0.1, but the installed stub package doesn't declare aclose() on
            # Redis (verified) even though it exists and works at runtime.
            await self._client.aclose()  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
