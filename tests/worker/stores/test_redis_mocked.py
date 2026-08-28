"""RedisCanvasStore: key naming, Lua fan-in wiring, and terminal-TTL, all against a mocked client.

No Docker here — every assertion is about what our code calls on the client, which
is exactly the class of bug (wrong method, wrong keys, missing await) that doesn't
need a real Redis to catch. Lua atomicity itself is a real-concurrency property that
genuinely needs a container — see ``test_redis_container.py``.
"""

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import pytest

from mint.worker.canvas.models import GroupNode, NodeOutcome, TaskNode
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.stores.redis import FAN_IN_SCRIPT, RedisCanvasStore

if TYPE_CHECKING:
    from pytest_mock.plugin import MockerFixture

CANVAS = "c1"


@pytest.fixture
def mock_client(mocker: "MockerFixture") -> AsyncMock:
    """Return a mocked async Redis client double.

    Deliberately NOT ``spec=Redis``: redis-py's command methods aren't detected
    as coroutine functions by ``inspect.iscoroutinefunction`` (verified — even
    bare ``create_autospec(Redis, instance=True)`` reproduces this), so spec'ing
    against the real class silently produces sync ``MagicMock`` children that
    can't be awaited. A bare ``AsyncMock()`` auto-asyncs every child attribute.

    ``register_script`` is the one exception and has to be overridden back to a
    sync mock: on a real client it is an ordinary method returning an
    ``AsyncScript``, not a coroutine function, and an auto-asynced child would
    hand the store a coroutine where it expects a callable script.
    """
    client = mocker.AsyncMock()
    client.register_script = mocker.MagicMock(return_value=mocker.AsyncMock())
    return client


@pytest.fixture
def store(mock_client: AsyncMock) -> RedisCanvasStore:
    """Return a store wired directly to the mock client, skipping real connection setup."""
    instance = RedisCanvasStore("redis://fake")
    instance._client = mock_client
    return instance


class TestKeyNamespacing:
    """Every key must be namespaced under `{namespace}:canvas:{canvas_id}:...`."""

    async def test_create_canvas_writes_namespaced_node_keys(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Mset must be called with one namespaced key per node, JSON-encoded."""
        node = TaskNode(id="t1", canvas_id=CANVAS, topic="topic")

        await store.create_canvas(CANVAS, {"t1": node})

        mock_client.mset.assert_awaited_once()
        (written,) = mock_client.mset.await_args.args
        assert written == {f"mint-worker:canvas:{CANVAS}:node:t1": node.model_dump_json().encode()}

    async def test_create_canvas_with_no_nodes_does_not_call_mset(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """An empty node map must be a no-op, not an mset({}) call."""
        await store.create_canvas(CANVAS, {})

        mock_client.mset.assert_not_awaited()

    async def test_get_node_reads_the_namespaced_key(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """get_node must read from exactly the same key create_canvas writes to."""
        node = TaskNode(id="t1", canvas_id=CANVAS, topic="topic")
        mock_client.get.return_value = node.model_dump_json().encode()

        result = await store.get_node(CANVAS, "t1")

        mock_client.get.assert_awaited_once_with(f"mint-worker:canvas:{CANVAS}:node:t1")
        assert result == node

    async def test_get_node_returns_none_when_missing(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A missing key must decode to None, not raise."""
        mock_client.get.return_value = None

        assert await store.get_node(CANVAS, "ghost") is None

    async def test_custom_namespace_is_honored(self, mocker: "MockerFixture") -> None:
        """A store built with a custom namespace must use it in every key."""
        mock_client = mocker.AsyncMock()
        store = RedisCanvasStore("redis://fake", namespace="easyrag")
        store._client = mock_client
        mock_client.get.return_value = None

        await store.get_node(CANVAS, "t1")

        mock_client.get.assert_awaited_once_with(f"easyrag:canvas:{CANVAS}:node:t1")


class TestNodeStatus:
    """set_node_status must read-modify-write, and no-op on a missing node."""

    async def test_set_node_status_on_a_missing_node_does_not_write(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """No node to update means no set() call at all."""
        mock_client.get.return_value = None

        await store.set_node_status(CANVAS, "ghost", NodeStatus.RUNNING)

        mock_client.set.assert_not_awaited()

    async def test_set_node_status_is_an_atomic_transition(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """The last client-side read-modify-write in this store, and the one that lost.

        Another process cancelling the node between the read and the write was
        silently overwritten, and `_complete`'s cancelled-node guard then never
        fired.
        """
        script = mock_client.register_script.return_value

        await store.set_node_status(CANVAS, "t1", NodeStatus.RUNNING)

        script.assert_awaited_once()
        assert script.await_args.kwargs["keys"] == [f"mint-worker:canvas:{CANVAS}:node:t1"]
        assert script.await_args.kwargs["args"][0] == NodeStatus.RUNNING.value
        mock_client.get.assert_not_awaited()
        mock_client.set.assert_not_awaited()

    async def test_set_node_status_does_not_overwrite_a_terminal_status(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A node that finished, errored or was cancelled has reached its conclusion."""
        script = mock_client.register_script.return_value

        await store.set_node_status(CANVAS, "t1", NodeStatus.FINISHED)

        allowed = script.await_args.kwargs["args"][1:]
        assert set(allowed) == {NodeStatus.PENDING.value, NodeStatus.RUNNING.value}


class TestFanIn:
    """mark_child_done must register and invoke the Lua script with the right keys/args."""

    async def test_mark_child_done_registers_the_script_once(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """The script is registered on first use and reused — not re-registered per call."""
        script = mock_client.register_script.return_value
        script.return_value = [1, 1, 0]

        await store.mark_child_done(CANVAS, "g", "leg1", 2)
        await store.mark_child_done(CANVAS, "g", "leg2", 2)

        mock_client.register_script.assert_called_once_with(FAN_IN_SCRIPT)
        assert script.await_count == 2

    async def test_mark_child_done_passes_the_right_keys_and_args(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """The script must receive the done-set key, fired key, child id, and num_children."""
        script = mock_client.register_script.return_value
        script.return_value = [1, 2, 1]

        await store.mark_child_done(CANVAS, "g", "leg2", 2)

        script.assert_awaited_once_with(
            keys=[
                f"mint-worker:canvas:{CANVAS}:group:g:done",
                f"mint-worker:canvas:{CANVAS}:group:g:fired",
            ],
            args=["leg2", 2],
        )

    async def test_mark_child_done_maps_the_script_result_to_group_progress(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """The [added, done_count, fired] array must map field-for-field."""
        script = mock_client.register_script.return_value
        script.return_value = [1, 2, 1]

        progress = await store.mark_child_done(CANVAS, "g", "leg2", 2)

        assert progress.added is True
        assert progress.done_count == 2
        assert progress.fired is True

    async def test_the_script_is_invoked_not_a_process_cached_sha(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Raw evalsha is never used — a Script object reloads itself after a SCRIPT FLUSH.

        A process-cached SHA plus a bare ``evalsha`` breaks permanently on
        ``NOSCRIPT`` (a flush, a restart, a failover to a replica that never saw
        the load), silently stopping every chord's fan-in until a redeploy.
        """
        mock_client.register_script.return_value.return_value = [1, 1, 0]

        await store.mark_child_done(CANVAS, "g", "leg1", 2)

        mock_client.evalsha.assert_not_awaited()
        mock_client.script_load.assert_not_awaited()


class TestResetGroupFired:
    """reset_group_fired must release the guard so a redelivery can re-fire the callback."""

    async def test_reset_group_fired_deletes_the_fired_key(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Releasing the guard is a DEL on that group's fired key, nothing else."""
        await store.reset_group_fired(CANVAS, "g")

        mock_client.delete.assert_awaited_once_with(
            f"mint-worker:canvas:{CANVAS}:group:g:fired",
        )


class TestTerminalTTL:
    """A terminal canvas status must expire every key tracked for that canvas; RUNNING must not."""

    async def test_running_status_sets_no_ttl(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Marking a canvas RUNNING must not call expire at all."""
        await store.set_canvas_status(CANVAS, CanvasStatus.RUNNING)

        mock_client.expire.assert_not_awaited()

    async def test_terminal_status_expires_every_tracked_key(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """FINISHED must expire every key returned by the tracked-key registry, plus itself."""
        node_key = f"mint-worker:canvas:{CANVAS}:node:t1"
        mock_client.smembers.return_value = {node_key.encode()}

        await store.set_canvas_status(CANVAS, CanvasStatus.FINISHED)

        expired_keys = {call.args[0] for call in mock_client.expire.await_args_list}
        # node_key comes back from the mocked SMEMBERS response, as real Redis would: bytes.
        # The registry/status keys are our own str variables, passed straight through.
        assert node_key.encode() in expired_keys
        assert f"mint-worker:canvas:{CANVAS}:status" in expired_keys
        assert f"mint-worker:canvas:{CANVAS}:keys" in expired_keys

    async def test_error_status_also_expires(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """ERROR is terminal too — same TTL treatment as FINISHED."""
        mock_client.smembers.return_value = set()

        await store.set_canvas_status(CANVAS, CanvasStatus.ERROR)

        mock_client.expire.assert_awaited()

    async def test_create_canvas_and_set_result_track_their_keys_for_later_expiry(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Every write that create_canvas/set_result/mark_child_done makes must be tracked."""
        node = TaskNode(id="t1", canvas_id=CANVAS, topic="topic")
        await store.create_canvas(CANVAS, {"t1": node})
        await store.set_result(CANVAS, "t1", NodeOutcome(node_id="t1", status=NodeStatus.FINISHED))

        sadd_calls = [call.args for call in mock_client.sadd.await_args_list]
        tracked = {key for _, *keys in sadd_calls for key in keys}
        assert f"mint-worker:canvas:{CANVAS}:node:t1" in tracked
        assert f"mint-worker:canvas:{CANVAS}:result:t1" in tracked


class TestClose:
    """close() must release the underlying client, and be a no-op if never connected."""

    async def test_close_calls_client_close(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """An already-created client must be closed."""
        await store.close()

        mock_client.aclose.assert_awaited_once()

    async def test_close_before_any_connection_is_a_no_op(self) -> None:
        """A store that never touched Redis must not construct a client just to close it."""
        store = RedisCanvasStore("redis://fake")

        await store.close()  # must not raise


class TestGetResults:
    """get_results must read via mget and return only the entries that exist."""

    async def test_empty_node_ids_short_circuits_without_calling_mget(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """No node ids requested must mean no round trip at all."""
        result = await store.get_results(CANVAS, [])

        assert result == {}
        mock_client.mget.assert_not_awaited()

    async def test_get_results_skips_missing_entries(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A None entry from mget must be omitted, not turned into a None value."""
        outcome = NodeOutcome(node_id="t1", status=NodeStatus.FINISHED, result="{}")
        mock_client.mget.return_value = [outcome.model_dump_json().encode(), None]

        result = await store.get_results(CANVAS, ["t1", "t2"])

        assert set(result) == {"t1"}
        assert result["t1"] == outcome


class TestGroupNodeRoundTrip:
    """The discriminated node union must round-trip a GroupNode too, not just TaskNode."""

    async def test_group_node_round_trips_through_the_mocked_client(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A GroupNode written then read back must decode to the same shape."""
        group = GroupNode(id="g", canvas_id=CANVAS, children=["leg1"], callback=None)
        mock_client.get.return_value = group.model_dump_json().encode()

        result = await store.get_node(CANVAS, "g")

        assert result == group


class TestClientConstruction:
    """The client property must lazily connect, once, via Redis.from_url."""

    async def test_client_property_constructs_via_from_url(self, mocker: "MockerFixture") -> None:
        """A store that never had its client injected must build one from its uri."""
        mock_from_url = mocker.patch(
            "mint.worker.stores.redis.Redis.from_url",
            return_value=mocker.AsyncMock(),
        )
        store = RedisCanvasStore("redis://example:6379/0")

        client = store.client

        mock_from_url.assert_called_once_with("redis://example:6379/0")
        assert client is store.client  # cached, not reconstructed on second access


class TestCancelNodes:
    """cancel_nodes must transition every listed node, atomically."""

    async def test_cancel_nodes_transitions_every_listed_node(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """One compare-and-set per node, rather than a read-modify-write per node."""
        script = mock_client.register_script.return_value

        await store.cancel_nodes(CANVAS, ["t1", "t2"])

        assert script.await_count == 2
        keys = [call.kwargs["keys"][0] for call in script.await_args_list]
        assert keys == [
            f"mint-worker:canvas:{CANVAS}:node:t1",
            f"mint-worker:canvas:{CANVAS}:node:t2",
        ]

    async def test_cancelling_is_refused_from_a_terminal_status(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A leg that genuinely finished did run; stamping it CANCELLED erases that.

        The guard lives in the Lua script, so the assertion is on which source
        statuses it is told to accept.
        """
        script = mock_client.register_script.return_value

        await store.cancel_nodes(CANVAS, ["t1"])

        allowed = script.await_args.kwargs["args"][1:]
        assert set(allowed) == {NodeStatus.PENDING.value, NodeStatus.RUNNING.value}
        assert NodeStatus.FINISHED.value not in allowed


class TestAtomicStatusTransitions:
    """A read-modify-write loses to a concurrent transition in another process."""

    async def test_mark_node_running_is_a_compare_and_set(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """`mark_node_running` documents that a CANCELLED node must not be resurrected.

        With get/check/set that only held within one process — and this store is
        the multi-process configuration by definition.
        """
        script = mock_client.register_script.return_value

        await store.mark_node_running(CANVAS, "t1")

        script.assert_awaited_once()
        args = script.await_args.kwargs["args"]
        assert args[0] == NodeStatus.RUNNING.value
        assert args[1:] == [NodeStatus.PENDING.value]

    async def test_a_transition_never_reads_then_writes(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """The whole point is that no gap exists between the check and the write."""
        await store.mark_node_running(CANVAS, "t1")

        mock_client.get.assert_not_awaited()
        mock_client.set.assert_not_awaited()


class TestGetResult:
    """get_result must round-trip a single NodeOutcome, or return None if unset."""

    async def test_get_result_round_trips(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A stored outcome must decode back to the same NodeOutcome."""
        outcome = NodeOutcome(node_id="t1", status=NodeStatus.FINISHED, result='{"x":1}')
        mock_client.get.return_value = outcome.model_dump_json().encode()

        result = await store.get_result(CANVAS, "t1")

        mock_client.get.assert_awaited_once_with(f"mint-worker:canvas:{CANVAS}:result:t1")
        assert result == outcome

    async def test_get_result_returns_none_when_unset(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """No result recorded yet must decode to None, not raise."""
        mock_client.get.return_value = None

        assert await store.get_result(CANVAS, "t1") is None


class TestGetCanvasStatus:
    """get_canvas_status must default to RUNNING and decode a stored status otherwise."""

    async def test_defaults_to_running_when_unset(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A canvas whose status was never set must read as RUNNING."""
        mock_client.get.return_value = None

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.RUNNING

    async def test_decodes_a_stored_terminal_status(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A previously-set FINISHED status must round-trip back as FINISHED."""
        mock_client.get.return_value = CanvasStatus.FINISHED.value.encode()

        assert await store.get_canvas_status(CANVAS) == CanvasStatus.FINISHED


class TestStatusKeyOutlivesItsData:
    """A canvas's status is its tombstone and must survive longer than the graph."""

    async def test_terminal_status_key_gets_the_longer_ttl(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Expiring the status with the data made a finished canvas read RUNNING again.

        `get_canvas_status` reports RUNNING for a missing key, so once the status
        expired a late replay walked into a canvas whose nodes were long gone and
        marked the finished canvas ERROR.
        """
        await store.set_canvas_status(CANVAS, CanvasStatus.FINISHED)

        expiries = {call.args[0]: call.args[1] for call in mock_client.expire.await_args_list}
        assert expiries[f"mint-worker:canvas:{CANVAS}:status"] == store.status_ttl_seconds
        assert store.status_ttl_seconds > store.terminal_ttl_seconds

    async def test_data_keys_keep_the_shorter_terminal_ttl(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Only the status is long-lived — the graph itself must still be reclaimed."""
        mock_client.smembers.return_value = {b"mint-worker:canvas:c1:node:t1"}

        await store.set_canvas_status(CANVAS, CanvasStatus.FINISHED)

        expiries = {call.args[0]: call.args[1] for call in mock_client.expire.await_args_list}
        assert expiries[b"mint-worker:canvas:c1:node:t1"] == store.terminal_ttl_seconds

    async def test_a_running_canvas_still_gets_no_ttl_at_all(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """In-flight data must never expire underneath a live canvas."""
        await store.set_canvas_status(CANVAS, CanvasStatus.RUNNING)

        mock_client.expire.assert_not_awaited()


class TestCanvasIdReuseResetsStatus:
    """A terminal status outlives its data by design, so creation must clear it."""

    async def test_create_canvas_writes_a_running_status(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """Nothing wrote the status on creation, so a retry inherited the old one."""
        node = TaskNode(id="t1", canvas_id=CANVAS, topic="t")

        await store.create_canvas(CANVAS, {"t1": node})

        written = {
            call.args[0]: call.args[1] for call in mock_client.set.await_args_list if call.args
        }
        assert written[f"mint-worker:canvas:{CANVAS}:status"] == b"running"

    async def test_create_canvas_does_not_expire_anything(
        self,
        store: RedisCanvasStore,
        mock_client: AsyncMock,
    ) -> None:
        """A freshly created canvas is live — nothing it owns may carry a TTL."""
        node = TaskNode(id="t1", canvas_id=CANVAS, topic="t")

        await store.create_canvas(CANVAS, {"t1": node})

        mock_client.expire.assert_not_awaited()
