"""RedisCanvasStore against a real Redis: exactly what mocking cannot prove.

Run standalone, memory-capped, per the project's standing rule (see the worker
plan's incident writeup): ``make test-worker-capped
TARGET=tests/worker/stores/test_redis_container.py``. Never bundled with another
broker's container test.
"""

import asyncio
from collections.abc import AsyncIterator, Iterator

import pytest
from testcontainers.community.redis import RedisContainer

from mint.worker.canvas.models import GroupNode, TaskNode
from mint.worker.enums import CanvasStatus, NodeStatus
from mint.worker.stores.redis import RedisCanvasStore

CANVAS = "c1"


@pytest.fixture(scope="module")
def redis_uri() -> Iterator[str]:
    """Start one Redis container for this module's tests; stop it when they're done."""
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def store(redis_uri: str) -> "AsyncIterator[RedisCanvasStore]":
    """Return a fresh store over the shared container, flushed before each test."""
    instance = RedisCanvasStore(redis_uri, namespace=f"test-{id(object())}")
    await instance.client.flushdb()
    yield instance
    await instance.close()


class TestFanInAtomicity:
    """The one property mocking cannot prove: SADD+SCARD+SETNX really is atomic."""

    async def test_fifty_concurrent_legs_fire_exactly_one_callback(
        self,
        store: RedisCanvasStore,
    ) -> None:
        """50 legs finishing concurrently must yield exactly one `fired=True`."""
        num_children = 50
        results = await asyncio.gather(
            *(
                store.mark_child_done(CANVAS, "g", f"leg{i}", num_children)
                for i in range(num_children)
            ),
        )

        fired_count = sum(1 for progress in results if progress.fired)
        assert fired_count == 1
        assert all(progress.added for progress in results)  # every leg is distinct
        assert max(progress.done_count for progress in results) == num_children

    async def test_repeated_runs_stay_exactly_one(self, redis_uri: str) -> None:
        """Repeat the concurrency race a few times to shake out flakes, not just once."""
        for run in range(5):
            store = RedisCanvasStore(redis_uri, namespace=f"race-{run}")
            num_children = 30
            results = await asyncio.gather(
                *(
                    store.mark_child_done(CANVAS, "g", f"leg{i}", num_children)
                    for i in range(num_children)
                ),
            )
            assert sum(1 for p in results if p.fired) == 1
            await store.close()

    async def test_duplicate_delivery_under_concurrency_does_not_inflate_the_count(
        self,
        store: RedisCanvasStore,
    ) -> None:
        """The same leg id delivered many times concurrently must count once, not fire early."""
        results = await asyncio.gather(
            *(store.mark_child_done(CANVAS, "g", "leg1", 2) for _ in range(20)),
        )

        assert sum(1 for progress in results if progress.added) == 1
        assert all(progress.done_count == 1 for progress in results)
        assert not any(progress.fired for progress in results)


class TestNamespacing:
    """Two canvases sharing a node id must not interfere — regression for bug #13."""

    async def test_two_canvases_with_the_same_node_id_are_isolated(
        self,
        store: RedisCanvasStore,
    ) -> None:
        """Writing the same node id under two canvas ids must not overwrite each other."""
        node_a = TaskNode(id="t1", canvas_id="canvas-a", topic="topic-a")
        node_b = TaskNode(id="t1", canvas_id="canvas-b", topic="topic-b")

        await store.create_canvas("canvas-a", {"t1": node_a})
        await store.create_canvas("canvas-b", {"t1": node_b})

        fetched_a = await store.get_node("canvas-a", "t1")
        fetched_b = await store.get_node("canvas-b", "t1")
        assert isinstance(fetched_a, TaskNode)
        assert isinstance(fetched_b, TaskNode)
        assert fetched_a.topic == "topic-a"
        assert fetched_b.topic == "topic-b"

    async def test_two_canvases_fan_in_independently(self, store: RedisCanvasStore) -> None:
        """The same group id under two canvases must not share fan-in state."""
        await store.mark_child_done("canvas-a", "g", "leg1", 2)
        progress = await store.mark_child_done("canvas-b", "g", "leg1", 1)

        assert progress.fired  # canvas-b's group of 1 fires on its own first leg
        assert not (await store.mark_child_done("canvas-a", "g", "leg1", 2)).fired


class TestTerminalTTL:
    """A terminal canvas's keys actually expire in real Redis; a running one's don't."""

    async def test_running_canvas_keys_have_no_ttl(self, store: RedisCanvasStore) -> None:
        """Persist (-1) means "exists, no TTL"; that must be true while RUNNING."""
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="t")})
        await store.set_canvas_status(CANVAS, CanvasStatus.RUNNING)

        ttl = await store.client.ttl(f"{store.namespace}:canvas:{CANVAS}:node:t1")
        assert ttl == -1

    async def test_finished_canvas_keys_get_a_real_ttl(self, store: RedisCanvasStore) -> None:
        """Once FINISHED, every tracked key must have a positive TTL, not persist forever."""
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="t")})

        await store.set_canvas_status(CANVAS, CanvasStatus.FINISHED)

        ttl = await store.client.ttl(f"{store.namespace}:canvas:{CANVAS}:node:t1")
        assert ttl > 0


class TestNodeStatusPersistence:
    """Node status transitions are actually written and readable back — regression for bug #13."""

    async def test_status_transitions_are_persisted_and_readable(
        self,
        store: RedisCanvasStore,
    ) -> None:
        """PENDING -> RUNNING -> CANCELLED must each be visible on the next read."""
        await store.create_canvas(CANVAS, {"t1": TaskNode(id="t1", canvas_id=CANVAS, topic="t")})
        initial = await store.get_node(CANVAS, "t1")
        assert initial is not None
        assert initial.status == NodeStatus.PENDING

        await store.set_node_status(CANVAS, "t1", NodeStatus.RUNNING)
        running = await store.get_node(CANVAS, "t1")
        assert running is not None
        assert running.status == NodeStatus.RUNNING

        await store.cancel_nodes(CANVAS, ["t1"])
        cancelled = await store.get_node(CANVAS, "t1")
        assert cancelled is not None
        assert cancelled.status == NodeStatus.CANCELLED

    async def test_group_node_round_trips_through_real_redis(
        self,
        store: RedisCanvasStore,
    ) -> None:
        """A GroupNode (not just TaskNode) must survive the real JSON round trip too."""
        group = GroupNode(id="g", canvas_id=CANVAS, children=["leg1", "leg2"], callback="cb")
        await store.create_canvas(CANVAS, {"g": group})

        fetched = await store.get_node(CANVAS, "g")

        assert fetched == group
