"""End-to-end integration tests: client → HTTP server → broker → FaultInjectingStore."""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.client.client import CascadqClient
from cascadq.config import BrokerConfig, ClientConfig
from cascadq.errors import BrokerFencedError, PayloadValidationError, QueueNotFoundError
from cascadq.server.app import create_app
from tests.support import FaultInjectingStore


@pytest.fixture
async def client(
    memory_store: FaultInjectingStore,
) -> AsyncGenerator[CascadqClient]:
    """Full stack: client → server → broker → memory store."""
    config = BrokerConfig(
        heartbeat_timeout_seconds=0.5,
        heartbeat_check_interval_seconds=0.2,
        compaction_interval_seconds=100.0,
    )
    broker = Broker(store=memory_store, config=config)
    await broker.start()
    app = create_app(store=memory_store, config=config, broker=broker)
    app.state.broker = broker
    transport = ASGITransport(app=app)
    http_client = AsyncClient(transport=transport, base_url="http://test")
    client_config = ClientConfig(
        base_url="http://test",
        heartbeat_interval_seconds=0.1,
        max_retries=2,
        retry_base_delay_seconds=0.01,
    )
    c = CascadqClient(config=client_config, http_client=http_client)
    yield c
    await c.close()
    await broker.stop()


class TestFIFOOrdering:
    async def test_claims_return_tasks_in_push_order(
        self, client: CascadqClient
    ) -> None:
        """Multiple consumers each get distinct tasks in FIFO order."""
        await client.create_queue("work")
        for i in range(4):
            await client.push("work", {"i": i})

        claimed_payloads = []
        for _ in range(4):
            claimed = await client.claim("work")
            assert claimed is not None
            claimed_payloads.append(claimed.payload)
            async with claimed:
                pass

        assert claimed_payloads == [{"i": i} for i in range(4)]

    async def test_claim_returns_none_after_all_consumed(
        self, client: CascadqClient
    ) -> None:
        await client.create_queue("work")
        await client.push("work", {"x": 1})

        claimed = await client.claim("work")
        assert claimed is not None
        async with claimed:
            pass

        result = await client.claim("work", timeout_seconds=0)
        assert result is None


class TestWorkerFailure:
    async def test_heartbeat_timeout_requeues_task(
        self, client: CascadqClient
    ) -> None:
        """When a worker stops sending heartbeats, the task is re-queued."""
        await client.create_queue("work")
        await client.push("work", {"url": "http://example.com"})

        # Claim but don't enter context manager (no heartbeats, no finish)
        claimed = await client.claim("work")
        assert claimed is not None
        original_payload = claimed.payload

        # Wait for heartbeat timeout (0.5s) + check interval (0.2s) + margin
        await asyncio.sleep(1.0)

        # Task should be re-queued and claimable by another worker
        reclaimed = await client.claim("work")
        assert reclaimed is not None
        assert reclaimed.payload == original_payload
        assert reclaimed.task_id != claimed.task_id  # new task_id

        async with reclaimed:
            pass


class TestBrokerFencing:
    async def test_fenced_broker_rejects_requests(
        self, client: CascadqClient, memory_store: FaultInjectingStore
    ) -> None:
        await client.create_queue("work")

        # Inject a CAS conflict to fence the broker
        memory_store.inject_conflict("queues/work.json")

        with pytest.raises(BrokerFencedError):
            await client.push("work", {"x": 1})


class TestPayloadValidation:
    async def test_schema_validated_push(self, client: CascadqClient) -> None:
        await client.create_queue(
            "typed",
            payload_schema={
                "type": "object",
                "properties": {"url": {"type": "string"}},
                "required": ["url"],
            },
        )
        # Valid payload succeeds
        await client.push("typed", {"url": "http://example.com"})

        # Invalid payload fails
        with pytest.raises(PayloadValidationError):
            await client.push("typed", {"bad": 123})


class TestSlowFlushResilience:
    """Edge-case tests for the heartbeat lifecycle fixes.

    Uses inject_write_delay to simulate R2 tail latency so the flush
    takes longer than the heartbeat timeout.
    """

    @pytest.fixture
    async def slow_flush_client(
        self, memory_store: FaultInjectingStore,
    ) -> AsyncGenerator[tuple[CascadqClient, FaultInjectingStore]]:
        """Full stack with aggressive heartbeat timing for slow-flush tests."""
        config = BrokerConfig(
            heartbeat_timeout_seconds=0.3,
            heartbeat_check_interval_seconds=0.1,
            compaction_interval_seconds=100.0,
        )
        broker = Broker(store=memory_store, config=config)
        await broker.start()
        app = create_app(store=memory_store, config=config, broker=broker)
        app.state.broker = broker
        transport = ASGITransport(app=app)
        http_client = AsyncClient(transport=transport, base_url="http://test")
        client_config = ClientConfig(
            base_url="http://test",
            heartbeat_interval_seconds=0.1,
        )
        client = CascadqClient(config=client_config, http_client=http_client)
        yield client, memory_store
        await client.close()
        await broker.stop()

    async def test_slow_claim_flush_does_not_requeue(
        self, slow_flush_client: tuple[CascadqClient, FaultInjectingStore],
    ) -> None:
        """Lease starts after commit: a slow claim flush must not cause
        the heartbeat checker to re-queue the task before the client
        receives the claim response."""
        client, store = slow_flush_client
        await client.create_queue("q")
        await client.push("q", {"x": 1})

        # Next flush (the claim) will take 0.8s — longer than
        # the 0.3s heartbeat timeout.  The heartbeat checker
        # must NOT re-queue because last_heartbeat is None until
        # after the flush commits.
        store.inject_write_delay("queues/q.json", 0.8)

        claimed = await client.claim("q")
        assert claimed is not None

        # Finish normally — proves the task was NOT re-queued
        async with claimed:
            pass

        result = await client.claim("q", timeout_seconds=0)
        assert result is None

    async def test_slow_finish_flush_heartbeats_keep_task_alive(
        self, slow_flush_client: tuple[CascadqClient, FaultInjectingStore],
    ) -> None:
        """Heartbeats continue during finish: a slow finish flush must not
        cause the heartbeat checker to re-queue the task while the
        completion is being persisted."""
        client, store = slow_flush_client
        await client.create_queue("q")
        await client.push("q", {"x": 1})

        claimed = await client.claim("q")
        assert claimed is not None

        async with claimed:
            # Inject delay on the NEXT write (the finish flush).
            # The 0.8s delay exceeds the 0.3s timeout, but heartbeats
            # (every 0.1s) keep the task alive during the flush.
            store.inject_write_delay("queues/q.json", 0.8)

        # If heartbeats had stopped before the finish RPC,
        # the task would have been re-queued and finish would
        # have raised TaskNotFoundError.  The fact that we got
        # here proves heartbeats kept the lease alive.
        result = await client.claim("q", timeout_seconds=0)
        assert result is None

    async def test_heartbeat_loop_exits_cleanly_after_finish(
        self, slow_flush_client: tuple[CascadqClient, FaultInjectingStore],
    ) -> None:
        """When finish() completes before the heartbeat loop is cancelled,
        the next heartbeat gets TaskNotClaimedError.  The loop should
        exit silently (not log a warning) because self._finished is True."""
        client, _ = slow_flush_client
        await client.create_queue("q")
        await client.push("q", {"x": 1})

        claimed = await client.claim("q")
        assert claimed is not None

        async with claimed:
            # Let heartbeats run, then finish on exit
            await asyncio.sleep(0.15)

        # Give the heartbeat loop time to fire one more
        # iteration after finish() set _finished = True
        await asyncio.sleep(0.1)

        # If the loop didn't exit cleanly, the heartbeat task
        # would still be running.  Verify it stopped.
        assert claimed._heartbeat_task is None


class TestQueueDeletion:
    async def test_delete_queue_rejects_subsequent_operations(
        self, client: CascadqClient
    ) -> None:
        await client.create_queue("ephemeral")
        await client.push("ephemeral", {"x": 1})
        await client.delete_queue("ephemeral")

        with pytest.raises(QueueNotFoundError):
            await client.push("ephemeral", {"x": 2})

        with pytest.raises(QueueNotFoundError):
            await client.claim("ephemeral")
