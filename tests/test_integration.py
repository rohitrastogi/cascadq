"""End-to-end integration tests: client → HTTP server → broker → InMemoryObjectStore."""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.client.client import CascadqClient
from cascadq.config import BrokerConfig, ClientConfig
from cascadq.errors import BrokerFencedError, PayloadValidationError, QueueNotFoundError
from cascadq.server.app import create_app
from cascadq.storage.memory import InMemoryObjectStore


@pytest.fixture
async def client(
    memory_store: InMemoryObjectStore,
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
        self, client: CascadqClient, memory_store: InMemoryObjectStore
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
