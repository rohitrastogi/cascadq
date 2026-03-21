"""Tests for CascadqClient retry logic and ClaimedTask lifecycle."""

import asyncio
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.server.app import create_app
from cascadq_client import (
    CascadqClient,
    ClientConfig,
    QueueAlreadyExistsError,
    QueueNotFoundError,
)
from tests.support import FaultInjectingStore


@pytest.fixture
async def server_client(
    memory_store: FaultInjectingStore,
    test_config: BrokerConfig,
) -> AsyncGenerator[tuple[CascadqClient, Broker]]:
    """Set up a full stack: client → HTTP server → broker → memory store."""
    broker = Broker(store=memory_store, config=test_config)
    await broker.start()
    app = create_app(store=memory_store, config=test_config, broker=broker)
    app.state.broker = broker
    transport = ASGITransport(app=app)
    http_client = AsyncClient(transport=transport, base_url="http://test")
    client_config = ClientConfig(
        base_url="http://test",
        heartbeat_interval_seconds=0.1,
        max_retries=2,
        retry_base_delay_seconds=0.01,
    )
    client = CascadqClient(config=client_config, http_client=http_client)
    yield client, broker
    await client.close()
    await broker.stop()


class TestClientLifecycle:
    async def test_push_claim_finish(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        await client.create_queue("q")
        await client.push("q", {"url": "http://example.com"})

        claimed = await client.claim("q")
        assert claimed is not None
        assert claimed.payload == {"url": "http://example.com"}

        async with claimed:
            pass  # finish happens on context manager exit

    async def test_claim_empty_returns_none(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        await client.create_queue("q")
        result = await client.claim("q", timeout_seconds=0)
        assert result is None


class TestClaimedTaskHeartbeat:
    async def test_heartbeat_is_sent_during_context(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        await client.create_queue("q")
        await client.push("q", {"x": 1})
        claimed = await client.claim("q")
        assert claimed is not None

        async with claimed:
            # Wait long enough for at least one heartbeat (interval=0.1s)
            await asyncio.sleep(0.25)

        # Task should be finished — claiming again should find nothing
        result = await client.claim("q", timeout_seconds=0)
        assert result is None

    async def test_explicit_finish_stops_heartbeat(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        await client.create_queue("q")
        await client.push("q", {"x": 1})
        claimed = await client.claim("q")
        assert claimed is not None

        async with claimed:
            await claimed.finish()
            # Double finish should be idempotent
            await claimed.finish()


class TestDomainErrors:
    async def test_push_to_nonexistent_queue_raises_queue_not_found(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        with pytest.raises(QueueNotFoundError):
            await client.push("nonexistent", {"x": 1})

    async def test_create_duplicate_queue_raises_already_exists(
        self, server_client: tuple[CascadqClient, Broker]
    ) -> None:
        client, _ = server_client
        await client.create_queue("q")
        with pytest.raises(QueueAlreadyExistsError):
            await client.create_queue("q")

    async def test_transient_flush_failure_retried_internally(
        self, memory_store: FaultInjectingStore,
    ) -> None:
        """A transient store error is retried by the broker internally;
        the client push succeeds without seeing a failure."""
        config = BrokerConfig(
            heartbeat_timeout_seconds=5.0,
            heartbeat_check_interval_seconds=100.0,
            compaction_interval_seconds=100.0,
            flush_retry_delay_seconds=0.01,
        )
        broker = Broker(store=memory_store, config=config)
        await broker.start()
        app = create_app(store=memory_store, config=config, broker=broker)
        app.state.broker = broker
        transport = ASGITransport(app=app)
        http_client = AsyncClient(transport=transport, base_url="http://test")
        client_config = ClientConfig(
            base_url="http://test", max_retries=0,
        )
        client = CascadqClient(config=client_config, http_client=http_client)
        try:
            await client.create_queue("q")
            memory_store.inject_transient_error("queues/q.json", count=1)
            # Push should succeed — broker retries the flush internally
            await client.push("q", {"x": 1})
        finally:
            await client.close()
            await broker.stop()
