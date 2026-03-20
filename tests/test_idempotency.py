"""Integration tests for client timeout + retry idempotency.

Uses inject_write_delay to make the store slow enough that the HTTP
client times out, then verifies the retry deduplicates correctly.
"""

from collections.abc import AsyncGenerator

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.client.client import CascadqClient
from cascadq.config import BrokerConfig, ClientConfig
from cascadq.server.app import create_app
from cascadq.storage.memory import InMemoryObjectStore


@pytest.fixture
async def stack(
    memory_store: InMemoryObjectStore,
) -> AsyncGenerator[tuple[CascadqClient, InMemoryObjectStore]]:
    """Full stack with a short HTTP timeout and longer retry delay.

    The HTTP timeout (0.1s) is shorter than the injected store delay
    (0.5s), so the first attempt times out.  The retry delay (0.6s)
    is long enough for the first write to complete before the retry
    hits the server.
    """
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
    http_client = AsyncClient(
        transport=transport,
        base_url="http://test",
        timeout=httpx.Timeout(0.1),
    )
    client_config = ClientConfig(
        base_url="http://test",
        heartbeat_interval_seconds=0.1,
        max_retries=3,
        retry_base_delay_seconds=0.6,
    )
    client = CascadqClient(config=client_config, http_client=http_client)
    yield client, memory_store
    await client.close()
    await broker.stop()


class TestPushIdempotency:
    async def test_push_idempotent_on_timeout(
        self,
        stack: tuple[CascadqClient, InMemoryObjectStore],
    ) -> None:
        """First push times out (store is slow), retry deduplicates."""
        client, store = stack
        await client.create_queue("q")

        # Make the next store write take 0.5s — longer than the 0.1s
        # HTTP timeout, so the client's first attempt will time out.
        store.inject_write_delay("queues/q.json", 0.5)

        await client.push("q", {"x": 1})

        # Exactly one task should exist
        claimed = await client.claim("q", timeout_seconds=0)
        assert claimed is not None
        assert claimed.payload == {"x": 1}
        async with claimed:
            pass

        # No second task
        second = await client.claim("q", timeout_seconds=0)
        assert second is None


class TestClaimIdempotency:
    async def test_claim_idempotent_on_timeout(
        self,
        stack: tuple[CascadqClient, InMemoryObjectStore],
    ) -> None:
        """First claim times out (store is slow), retry returns same task."""
        client, store = stack
        await client.create_queue("q")
        await client.push("q", {"x": 1})

        # Make the next store write take 0.5s — the claim flush will
        # be slow, causing the client's first attempt to time out.
        store.inject_write_delay("queues/q.json", 0.5)

        claimed = await client.claim("q", timeout_seconds=1)
        assert claimed is not None
        assert claimed.payload == {"x": 1}
        async with claimed:
            pass

        # No second task — the retry returned the same claim
        second = await client.claim("q", timeout_seconds=0)
        assert second is None
