"""Tests for HTTP server routes via in-process ASGI client."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.server.app import create_app
from cascadq.storage.memory import InMemoryObjectStore


@pytest.fixture
async def client(
    memory_store: InMemoryObjectStore,
    test_config: BrokerConfig,
) -> AsyncGenerator[AsyncClient]:
    broker = Broker(store=memory_store, config=test_config)
    await broker.start()
    app = create_app(store=memory_store, config=test_config, broker=broker)
    # Set broker on state since ASGITransport doesn't trigger lifespan
    app.state.broker = broker
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await broker.stop()


class TestQueueManagement:
    async def test_create_queue(self, client: AsyncClient) -> None:
        resp = await client.post(
            "/queues", json={"name": "work", "payload_schema": {}}
        )
        assert resp.status_code == 201

    async def test_create_duplicate_queue(self, client: AsyncClient) -> None:
        await client.post("/queues", json={"name": "work"})
        resp = await client.post("/queues", json={"name": "work"})
        assert resp.status_code == 409

    async def test_delete_queue(self, client: AsyncClient) -> None:
        await client.post("/queues", json={"name": "work"})
        resp = await client.delete("/queues/work")
        assert resp.status_code == 204

    async def test_delete_nonexistent_queue(self, client: AsyncClient) -> None:
        resp = await client.delete("/queues/nope")
        assert resp.status_code == 404


class TestPushClaimFinish:
    async def test_full_lifecycle(self, client: AsyncClient) -> None:
        await client.post("/queues", json={"name": "q"})

        resp = await client.post(
            "/queues/q/push", json={"payload": {"url": "http://a"}}
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]

        resp = await client.post(
            "/queues/q/claim", json={"consumer_id": "w1"}
        )
        assert resp.status_code == 200
        claim_data = resp.json()
        assert claim_data["task_id"] == task_id
        assert claim_data["payload"] == {"url": "http://a"}
        sequence = claim_data["sequence"]

        resp = await client.post(
            "/queues/q/heartbeat", json={"task_id": task_id}
        )
        assert resp.status_code == 204

        resp = await client.post(
            "/queues/q/finish",
            json={"task_id": task_id, "sequence": sequence},
        )
        assert resp.status_code == 204

    async def test_claim_empty_returns_204(self, client: AsyncClient) -> None:
        await client.post("/queues", json={"name": "q"})
        resp = await client.post(
            "/queues/q/claim", json={"consumer_id": "w1"}
        )
        assert resp.status_code == 204

    async def test_push_to_nonexistent_queue(self, client: AsyncClient) -> None:
        resp = await client.post("/queues/nope/push", json={"payload": {}})
        assert resp.status_code == 404

    async def test_finish_unclaimed_task(self, client: AsyncClient) -> None:
        await client.post("/queues", json={"name": "q"})
        resp = await client.post("/queues/q/push", json={"payload": {}})
        task_id = resp.json()["task_id"]
        resp = await client.post(
            "/queues/q/finish",
            json={"task_id": task_id, "sequence": 0},
        )
        assert resp.status_code == 409


class TestPayloadValidation:
    async def test_invalid_payload_returns_422(self, client: AsyncClient) -> None:
        await client.post(
            "/queues",
            json={
                "name": "q",
                "payload_schema": {
                    "type": "object",
                    "properties": {"url": {"type": "string"}},
                    "required": ["url"],
                },
            },
        )
        resp = await client.post(
            "/queues/q/push", json={"payload": {"bad": 1}}
        )
        assert resp.status_code == 422

    async def test_malformed_request_returns_422(
        self, client: AsyncClient
    ) -> None:
        await client.post("/queues", json={"name": "q"})
        resp = await client.post("/queues/q/push", json={"wrong_field": 1})
        assert resp.status_code == 422


class TestStoreLifecycle:
    async def test_store_lifecycle_is_entered_and_exited(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        """The store_lifecycle context manager is managed by the app lifespan."""
        entered = False
        exited = False

        @asynccontextmanager
        async def fake_lifecycle():
            nonlocal entered, exited
            entered = True
            yield
            exited = True

        lifecycle = fake_lifecycle()
        broker = Broker(store=memory_store, config=test_config)
        app = create_app(
            store=memory_store,
            config=test_config,
            store_lifecycle=lifecycle,
        )
        # Manually run the lifespan to verify the context manager
        # is entered/exited (ASGITransport doesn't trigger lifespan).
        async with app.router.lifespan_context(app):
            assert entered
            assert app.state.broker is not None

        assert exited
        await broker.stop()


class TestErrorCodes:
    async def test_error_response_includes_code_field(
        self, client: AsyncClient
    ) -> None:
        """Error responses should include a 'code' field for disambiguation."""
        resp = await client.delete("/queues/nonexistent")
        assert resp.status_code == 404
        body = resp.json()
        assert body["code"] == "queue_not_found"
        assert "error" in body
