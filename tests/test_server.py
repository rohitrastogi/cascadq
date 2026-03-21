"""Tests for HTTP server routes via in-process ASGI client."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest
from httpx import ASGITransport, AsyncClient

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.errors import BrokerFencedError
from cascadq.server.app import create_app
from tests.support import FaultInjectingStore


@pytest.fixture
async def client(
    memory_store: FaultInjectingStore,
    test_config: BrokerConfig,
) -> AsyncGenerator[AsyncClient]:
    broker = Broker(store=memory_store, config=test_config)
    await broker.start()
    app = create_app(store=memory_store, config=test_config, broker=broker)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    await broker.stop()


class TestHealthProbes:
    async def test_healthz_returns_200(self, client: AsyncClient) -> None:
        resp = await client.get("/healthz")
        assert resp.status_code == 200

    async def test_readyz_returns_200_when_broker_started(
        self, client: AsyncClient,
    ) -> None:
        resp = await client.get("/readyz")
        assert resp.status_code == 200

    async def test_readyz_returns_503_before_broker_starts(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig,
    ) -> None:
        app = create_app(store=memory_store, config=test_config)
        # Don't trigger lifespan — broker is never set on app.state
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/readyz")
            assert resp.status_code == 503
            assert resp.json()["code"] == "not_ready"

    async def test_readyz_returns_503_when_broker_not_started(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig,
    ) -> None:
        broker = Broker(store=memory_store, config=test_config)
        app = create_app(store=memory_store, config=test_config, broker=broker)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/readyz")
            assert resp.status_code == 503
            assert resp.json()["code"] == "not_ready"

    async def test_readyz_returns_503_after_broker_stopped(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig,
    ) -> None:
        broker = Broker(store=memory_store, config=test_config)
        await broker.start()
        app = create_app(store=memory_store, config=test_config, broker=broker)
        await broker.stop()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/readyz")
            assert resp.status_code == 503
            assert resp.json()["code"] == "not_ready"

    async def test_readyz_returns_503_when_broker_fenced(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig,
    ) -> None:
        broker = Broker(store=memory_store, config=test_config)
        await broker.start()
        await broker.create_queue("q")

        app = create_app(store=memory_store, config=test_config, broker=broker)

        transport = ASGITransport(app=app)

        async with AsyncClient(transport=transport, base_url="http://test") as c:
            # Fence the queue via CAS conflict
            memory_store.inject_conflict("queues/q.json")
            with pytest.raises(BrokerFencedError):
                await broker.push("q", {"x": 1}, "k1")

            resp = await c.get("/readyz")
            assert resp.status_code == 503
            assert resp.json()["code"] == "broker_fenced"

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
            "/queues/q/push",
            json={"payload": {"url": "http://a"}, "push_key": "p1"},
        )
        assert resp.status_code == 204

        resp = await client.post(
            "/queues/q/claim", json={"claim_key": "c1"}
        )
        assert resp.status_code == 200
        claim_data = resp.json()
        task_id = claim_data["task_id"]
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
            "/queues/q/claim",
            json={"claim_key": "c1", "timeout_seconds": 0},
        )
        assert resp.status_code == 204

    async def test_push_to_nonexistent_queue(self, client: AsyncClient) -> None:
        resp = await client.post(
            "/queues/nope/push",
            json={"payload": {}, "push_key": "p1"},
        )
        assert resp.status_code == 404

    async def test_finish_nonexistent_task_returns_404(
        self, client: AsyncClient,
    ) -> None:
        await client.post("/queues", json={"name": "q"})
        resp = await client.post(
            "/queues/q/finish",
            json={"task_id": "nonexistent", "sequence": 999},
        )
        assert resp.status_code == 404


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
            "/queues/q/push",
            json={"payload": {"bad": 1}, "push_key": "p1"},
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
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
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
