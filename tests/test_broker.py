"""Tests for Broker lifecycle and public API."""

import pytest

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.errors import (
    BrokerFencedError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
)
from cascadq.models import QueueFile, QueueMetadata, TaskStatus, serialize_queue_file
from cascadq.storage.memory import InMemoryObjectStore

from .conftest import make_idempotency_key as _key


async def _start_broker(
    store: InMemoryObjectStore,
    config: BrokerConfig,
) -> Broker:
    broker = Broker(store=store, config=config, clock=lambda: 1000.0)
    await broker.start()
    return broker


class TestBrokerLifecycle:
    async def test_start_discovers_existing_queues(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        qf = QueueFile(
            metadata=QueueMetadata(created_at=500.0, payload_schema={}),
        )
        await memory_store.write_new(
            "queues/existing.json", serialize_queue_file(qf)
        )

        broker = Broker(
            store=memory_store, config=test_config, clock=lambda: 1000.0
        )
        await broker.start()

        await broker.push("existing", {"data": 1}, _key())
        await broker.stop()


class TestCreateDeleteQueue:
    async def test_create_and_push(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        await broker.push("work", {"job": "test"}, _key())
        await broker.stop()

    async def test_create_duplicate_raises(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        with pytest.raises(QueueAlreadyExistsError):
            await broker.create_queue("work")
        await broker.stop()

    async def test_delete_removes_queue(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        await broker.delete_queue("work")
        with pytest.raises(QueueNotFoundError):
            await broker.push("work", {}, _key())
        await broker.stop()

    async def test_delete_nonexistent_raises(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        with pytest.raises(QueueNotFoundError):
            await broker.delete_queue("nope")
        await broker.stop()


class TestPushClaimFinish:
    async def test_full_task_lifecycle(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")

        await broker.push("q", {"url": "http://example.com"}, _key())
        task = await broker.claim("q", _key())
        assert task.status == TaskStatus.claimed
        assert task.payload == {"url": "http://example.com"}

        await broker.finish("q", task.task_id, task.sequence)
        await broker.stop()

    async def test_claim_empty_raises(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")
        with pytest.raises(QueueEmptyError):
            await broker.claim("q", _key(), timeout_seconds=0)
        await broker.stop()

    async def test_push_to_nonexistent_queue_raises(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        with pytest.raises(QueueNotFoundError):
            await broker.push("nope", {}, _key())
        await broker.stop()


class TestBrokerFencing:
    async def test_cas_conflict_fences_broker(
        self, memory_store: InMemoryObjectStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")

        memory_store.inject_conflict("queues/q.json")
        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 1}, _key())

        assert broker.is_fenced

        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 2}, _key())
        await broker.stop()
