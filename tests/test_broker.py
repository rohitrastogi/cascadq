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


@pytest.fixture
def config() -> BrokerConfig:
    return BrokerConfig(
        heartbeat_timeout_seconds=5.0,
        heartbeat_check_interval_seconds=100.0,  # effectively disabled
        compaction_interval_seconds=100.0,  # effectively disabled
    )


async def _start_broker(
    store: InMemoryObjectStore,
    config: BrokerConfig,
) -> Broker:
    broker = Broker(store=store, config=config, clock=lambda: 1000.0)
    await broker.start()
    return broker


class TestBrokerLifecycle:
    async def test_start_writes_broker_info(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        data, _ = await memory_store.read("broker.json")
        assert b"broker_id" in data
        await broker.stop()

    async def test_start_discovers_existing_queues(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        qf = QueueFile(
            metadata=QueueMetadata(created_at=500.0, payload_schema={}),
        )
        await memory_store.write_new(
            "queues/existing.json", serialize_queue_file(qf)
        )

        broker = Broker(
            store=memory_store, config=config, clock=lambda: 1000.0
        )
        await broker.start()

        task_id = await broker.push("existing", {"data": 1})
        assert task_id
        await broker.stop()


class TestCreateDeleteQueue:
    async def test_create_and_push(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("work")
        task_id = await broker.push("work", {"job": "test"})
        assert isinstance(task_id, str)
        await broker.stop()

    async def test_create_duplicate_raises(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("work")
        with pytest.raises(QueueAlreadyExistsError):
            await broker.create_queue("work")
        await broker.stop()

    async def test_delete_removes_queue(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("work")
        await broker.delete_queue("work")
        with pytest.raises(QueueNotFoundError):
            await broker.push("work", {})
        await broker.stop()

    async def test_delete_nonexistent_raises(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        with pytest.raises(QueueNotFoundError):
            await broker.delete_queue("nope")
        await broker.stop()


class TestPushClaimFinish:
    async def test_full_task_lifecycle(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("q")

        task_id = await broker.push("q", {"url": "http://example.com"})
        task = await broker.claim("q", "worker-1")
        assert task.task_id == task_id
        assert task.status == TaskStatus.claimed
        assert task.payload == {"url": "http://example.com"}

        await broker.finish("q", task.task_id)
        await broker.stop()

    async def test_claim_empty_raises(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("q")
        with pytest.raises(QueueEmptyError):
            await broker.claim("q", "worker-1")
        await broker.stop()

    async def test_push_to_nonexistent_queue_raises(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        with pytest.raises(QueueNotFoundError):
            await broker.push("nope", {})
        await broker.stop()


class TestBrokerFencing:
    async def test_cas_conflict_fences_broker(
        self, memory_store: InMemoryObjectStore, config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, config)
        await broker.create_queue("q")

        memory_store.inject_conflict("queues/q.json")
        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 1})

        assert broker.is_fenced

        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 2})
        await broker.stop()
