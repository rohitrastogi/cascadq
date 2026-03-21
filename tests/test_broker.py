"""Tests for Broker lifecycle and public API."""

import asyncio
from dataclasses import replace

import pytest

from cascadq.broker.broker import Broker
from cascadq.config import BrokerConfig
from cascadq.errors import (
    BrokerFencedError,
    FlushExhaustedError,
    QueueAlreadyExistsError,
    QueueEmptyError,
    QueueNotFoundError,
)
from cascadq.models import QueueMeta, Snapshot, TaskStatus, serialize_snapshot
from tests.support import FaultInjectingStore

from .conftest import make_key as _key


async def _start_broker(
    store: FaultInjectingStore,
    config: BrokerConfig,
) -> Broker:
    broker = Broker(store=store, config=config, clock=lambda: 1000.0)
    await broker.start()
    return broker


class TestBrokerLifecycle:
    async def test_start_discovers_existing_queues(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        qf = Snapshot(
            metadata=QueueMeta(created_at=500.0, payload_schema={}),
        )
        await memory_store.write_new(
            "queues/existing.json", serialize_snapshot(qf)
        )

        broker = Broker(
            store=memory_store, config=test_config, clock=lambda: 1000.0
        )
        await broker.start()

        await broker.push("existing", {"data": 1}, _key())
        await broker.stop()


class TestCreateDeleteQueue:
    async def test_create_and_push(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        await broker.push("work", {"job": "test"}, _key())
        await broker.stop()

    async def test_create_duplicate_raises(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        with pytest.raises(QueueAlreadyExistsError):
            await broker.create_queue("work")
        await broker.stop()

    async def test_delete_removes_queue(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("work")
        await broker.delete_queue("work")
        with pytest.raises(QueueNotFoundError):
            await broker.push("work", {}, _key())
        await broker.stop()

    async def test_delete_nonexistent_raises(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        with pytest.raises(QueueNotFoundError):
            await broker.delete_queue("nope")
        await broker.stop()


class TestPushClaimFinish:
    async def test_full_task_lifecycle(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
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
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")
        with pytest.raises(QueueEmptyError):
            await broker.claim("q", _key(), timeout_seconds=0)
        await broker.stop()

    async def test_push_to_nonexistent_queue_raises(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        with pytest.raises(QueueNotFoundError):
            await broker.push("nope", {}, _key())
        await broker.stop()


class TestBrokerFencing:
    async def test_cas_conflict_fences_only_one_queue(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")
        await broker.create_queue("healthy")

        memory_store.inject_conflict("queues/q.json")
        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 1}, _key())

        assert broker.is_fenced

        with pytest.raises(BrokerFencedError):
            await broker.push("q", {"x": 2}, _key())

        await broker.push("healthy", {"x": 3}, _key())
        await broker.stop()

    async def test_blocking_claim_unblocks_on_fencing(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        """A claim blocked on an empty queue should fail promptly when fenced."""
        broker = await _start_broker(memory_store, test_config)
        await broker.create_queue("q")

        async def fence_after_delay() -> None:
            await asyncio.sleep(0.05)
            memory_store.inject_conflict("queues/q.json")
            # Push to queue q — this triggers a flush that hits the
            # conflict on queues/q.json, fencing the broker.
            try:
                await broker.push("q", {"x": 1}, _key())
            except BrokerFencedError:
                pass

        fence_task = asyncio.create_task(fence_after_delay())
        with pytest.raises(BrokerFencedError):
            await broker.claim("q", _key())
        await fence_task
        await broker.stop()

    async def test_flush_exhaustion_recovers_automatically(
        self, memory_store: FaultInjectingStore, test_config: BrokerConfig
    ) -> None:
        """After flush exhaustion, the flusher enters recovering state
        and automatically reloads from durable state in the background.
        Once recovered, the queue accepts requests again."""
        broker = await _start_broker(
            memory_store,
            replace(
                test_config,
                max_consecutive_flush_failures=2,
                flush_retry_delay_seconds=0.01,
                flush_recovery_interval_seconds=0.1,
            ),
        )
        await broker.create_queue("q")
        await broker.create_queue("healthy")

        memory_store.inject_transient_error("queues/q.json", count=2)
        with pytest.raises(FlushExhaustedError):
            await broker.push("q", {"x": 1}, _key())

        # Other queues are unaffected
        await broker.push("healthy", {"x": 2}, _key())

        # Queue recovers in the background — retry until it accepts
        for _ in range(50):
            try:
                await broker.push("q", {"x": 3}, _key())
                break
            except FlushExhaustedError:
                await asyncio.sleep(0.1)
        else:
            pytest.fail("queue did not recover within timeout")

        task = await broker.claim("q", _key())
        assert task.payload == {"x": 3}

        await broker.stop()
