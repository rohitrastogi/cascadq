"""Tests for flush loop and CAS fencing behavior."""

import asyncio

import pytest

from cascadq.broker.flush import FlushCoordinator
from cascadq.broker.queue_state import QueueState
from cascadq.errors import BrokerFencedError
from cascadq.models import QueueFile, QueueMetadata, serialize_queue_file
from cascadq.storage.memory import InMemoryObjectStore


async def _setup_coordinator(
    store: InMemoryObjectStore,
    queue_names: list[str] | None = None,
    max_consecutive_failures: int = 3,
    retry_delay_seconds: float = 1.0,
) -> tuple[FlushCoordinator, dict[str, QueueState]]:
    """Create a coordinator with queues already persisted in the store."""
    names = queue_names or ["test"]
    states: dict[str, QueueState] = {}

    for name in names:
        qf = QueueFile(
            metadata=QueueMetadata(created_at=1000.0, payload_schema={}),
        )
        key = f"queues/{name}.json"
        version = await store.write_new(key, serialize_queue_file(qf))
        state = QueueState(name=name, queue_file=qf, version=version)
        states[name] = state

    coord = FlushCoordinator(
        store=store,
        prefix="",
        queue_states=states,
        max_consecutive_failures=max_consecutive_failures,
        retry_delay_seconds=retry_delay_seconds,
    )
    return coord, states


class TestFlushCoordinator:
    async def test_flush_cycle_resolves_waiter(
        self, memory_store: InMemoryObjectStore
    ) -> None:
        coord, states = await _setup_coordinator(memory_store)

        waiter = states["test"].push("t1", {}, now=100.0)
        coord.start()
        coord.notify()

        await asyncio.wait_for(waiter.wait(), timeout=2.0)
        data, _ = await memory_store.read("queues/test.json")
        assert b"t1" in data
        await coord.stop()

    async def test_cas_conflict_fences_all_queues(
        self, memory_store: InMemoryObjectStore
    ) -> None:
        coord, states = await _setup_coordinator(memory_store, ["a", "b"])

        waiter_a = states["a"].push("t1", {}, now=100.0)
        waiter_b = states["b"].push("t2", {}, now=100.0)

        memory_store.inject_conflict("queues/a.json")
        coord.start()
        coord.notify()

        with pytest.raises(BrokerFencedError):
            await asyncio.wait_for(waiter_a.wait(), timeout=2.0)
        with pytest.raises(BrokerFencedError):
            await asyncio.wait_for(waiter_b.wait(), timeout=2.0)

        assert coord.is_fenced
        await coord.stop()

    async def test_transient_failure_retries_and_succeeds(
        self, memory_store: InMemoryObjectStore
    ) -> None:
        """A single transient failure keeps waiters pending; the next
        flush cycle succeeds and resolves them."""
        coord, states = await _setup_coordinator(
            memory_store,
            max_consecutive_failures=3,
            retry_delay_seconds=0.01,
        )

        waiter = states["test"].push("t1", {}, now=100.0)
        memory_store.inject_transient_error("queues/test.json", count=1)
        coord.start()
        coord.notify()

        # Waiter should eventually resolve after the retry succeeds
        await asyncio.wait_for(waiter.wait(), timeout=5.0)
        assert not coord.is_fenced

        data, _ = await memory_store.read("queues/test.json")
        assert b"t1" in data
        await coord.stop()

    async def test_transient_exhaustion_fences_broker(
        self, memory_store: InMemoryObjectStore
    ) -> None:
        """Exhausting transient retries fences the broker."""
        coord, states = await _setup_coordinator(
            memory_store,
            max_consecutive_failures=2,
            retry_delay_seconds=0.01,
        )

        waiter = states["test"].push("t1", {}, now=100.0)
        memory_store.inject_transient_error("queues/test.json", count=2)
        coord.start()
        coord.notify()

        with pytest.raises(BrokerFencedError):
            await asyncio.wait_for(waiter.wait(), timeout=5.0)
        assert coord.is_fenced
        await coord.stop()

    async def test_double_buffering_across_flushes(
        self, memory_store: InMemoryObjectStore
    ) -> None:
        """Consecutive pushes are flushed in separate batches."""
        coord, states = await _setup_coordinator(memory_store)
        coord.start()

        waiter1 = states["test"].push("t1", {}, now=100.0)
        coord.notify()
        await asyncio.wait_for(waiter1.wait(), timeout=2.0)

        waiter2 = states["test"].push("t2", {}, now=101.0)
        coord.notify()
        await asyncio.wait_for(waiter2.wait(), timeout=2.0)

        data, _ = await memory_store.read("queues/test.json")
        assert b"t1" in data
        assert b"t2" in data
        await coord.stop()
