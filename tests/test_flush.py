"""Tests for queue-local flush behavior and fencing."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest

from cascadq.broker.queue_flusher import QueueFlusher
from cascadq.broker.queue_state import QueueState
from cascadq.errors import BrokerFencedError, FlushExhaustedError
from cascadq.models import QueueFile, QueueMetadata, serialize_queue_file
from tests.support import FaultInjectingStore

from .conftest import make_idempotency_key as _key


async def _make_flusher(
    store: FaultInjectingStore,
    name: str = "test",
    *,
    max_consecutive_failures: int = 3,
    retry_delay_seconds: float = 0.01,
    recovery_interval_seconds: float = 0.1,
) -> QueueFlusher:
    """Create one queue flusher with a persisted backing object."""
    queue_file = QueueFile(
        metadata=QueueMetadata(created_at=1000.0, payload_schema={}),
    )
    version = await store.write_new(
        f"queues/{name}.json",
        serialize_queue_file(queue_file),
    )
    state = QueueState(name=name, queue_file=queue_file, version=version)
    return QueueFlusher(
        store=store,
        prefix="",
        state=state,
        max_consecutive_failures=max_consecutive_failures,
        retry_delay_seconds=retry_delay_seconds,
        recovery_interval_seconds=recovery_interval_seconds,
    )


@asynccontextmanager
async def _running_flusher(
    store: FaultInjectingStore,
    name: str = "test",
    **kwargs: float | int,
) -> AsyncGenerator[QueueFlusher]:
    """Create, start, yield, and stop a flusher."""
    flusher = await _make_flusher(store, name, **kwargs)  # type: ignore[arg-type]
    flusher.start()
    try:
        yield flusher
    finally:
        await flusher.stop()


class TestQueueFlusher:
    async def test_flush_resolves_waiter(
        self, memory_store: FaultInjectingStore
    ) -> None:
        async with _running_flusher(memory_store) as flusher:
            waiter = flusher.push("t1", {}, now=100.0, idempotency_key=_key())

            await asyncio.wait_for(waiter.wait(), timeout=2.0)
            data, _ = await memory_store.read("queues/test.json")
            assert b"t1" in data

    async def test_conflict_fences_only_that_queue(
        self, memory_store: FaultInjectingStore
    ) -> None:
        async with (
            _running_flusher(memory_store, "a") as flusher_a,
            _running_flusher(memory_store, "b") as flusher_b,
        ):
            waiter_a = flusher_a.push("t1", {}, now=100.0, idempotency_key=_key())
            waiter_b = flusher_b.push("t2", {}, now=100.0, idempotency_key=_key())

            memory_store.inject_conflict("queues/a.json")

            with pytest.raises(BrokerFencedError):
                await asyncio.wait_for(waiter_a.wait(), timeout=2.0)
            await asyncio.wait_for(waiter_b.wait(), timeout=2.0)

            assert flusher_a.is_fenced
            assert not flusher_b.is_fenced
            data_b, _ = await memory_store.read("queues/b.json")
            assert b"t2" in data_b

    async def test_transient_failure_retries_and_succeeds(
        self, memory_store: FaultInjectingStore
    ) -> None:
        async with _running_flusher(
            memory_store, max_consecutive_failures=3,
        ) as flusher:
            waiter = flusher.push("t1", {}, now=100.0, idempotency_key=_key())
            memory_store.inject_transient_error("queues/test.json", count=1)

            await asyncio.wait_for(waiter.wait(), timeout=5.0)
            assert not flusher.is_fenced

            data, _ = await memory_store.read("queues/test.json")
            assert b"t1" in data

    async def test_transient_exhaustion_enters_recovering(
        self, memory_store: FaultInjectingStore
    ) -> None:
        """Flush exhaustion enters recovering state (not fenced).
        The flusher will try to reload from durable state."""
        async with _running_flusher(
            memory_store, max_consecutive_failures=2,
        ) as flusher:
            waiter = flusher.push("t1", {}, now=100.0, idempotency_key=_key())
            memory_store.inject_transient_error("queues/test.json", count=2)

            with pytest.raises(FlushExhaustedError):
                await asyncio.wait_for(waiter.wait(), timeout=5.0)
            assert flusher.is_recovering
            assert not flusher.is_fenced

    async def test_slow_queue_does_not_block_other_queue(
        self, memory_store: FaultInjectingStore
    ) -> None:
        async with (
            _running_flusher(memory_store, "slow") as slow_flusher,
            _running_flusher(memory_store, "fast") as fast_flusher,
        ):
            slow_waiter = slow_flusher.push("slow-task", {}, 100.0, _key())
            fast_waiter = fast_flusher.push("fast-task", {}, 100.0, _key())

            memory_store.inject_write_delay("queues/slow.json", 0.5)

            await asyncio.wait_for(fast_waiter.wait(), timeout=0.2)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(slow_waiter.wait(), timeout=0.2)

            await asyncio.wait_for(slow_waiter.wait(), timeout=2.0)

    async def test_recovery_fences_on_version_mismatch(
        self, memory_store: FaultInjectingStore
    ) -> None:
        """If another writer changed the durable version during flush
        exhaustion, recovery must fence instead of silently adopting
        the foreign state."""
        async with _running_flusher(
            memory_store, max_consecutive_failures=2,
        ) as flusher:
            waiter = flusher.push("t1", {}, now=100.0, idempotency_key=_key())
            memory_store.inject_transient_error("queues/test.json", count=2)

            with pytest.raises(FlushExhaustedError):
                await asyncio.wait_for(waiter.wait(), timeout=5.0)
            assert flusher.is_recovering

            # Simulate another broker writing to the same key
            current_data, current_version = await memory_store.read(
                "queues/test.json",
            )
            await memory_store.write(
                "queues/test.json", current_data, current_version,
            )

            # Let recovery run — it should detect the version mismatch
            for _ in range(50):
                if not flusher.is_recovering:
                    break
                await asyncio.sleep(0.1)

            assert flusher.is_fenced
            assert isinstance(flusher.shutdown_error, BrokerFencedError)
