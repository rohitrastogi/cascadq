"""Tests for FlushBuffer coordination logic."""

import asyncio

import pytest

from cascadq.broker.flush_buffer import FlushBuffer


class TestRecordMutation:
    def test_marks_dirty(self) -> None:
        buf = FlushBuffer()
        assert not buf.needs_flush
        buf.record_mutation()
        assert buf.needs_flush

    def test_increments_generation(self) -> None:
        buf = FlushBuffer()
        g0 = buf.generation
        buf.record_mutation()
        assert buf.generation == g0 + 1


class TestRecordWaiter:
    def test_does_not_mark_dirty(self) -> None:
        buf = FlushBuffer()
        buf.record_waiter()
        # needs_flush is True because there are pending waiters,
        # but the generation hasn't advanced (not dirty).
        assert buf.needs_flush
        batch = buf.begin_flush()
        assert not batch.is_dirty
        assert len(batch.waiters) == 1


class TestMarkDirty:
    def test_without_waiter(self) -> None:
        buf = FlushBuffer()
        buf.mark_dirty()
        assert buf.needs_flush
        batch = buf.begin_flush()
        assert batch.is_dirty
        assert len(batch.waiters) == 0


class TestBeginFlush:
    def test_captures_and_clears(self) -> None:
        buf = FlushBuffer()
        w1 = buf.record_mutation()
        w2 = buf.record_mutation()
        batch = buf.begin_flush()
        assert batch.waiters == [w1, w2]
        assert batch.is_dirty
        assert batch.generation == 2
        # Buffer is now empty
        empty_batch = buf.begin_flush()
        assert len(empty_batch.waiters) == 0


class TestCompleteFlush:
    async def test_resolves_waiters(self) -> None:
        buf = FlushBuffer()
        w1 = buf.record_mutation()
        w2 = buf.record_mutation()
        batch = buf.begin_flush()
        buf.complete_flush(batch)
        # Waiters should resolve without blocking
        await asyncio.wait_for(w1.wait(), timeout=1.0)
        await asyncio.wait_for(w2.wait(), timeout=1.0)

    def test_preserves_concurrent_dirty(self) -> None:
        """A mutation arriving between begin_flush and complete_flush
        must keep the buffer dirty (generation invariant)."""
        buf = FlushBuffer()
        buf.record_mutation()
        batch = buf.begin_flush()
        # New mutation arrives during the S3 write
        buf.record_mutation()
        buf.complete_flush(batch)
        # Buffer should still need a flush for the concurrent mutation
        assert buf.needs_flush


class TestFailFlush:
    def test_requeues_waiters(self) -> None:
        buf = FlushBuffer()
        w1 = buf.record_mutation()
        batch = buf.begin_flush()
        # New waiter arrives during the failed flush attempt
        w2 = buf.record_mutation()
        buf.fail_flush(batch)
        # w1 should be prepended before w2
        next_batch = buf.begin_flush()
        assert next_batch.waiters == [w1, w2]


class TestRejectAll:
    async def test_fails_and_drains(self) -> None:
        buf = FlushBuffer()
        w1 = buf.record_mutation()
        w2 = buf.record_mutation()
        error = RuntimeError("fenced")
        buf.reject_all(error)
        with pytest.raises(RuntimeError, match="fenced"):
            await asyncio.wait_for(w1.wait(), timeout=1.0)
        with pytest.raises(RuntimeError, match="fenced"):
            await asyncio.wait_for(w2.wait(), timeout=1.0)
        # Buffer should be drained
        batch = buf.begin_flush()
        assert len(batch.waiters) == 0
