"""Tests for per-queue state management.

Covers FIFO ordering, claim/finish lifecycle, heartbeat timeout
with front-of-queue re-queue, compaction, and schema validation.
"""

import pytest

from cascadq.broker.queue_state import QueueState
from cascadq.errors import (
    PayloadValidationError,
    QueueEmptyError,
    TaskNotClaimedError,
    TaskNotFoundError,
)
from cascadq.models import QueueFile, QueueMetadata, TaskStatus


def _make_state(
    name: str = "test",
    payload_schema: dict | None = None,
    created_at: float = 1000.0,
) -> QueueState:
    schema = payload_schema or {}
    qf = QueueFile(
        metadata=QueueMetadata(created_at=created_at, payload_schema=schema),
        next_sequence=0,
        tasks=[],
    )
    return QueueState(name=name, queue_file=qf, version=1)


class TestFIFOOrdering:
    def test_claims_return_tasks_in_push_order(self) -> None:
        state = _make_state()
        state.push("t1", {"x": 1}, now=100.0)
        state.push("t2", {"x": 2}, now=101.0)
        state.push("t3", {"x": 3}, now=102.0)

        task1, _ = state.claim(now=200.0)
        task2, _ = state.claim(now=201.0)
        task3, _ = state.claim(now=202.0)

        assert task1.task_id == "t1"
        assert task2.task_id == "t2"
        assert task3.task_id == "t3"


class TestClaimFinishLifecycle:
    def test_claim_then_finish(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        task, _ = state.claim(now=200.0)
        assert task.status == TaskStatus.claimed
        state.finish("t1", sequence=0)
        snapshot = state.snapshot()
        assert snapshot.tasks[0].status == TaskStatus.completed

    def test_claim_empty_queue_raises(self) -> None:
        state = _make_state()
        with pytest.raises(QueueEmptyError):
            state.claim(now=100.0)

    def test_finish_unclaimed_task_raises(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        with pytest.raises(TaskNotClaimedError):
            state.finish("t1", sequence=0)

    def test_finish_nonexistent_task_raises(self) -> None:
        state = _make_state()
        with pytest.raises(TaskNotFoundError):
            state.finish("no-such-task", sequence=999)

    def test_finish_already_completed_is_idempotent(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.claim(now=200.0)
        state.finish("t1", sequence=0)
        state.finish("t1", sequence=0)
        snapshot = state.snapshot()
        assert snapshot.tasks[0].status == TaskStatus.completed

    def test_finish_after_compaction_is_idempotent(self) -> None:
        """Retry of a finish after the task was compacted away."""
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.claim(now=200.0)
        state.finish("t1", sequence=0)
        state.compact(now=300.0)
        # t1 is gone, but sequence 0 <= compacted_through_sequence (0)
        state.finish("t1", sequence=0)

    def test_finish_unknown_task_above_watermark_raises(self) -> None:
        """A genuinely unknown task_id with sequence above the watermark."""
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.claim(now=200.0)
        state.finish("t1", sequence=0)
        state.compact(now=300.0)
        # Watermark is now 0.  Sequence 5 is above it → real error.
        with pytest.raises(TaskNotFoundError):
            state.finish("bogus", sequence=5)


class TestClaimIdempotency:
    def test_same_key_returns_same_task(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        task1, _ = state.claim(now=200.0, idempotency_key="k1")
        task2, _ = state.claim(now=201.0, idempotency_key="k1")
        assert task1.task_id == task2.task_id

    def test_different_keys_claim_different_tasks(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.push("t2", {}, now=101.0)
        task1, _ = state.claim(now=200.0, idempotency_key="k1")
        task2, _ = state.claim(now=201.0, idempotency_key="k2")
        assert task1.task_id != task2.task_id

    def test_replay_after_requeue_claims_new_task(self) -> None:
        """If the originally claimed task was re-queued (heartbeat timeout),
        a retry with the same key should claim a fresh task."""
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.push("t2", {}, now=101.0)
        task1, _ = state.claim(now=200.0, idempotency_key="k1")
        # Re-queue t1 via heartbeat timeout
        state.timeout_expired_claims(
            now=250.0, timeout_seconds=30.0,
            next_task_id_fn=lambda: "t1-retry",
        )
        # Retry with same key — original claim is gone, should claim next
        task_retry, _ = state.claim(now=260.0, idempotency_key="k1")
        assert task_retry.task_id != task1.task_id


class TestHeartbeatTimeout:
    def test_expired_claim_is_requeued_to_front(self) -> None:
        state = _make_state()
        state.push("t1", {"x": 1}, now=100.0)
        state.push("t2", {"x": 2}, now=101.0)
        state.claim(now=200.0)

        # t1 is claimed with last_heartbeat=200.0
        # Timeout at now=250, timeout_seconds=30 → 250-200=50 > 30
        id_counter = iter(["t1-retry"])
        state.timeout_expired_claims(
            now=250.0, timeout_seconds=30.0,
            next_task_id_fn=lambda: next(id_counter),
        )

        # Re-queued task should be claimed before t2
        task, _ = state.claim(now=260.0)
        assert task.task_id == "t1-retry"
        assert task.payload == {"x": 1}

        task2, _ = state.claim(now=261.0)
        assert task2.task_id == "t2"

    def test_heartbeat_prevents_timeout(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.claim(now=200.0)
        state.heartbeat("t1", now=225.0)

        # now=250, timeout=30 → 250-225=25, not expired
        state.timeout_expired_claims(
            now=250.0, timeout_seconds=30.0,
            next_task_id_fn=lambda: "should-not-be-called",
        )
        # t1 should still be claimed, not re-queued
        with pytest.raises(QueueEmptyError):
            state.claim(now=260.0)

    def test_finish_old_task_id_after_requeue_raises(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.claim(now=200.0)

        state.timeout_expired_claims(
            now=250.0, timeout_seconds=30.0,
            next_task_id_fn=lambda: "t1-retry",
        )
        # Old task_id "t1" no longer exists (and sequence 0 is above
        # the compaction watermark since no compaction has run)
        with pytest.raises(TaskNotFoundError):
            state.finish("t1", sequence=0)


class TestCompaction:
    def test_compact_removes_completed_tasks(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.push("t2", {}, now=101.0)
        state.claim(now=200.0)
        state.finish("t1", sequence=0)

        state.compact(now=300.0)
        snapshot = state.snapshot()
        assert len(snapshot.tasks) == 1
        assert snapshot.tasks[0].task_id == "t2"

    def test_compact_noop_when_no_completed(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.mark_clean()
        state.compact(now=200.0)
        assert not state.is_dirty

    def test_idempotency_key_ttl_cleanup(self) -> None:
        """Idempotency keys older than the TTL are removed during compaction."""
        state = _make_state()
        state.push("t1", {}, now=100.0, idempotency_key="key1")
        # Compact well past the 300s TTL
        state.compact(now=500.0)
        # Key should be expired
        assert "key1" not in state.snapshot().idempotency_keys


class TestPayloadValidation:
    def test_invalid_payload_raises(self) -> None:
        state = _make_state(payload_schema={
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        })
        with pytest.raises(PayloadValidationError):
            state.push("t1", {"not_url": 123}, now=100.0)

    def test_valid_payload_accepted(self) -> None:
        state = _make_state(payload_schema={
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        })
        state.push("t1", {"url": "http://example.com"}, now=100.0)
        snapshot = state.snapshot()
        assert len(snapshot.tasks) == 1


class TestSnapshot:
    def test_snapshot_returns_tasks_sorted_by_sequence(self) -> None:
        state = _make_state()
        state.push("t1", {}, now=100.0)
        state.push("t2", {}, now=101.0)
        state.push("t3", {}, now=102.0)
        state.claim(now=200.0)

        # Timeout t1 → re-queued with negative sequence
        state.timeout_expired_claims(
            now=250.0, timeout_seconds=30.0,
            next_task_id_fn=lambda: "t1r",
        )
        snapshot = state.snapshot()
        # Re-queued task has min_seq - 1, so it sorts before t2 and t3
        assert snapshot.tasks[0].task_id == "t1r"
        assert snapshot.tasks[0].sequence < snapshot.tasks[1].sequence
