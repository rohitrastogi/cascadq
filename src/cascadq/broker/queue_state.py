"""Per-queue in-memory state and mutation methods."""

from __future__ import annotations

import asyncio
import heapq
import logging
from collections.abc import Callable

import jsonschema

from cascadq.errors import (
    PayloadValidationError,
    QueueEmptyError,
    TaskNotClaimedError,
    TaskNotFoundError,
)
from cascadq.models import (
    IdempotencyRecord,
    QueueFile,
    QueueMetadata,
    Task,
    TaskStatus,
)
from cascadq.storage.protocol import VersionToken

logger = logging.getLogger(__name__)


class FlushWaiter:
    """A waiter that blocks a client request until the next successful flush.

    The flush loop resolves it by calling set_result() or set_error().
    Callers check the result via the event and error slot.
    """

    __slots__ = ("_error", "_event")

    def __init__(self) -> None:
        self._event = asyncio.Event()
        self._error: Exception | None = None

    def set_result(self) -> None:
        self._event.set()

    def set_error(self, error: Exception) -> None:
        self._error = error
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()
        if self._error is not None:
            raise self._error


class QueueState:
    """Mutable in-memory state for a single queue.

    All time-dependent methods accept an explicit `now: float` parameter
    for deterministic testing.
    """

    def __init__(
        self,
        name: str,
        queue_file: QueueFile,
        version: VersionToken,
        idempotency_ttl_seconds: float = 300.0,
    ) -> None:
        self.name = name
        self._metadata = queue_file.metadata
        self._next_sequence = queue_file.next_sequence
        self._compacted_through_sequence = queue_file.compacted_through_sequence
        self._idempotency_ttl = idempotency_ttl_seconds
        self._idempotency_keys: dict[str, IdempotencyRecord] = dict(
            queue_file.idempotency_keys
        )
        self._tasks: dict[str, Task] = {t.task_id: t for t in queue_file.tasks}
        self._claim_key_index: dict[str, str] = {
            t.claim_idempotency_key: t.task_id
            for t in queue_file.tasks
            if t.claim_idempotency_key
        }
        self.version = version
        self._write_buffer: list[FlushWaiter] = []
        self._dirty = False
        self._pending_heap: list[tuple[int, str]] = [
            (t.sequence, t.task_id)
            for t in queue_file.tasks
            if t.status == TaskStatus.pending
        ]
        heapq.heapify(self._pending_heap)
        self._push_event = asyncio.Event()
        self._sorted_task_ids: list[str] | None = None
        # Compile the JSON Schema validator once rather than on every push.
        schema = self._metadata.payload_schema
        if schema:
            cls = jsonschema.validators.validator_for(schema)
            self._schema_validator: jsonschema.protocols.Validator | None = cls(schema)
        else:
            self._schema_validator = None

    @property
    def metadata(self) -> QueueMetadata:
        return self._metadata

    def push(
        self,
        task_id: str,
        payload: dict,
        now: float,
        idempotency_key: str,
    ) -> FlushWaiter:
        """Add a new task to the queue. Validates payload against schema.

        If *idempotency_key* was already used, this is a no-op.
        """
        existing = self._idempotency_keys.get(idempotency_key)
        if existing is not None:
            return self._append_waiter()

        if self._schema_validator is not None:
            try:
                self._schema_validator.validate(payload)
            except jsonschema.ValidationError as e:
                raise PayloadValidationError(str(e.message)) from e

        task = Task(
            task_id=task_id,
            sequence=self._next_sequence,
            created_at=now,
            status=TaskStatus.pending,
            payload=payload,
        )
        self._next_sequence += 1
        self._tasks[task.task_id] = task
        self._sorted_task_ids = None
        heapq.heappush(self._pending_heap, (task.sequence, task.task_id))
        self._push_event.set()
        self._idempotency_keys[idempotency_key] = IdempotencyRecord(
            task_id=task_id, created_at=now,
        )
        self._dirty = True
        return self._append_waiter()

    def claim(
        self, now: float, idempotency_key: str,
    ) -> tuple[Task, FlushWaiter]:
        """Claim the pending task with the lowest sequence number.

        If *idempotency_key* matches a task that is still claimed,
        the same task is returned (replay).  If the original task
        was re-queued or completed, the stale index entry is removed
        and a fresh claim proceeds.
        """
        existing_id = self._claim_key_index.get(idempotency_key)
        if existing_id is not None:
            task = self._tasks.get(existing_id)
            if task is not None and task.status == TaskStatus.claimed:
                return task, self._append_waiter()
            del self._claim_key_index[idempotency_key]

        task = self._pop_next_pending()
        if task is None:
            raise QueueEmptyError(f"no pending tasks in queue {self.name!r}")
        claimed = task.claim(now, claim_idempotency_key=idempotency_key)
        self._tasks[claimed.task_id] = claimed
        self._claim_key_index[idempotency_key] = claimed.task_id
        self._dirty = True
        return claimed, self._append_waiter()

    def heartbeat(self, task_id: str, now: float) -> None:
        """Update the heartbeat timestamp for a claimed task.

        Returns immediately after the in-memory update — no waiter is
        created.  The updated timestamp is flushed to durable storage
        as part of the next normal flush cycle (backup for recovery),
        but the heartbeat RPC should not block on that flush.
        """
        task = self._get_task(task_id)
        if task.status != TaskStatus.claimed:
            raise TaskNotClaimedError(
                f"task {task_id!r} is not claimed (status={task.status.value})"
            )
        self._tasks[task_id] = task.heartbeat(now)
        self._dirty = True

    def finish(
        self,
        task_id: str,
        sequence: int,
    ) -> FlushWaiter:
        """Mark a claimed task as completed.

        Idempotent: finishing an already-completed task or a task that
        has been compacted away (sequence <= watermark) is a no-op.
        This handles retries where the first attempt succeeded but
        the client never received the response.
        """
        task = self._tasks.get(task_id)
        if task is None:
            if sequence <= self._compacted_through_sequence:
                return self._append_waiter()
            raise TaskNotFoundError(
                f"task {task_id!r} not found in queue {self.name!r}"
            )
        if task.status == TaskStatus.completed:
            return self._append_waiter()
        if task.status != TaskStatus.claimed:
            raise TaskNotClaimedError(
                f"task {task_id!r} is not claimed (status={task.status.value})"
            )
        self._tasks[task_id] = task.finish()
        self._dirty = True
        return self._append_waiter()

    def timeout_expired_claims(
        self, now: float, timeout_seconds: float, next_task_id_fn: Callable[[], str]
    ) -> None:
        """Re-queue tasks whose heartbeat has expired.

        Re-queued tasks get sequence numbers lower than all current tasks
        so they sort to the front of the queue.
        """
        expired = [
            t
            for t in self._tasks.values()
            if t.status == TaskStatus.claimed
            and t.last_heartbeat is not None
            and (now - t.last_heartbeat) > timeout_seconds
        ]
        if not expired:
            return
        # Place re-queued tasks ahead of all pending tasks.
        # The heap minimum is O(1); fall back to scanning _tasks
        # only when the heap is empty (all tasks are claimed/completed).
        if self._pending_heap:
            next_seq = self._pending_heap[0][0] - 1
        else:
            next_seq = min(
                (t.sequence for t in self._tasks.values()), default=0
            ) - 1
        for task in expired:
            logger.info(
                "Heartbeat timeout for task %s in queue %s, re-queuing",
                task.task_id,
                self.name,
            )
            if task.claim_idempotency_key:
                self._claim_key_index.pop(task.claim_idempotency_key, None)
            del self._tasks[task.task_id]
            new_id = next_task_id_fn()
            new_task = Task(
                task_id=new_id,
                sequence=next_seq,
                created_at=task.created_at,
                status=TaskStatus.pending,
                payload=task.payload,
            )
            self._tasks[new_id] = new_task
            heapq.heappush(self._pending_heap, (next_seq, new_id))
            next_seq -= 1
        self._sorted_task_ids = None
        self._push_event.set()
        self._dirty = True

    async def wait_for_push(self, timeout: float | None) -> None:
        """Block until a push or re-queue signals new pending work.

        Clears the event first so we only wake on *new* pushes.
        Safe against races: single-threaded asyncio means no yield
        between the failed claim() and the clear().
        """
        self._push_event.clear()
        await asyncio.wait_for(self._push_event.wait(), timeout)

    def wake_blocked_claims(self) -> None:
        """Wake any claim() calls blocked on wait_for_push().

        Called by the flush coordinator on fencing so that blocked
        claims promptly see the shutdown error instead of hanging.
        """
        self._push_event.set()

    def compact(self, now: float) -> None:
        """Remove completed tasks and expired idempotency keys."""
        completed = [
            t for t in self._tasks.values()
            if t.status == TaskStatus.completed
        ]
        # Expire idempotency keys older than the TTL
        cutoff = now - self._idempotency_ttl
        expired_keys = [
            k for k, r in self._idempotency_keys.items()
            if r.created_at < cutoff
        ]
        if not completed and not expired_keys:
            return
        if completed:
            max_seq = max(t.sequence for t in completed)
            if max_seq > self._compacted_through_sequence:
                self._compacted_through_sequence = max_seq
            for t in completed:
                if t.claim_idempotency_key:
                    self._claim_key_index.pop(
                        t.claim_idempotency_key, None,
                    )
                del self._tasks[t.task_id]
        for k in expired_keys:
            del self._idempotency_keys[k]
        # Rebuild the heap to discard stale entries that accumulate
        # from claimed tasks and heartbeat-timeout re-queues.
        self._pending_heap = [
            (t.sequence, t.task_id)
            for t in self._tasks.values()
            if t.status == TaskStatus.pending
        ]
        heapq.heapify(self._pending_heap)
        self._sorted_task_ids = None
        logger.info(
            "Compacted %d completed tasks from queue %s",
            len(completed),
            self.name,
        )
        self._dirty = True

    def snapshot(self) -> QueueFile:
        """Return the current state as an immutable QueueFile.

        Caches the sorted task-id order so flushes triggered by
        data-only changes (heartbeat, claim, finish) skip the sort.
        """
        if self._sorted_task_ids is None:
            self._sorted_task_ids = sorted(
                self._tasks, key=lambda tid: self._tasks[tid].sequence,
            )
        tasks = [self._tasks[tid] for tid in self._sorted_task_ids]
        return QueueFile(
            metadata=self._metadata,
            next_sequence=self._next_sequence,
            compacted_through_sequence=self._compacted_through_sequence,
            tasks=tasks,
            idempotency_keys=dict(self._idempotency_keys),
        )

    def swap_write_buffer(self) -> list[FlushWaiter]:
        """Swap the write buffer for double-buffering.

        Returns the current buffer and replaces it with an empty one.
        """
        buffer = self._write_buffer
        self._write_buffer = []
        return buffer

    def prepend_waiters(self, waiters: list[FlushWaiter]) -> None:
        """Re-insert waiters at the front of the write buffer.

        Used by the flush coordinator to keep waiters pending after a
        transient flush failure so they are re-collected on the next cycle.
        """
        self._write_buffer = waiters + self._write_buffer

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    @property
    def has_pending_waiters(self) -> bool:
        return len(self._write_buffer) > 0

    def mark_clean(self) -> None:
        self._dirty = False

    def _pop_next_pending(self) -> Task | None:
        """Pop the lowest-sequence pending task from the heap.

        Skips stale entries where the task was claimed, finished,
        or removed (e.g., by timeout re-queue of the original).
        """
        while self._pending_heap:
            _seq, task_id = heapq.heappop(self._pending_heap)
            task = self._tasks.get(task_id)
            if task is not None and task.status == TaskStatus.pending:
                return task
        return None

    def _get_task(self, task_id: str) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise TaskNotFoundError(
                f"task {task_id!r} not found in queue {self.name!r}"
            )
        return task

    def _append_waiter(self) -> FlushWaiter:
        waiter = FlushWaiter()
        self._write_buffer.append(waiter)
        return waiter

