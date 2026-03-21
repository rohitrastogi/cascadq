"""Per-queue in-memory state and mutation methods."""

from __future__ import annotations

import asyncio
import heapq
import logging
from collections.abc import Callable
from dataclasses import dataclass

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


@dataclass(slots=True)
class ClaimResult:
    """Result of a claim operation."""

    task: Task
    mutated: bool


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
        self._pending_heap: list[tuple[int, str]] = [
            (t.sequence, t.task_id)
            for t in queue_file.tasks
            if t.status == TaskStatus.pending
        ]
        heapq.heapify(self._pending_heap)
        self._pending_delivery: set[str] = set()
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
    ) -> bool:
        """Add a new task to the queue. Validates payload against schema.

        Returns True if a new task was created, False if idempotent replay.
        """
        existing = self._idempotency_keys.get(idempotency_key)
        if existing is not None:
            return False

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
        return True

    def claim(
        self, now: float, idempotency_key: str,
    ) -> ClaimResult:
        """Claim the pending task with the lowest sequence number.

        Sets ``last_heartbeat`` immediately so the lease timestamp is
        durable in the same flush as the claim.  The task is marked as
        pending delivery so the heartbeat checker skips it until the
        claim response reaches the client.
        """
        existing_id = self._claim_key_index.get(idempotency_key)
        if existing_id is not None:
            task = self._tasks.get(existing_id)
            if task is not None and task.status == TaskStatus.claimed:
                return ClaimResult(task, False)
            del self._claim_key_index[idempotency_key]

        task = self._pop_next_pending()
        if task is None:
            raise QueueEmptyError(f"no pending tasks in queue {self.name!r}")
        claimed = task.claim(now, claim_idempotency_key=idempotency_key)
        self._tasks[claimed.task_id] = claimed
        self._claim_key_index[idempotency_key] = claimed.task_id
        self._pending_delivery.add(claimed.task_id)
        return ClaimResult(claimed, True)

    def confirm_delivery(self, task_id: str) -> None:
        """Mark a claim as delivered to the client (in-memory only).

        After this, the heartbeat checker can timeout the task if the
        client stops sending heartbeats.
        """
        self._pending_delivery.discard(task_id)

    def heartbeat(self, task_id: str, now: float) -> None:
        """Renew the heartbeat lease for a claimed task.

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

    def finish(
        self,
        task_id: str,
        sequence: int,
    ) -> bool:
        """Mark a claimed task as completed.

        Returns True if the task was newly completed, False if idempotent
        replay.  Finishing a task compacted away (sequence <= watermark)
        is also a no-op.
        """
        task = self._tasks.get(task_id)
        if task is None:
            if sequence <= self._compacted_through_sequence:
                return False
            raise TaskNotFoundError(
                f"task {task_id!r} not found in queue {self.name!r}"
            )
        if task.status == TaskStatus.completed:
            return False
        if task.status != TaskStatus.claimed:
            raise TaskNotClaimedError(
                f"task {task_id!r} is not claimed (status={task.status.value})"
            )
        self._tasks[task_id] = task.finish()
        return True

    def timeout_expired_claims(
        self, now: float, timeout_seconds: float, next_task_id_fn: Callable[[], str]
    ) -> int:
        """Re-queue tasks whose heartbeat has expired. Returns the count.

        Re-queued tasks get sequence numbers lower than all current tasks
        so they sort to the front of the queue.
        """
        expired = [
            t
            for t in self._tasks.values()
            if t.status == TaskStatus.claimed
            and t.last_heartbeat is not None
            and t.task_id not in self._pending_delivery
            and (now - t.last_heartbeat) > timeout_seconds
        ]
        if not expired:
            return 0
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
            age = now - task.last_heartbeat if task.last_heartbeat else float("inf")
            logger.info(
                "Heartbeat timeout for task %s in queue %s "
                "(last_heartbeat %.1fs ago, timeout=%.1fs), re-queuing",
                task.task_id,
                self.name,
                age,
                timeout_seconds,
            )
            if task.claim_idempotency_key:
                self._claim_key_index.pop(task.claim_idempotency_key, None)
            self._pending_delivery.discard(task.task_id)
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
        return len(expired)

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

        Called by the queue flusher on fencing so that blocked
        claims promptly see the shutdown error instead of hanging.
        """
        self._push_event.set()

    def compact(self, now: float) -> int:
        """Remove completed tasks and expired idempotency keys.

        Returns the number of completed tasks removed.
        """
        completed: list[Task] = []
        pending_heap: list[tuple[int, str]] = []
        for t in self._tasks.values():
            if t.status == TaskStatus.completed:
                completed.append(t)
            elif t.status == TaskStatus.pending:
                pending_heap.append((t.sequence, t.task_id))
        # Expire idempotency keys older than the TTL
        cutoff = now - self._idempotency_ttl
        expired_keys = [
            k for k, r in self._idempotency_keys.items()
            if r.created_at < cutoff
        ]
        if not completed and not expired_keys:
            return 0
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
        # pending_heap was built from the single pass above and
        # excludes completed tasks (already filtered).
        self._pending_heap = pending_heap
        heapq.heapify(self._pending_heap)
        self._sorted_task_ids = None
        logger.info(
            "Compacted %d completed tasks from queue %s",
            len(completed),
            self.name,
        )
        return len(completed)

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

