"""Per-queue in-memory state and mutation methods."""

from __future__ import annotations

import logging
from collections import deque
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
    PushRecord,
    QueueMeta,
    Snapshot,
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


class TaskQueue:
    """Mutable in-memory state for a single queue.

    All time-dependent methods accept an explicit `now: float` parameter
    for deterministic testing.
    """

    def __init__(
        self,
        name: str,
        queue_file: Snapshot,
        version: VersionToken,
        push_key_ttl_seconds: float = 300.0,
    ) -> None:
        self.name = name
        self._metadata = queue_file.metadata
        self._next_sequence = queue_file.next_sequence
        self._compaction_watermark = queue_file.compaction_watermark
        self._push_key_ttl = push_key_ttl_seconds
        self._push_keys: dict[str, PushRecord] = dict(
            queue_file.push_keys
        )
        self._tasks: dict[str, Task] = {t.task_id: t for t in queue_file.tasks}
        self._claim_keys: dict[str, str] = {
            t.claim_key: t.task_id
            for t in queue_file.tasks
            if t.claim_key
        }
        self.version = version
        pending = sorted(
            (t for t in queue_file.tasks if t.status == TaskStatus.pending),
            key=lambda t: t.sequence,
        )
        self._claim_order: deque[str] = deque(t.task_id for t in pending)
        self._pending_activation: set[str] = set()
        # Compile the JSON Schema validator once rather than on every push.
        schema = self._metadata.payload_schema
        if schema:
            cls = jsonschema.validators.validator_for(schema)
            self._schema_validator: jsonschema.protocols.Validator | None = cls(schema)
        else:
            self._schema_validator = None

    @property
    def metadata(self) -> QueueMeta:
        return self._metadata

    def push(
        self,
        task_id: str,
        payload: dict,
        now: float,
        push_key: str,
    ) -> bool:
        """Add a new task to the queue. Validates payload against schema.

        Returns True if a new task was created, False if idempotent replay.
        """
        if push_key in self._push_keys:
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
        self._claim_order.append(task.task_id)
        self._push_keys[push_key] = PushRecord(
            task_id=task_id, created_at=now,
        )
        return True

    def claim(
        self, now: float, claim_key: str,
    ) -> ClaimResult:
        """Claim the pending task with the lowest sequence number.

        Sets ``last_heartbeat`` immediately so the lease timestamp is
        durable in the same flush as the claim.  The task's lease is
        inactive until ``activate_lease`` is called, so the heartbeat
        checker skips it until the claim response reaches the client.
        """
        existing_id = self._claim_keys.get(claim_key)
        if existing_id is not None:
            task = self._tasks.get(existing_id)
            if task is not None and task.status == TaskStatus.claimed:
                return ClaimResult(task, False)
            del self._claim_keys[claim_key]

        task = self._pop_next_pending()
        if task is None:
            raise QueueEmptyError(f"no pending tasks in queue {self.name!r}")
        task.claim(now, claim_key=claim_key)
        self._claim_keys[claim_key] = task.task_id
        self._pending_activation.add(task.task_id)
        return ClaimResult(task, True)

    def activate_lease(self, task_id: str) -> None:
        """Make a claimed task eligible for heartbeat timeout.

        Called after the claim response reaches the client, so the
        heartbeat checker doesn't re-queue a task whose claim is
        still in flight.
        """
        self._pending_activation.discard(task_id)

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
        task.heartbeat(now)

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
            if sequence <= self._compaction_watermark:
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
        task.finish()
        return True

    def requeue_expired(
        self, now: float, timeout_seconds: float, next_task_id_fn: Callable[[], str]
    ) -> int:
        """Re-queue tasks whose heartbeat has expired. Returns the count.

        Re-queued tasks are placed at the front of the pending queue.
        """
        expired = [
            t
            for t in self._tasks.values()
            if t.status == TaskStatus.claimed
            and t.last_heartbeat is not None
            and t.task_id not in self._pending_activation
            and (now - t.last_heartbeat) > timeout_seconds
        ]
        if not expired:
            return 0
        min_seq = min(t.sequence for t in self._tasks.values()) - 1
        # Build re-queued tasks in reverse so appendleft preserves order.
        requeued: list[str] = []
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
            if task.claim_key:
                self._claim_keys.pop(task.claim_key, None)
            self._pending_activation.discard(task.task_id)
            del self._tasks[task.task_id]
            new_id = next_task_id_fn()
            new_task = Task(
                task_id=new_id,
                sequence=min_seq,
                created_at=task.created_at,
                status=TaskStatus.pending,
                payload=task.payload,
            )
            self._tasks[new_id] = new_task
            requeued.append(new_id)
            min_seq -= 1
        for task_id in reversed(requeued):
            self._claim_order.appendleft(task_id)
        return len(expired)

    def compact(self, now: float) -> int:
        """Remove completed tasks and expired push keys.

        Returns the number of completed tasks removed.
        """
        completed = [
            t for t in self._tasks.values()
            if t.status == TaskStatus.completed
        ]
        # Expire push keys older than the TTL
        cutoff = now - self._push_key_ttl
        expired_keys = [
            k for k, r in self._push_keys.items()
            if r.created_at < cutoff
        ]
        if not completed and not expired_keys:
            return 0
        if completed:
            max_seq = max(t.sequence for t in completed)
            if max_seq > self._compaction_watermark:
                self._compaction_watermark = max_seq
            for t in completed:
                if t.claim_key:
                    self._claim_keys.pop(
                        t.claim_key, None,
                    )
                del self._tasks[t.task_id]
        for k in expired_keys:
            del self._push_keys[k]
        logger.info(
            "Compacted %d completed tasks from queue %s",
            len(completed),
            self.name,
        )
        return len(completed)

    def snapshot(self) -> Snapshot:
        """Return the current state as a Snapshot for serialization."""
        return Snapshot(
            metadata=self._metadata,
            next_sequence=self._next_sequence,
            compaction_watermark=self._compaction_watermark,
            tasks=list(self._tasks.values()),
            push_keys=dict(self._push_keys),
        )

    def _pop_next_pending(self) -> Task | None:
        """Pop the next pending task from the front of the queue.

        Skips stale entries where the task was claimed, finished,
        or removed (e.g., by re-queue of the original).
        """
        while self._claim_order:
            task_id = self._claim_order.popleft()
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

