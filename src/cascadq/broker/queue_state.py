"""Per-queue in-memory state and mutation methods."""

from __future__ import annotations

import asyncio
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
    ) -> None:
        self.name = name
        self._metadata = queue_file.metadata
        self._next_sequence = queue_file.next_sequence
        self._tasks: dict[str, Task] = {t.task_id: t for t in queue_file.tasks}
        self.version = version
        self._write_buffer: list[FlushWaiter] = []
        self._dirty = False

    @property
    def metadata(self) -> QueueMetadata:
        return self._metadata

    def push(self, task_id: str, payload: dict, now: float) -> FlushWaiter:
        """Add a new task to the queue. Validates payload against schema."""
        schema = self._metadata.payload_schema
        if schema:
            try:
                jsonschema.validate(payload, schema)
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
        self._dirty = True
        return self._append_waiter()

    def claim(self, consumer_id: str, now: float) -> tuple[Task, FlushWaiter]:
        """Claim the pending task with the lowest sequence number."""
        pending = [
            t for t in self._tasks.values() if t.status == TaskStatus.pending
        ]
        if not pending:
            raise QueueEmptyError(f"no pending tasks in queue {self.name!r}")
        pending.sort(key=lambda t: t.sequence)
        task = pending[0]
        claimed = task.claim(consumer_id, now)
        self._tasks[claimed.task_id] = claimed
        self._dirty = True
        return claimed, self._append_waiter()

    def heartbeat(self, task_id: str, now: float) -> FlushWaiter:
        """Update the heartbeat timestamp for a claimed task."""
        task = self._get_task(task_id)
        if task.status != TaskStatus.claimed:
            raise TaskNotClaimedError(
                f"task {task_id!r} is not claimed (status={task.status.value})"
            )
        self._tasks[task_id] = task.heartbeat(now)
        self._dirty = True
        return self._append_waiter()

    def finish(self, task_id: str) -> FlushWaiter:
        """Mark a claimed task as completed."""
        task = self._get_task(task_id)
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
        # Compute the base sequence once, then decrement for each re-queued task
        next_seq = min(
            (t.sequence for t in self._tasks.values()), default=0
        ) - 1
        for task in expired:
            logger.info(
                "Heartbeat timeout for task %s in queue %s, re-queuing",
                task.task_id,
                self.name,
            )
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
            next_seq -= 1
        self._dirty = True

    def compact(self) -> None:
        """Remove completed tasks from state."""
        completed = [
            tid for tid, t in self._tasks.items()
            if t.status == TaskStatus.completed
        ]
        if not completed:
            return
        for tid in completed:
            del self._tasks[tid]
        logger.info(
            "Compacted %d completed tasks from queue %s",
            len(completed),
            self.name,
        )
        self._dirty = True

    def snapshot(self) -> QueueFile:
        """Return the current state as an immutable QueueFile."""
        tasks = sorted(self._tasks.values(), key=lambda t: t.sequence)
        return QueueFile(
            metadata=self._metadata,
            next_sequence=self._next_sequence,
            tasks=tasks,
        )

    def swap_write_buffer(self) -> list[FlushWaiter]:
        """Swap the write buffer for double-buffering.

        Returns the current buffer and replaces it with an empty one.
        """
        buffer = self._write_buffer
        self._write_buffer = []
        return buffer

    @property
    def is_dirty(self) -> bool:
        return self._dirty

    @property
    def has_pending_waiters(self) -> bool:
        return len(self._write_buffer) > 0

    def mark_clean(self) -> None:
        self._dirty = False

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
