"""Core domain models for CAScadq."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

import orjson


class TaskStatus(StrEnum):
    pending = "pending"
    claimed = "claimed"
    completed = "completed"


@dataclass(slots=True)
class Task:
    """Mutable in-memory task representation.

    Mutation methods modify the instance in-place and return self
    so callers can use the same assignment pattern as before.
    """

    task_id: str
    sequence: int
    created_at: float
    status: TaskStatus
    payload: dict
    last_heartbeat: float | None = None
    claim_idempotency_key: str = ""

    def claim(self, now: float, claim_idempotency_key: str) -> Task:
        self.status = TaskStatus.claimed
        self.last_heartbeat = now
        self.claim_idempotency_key = claim_idempotency_key
        return self

    def heartbeat(self, now: float) -> Task:
        self.last_heartbeat = now
        return self

    def finish(self) -> Task:
        self.status = TaskStatus.completed
        return self


@dataclass(frozen=True)
class QueueMetadata:
    created_at: float
    payload_schema: dict = field(default_factory=dict)


@dataclass(frozen=True)
class IdempotencyRecord:
    """Durable record of a push idempotency key and its creation time."""

    task_id: str
    created_at: float


@dataclass(frozen=True)
class QueueFile:
    metadata: QueueMetadata
    next_sequence: int = 0
    compacted_through_sequence: int = -1
    tasks: list[Task] = field(default_factory=list)
    idempotency_keys: dict[str, IdempotencyRecord] = field(default_factory=dict)


def serialize_queue_file(qf: QueueFile) -> bytes:
    """Serialize a QueueFile to JSON bytes."""
    data = {
        "metadata": {
            "created_at": qf.metadata.created_at,
            "payload_schema": qf.metadata.payload_schema,
        },
        "next_sequence": qf.next_sequence,
        "compacted_through_sequence": qf.compacted_through_sequence,
        "idempotency_keys": {
            k: {"task_id": r.task_id, "created_at": r.created_at}
            for k, r in qf.idempotency_keys.items()
        },
        "tasks": [
            {
                "task_id": t.task_id,
                "sequence": t.sequence,
                "created_at": t.created_at,
                "status": t.status.value,
                "payload": t.payload,
                "last_heartbeat": t.last_heartbeat,
                "claim_idempotency_key": t.claim_idempotency_key,
            }
            for t in qf.tasks
        ],
    }
    return orjson.dumps(data)


def deserialize_queue_file(raw: bytes) -> QueueFile:
    """Deserialize JSON bytes into a QueueFile."""
    data = orjson.loads(raw)
    meta = data["metadata"]
    tasks = [
        Task(
            task_id=t["task_id"],
            sequence=t["sequence"],
            created_at=t["created_at"],
            status=TaskStatus(t["status"]),
            payload=t["payload"],
            last_heartbeat=t.get("last_heartbeat"),
            claim_idempotency_key=t.get("claim_idempotency_key", ""),
        )
        for t in data["tasks"]
    ]
    return QueueFile(
        metadata=QueueMetadata(
            created_at=meta["created_at"],
            payload_schema=meta.get("payload_schema", {}),
        ),
        next_sequence=data["next_sequence"],
        compacted_through_sequence=data.get("compacted_through_sequence", -1),
        tasks=tasks,
        idempotency_keys={
            k: IdempotencyRecord(task_id=v["task_id"], created_at=v["created_at"])
            for k, v in data.get("idempotency_keys", {}).items()
        },
    )
