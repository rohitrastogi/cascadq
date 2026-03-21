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

    Mutation methods modify the instance in-place. TaskQueue owns
    all Task instances and mutates them directly.
    """

    task_id: str
    sequence: int
    created_at: float
    status: TaskStatus
    payload: dict
    last_heartbeat: float | None = None
    claim_key: str = ""

    def claim(self, now: float, claim_key: str) -> None:
        self.status = TaskStatus.claimed
        self.last_heartbeat = now
        self.claim_key = claim_key

    def heartbeat(self, now: float) -> None:
        self.last_heartbeat = now

    def finish(self) -> None:
        self.status = TaskStatus.completed


@dataclass(frozen=True)
class QueueMeta:
    created_at: float
    payload_schema: dict = field(default_factory=dict)


@dataclass(frozen=True)
class PushRecord:
    """Durable record of a push key and its creation time."""

    task_id: str
    created_at: float


@dataclass(frozen=True)
class Snapshot:
    metadata: QueueMeta
    next_sequence: int = 0
    compaction_watermark: int = -1
    tasks: list[Task] = field(default_factory=list)
    push_keys: dict[str, PushRecord] = field(default_factory=dict)


def serialize_snapshot(qf: Snapshot) -> bytes:
    """Serialize a Snapshot to JSON bytes."""
    data = {
        "metadata": {
            "created_at": qf.metadata.created_at,
            "payload_schema": qf.metadata.payload_schema,
        },
        "next_sequence": qf.next_sequence,
        "compaction_watermark": qf.compaction_watermark,
        "push_keys": {
            k: {"task_id": r.task_id, "created_at": r.created_at}
            for k, r in qf.push_keys.items()
        },
        "tasks": [
            {
                "task_id": t.task_id,
                "sequence": t.sequence,
                "created_at": t.created_at,
                "status": t.status.value,
                "payload": t.payload,
                "last_heartbeat": t.last_heartbeat,
                "claim_key": t.claim_key,
            }
            for t in qf.tasks
        ],
    }
    return orjson.dumps(data)


def deserialize_snapshot(raw: bytes) -> Snapshot:
    """Deserialize JSON bytes into a Snapshot."""
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
            claim_key=t.get("claim_key", ""),
        )
        for t in data["tasks"]
    ]
    return Snapshot(
        metadata=QueueMeta(
            created_at=meta["created_at"],
            payload_schema=meta.get("payload_schema", {}),
        ),
        next_sequence=data["next_sequence"],
        compaction_watermark=data.get("compaction_watermark", -1),
        tasks=tasks,
        push_keys={
            k: PushRecord(task_id=v["task_id"], created_at=v["created_at"])
            for k, v in data.get("push_keys", {}).items()
        },
    )
