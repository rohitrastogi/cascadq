"""Core domain models for CAScadq.

All models are frozen dataclasses — mutations create new instances.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from enum import StrEnum


class TaskStatus(StrEnum):
    pending = "pending"
    claimed = "claimed"
    completed = "completed"


@dataclass(frozen=True)
class Task:
    task_id: str
    sequence: int
    created_at: float
    status: TaskStatus
    payload: dict
    last_heartbeat: float | None = None
    claimed_by: str | None = None

    def claim(self, consumer_id: str, now: float) -> Task:
        return replace(
            self,
            status=TaskStatus.claimed,
            claimed_by=consumer_id,
            last_heartbeat=now,
        )

    def heartbeat(self, now: float) -> Task:
        return replace(self, last_heartbeat=now)

    def finish(self) -> Task:
        return replace(self, status=TaskStatus.completed)


@dataclass(frozen=True)
class QueueMetadata:
    created_at: float
    payload_schema: dict = field(default_factory=dict)


@dataclass(frozen=True)
class QueueFile:
    metadata: QueueMetadata
    next_sequence: int = 0
    tasks: list[Task] = field(default_factory=list)


@dataclass(frozen=True)
class BrokerInfo:
    """Written to broker.json on startup. Purely for debuggability."""

    broker_id: str
    host: str
    started_at: float


def serialize_queue_file(qf: QueueFile) -> bytes:
    """Serialize a QueueFile to JSON bytes."""
    data = {
        "metadata": {
            "created_at": qf.metadata.created_at,
            "payload_schema": qf.metadata.payload_schema,
        },
        "next_sequence": qf.next_sequence,
        "tasks": [
            {
                "task_id": t.task_id,
                "sequence": t.sequence,
                "created_at": t.created_at,
                "status": t.status.value,
                "payload": t.payload,
                "last_heartbeat": t.last_heartbeat,
                "claimed_by": t.claimed_by,
            }
            for t in qf.tasks
        ],
    }
    return json.dumps(data, separators=(",", ":")).encode()


def deserialize_queue_file(raw: bytes) -> QueueFile:
    """Deserialize JSON bytes into a QueueFile."""
    data = json.loads(raw)
    meta = data["metadata"]
    tasks = [
        Task(
            task_id=t["task_id"],
            sequence=t["sequence"],
            created_at=t["created_at"],
            status=TaskStatus(t["status"]),
            payload=t["payload"],
            last_heartbeat=t.get("last_heartbeat"),
            claimed_by=t.get("claimed_by"),
        )
        for t in data["tasks"]
    ]
    return QueueFile(
        metadata=QueueMetadata(
            created_at=meta["created_at"],
            payload_schema=meta.get("payload_schema", {}),
        ),
        next_sequence=data["next_sequence"],
        tasks=tasks,
    )


def serialize_broker_info(info: BrokerInfo) -> bytes:
    data = {
        "broker_id": info.broker_id,
        "host": info.host,
        "started_at": info.started_at,
    }
    return json.dumps(data, separators=(",", ":")).encode()
