"""Event recording for stress test observability and validation."""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class EventKind(StrEnum):
    push_started = "push_started"
    push_succeeded = "push_succeeded"
    claim_succeeded = "claim_succeeded"
    finish_succeeded = "finish_succeeded"
    consumer_abandoned_claim = "consumer_abandoned_claim"


@dataclass(frozen=True)
class Event:
    """A single recorded event from a stress test run."""

    kind: EventKind
    timestamp: float
    queue_name: str
    logical_id: str
    task_id: str = ""
    worker_id: str = ""


class EventRecorder:
    """Collects events from worker JSONL files after a scenario completes.

    Workers write events as JSON lines during execution. After all workers
    exit, the test process calls ``from_event_files`` to load and merge
    the event streams, sorted by wall-clock timestamp.
    """

    def __init__(self) -> None:
        self._events: list[Event] = []
        self._counts: dict[EventKind, int] = {k: 0 for k in EventKind}

    @classmethod
    def from_event_files(cls, paths: Iterable[Path]) -> EventRecorder:
        """Load events from worker JSONL files and sort by timestamp."""
        recorder = cls()
        for path in paths:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    data = json.loads(line)
                    event = Event(
                        kind=EventKind(data["kind"]),
                        timestamp=data["timestamp"],
                        queue_name=data["queue_name"],
                        logical_id=data["logical_id"],
                        task_id=data.get("task_id", ""),
                        worker_id=data.get("worker_id", ""),
                    )
                    recorder._events.append(event)
                    recorder._counts[event.kind] += 1
        recorder._events.sort(key=lambda e: e.timestamp)
        return recorder

    def record(
        self,
        kind: EventKind,
        queue_name: str,
        logical_id: str,
        task_id: str = "",
        worker_id: str = "",
    ) -> None:
        """Record an event directly (used by in-process monitoring)."""
        self._events.append(
            Event(
                kind=kind,
                timestamp=time.time(),
                queue_name=queue_name,
                logical_id=logical_id,
                task_id=task_id,
                worker_id=worker_id,
            )
        )
        self._counts[kind] += 1

    @property
    def events(self) -> tuple[Event, ...]:
        """Return an immutable snapshot of the event list."""
        return tuple(self._events)

    def count(self, kind: EventKind) -> int:
        return self._counts[kind]
