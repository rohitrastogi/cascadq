"""Prometheus metrics for the CAScadq broker."""

from __future__ import annotations

from collections import Counter as PyCounter

from prometheus_client import Counter, Gauge, Histogram

from cascadq.models import FlusherStatus, Snapshot, TaskStatus

# -- Flush (Tier 1) ----------------------------------------------------------

flush_duration_seconds = Histogram(
    "cascadq_flush_duration_seconds",
    "Time to flush a queue's state to storage",
    ["queue"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

# -- Task lifecycle counters (Tier 1) ----------------------------------------

tasks_pushed_total = Counter(
    "cascadq_tasks_pushed_total",
    "Tasks pushed",
    ["queue"],
)

tasks_claimed_total = Counter(
    "cascadq_tasks_claimed_total",
    "Tasks claimed",
    ["queue"],
)

tasks_requeued_total = Counter(
    "cascadq_tasks_requeued_total",
    "Tasks re-queued due to heartbeat timeout",
    ["queue"],
)

queue_dwell_seconds = Histogram(
    "cascadq_queue_dwell_seconds",
    "Time a task waited in the queue before being claimed (push to claim)",
    ["queue"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
)

# -- Queue depth gauges (Tier 1) ---------------------------------------------

queue_pending_tasks = Gauge(
    "cascadq_queue_pending_tasks",
    "Number of pending tasks",
    ["queue"],
)

queue_claimed_tasks = Gauge(
    "cascadq_queue_claimed_tasks",
    "Number of claimed (in-flight) tasks",
    ["queue"],
)

queue_status = Gauge(
    "cascadq_queue_status",
    "Queue flusher status (1=healthy, 2=recovering, 3=fenced)",
    ["queue"],
)

# -- Flush errors (Tier 2) ---------------------------------------------------

flush_errors_total = Counter(
    "cascadq_flush_errors_total",
    "Flush failures by error type",
    ["queue", "error"],
)

# -- Hedging (Tier 2) --------------------------------------------------------

hedge_total = Counter(
    "cascadq_hedge_total",
    "Hedged write outcomes",
    ["outcome"],
)

# -- RPC latency (Tier 2) ----------------------------------------------------

rpc_duration_seconds = Histogram(
    "cascadq_rpc_duration_seconds",
    "RPC handler latency",
    ["operation"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# -- Recovery & compaction ---------------------------------------------------

recovery_events_total = Counter(
    "cascadq_recovery_events_total",
    "Recovery attempts by outcome",
    ["queue", "outcome"],
)

compaction_tasks_removed_total = Counter(
    "cascadq_compaction_tasks_removed_total",
    "Completed tasks removed by compaction",
    ["queue"],
)

# -- Helpers -----------------------------------------------------------------

_STATUS_VALUES = {"healthy": 1, "recovering": 2, "fenced": 3}


def set_queue_status(queue: str, status: FlusherStatus) -> None:
    """Update the queue status gauge from a FlusherStatus value."""
    queue_status.labels(queue=queue).set(_STATUS_VALUES.get(status, 0))


def reset_queue_gauges(queue: str, snapshot: Snapshot) -> None:
    """Set pending/claimed gauges from a snapshot (single pass)."""
    counts: PyCounter[TaskStatus] = PyCounter(t.status for t in snapshot.tasks)
    queue_pending_tasks.labels(queue=queue).set(counts[TaskStatus.pending])
    queue_claimed_tasks.labels(queue=queue).set(counts[TaskStatus.claimed])
