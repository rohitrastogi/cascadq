"""Performance metrics computation and logging for stress tests."""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass

from .events import Event, EventKind, EventRecorder

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScenarioMetrics:
    """Computed performance metrics for a stress test scenario."""

    push_latency_p50: float
    push_latency_p95: float
    push_latency_p99: float
    claim_to_finish_p50: float
    claim_to_finish_p95: float
    claim_to_finish_p99: float
    throughput: float
    requeue_count: int


def compute_metrics(
    recorder: EventRecorder,
    wall_clock_seconds: float,
) -> ScenarioMetrics:
    """Derive performance metrics from the recorded event stream."""
    events = recorder.events

    push_sorted = sorted(_push_latencies(events))
    ctf_sorted = sorted(_claim_to_finish_latencies(events))
    throughput = (
        recorder.count(EventKind.finish_succeeded) / wall_clock_seconds
        if wall_clock_seconds > 0
        else 0.0
    )

    return ScenarioMetrics(
        push_latency_p50=_percentile(push_sorted, 50),
        push_latency_p95=_percentile(push_sorted, 95),
        push_latency_p99=_percentile(push_sorted, 99),
        claim_to_finish_p50=_percentile(ctf_sorted, 50),
        claim_to_finish_p95=_percentile(ctf_sorted, 95),
        claim_to_finish_p99=_percentile(ctf_sorted, 99),
        throughput=throughput,
        requeue_count=_count_requeues(events),
    )


def log_metrics(scenario_name: str, metrics: ScenarioMetrics) -> None:
    """Log metrics as structured JSON for comparison across runs."""
    data = {
        "scenario": scenario_name,
        "push_latency_p50_ms": round(metrics.push_latency_p50 * 1000, 2),
        "push_latency_p95_ms": round(metrics.push_latency_p95 * 1000, 2),
        "push_latency_p99_ms": round(metrics.push_latency_p99 * 1000, 2),
        "claim_to_finish_p50_ms": round(
            metrics.claim_to_finish_p50 * 1000, 2
        ),
        "claim_to_finish_p95_ms": round(
            metrics.claim_to_finish_p95 * 1000, 2
        ),
        "claim_to_finish_p99_ms": round(
            metrics.claim_to_finish_p99 * 1000, 2
        ),
        "throughput_tasks_per_sec": round(metrics.throughput, 2),
        "requeue_count": metrics.requeue_count,
    }
    logger.info("Metrics: %s", json.dumps(data))


# -- Internal ----------------------------------------------------------------


def _push_latencies(events: tuple[Event, ...]) -> list[float]:
    """Compute push_started → push_succeeded latency per logical_id.

    Both events come from the same producer process, so their
    timestamps are directly comparable.
    """
    started: dict[str, float] = {}
    latencies: list[float] = []
    for e in events:
        if e.kind == EventKind.push_started:
            started[e.logical_id] = e.timestamp
        elif e.kind == EventKind.push_succeeded:
            start = started.get(e.logical_id)
            if start is not None:
                latencies.append(e.timestamp - start)
    return latencies


def _claim_to_finish_latencies(events: tuple[Event, ...]) -> list[float]:
    """Compute last claim_succeeded → finish_succeeded per logical_id.

    A task may be claimed multiple times (after abandonment).  We use
    the *last* claim before each finish to measure the successful
    processing time.  Both events come from the same consumer process.
    """
    last_claim: dict[str, float] = {}
    latencies: list[float] = []
    for e in events:
        if e.kind == EventKind.claim_succeeded:
            last_claim[e.logical_id] = e.timestamp
        elif e.kind == EventKind.finish_succeeded:
            claim_ts = last_claim.get(e.logical_id)
            if claim_ts is not None:
                latencies.append(e.timestamp - claim_ts)
    return latencies


def _count_requeues(events: tuple[Event, ...]) -> int:
    """Count logical_ids that were claimed more than once."""
    claim_counts: dict[str, int] = {}
    for e in events:
        if e.kind == EventKind.claim_succeeded:
            claim_counts[e.logical_id] = claim_counts.get(e.logical_id, 0) + 1
    return sum(1 for c in claim_counts.values() if c > 1)


def _percentile(sorted_values: list[float], pct: int) -> float:
    """Compute a percentile from a pre-sorted list."""
    if not sorted_values:
        return 0.0
    return statistics.quantiles(sorted_values, n=100)[pct - 1]
