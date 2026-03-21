"""Invariant validation for stress test event logs."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .config import ScenarioConfig
from .events import Event, EventKind, EventRecorder

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Outcome of validating a stress test scenario."""

    passed: bool = True
    violations: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.passed = False
        self.violations.append(message)
        logger.error("VIOLATION: %s", message)


def validate_events(
    recorder: EventRecorder,
    scenario: ScenarioConfig,
    check_fifo: bool = True,
) -> ValidationResult:
    """Check correctness invariants against the recorded event stream.

    Invariants:
    1. No lost tasks — pushed logical_ids == finished logical_ids
    2. No overlapping claims — claim intervals don't overlap per logical_id
    3. FIFO ordering — first claim order matches push order (optional)
    4. Queue isolation — all events for a logical_id are on the expected queue
    """
    result = ValidationResult()
    events = recorder.events

    pushed = _ids_by_kind(events, EventKind.push_succeeded)
    finished = _ids_by_kind(events, EventKind.finish_succeeded)

    _check_no_lost_tasks(result, pushed, finished)
    _check_no_overlapping_claims(result, events, scenario)
    if check_fifo:
        _check_fifo(result, events)
    _check_queue_isolation(result, events)

    if result.passed:
        logger.info("All invariants passed")
    else:
        logger.error(
            "%d violation(s) found", len(result.violations),
        )

    return result


# -- Per-invariant checks ----------------------------------------------------


def _check_no_lost_tasks(
    result: ValidationResult,
    pushed: set[str],
    finished: set[str],
) -> None:
    """Every pushed logical_id must be finished exactly once."""
    lost = pushed - finished
    extra = finished - pushed

    if lost:
        result.fail(
            f"Lost {len(lost)} tasks (pushed but not finished): "
            f"{sorted(lost)[:10]}"
        )
    if extra:
        result.fail(
            f"Found {len(extra)} unexpected finishes (finished but not pushed): "
            f"{sorted(extra)[:10]}"
        )


def _check_no_overlapping_claims(
    result: ValidationResult,
    events: tuple[Event, ...],
    scenario: ScenarioConfig,
) -> None:
    """For each logical_id, claim intervals must not overlap.

    A claim interval runs from ``claim_succeeded`` to the earlier of
    ``finish_succeeded`` or ``consumer_abandoned_claim``.

    We intentionally stop abandoned claims at the client-side abandon
    event instead of adding ``heartbeat_timeout_seconds``. The server
    starts lease timing before the claim response reaches the client,
    so client-observed events cannot reconstruct the exact server-side
    lease end for abandoned work without false overlap reports.
    """
    timeout = scenario.heartbeat_timeout_seconds

    # Group events by logical_id
    by_lid: dict[str, list[Event]] = {}
    for e in events:
        by_lid.setdefault(e.logical_id, []).append(e)

    for lid, lid_events in by_lid.items():
        intervals: list[tuple[float, float]] = []

        for e in lid_events:
            if e.kind == EventKind.claim_succeeded:
                claim_start = e.timestamp
                claim_end = _find_claim_end(lid_events, e, timeout)
                intervals.append((claim_start, claim_end))

        # Check pairwise overlap
        intervals.sort()
        for i in range(len(intervals) - 1):
            _, end_a = intervals[i]
            start_b, _ = intervals[i + 1]
            if start_b < end_a:
                result.fail(
                    f"Overlapping claims for {lid}: "
                    f"interval ending at {end_a:.4f} overlaps "
                    f"interval starting at {start_b:.4f}"
                )


def _find_claim_end(
    lid_events: list[Event],
    claim_event: Event,
    timeout: float,
) -> float:
    """Find when a claim interval ends.

    Looks for the next finish or abandon event after the claim timestamp
    from the same worker.
    """
    for e in lid_events:
        if e.timestamp <= claim_event.timestamp:
            continue
        if e.worker_id != claim_event.worker_id:
            continue
        if e.kind == EventKind.finish_succeeded:
            return e.timestamp
        if e.kind == EventKind.consumer_abandoned_claim:
            return e.timestamp
    # No explicit end found — treat as abandoned with timeout
    return claim_event.timestamp + timeout


def _check_fifo(
    result: ValidationResult,
    events: tuple[Event, ...],
) -> None:
    """Per-producer FIFO: each producer's tasks are claimed in push order.

    With concurrent producers, the global push order is non-deterministic
    (depends on network timing). But within a single producer's sequential
    stream, the broker must preserve ordering.
    """
    # Group pushed logical_ids by producer, preserving push order
    push_order_by_producer: dict[str, list[str]] = {}
    for e in events:
        if e.kind == EventKind.push_succeeded:
            push_order_by_producer.setdefault(e.worker_id, []).append(
                e.logical_id
            )

    # Build first-claim position per queue
    first_claim_position: dict[str, dict[str, int]] = {}
    seen: set[str] = set()
    for e in events:
        if e.kind == EventKind.claim_succeeded and e.logical_id not in seen:
            seen.add(e.logical_id)
            q_positions = first_claim_position.setdefault(e.queue_name, {})
            q_positions[e.logical_id] = len(q_positions)

    # For each producer, verify its tasks' claim positions are monotonic
    for producer_id, pushed_ids in push_order_by_producer.items():
        if not pushed_ids:
            continue
        # Determine which queue this producer pushed to (all same queue)
        queue = next(
            e.queue_name
            for e in events
            if e.kind == EventKind.push_succeeded
            and e.worker_id == producer_id
        )
        positions = first_claim_position.get(queue, {})
        claim_positions = [
            positions[lid] for lid in pushed_ids if lid in positions
        ]
        for i in range(len(claim_positions) - 1):
            if claim_positions[i] > claim_positions[i + 1]:
                result.fail(
                    f"FIFO violation on queue {queue!r} for {producer_id}: "
                    f"{pushed_ids[i]} (claim pos {claim_positions[i]}) "
                    f"was claimed after "
                    f"{pushed_ids[i + 1]} (claim pos {claim_positions[i + 1]})"
                )
                break


def _check_queue_isolation(
    result: ValidationResult,
    events: tuple[Event, ...],
) -> None:
    """Every event for a logical_id must be on the same queue."""
    lid_queue: dict[str, str] = {}
    for e in events:
        if e.logical_id in lid_queue:
            if e.queue_name != lid_queue[e.logical_id]:
                result.fail(
                    f"Queue isolation violation: {e.logical_id} seen on "
                    f"{lid_queue[e.logical_id]!r} and {e.queue_name!r}"
                )
        else:
            lid_queue[e.logical_id] = e.queue_name


# -- Helpers -----------------------------------------------------------------


def _ids_by_kind(
    events: tuple[Event, ...], kind: EventKind,
) -> set[str]:
    return {e.logical_id for e in events if e.kind == kind}
