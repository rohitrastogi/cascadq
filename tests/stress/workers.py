"""Process orchestration: spawn producer/consumer subprocesses, collect events."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from pathlib import Path

from .config import ConsumerBehavior, QueueSpec, ScenarioConfig
from .events import EventRecorder

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = str(Path(__file__).parent / "_worker.py")


def run_workers(
    base_url: str,
    scenario: ScenarioConfig,
    queue_specs: tuple[QueueSpec, ...],
    tmpdir: Path,
) -> EventRecorder:
    """Spawn producer and consumer subprocesses and collect their events.

    Blocks until all workers finish. Returns an EventRecorder with the
    merged event streams from all worker JSONL files.
    """
    processes: list[tuple[subprocess.Popen[bytes], str]] = []
    event_files: list[Path] = []

    claim_timeout = scenario.claim_timeout_seconds

    for qs in queue_specs:
        logical_ids = _generate_logical_ids(scenario.name, qs)
        slices = _partition(logical_ids, qs.producer_count)

        # Spawn producers
        for i, id_slice in enumerate(slices):
            wid = f"producer-{qs.name}-{i}"
            event_path = tmpdir / f"{wid}.jsonl"
            event_files.append(event_path)
            config = {
                "role": "producer",
                "base_url": base_url,
                "queue_name": qs.name,
                "logical_ids": id_slice,
                "event_file": str(event_path),
                "worker_id": wid,
                "heartbeat_interval": scenario.heartbeat_interval_seconds,
            }
            processes.append((_spawn_worker(config, tmpdir), wid))

        # Spawn consumers
        behaviors = _resolve_behaviors(qs)
        for i in range(qs.consumer_count):
            wid = f"consumer-{qs.name}-{i}"
            event_path = tmpdir / f"{wid}.jsonl"
            event_files.append(event_path)
            config = {
                "role": "consumer",
                "base_url": base_url,
                "queue_name": qs.name,
                "event_file": str(event_path),
                "worker_id": wid,
                "behavior": behaviors[i],
                "processing_delay": scenario.processing_delay_seconds,
                "processing_jitter": scenario.processing_jitter_seconds,
                "heartbeat_interval": scenario.heartbeat_interval_seconds,
                "claim_timeout_seconds": claim_timeout,
                "abandon_backoff_seconds": scenario.abandon_backoff_seconds,
            }
            processes.append((_spawn_worker(config, tmpdir), wid))

    # Wait for all workers
    failures: list[str] = []
    for proc, wid in processes:
        proc.wait()
        if proc.returncode != 0:
            stderr_path = tmpdir / f"{wid}.stderr.log"
            stderr_tail = ""
            if stderr_path.exists():
                stderr_tail = stderr_path.read_text()[-500:]
            failures.append(
                f"{wid} exited with code {proc.returncode}: {stderr_tail}"
            )

    if failures:
        raise RuntimeError(
            f"{len(failures)} worker(s) failed:\n" + "\n---\n".join(failures)
        )

    # Collect events
    existing_files = [p for p in event_files if p.exists()]
    logger.info(
        "Collecting events from %d files", len(existing_files),
    )
    return EventRecorder.from_event_files(existing_files)


# -- Helpers -----------------------------------------------------------------


def _spawn_worker(
    config: dict, tmpdir: Path,
) -> subprocess.Popen[bytes]:
    """Write config JSON and spawn a worker subprocess."""
    worker_id = config["worker_id"]
    config_path = tmpdir / f"{worker_id}.config.json"
    config_path.write_text(json.dumps(config))
    logger.info("Spawning %s", worker_id)
    stderr_path = tmpdir / f"{worker_id}.stderr.log"
    stderr_file = open(stderr_path, "w")  # noqa: SIM115
    return subprocess.Popen(
        [sys.executable, _WORKER_SCRIPT, str(config_path)],
        stdout=subprocess.DEVNULL,
        stderr=stderr_file,
    )


def _generate_logical_ids(
    scenario_name: str, qs: QueueSpec,
) -> list[str]:
    """Generate stable logical IDs for a queue's tasks."""
    return [
        f"{scenario_name}-{qs.name}-{i:06d}"
        for i in range(qs.task_count)
    ]


def _partition(items: list[str], n: int) -> list[list[str]]:
    """Split *items* into *n* roughly equal slices."""
    k, remainder = divmod(len(items), n)
    slices: list[list[str]] = []
    start = 0
    for i in range(n):
        end = start + k + (1 if i < remainder else 0)
        slices.append(items[start:end])
        start = end
    return slices


def _resolve_behaviors(qs: QueueSpec) -> list[str]:
    """Map consumer index to behavior string.

    If ``consumer_behaviors`` is shorter than ``consumer_count``,
    remaining consumers default to ``"normal"``.
    """
    behaviors: list[str] = []
    for i in range(qs.consumer_count):
        if i < len(qs.consumer_behaviors):
            behaviors.append(qs.consumer_behaviors[i].value)
        else:
            behaviors.append(ConsumerBehavior.normal.value)
    return behaviors

