"""Stress test scenarios validating correctness under concurrent load."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
from cascadq_client import CascadqClient, ClientConfig

from cascadq.models import TaskStatus, deserialize_snapshot

from .config import ConsumerBehavior, QueueSpec, ScenarioConfig
from .events import EventKind, EventRecorder
from .metrics import compute_metrics, log_metrics
from .stack import stress_stack
from .validator import validate_events
from .workers import run_workers

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.stress


# -- Helpers -----------------------------------------------------------------


async def _create_queues(base_url: str, queue_names: list[str]) -> None:
    """Create queues on the server before spawning workers."""
    client = CascadqClient(config=ClientConfig(base_url=base_url))
    try:
        for name in queue_names:
            await client.create_queue(name)
    finally:
        await client.close()


def _make_store() -> object:
    """Build an S3ObjectStore from environment variables."""
    from cascadq.storage.s3 import S3ObjectStore

    return S3ObjectStore(
        bucket=os.environ["CASCADQ_S3_BUCKET"],
        access_key_id=os.environ["CASCADQ_S3_ACCESS_KEY_ID"],
        secret_access_key=os.environ["CASCADQ_S3_SECRET_ACCESS_KEY"],
        endpoint_url=os.environ.get("CASCADQ_S3_ENDPOINT_URL"),
        region=os.environ.get("CASCADQ_S3_REGION", "auto"),
    )


async def _cleanup_prefix(prefix: str) -> None:
    """Delete all S3 objects under the prefix after a test."""
    store = _make_store()
    async with store:
        keys = await store.list_prefix(prefix)
        for key in keys:
            await store.delete(key)
    logger.info("Cleaned up %d objects under %s", len(keys), prefix)


async def _wait_for_compaction(
    store: object,
    key: str,
    *,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> list[object]:
    """Poll durable state until compaction removes all completed tasks."""
    deadline = time.monotonic() + timeout_seconds
    completed: list[object] = []
    while True:
        data, _ = await store.read(key)
        queue_file = deserialize_snapshot(data)
        completed = [
            task for task in queue_file.tasks
            if task.status == TaskStatus.completed
        ]
        if not completed:
            return completed
        if time.monotonic() >= deadline:
            return completed
        await asyncio.sleep(poll_interval_seconds)


async def _run_standard_scenario(
    scenario: ScenarioConfig,
    tmp_path: Path,
    check_fifo: bool = True,
) -> None:
    """Run a scenario end-to-end: server, workers, validate, metrics."""
    prefix = f"stress-{scenario.name}-{uuid4().hex[:8]}/"

    async with stress_stack(scenario, prefix) as stack:
        queue_names = [qs.name for qs in scenario.queues]
        await _create_queues(stack.base_url, queue_names)

        t0 = time.time()
        recorder = await asyncio.to_thread(
            run_workers, stack.base_url, scenario, scenario.queues, tmp_path,
        )
        wall_clock = time.time() - t0

    try:
        result = validate_events(recorder, scenario, check_fifo=check_fifo)
        metrics = compute_metrics(recorder, wall_clock)
        log_metrics(scenario.name, metrics)
        assert result.passed, "\n".join(result.violations)
    finally:
        await _cleanup_prefix(prefix)


# -- Scenarios ---------------------------------------------------------------


class TestThroughputModerate:
    """4 producers, 4 consumers, 500 tasks, 1 queue."""

    async def test_throughput_moderate(self, r2_env: dict, tmp_path: Path) -> None:
        scenario = ScenarioConfig(
            name="throughput-moderate",
            queues=(
                QueueSpec(
                    name="work",
                    producer_count=4,
                    consumer_count=4,
                    task_count=500,
                ),
            ),
        )
        await _run_standard_scenario(scenario, tmp_path)


class TestHighContention:
    """16 producers, 16 consumers, 2000 tasks, 1 queue, jitter."""

    async def test_high_contention(self, r2_env: dict, tmp_path: Path) -> None:
        scenario = ScenarioConfig(
            name="high-contention",
            queues=(
                QueueSpec(
                    name="work",
                    producer_count=16,
                    consumer_count=16,
                    task_count=2000,
                ),
            ),
            processing_delay_seconds=0.005,
            processing_jitter_seconds=0.01,
        )
        await _run_standard_scenario(scenario, tmp_path)


class TestHeartbeatTimeout:
    """4 producers, 6 consumers (2 abandon), 200 tasks, short timeouts."""

    async def test_heartbeat_timeout(self, r2_env: dict, tmp_path: Path) -> None:
        scenario = ScenarioConfig(
            name="heartbeat-timeout",
            queues=(
                QueueSpec(
                    name="work",
                    producer_count=4,
                    consumer_count=6,
                    task_count=200,
                    consumer_behaviors=(
                        ConsumerBehavior.normal,
                        ConsumerBehavior.normal,
                        ConsumerBehavior.normal,
                        ConsumerBehavior.normal,
                        ConsumerBehavior.abandon,
                        ConsumerBehavior.abandon,
                    ),
                ),
            ),
            heartbeat_timeout_seconds=1.0,
            heartbeat_check_interval_seconds=0.3,
            heartbeat_interval_seconds=0.2,
        )
        await _run_standard_scenario(scenario, tmp_path, check_fifo=False)


class TestMultiQueueIsolation:
    """4 queues, 2 producers + 2 consumers each, 250 tasks each."""

    async def test_multi_queue_isolation(
        self, r2_env: dict, tmp_path: Path,
    ) -> None:
        queues = tuple(
            QueueSpec(
                name=f"queue-{i}",
                producer_count=2,
                consumer_count=2,
                task_count=250,
            )
            for i in range(4)
        )
        scenario = ScenarioConfig(
            name="multi-queue-isolation",
            queues=queues,
        )
        await _run_standard_scenario(scenario, tmp_path)


class TestCompaction:
    """4 producers, 4 consumers, 1000 tasks, aggressive compaction."""

    async def test_compaction(self, r2_env: dict, tmp_path: Path) -> None:
        scenario = ScenarioConfig(
            name="compaction",
            queues=(
                QueueSpec(
                    name="work",
                    producer_count=4,
                    consumer_count=4,
                    task_count=1000,
                ),
            ),
            compaction_interval_seconds=1.0,
        )
        prefix = f"stress-compaction-{uuid4().hex[:8]}/"

        async with stress_stack(scenario, prefix) as stack:
            await _create_queues(stack.base_url, ["work"])

            t0 = time.time()
            recorder = await asyncio.to_thread(
                run_workers,
                stack.base_url,
                scenario,
                scenario.queues,
                tmp_path,
            )
            wall_clock = time.time() - t0

        store = _make_store()
        try:
            async with store:
                completed = await _wait_for_compaction(
                    store,
                    f"{prefix}queues/work.json",
                    timeout_seconds=15.0,
                    poll_interval_seconds=0.5,
                )
                assert len(completed) == 0, (
                    f"Compaction should have removed all completed tasks, "
                    f"found {len(completed)}"
                )
                logger.info(
                    "Compaction check passed: no completed tasks remain "
                    "in durable state"
                )

            result = validate_events(recorder, scenario)
            metrics = compute_metrics(recorder, wall_clock)
            log_metrics(scenario.name, metrics)
            assert result.passed, "\n".join(result.violations)
        finally:
            await _cleanup_prefix(prefix)


class TestBrokerRestart:
    """2P / 2C / 300 tasks — stop broker, restart with fresh consumers."""

    async def test_broker_restart(self, r2_env: dict, tmp_path: Path) -> None:
        scenario = ScenarioConfig(
            name="broker-restart",
            queues=(
                QueueSpec(
                    name="work",
                    producer_count=2,
                    consumer_count=2,
                    task_count=300,
                ),
            ),
        )
        prefix = f"stress-restart-{uuid4().hex[:8]}/"
        phase1_dir = tmp_path / "phase1"
        phase1_dir.mkdir()
        phase2_dir = tmp_path / "phase2"
        phase2_dir.mkdir()

        # Phase 1: push all tasks, process ~half, then stop
        async with stress_stack(scenario, prefix) as stack:
            await _create_queues(stack.base_url, ["work"])

            # Run all producers + consumers; consumers will exit after
            # max_empty_claims of idle time
            recorder_p1 = await asyncio.to_thread(
                run_workers,
                stack.base_url,
                scenario,
                scenario.queues,
                phase1_dir,
            )

        finished_p1 = recorder_p1.count(EventKind.finish_succeeded)
        logger.info("Phase 1 finished %d tasks", finished_p1)

        # Phase 2: restart server, consumers only (no new pushes)
        consumers_only = (
            QueueSpec(
                name="work",
                producer_count=0,
                consumer_count=2,
                task_count=0,  # no new tasks to push
            ),
        )
        remaining = 300 - finished_p1
        if remaining > 0:
            async with stress_stack(scenario, prefix) as stack:
                await asyncio.to_thread(
                    run_workers,
                    stack.base_url,
                    scenario,
                    consumers_only,
                    phase2_dir,
                )

        # Merge events from both phases
        all_files = list(phase1_dir.glob("*.jsonl")) + list(
            phase2_dir.glob("*.jsonl")
        )
        merged = EventRecorder.from_event_files(all_files)

        try:
            result = validate_events(merged, scenario, check_fifo=False)
            total_finished = merged.count(EventKind.finish_succeeded)
            logger.info(
                "Restart scenario: %d total finished (%d phase1 + %d phase2)",
                total_finished,
                finished_p1,
                total_finished - finished_p1,
            )
            assert result.passed, "\n".join(result.violations)
        finally:
            await _cleanup_prefix(prefix)
