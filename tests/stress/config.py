"""Scenario configuration types for stress tests."""

from dataclasses import dataclass
from enum import StrEnum


class ConsumerBehavior(StrEnum):
    normal = "normal"
    abandon = "abandon"


@dataclass(frozen=True)
class QueueSpec:
    """Configuration for a single queue within a stress scenario."""

    name: str
    producer_count: int
    consumer_count: int
    task_count: int
    consumer_behaviors: tuple[ConsumerBehavior, ...] = ()


@dataclass(frozen=True)
class ScenarioConfig:
    """Top-level configuration for a stress test scenario."""

    name: str
    queues: tuple[QueueSpec, ...]
    heartbeat_timeout_seconds: float = 2.0
    heartbeat_check_interval_seconds: float = 0.5
    heartbeat_interval_seconds: float = 0.3
    compaction_interval_seconds: float = 100.0
    processing_delay_seconds: float = 0.0
    processing_jitter_seconds: float = 0.0
    claim_timeout_seconds: float = 10.0
    abandon_backoff_seconds: float = 0.25
