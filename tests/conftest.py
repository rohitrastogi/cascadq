"""Shared fixtures for CAScadq tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from cascadq.config import BrokerConfig
from cascadq.models import QueueMeta, Snapshot

from .support import FaultInjectingStore


def make_key() -> str:
    """Generate a unique key for push/claim dedup in tests."""
    return uuid4().hex


@pytest.fixture
def memory_store() -> FaultInjectingStore:
    return FaultInjectingStore()


@pytest.fixture
def sample_metadata() -> QueueMeta:
    return QueueMeta(created_at=1000.0, payload_schema={})


@pytest.fixture
def sample_queue_file(sample_metadata: QueueMeta) -> Snapshot:
    return Snapshot(metadata=sample_metadata, next_sequence=0, tasks=[])


@pytest.fixture
def schema_metadata() -> QueueMeta:
    """Metadata with a JSON Schema requiring a 'url' string field."""
    return QueueMeta(
        created_at=1000.0,
        payload_schema={
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        },
    )


@pytest.fixture
def test_config() -> BrokerConfig:
    """Broker config with background workers effectively disabled for tests."""
    return BrokerConfig(
        heartbeat_timeout_seconds=5.0,
        heartbeat_check_interval_seconds=100.0,
        compaction_interval_seconds=100.0,
    )
