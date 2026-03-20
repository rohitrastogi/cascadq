"""Shared fixtures for CAScadq tests."""

from __future__ import annotations

import pytest

from cascadq.models import QueueFile, QueueMetadata
from cascadq.storage.memory import InMemoryObjectStore


@pytest.fixture
def memory_store() -> InMemoryObjectStore:
    return InMemoryObjectStore()


@pytest.fixture
def sample_metadata() -> QueueMetadata:
    return QueueMetadata(created_at=1000.0, payload_schema={})


@pytest.fixture
def sample_queue_file(sample_metadata: QueueMetadata) -> QueueFile:
    return QueueFile(metadata=sample_metadata, next_sequence=0, tasks=[])


@pytest.fixture
def schema_metadata() -> QueueMetadata:
    """Metadata with a JSON Schema requiring a 'url' string field."""
    return QueueMetadata(
        created_at=1000.0,
        payload_schema={
            "type": "object",
            "properties": {"url": {"type": "string"}},
            "required": ["url"],
        },
    )
