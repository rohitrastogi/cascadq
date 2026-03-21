"""Helpers for reading broker Prometheus metrics in stress tests."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from prometheus_client.parser import text_string_to_metric_families

MetricKey = tuple[str, tuple[tuple[str, str], ...]]


@dataclass(frozen=True)
class MetricsSnapshot:
    """Parsed Prometheus text exposition for one stress-test broker."""

    values: dict[MetricKey, float]

    def get(self, name: str, **labels: str) -> float:
        """Return the metric sample value for one exact label set."""
        key = (name, tuple(sorted(labels.items())))
        return self.values.get(key, 0.0)


async def fetch_metrics(base_url: str) -> MetricsSnapshot:
    """Fetch and parse the broker's `/metrics` endpoint."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{base_url}/metrics")
        response.raise_for_status()

    values: dict[MetricKey, float] = {}
    for family in text_string_to_metric_families(response.text):
        for sample in family.samples:
            key = (sample.name, tuple(sorted(sample.labels.items())))
            values[key] = float(sample.value)
    return MetricsSnapshot(values)
