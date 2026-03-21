"""Client-side configuration for CAScadq."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ClientConfig:
    """Client-side configuration."""

    base_url: str = "http://localhost:8000"
    heartbeat_interval_seconds: float = 10.0
    max_retries: int = 3
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 30.0
