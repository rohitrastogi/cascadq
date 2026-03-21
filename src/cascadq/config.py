"""Configuration models for CAScadq broker and client."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# YAML config file models (untrusted boundary — Pydantic for validation)
# ---------------------------------------------------------------------------


class ServerListenConfig(BaseModel, frozen=True, extra="forbid"):
    """Server network and logging settings."""

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: Literal["debug", "info", "warning", "error"] = "info"


class StorageConfig(BaseModel, frozen=True, extra="forbid"):
    """Object storage settings.

    S3 credentials (``CASCADQ_S3_ACCESS_KEY_ID``,
    ``CASCADQ_S3_SECRET_ACCESS_KEY``) are always read from environment
    variables and never appear in this model.
    """

    backend: Literal["memory", "s3"] = "memory"
    prefix: str = ""
    bucket: str = ""
    endpoint_url: str | None = None
    region: str = "auto"
    hedge_after_seconds: float = 2.0


class BrokerTuningConfig(BaseModel, frozen=True, extra="forbid"):
    """Broker behavior tuning (``broker:`` section of the YAML config)."""

    heartbeat_timeout_seconds: float = 30.0
    heartbeat_check_interval_seconds: float = 5.0
    compaction_interval_seconds: float = 60.0
    compress_snapshots: bool = False
    max_consecutive_flush_failures: int = 3
    flush_retry_delay_seconds: float = 1.0
    flush_recovery_interval_seconds: float = 5.0
    idempotency_ttl_seconds: float = 300.0


class ServerConfig(BaseModel, frozen=True, extra="forbid"):
    """Top-level configuration loaded from a YAML file.

    S3 credentials are always read from environment variables and are
    intentionally excluded from the config file.
    """

    server: ServerListenConfig = ServerListenConfig()
    storage: StorageConfig = StorageConfig()
    broker: BrokerTuningConfig = BrokerTuningConfig()


def load_server_config(path: Path) -> ServerConfig:
    """Load and validate server configuration from a YAML file.

    Returns a default ``ServerConfig`` if the file is empty.

    Raises:
        FileNotFoundError: If the file does not exist.
        pydantic.ValidationError: If the file contents are invalid.
    """
    import yaml  # server-only dependency

    raw = yaml.safe_load(path.read_text())
    if raw is None:
        return ServerConfig()
    return ServerConfig.model_validate(raw)


# ---------------------------------------------------------------------------
# Internal configs (trusted state — plain dataclasses)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BrokerConfig:
    """Internal runtime config passed to the broker."""

    host: str = "0.0.0.0"
    port: int = 8000
    heartbeat_timeout_seconds: float = 30.0
    heartbeat_check_interval_seconds: float = 5.0
    compaction_interval_seconds: float = 60.0
    compress_snapshots: bool = False
    max_consecutive_flush_failures: int = 3
    flush_retry_delay_seconds: float = 1.0
    flush_recovery_interval_seconds: float = 5.0
    idempotency_ttl_seconds: float = 300.0
    storage_prefix: str = ""


@dataclass(frozen=True)
class ClientConfig:
    """Client-side configuration."""

    base_url: str = "http://localhost:8000"
    heartbeat_interval_seconds: float = 10.0
    max_retries: int = 3
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 30.0
