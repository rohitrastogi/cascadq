"""Configuration models for CAScadq broker and client."""

from pydantic import BaseModel


class BrokerConfig(BaseModel, frozen=True):
    """Broker-side configuration."""

    host: str = "0.0.0.0"
    port: int = 8000
    heartbeat_timeout_seconds: float = 30.0
    heartbeat_check_interval_seconds: float = 5.0
    compaction_interval_seconds: float = 60.0
    max_consecutive_flush_failures: int = 3
    flush_retry_delay_seconds: float = 1.0
    storage_prefix: str = ""


class ClientConfig(BaseModel, frozen=True):
    """Client-side configuration."""

    base_url: str = "http://localhost:8000"
    heartbeat_interval_seconds: float = 10.0
    max_retries: int = 3
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 30.0
