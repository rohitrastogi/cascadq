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
    flush_recovery_interval_seconds: float = 5.0
    idempotency_ttl_seconds: float = 300.0
    storage_prefix: str = ""


class ClientConfig(BaseModel, frozen=True):
    """Client-side configuration."""

    base_url: str = "http://localhost:8000"
    heartbeat_interval_seconds: float = 10.0
    max_retries: int = 3
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 30.0


class S3Config(BaseModel, frozen=True):
    """S3-compatible storage configuration.

    Set ``endpoint_url`` for non-AWS services (R2, MinIO, etc.).
    Leave it as ``None`` for standard AWS S3.

    ``hedge_after_seconds`` enables speculative write retries: if a
    conditional write hasn't completed after this many seconds, a
    second write is fired in parallel.  Set to ``0`` to disable.
    """

    bucket: str
    endpoint_url: str | None = None
    access_key_id: str
    secret_access_key: str
    region: str = "auto"
    hedge_after_seconds: float = 2.0
