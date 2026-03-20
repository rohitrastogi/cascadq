"""CLI entrypoint for the CAScadq broker.

Usage:
    python -m cascadq --config config.yaml     # recommended
    python -m cascadq                           # in-memory store (development)

S3 credentials (always from environment variables):
    CASCADQ_S3_ACCESS_KEY_ID    (required for S3 backend)
    CASCADQ_S3_SECRET_ACCESS_KEY (required for S3 backend)
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import cast

try:
    import uvicorn
except ImportError:
    print(
        "Server dependencies not installed.\n"
        'Install with: pip install "cascadq[server]"',
        file=sys.stderr,
    )
    sys.exit(1)

from cascadq.config import (
    BrokerConfig,
    ServerConfig,
    load_server_config,
)
from cascadq.server.app import create_app
from cascadq.storage.memory import InMemoryObjectStore
from cascadq.storage.protocol import ObjectStore

logger = logging.getLogger(__name__)

_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cascadq",
        description="CAScadq broker — object-storage-backed task queue",
    )
    parser.add_argument(
        "--config",
        default=os.environ.get("CASCADQ_CONFIG"),
        help="path to YAML config file (or set CASCADQ_CONFIG env var)",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="bind host (overrides config file, default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="bind port (overrides config file, default: 8000)",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=list(_LOG_LEVELS),
        help="log level (overrides config file, default: info)",
    )
    return parser.parse_args()


def _load_config(args: argparse.Namespace) -> ServerConfig:
    """Load config from YAML file, then apply CLI overrides."""
    cfg = load_server_config(Path(args.config)) if args.config else ServerConfig()

    server_overrides: dict[str, object] = {}
    if args.host is not None:
        server_overrides["host"] = args.host
    if args.port is not None:
        server_overrides["port"] = args.port
    if args.log_level is not None:
        server_overrides["log_level"] = args.log_level

    if server_overrides:
        cfg = cfg.model_copy(
            update={"server": cfg.server.model_copy(update=server_overrides)}
        )
    return cfg


def _build_s3_store(cfg: ServerConfig) -> ObjectStore:
    """Build an S3ObjectStore from config file + env credentials."""
    from cascadq.storage.s3 import S3ObjectStore

    access_key_id = os.environ.get("CASCADQ_S3_ACCESS_KEY_ID", "")
    secret_access_key = os.environ.get("CASCADQ_S3_SECRET_ACCESS_KEY", "")

    missing: list[str] = []
    if not cfg.storage.bucket:
        missing.append("storage.bucket (in config file)")
    if not access_key_id:
        missing.append("CASCADQ_S3_ACCESS_KEY_ID (env var)")
    if not secret_access_key:
        missing.append("CASCADQ_S3_SECRET_ACCESS_KEY (env var)")
    if missing:
        logger.error("Missing required S3 settings: %s", ", ".join(missing))
        sys.exit(1)

    return S3ObjectStore(
        bucket=cfg.storage.bucket,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        endpoint_url=cfg.storage.endpoint_url,
        region=cfg.storage.region,
        hedge_after_seconds=cfg.storage.hedge_after_seconds,
    )


def _to_broker_config(cfg: ServerConfig) -> BrokerConfig:
    """Convert YAML-facing ServerConfig to the internal BrokerConfig."""
    return BrokerConfig(
        **cfg.broker.model_dump(),
        host=cfg.server.host,
        port=cfg.server.port,
        storage_prefix=cfg.storage.prefix,
    )


def main() -> None:
    args = _parse_args()
    cfg = _load_config(args)

    logging.basicConfig(level=_LOG_LEVELS[cfg.server.log_level])
    for name in ("botocore", "aiobotocore", "urllib3"):
        logging.getLogger(name).setLevel(logging.WARNING)

    store: ObjectStore
    store_lifecycle: AbstractAsyncContextManager | None = None
    if cfg.storage.backend == "s3":
        store = _build_s3_store(cfg)
        store_lifecycle = cast(AbstractAsyncContextManager, store)
    else:
        logger.info(
            "Using in-memory storage (not persisted across restarts)"
        )
        store = InMemoryObjectStore()

    broker_config = _to_broker_config(cfg)
    app = create_app(
        store=store, config=broker_config, store_lifecycle=store_lifecycle
    )
    uvicorn.run(app, host=cfg.server.host, port=cfg.server.port)


if __name__ == "__main__":
    main()
