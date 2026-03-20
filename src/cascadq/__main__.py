"""CLI entrypoint for the CAScadq broker.

Usage:
    python -m cascadq                     # in-memory store (development)
    python -m cascadq --storage s3        # S3-compatible store (requires env vars)

S3 environment variables:
    CASCADQ_S3_BUCKET           (required)
    CASCADQ_S3_ACCESS_KEY_ID    (required)
    CASCADQ_S3_SECRET_ACCESS_KEY (required)
    CASCADQ_S3_ENDPOINT_URL     (optional, for R2/MinIO)
    CASCADQ_S3_REGION           (optional, default "auto")
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from contextlib import AbstractAsyncContextManager
from typing import cast

import uvicorn

from cascadq.config import BrokerConfig
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


def _build_s3_store() -> ObjectStore:
    """Build an S3ObjectStore from environment variables.

    Validates that all required env vars are set before constructing
    the store. Uses S3Config for typed field definitions.
    """
    from cascadq.config import S3Config
    from cascadq.storage.s3 import S3ObjectStore

    required = {
        "bucket": "CASCADQ_S3_BUCKET",
        "access_key_id": "CASCADQ_S3_ACCESS_KEY_ID",
        "secret_access_key": "CASCADQ_S3_SECRET_ACCESS_KEY",
    }
    missing = [v for v in required.values() if not os.environ.get(v)]
    if missing:
        logger.error(
            "Missing required S3 env vars: %s", ", ".join(missing)
        )
        sys.exit(1)

    config = S3Config(
        bucket=os.environ[required["bucket"]],
        access_key_id=os.environ[required["access_key_id"]],
        secret_access_key=os.environ[required["secret_access_key"]],
        endpoint_url=os.environ.get("CASCADQ_S3_ENDPOINT_URL"),
        region=os.environ.get("CASCADQ_S3_REGION", "auto"),
    )
    return S3ObjectStore(
        bucket=config.bucket,
        access_key_id=config.access_key_id,
        secret_access_key=config.secret_access_key,
        endpoint_url=config.endpoint_url,
        region=config.region,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cascadq",
        description="CAScadq broker — object-storage-backed task queue",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="bind host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="bind port (default: 8000)",
    )
    parser.add_argument(
        "--storage",
        choices=["memory", "s3"],
        default="memory",
        help="storage backend (default: memory)",
    )
    parser.add_argument(
        "--storage-prefix",
        default="",
        help="key prefix in object storage (default: none)",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=list(_LOG_LEVELS),
        help="log level (default: info)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=_LOG_LEVELS[args.log_level])

    store: ObjectStore
    store_lifecycle: AbstractAsyncContextManager | None = None
    if args.storage == "s3":
        store = _build_s3_store()
        # S3ObjectStore implements __aenter__/__aexit__ for session
        # management; create_app's lifespan will call start()/close().
        store_lifecycle = cast(AbstractAsyncContextManager, store)
    else:
        logger.info(
            "Using in-memory storage (not persisted across restarts)"
        )
        store = InMemoryObjectStore()

    config = BrokerConfig(
        host=args.host,
        port=args.port,
        storage_prefix=args.storage_prefix,
    )
    app = create_app(
        store=store, config=config, store_lifecycle=store_lifecycle
    )
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
