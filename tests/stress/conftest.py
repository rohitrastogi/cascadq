"""Fixtures for stress tests."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(scope="session")
def r2_env() -> dict[str, str]:
    """R2/S3 environment variables. Skip the session if not configured."""
    pytest.importorskip(
        "aiobotocore", reason="stress tests require aiobotocore (install s3 extra)"
    )
    required = [
        "CASCADQ_S3_BUCKET",
        "CASCADQ_S3_ACCESS_KEY_ID",
        "CASCADQ_S3_SECRET_ACCESS_KEY",
    ]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        pytest.skip(f"R2 env vars not set: {', '.join(missing)}")

    env: dict[str, str] = {}
    for key in [*required, "CASCADQ_S3_ENDPOINT_URL", "CASCADQ_S3_REGION"]:
        val = os.environ.get(key)
        if val:
            env[key] = val
    return env
