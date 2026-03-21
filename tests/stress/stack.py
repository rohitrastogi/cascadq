"""Server subprocess lifecycle for stress tests."""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import subprocess
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import mkstemp

import httpx
import yaml

from .config import ScenarioConfig

logger = logging.getLogger(__name__)


@dataclass
class StressStack:
    """Running server subprocess and its connection info."""

    base_url: str
    prefix: str
    port: int
    server_process: subprocess.Popen[bytes]


@asynccontextmanager
async def stress_stack(
    scenario: ScenarioConfig,
    prefix: str,
) -> AsyncGenerator[StressStack]:
    """Start a server subprocess backed by S3 and yield the stack.

    The caller supplies a unique *prefix* for storage isolation.
    R2/S3 credentials are read from the environment by the server
    process.
    """
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"

    server_log_fd, server_log_path = mkstemp(suffix=".server.log")
    server_log = Path(server_log_path)
    server_log_fh = os.fdopen(server_log_fd, "w")
    config_path = _write_server_config(scenario, prefix, port)
    logger.info("Server log: %s", server_log)
    server_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "cascadq",
            "--config",
            str(config_path),
            "--port", str(port),
            "--log-level", "debug",
        ],
        stdout=subprocess.DEVNULL,
        stderr=server_log_fh,
    )
    try:
        await _wait_for_server(base_url)
        logger.info("Server ready on port %d (prefix=%s)", port, prefix)
        yield StressStack(
            base_url=base_url,
            prefix=prefix,
            port=port,
            server_process=server_proc,
        )
    finally:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait()
        server_log_fh.close()
        config_path.unlink(missing_ok=True)
        logger.info("Server on port %d stopped", port)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_server_config(
    scenario: ScenarioConfig,
    prefix: str,
    port: int,
) -> Path:
    """Write a temporary YAML config file for the stress-test broker."""
    config = {
        "server": {
            "host": "127.0.0.1",
            "port": port,
            "log_level": "debug",
        },
        "storage": {
            "backend": "s3",
            "prefix": prefix,
            "bucket": os.environ["CASCADQ_S3_BUCKET"],
            "endpoint_url": os.environ.get("CASCADQ_S3_ENDPOINT_URL"),
            "region": os.environ.get("CASCADQ_S3_REGION", "auto"),
        },
        "broker": {
            "heartbeat_timeout_seconds": scenario.heartbeat_timeout_seconds,
            "heartbeat_check_interval_seconds": (
                scenario.heartbeat_check_interval_seconds
            ),
            "compaction_interval_seconds": scenario.compaction_interval_seconds,
            "compress_snapshots": scenario.compress_snapshots,
        },
    }

    config_fd, config_path = mkstemp(suffix=".server.yaml")
    try:
        with os.fdopen(config_fd, "w") as config_fh:
            yaml.safe_dump(config, config_fh, sort_keys=False)
    except Exception:
        Path(config_path).unlink(missing_ok=True)
        raise
    return Path(config_path)


async def _wait_for_server(
    base_url: str, timeout: float = 15.0
) -> None:
    """Poll the server until it responds to any HTTP request."""
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient() as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                await client.get(f"{base_url}/")
                return
            except httpx.ConnectError:
                await asyncio.sleep(0.1)
    raise TimeoutError(
        f"Server at {base_url} did not start within {timeout}s"
    )
