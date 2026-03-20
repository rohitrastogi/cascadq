"""Server subprocess lifecycle for stress tests."""

from __future__ import annotations

import asyncio
import logging
import socket
import subprocess
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx

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
    process (``python -m cascadq --storage s3``).
    """
    port = _find_free_port()
    base_url = f"http://127.0.0.1:{port}"

    server_proc = subprocess.Popen(
        [
            sys.executable, "-m", "cascadq",
            "--storage", "s3",
            "--port", str(port),
            "--storage-prefix", prefix,
            "--log-level", "debug",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
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
        stderr_bytes = server_proc.stderr.read() if server_proc.stderr else b""
        if stderr_bytes:
            lines = stderr_bytes.decode(errors="replace").splitlines()
            # Log heartbeat timeouts and S3 timings
            for line in lines:
                if "timeout" in line.lower() or "S3 write" in line:
                    logger.info("server: %s", line)
        logger.info("Server on port %d stopped", port)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


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
