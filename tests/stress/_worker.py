"""Subprocess entry point for stress test producers and consumers.

Run directly::

    python _worker.py <config.json>

The config JSON determines whether this is a producer or consumer.
Only imports from the installed ``cascadq`` package and stdlib.
"""

from __future__ import annotations

import asyncio
import json
import random
import sys
import time
from typing import Any, TextIO


def main() -> None:
    with open(sys.argv[1]) as f:
        config = json.load(f)

    role = config["role"]
    if role == "producer":
        asyncio.run(_run_producer(config))
    elif role == "consumer":
        asyncio.run(_run_consumer(config))
    else:
        sys.exit(f"unknown role: {role}")


def _make_client(config: dict) -> Any:
    """Build a CascadqClient from worker config.

    Returns untyped to avoid top-level imports of cascadq (this script
    must be runnable as a standalone subprocess).  Uses a 120s HTTP timeout
    because real S3 flush cycles under high concurrency can be slow.
    """
    import httpx

    from cascadq.client.client import CascadqClient
    from cascadq.config import ClientConfig

    http_client = httpx.AsyncClient(
        base_url=config["base_url"],
        timeout=httpx.Timeout(120.0),
    )
    return CascadqClient(
        config=ClientConfig(
            base_url=config["base_url"],
            heartbeat_interval_seconds=config["heartbeat_interval"],
        ),
        http_client=http_client,
    )


# -- Producer ---------------------------------------------------------------


async def _run_producer(config: dict) -> None:
    client = _make_client(config)
    queue = config["queue_name"]
    wid = config["worker_id"]

    try:
        with open(config["event_file"], "w", buffering=1) as ef:
            for lid in config["logical_ids"]:
                _write_event(ef, "push_started", queue, lid, worker_id=wid)
                await client.push(queue, {"logical_id": lid})
                _write_event(
                    ef, "push_succeeded", queue, lid, worker_id=wid,
                )
    finally:
        await client.close()


# -- Consumer ---------------------------------------------------------------


async def _run_consumer(config: dict) -> None:
    client = _make_client(config)
    queue = config["queue_name"]
    wid = config["worker_id"]
    behavior = config["behavior"]
    delay = config["processing_delay"]
    jitter = config["processing_jitter"]
    max_empty = config["max_empty_claims"]

    try:
        empty_streak = 0
        with open(config["event_file"], "w", buffering=1) as ef:
            while empty_streak < max_empty:
                claimed = await client.claim(queue)
                if claimed is None:
                    empty_streak += 1
                    await asyncio.sleep(0.05)
                    continue
                empty_streak = 0

                lid = claimed.payload["logical_id"]
                _write_event(
                    ef, "claim_succeeded", queue, lid,
                    task_id=claimed.task_id, worker_id=wid,
                )

                if behavior == "abandon":
                    _write_event(
                        ef, "consumer_abandoned_claim", queue, lid,
                        task_id=claimed.task_id, worker_id=wid,
                    )
                    continue

                async with claimed:
                    sleep_time = delay + random.uniform(0, jitter)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                _write_event(
                    ef, "finish_succeeded", queue, lid,
                    task_id=claimed.task_id, worker_id=wid,
                )
    finally:
        await client.close()


# -- Shared -----------------------------------------------------------------


def _write_event(
    f: TextIO,
    kind: str,
    queue_name: str,
    logical_id: str,
    task_id: str = "",
    worker_id: str = "",
) -> None:
    line = json.dumps(
        {
            "kind": kind,
            "timestamp": time.time(),
            "queue_name": queue_name,
            "logical_id": logical_id,
            "task_id": task_id,
            "worker_id": worker_id,
        },
        separators=(",", ":"),
    )
    f.write(line + "\n")


if __name__ == "__main__":
    main()
