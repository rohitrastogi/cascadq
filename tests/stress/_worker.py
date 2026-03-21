"""Subprocess entry point for stress test producers and consumers.

Run directly::

    python _worker.py <config.json>

The config JSON determines whether this is a producer or consumer.
Only imports from the installed ``cascadq`` package and stdlib.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import time
from typing import Any, TextIO


def main() -> None:
    _configure_logging()
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
    payload_bytes = config["payload_bytes"]

    try:
        with open(config["event_file"], "w", buffering=1) as ef:
            for lid in config["logical_ids"]:
                _write_event(ef, "push_started", queue, lid, worker_id=wid)
                await client.push(queue, _build_payload(lid, payload_bytes))
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
    timeout = config["claim_timeout_seconds"]
    abandon_backoff = config["abandon_backoff_seconds"]
    abandon_claim_limit = config.get("abandon_claim_limit")
    abandoned_claims = 0

    try:
        with open(config["event_file"], "w", buffering=1) as ef:
            while True:
                claimed = await client.claim(queue, timeout_seconds=timeout)
                if claimed is None:
                    break

                lid = claimed.payload["logical_id"]
                _write_event(
                    ef, "claim_succeeded", queue, lid,
                    task_id=claimed.task_id, worker_id=wid,
                )

                should_abandon = (
                    behavior == "abandon"
                    and (
                        abandon_claim_limit is None
                        or abandoned_claims < abandon_claim_limit
                    )
                )

                if should_abandon:
                    _write_event(
                        ef, "consumer_abandoned_claim", queue, lid,
                        task_id=claimed.task_id, worker_id=wid,
                    )
                    abandoned_claims += 1
                    await asyncio.sleep(abandon_backoff)
                    continue

                async with claimed:
                    sleep_time = delay + random.uniform(0, jitter)
                    if sleep_time > 0:
                        await asyncio.sleep(sleep_time)

                if claimed.finish_acknowledged:
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


def _configure_logging() -> None:
    """Emit client task-trace logs in worker stderr when enabled."""
    level = (
        logging.INFO
        if os.environ.get("CASCADQ_TRACE_TASK_LIFECYCLE") == "1"
        else logging.WARNING
    )
    logging.basicConfig(level=level)


def _build_payload(logical_id: str, payload_bytes: int) -> dict[str, Any]:
    """Build a stress-test payload with optional realistic nested JSON."""
    payload: dict[str, Any] = {"logical_id": logical_id}
    if payload_bytes > 0:
        payload["document"] = _build_document(logical_id, payload_bytes)
    return payload


def _build_document(logical_id: str, payload_bytes: int) -> dict[str, Any]:
    """Build a nested JSON document near the requested encoded size."""
    rng = random.Random(logical_id)
    vocabulary = [
        "latency", "throughput", "queue", "storage", "recovery", "worker",
        "snapshot", "payload", "heartbeat", "compaction", "broker",
        "timeout", "contention", "checkpoint", "document", "ingestion",
        "replica", "routing", "consumer", "producer", "backpressure",
        "serialization", "envelope", "retry", "durability", "flush",
    ]
    document: dict[str, Any] = {
        "title": f"Document for {logical_id}",
        "source": "stress-suite",
        "tags": rng.sample(vocabulary, k=4),
        "attributes": {
            "priority": rng.choice(["low", "normal", "high"]),
            "language": rng.choice(["en", "es", "fr", "de"]),
            "region": rng.choice(["us-west", "us-east", "eu-central"]),
            "account_id": f"acct-{rng.getrandbits(48):012x}",
        },
        "sections": [],
    }
    sections: list[dict[str, Any]] = document["sections"]
    while len(
        json.dumps({"logical_id": logical_id, "document": document})
    ) < payload_bytes:
        index = len(sections)
        sections.append(
            {
                "heading": f"Section {index}",
                "paragraphs": [
                    _build_paragraph(rng, vocabulary, index, paragraph)
                    for paragraph in range(3)
                ],
                "facts": {
                    "attempt": index,
                    "checksum": f"{logical_id}-{rng.getrandbits(32):08x}",
                    "active": index % 2 == 0,
                    "score": round(rng.random(), 6),
                    "trace_id": f"tr-{rng.getrandbits(64):016x}",
                },
                "entities": [
                    {
                        "name": f"entity-{rng.getrandbits(24):06x}",
                        "type": rng.choice(["customer", "invoice", "session"]),
                        "value": rng.randint(1, 1_000_000),
                    }
                    for _ in range(3)
                ],
            }
        )
    return document


def _build_paragraph(
    rng: random.Random,
    vocabulary: list[str],
    section_index: int,
    paragraph_index: int,
) -> str:
    """Build a varied paragraph with deterministic pseudo-random content."""
    words = rng.sample(vocabulary, k=min(10, len(vocabulary)))
    tokens = [f"{word}-{rng.randint(100, 999)}" for word in words]
    detail = " ".join(tokens)
    return (
        f"Section {section_index} paragraph {paragraph_index} "
        f"records trace {rng.getrandbits(40):010x} with fields {detail}. "
        f"Observed batch={rng.randint(1, 500)} "
        f"window={rng.randint(10, 900)} "
        f"status={rng.choice(['ok', 'retry', 'delayed'])}."
    )


if __name__ == "__main__":
    main()
