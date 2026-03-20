"""Hedged write: speculative retry for tail latency mitigation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

from cascadq.errors import ConflictError
from cascadq.storage.protocol import VersionToken

logger = logging.getLogger(__name__)

WriteFunc = Callable[
    [str, bytes, VersionToken], Coroutine[Any, Any, VersionToken]
]


async def hedged_write(
    write_fn: WriteFunc,
    key: str,
    data: bytes,
    expected_version: VersionToken,
    hedge_after: float,
) -> VersionToken:
    """Race a primary write against a speculative hedge.

    Both writes carry the same CAS precondition (e.g., ``If-Match``),
    so the backing store serializes them: exactly one succeeds, the
    other gets a CAS conflict.  Returns the winner's new version
    token and suppresses the loser's self-inflicted conflict.

    If both fail (another writer changed the version), the conflict
    is real and is propagated to the caller.
    """
    t0 = time.monotonic()
    primary = asyncio.create_task(write_fn(key, data, expected_version))

    done, _ = await asyncio.wait({primary}, timeout=hedge_after)
    if done:
        return primary.result()

    # Primary is slow — fire a hedge
    logger.debug("hedging write for %s after %.1fs", key, time.monotonic() - t0)
    hedge = asyncio.create_task(write_fn(key, data, expected_version))
    done, pending = await asyncio.wait(
        {primary, hedge}, return_when=asyncio.FIRST_COMPLETED,
    )
    for t in pending:
        t.cancel()

    # Pick the successful result.  The loser gets a CAS conflict
    # because the version changed when the winner landed — that is
    # a harmless self-conflict.
    result: VersionToken | None = None
    conflict: ConflictError | None = None
    for t in done:
        try:
            result = t.result()
        except ConflictError as e:
            conflict = e

    for t in pending:
        try:
            await t
        except (asyncio.CancelledError, ConflictError):
            pass

    if result is not None:
        logger.info("hedged write landed for %s %.3fs", key, time.monotonic() - t0)
        return result

    # Both writes got CAS conflicts — real conflict from another writer
    assert conflict is not None
    raise conflict
