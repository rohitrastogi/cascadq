"""Hedged write: speculative retry for tail latency mitigation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

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

    # Wait for BOTH to settle — a first-completed conflict doesn't
    # mean the hedge will also fail (it may be a self-conflict).
    tasks = {primary, hedge}
    result, error = await _collect_results(tasks)

    if result is not None:
        logger.info("hedged write landed for %s %.3fs", key, time.monotonic() - t0)
        return result

    assert error is not None
    raise error


async def _collect_results(
    tasks: set[asyncio.Task[VersionToken]],
) -> tuple[VersionToken | None, BaseException | None]:
    """Wait for all tasks, return (first_success, first_error).

    Cancels remaining tasks as soon as one succeeds.  If all fail,
    returns the first non-cancellation error.
    """
    result: VersionToken | None = None
    error: BaseException | None = None

    while tasks:
        done, tasks = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED,
        )
        for t in done:
            try:
                result = t.result()
            except asyncio.CancelledError:
                pass
            except BaseException as e:
                if error is None:
                    error = e

        if result is not None:
            # Winner found — cancel the rest
            for t in tasks:
                t.cancel()
            for t in tasks:
                try:
                    await t
                except (asyncio.CancelledError, BaseException):
                    pass
            return result, None

    return None, error
