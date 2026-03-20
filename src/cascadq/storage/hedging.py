"""Hedged write: speculative retry for tail latency mitigation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine
from typing import Any

from cascadq import metrics
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
        metrics.hedge_total.labels(outcome="no_hedge_needed").inc()
        return primary.result()

    # Primary is slow — fire a hedge
    metrics.hedge_total.labels(outcome="hedge_fired").inc()
    logger.debug("hedging write for %s after %.1fs", key, time.monotonic() - t0)
    hedge = asyncio.create_task(write_fn(key, data, expected_version))

    winner, pending = await asyncio.wait(
        {primary, hedge},
        return_when=asyncio.FIRST_COMPLETED,
    )
    first = next(iter(winner))
    try:
        result = first.result()
    except asyncio.CancelledError:
        raise RuntimeError("hedged write task was unexpectedly cancelled") from None
    except ConflictError as first_conflict:
        other = next(iter(pending))
        try:
            return await other
        except asyncio.CancelledError:
            raise RuntimeError(
                "hedged write task was unexpectedly cancelled"
            ) from None
        except BaseException:
            raise first_conflict from None
    except BaseException as first_error:
        other = next(iter(pending))
        try:
            return await other
        except asyncio.CancelledError:
            raise RuntimeError(
                "hedged write task was unexpectedly cancelled"
            ) from None
        except BaseException:
            raise first_error from None

    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except (asyncio.CancelledError, BaseException):
            pass

    outcome = "hedge_won" if first is hedge else "primary_won"
    metrics.hedge_total.labels(outcome=outcome).inc()
    if first is hedge:
        logger.info("hedge beat primary for %s %.3fs", key, time.monotonic() - t0)
    return result
