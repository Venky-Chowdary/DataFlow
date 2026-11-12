"""Stop what the API process started in the background (audit M-9).

The lifespan starts four threads (mail-outbox sweeper, API claim loop, orphan
sweeper, stall watchdog) and the local transfer pool, and keeps asyncio tasks
for the schedule and training loops. Shutdown — uvicorn turns SIGTERM into the lifespan teardown — calls
:func:`stop_background_threads` and :func:`cancel_background_tasks`, so nothing
outlives the app: no sweep after the store is gone, no claim after the drain.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)


def _stoppers() -> list[tuple[str, Callable[[], None]]]:
    from services.job_watchdog import stop_stall_watchdog
    from services.orphan_jobs import stop_orphan_sweeper
    from services.signup_service import stop_mail_outbox_sweeper
    from services.transfer_scheduler import drain as drain_transfer_pool
    from services.worker_fleet import stop_api_claim_loop

    # Sweepers first: they must not judge a job the claim loop is handing back.
    # The claim loop then stops claiming and drains (bounded by
    # WORKER_DRAIN_SECONDS); the API's own transfer pool (single node without
    # a worker fleet) drains the same way (ops review M12); the mail sweeper
    # goes last.
    return [
        ("orphan sweeper", stop_orphan_sweeper),
        ("stall watchdog", stop_stall_watchdog),
        ("API claim loop", stop_api_claim_loop),
        ("transfer pool", drain_transfer_pool),
        ("mail outbox sweeper", stop_mail_outbox_sweeper),
    ]


def stop_background_threads() -> list[str]:
    """Stop and join every background thread; returns the names that failed to stop cleanly."""
    failed: list[str] = []
    for name, stop in _stoppers():
        try:
            stop()
        except Exception:  # noqa: BLE001 - one bad stop must not skip the others
            logger.warning("could not stop the %s", name, exc_info=True)
            failed.append(name)
    return failed


async def cancel_background_tasks(tasks: Iterable[asyncio.Task | None]) -> None:
    """Cancel the loop tasks the lifespan kept and wait until each has ended."""
    pending = [task for task in tasks if task is not None and not task.done()]
    for task in pending:
        task.cancel()
    for task in pending:
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001 - a loop that died on its own is already stopped
            logger.debug("background task ended with an error during shutdown", exc_info=True)
