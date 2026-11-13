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
import threading
import time
from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)


def shutdown_budget_seconds() -> float:
    """Total time the teardown may spend stopping and joining (``API_SHUTDOWN_BUDGET_SECONDS``, default 25).

    The API's termination grace period must hold preStop + uvicorn's graceful
    shutdown + this + a margin; the chart checks 5 + 10 + 25 + 5 = 45.
    """
    from services.brand_env import getenv_brand

    try:
        return max(0.0, float(getenv_brand("API_SHUTDOWN_BUDGET_SECONDS", "25") or "25"))
    except ValueError:
        return 25.0


def _stoppers() -> list[tuple[str, Callable[[float], object], float]]:
    """(name, stop(timeout), the stop's own longest wait), in the order they run."""
    from services.job_watchdog import stop_stall_watchdog
    from services.orphan_jobs import stop_orphan_sweeper
    from services.signup_service import stop_mail_outbox_sweeper
    from services.transfer_scheduler import drain as drain_transfer_pool
    from services.worker_fleet import stop_api_claim_loop, worker_drain_seconds

    # Sweepers first: they must not judge a job the claim loop is handing back.
    # The claim loop then stops claiming and drains (bounded by
    # WORKER_DRAIN_SECONDS); the API's own transfer pool (single node without
    # a worker fleet) drains the same way (ops review M12); the mail sweeper
    # goes last.
    drain = worker_drain_seconds()
    return [
        ("orphan sweeper", stop_orphan_sweeper, 5.0),
        ("stall watchdog", stop_stall_watchdog, 5.0),
        ("API claim loop", stop_api_claim_loop, drain + 5.0),
        ("transfer pool", drain_transfer_pool, drain),
        ("mail outbox sweeper", stop_mail_outbox_sweeper, 5.0),
        ("telemetry flush", flush_telemetry, TELEMETRY_FLUSH_SECONDS),
    ]


#: Longest wait for the OpenTelemetry log and trace exporters to flush.
TELEMETRY_FLUSH_SECONDS = 3.0


def flush_telemetry(timeout: float) -> bool:
    """Flush and shut down the OTel log and trace providers, waiting at most ``timeout``.

    An unreachable collector can block an exporter's flush well past the
    shutdown budget; the flush runs on a daemon thread that the exiting
    process abandons (ops review round 2, Low). True when it finished.
    """
    def _flush() -> None:
        from services.otel_logging import shutdown_otel_logging
        from services.tracing import shutdown_tracing

        try:
            shutdown_otel_logging()
        except Exception as exc:  # noqa: BLE001 - shutdown is best-effort
            logger.debug("OpenTelemetry log shutdown failed: %s", exc)
        try:
            shutdown_tracing()
        except Exception as exc:  # noqa: BLE001 - shutdown is best-effort
            logger.debug("tracing shutdown failed: %s", exc)

    worker = threading.Thread(target=_flush, name="df-telemetry-flush", daemon=True)
    worker.start()
    worker.join(max(0.0, timeout))
    return not worker.is_alive()


def stop_background_threads(budget_seconds: float | None = None) -> list[str]:
    """Stop and join every background thread within one budget; returns the names that failed.

    Each stop gets its own longest wait or what is left of the budget,
    whichever is less, so the joins together never outlast the budget
    (default :func:`shutdown_budget_seconds`) and the process exits before
    the orchestrator's SIGKILL. A drain cut short by the budget keeps its
    leases: nothing else writes while its writers may still be writing.
    """
    budget = shutdown_budget_seconds() if budget_seconds is None else max(0.0, float(budget_seconds))
    deadline = time.monotonic() + budget
    failed: list[str] = []
    for name, stop, longest in _stoppers():
        try:
            stop(max(0.0, min(longest, deadline - time.monotonic())))
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
