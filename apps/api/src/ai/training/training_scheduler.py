"""
Datawrap — Training Agent Scheduler

Background loop that keeps AI models trained on universal data.
Runs independently from the customer-facing Copilot.
"""

from __future__ import annotations

import asyncio
import logging

from .training_runtime import TrainingRuntime

logger = logging.getLogger("datatransfer.training_agent")

TRAINING_INTERVAL_SECONDS = 30 * 60  # retrain every 30 minutes
TRAINING_INITIAL_DELAY_SECONDS = 60  # first periodic run, after the API is up
BACKGROUND_TRAINING_DELAY_SECONDS = 120  # one-off warm-up run (src.main lifespan)


def training_enabled() -> bool:
    """Whether the lifespan starts background training.

    Unset: on outside production, off in production. Set: only
    ``1/true/yes/on`` enable it; an empty value (``DATAWRAP_TRAINING=``, which
    shadows ``DATAFLOW_TRAINING``) or an unrecognised one leaves it off.
    """
    from services.brand_env import getenv_brand, getenv_brand_bool
    from services.platform_config import is_production

    if getenv_brand("TRAINING") is None:
        return not is_production()
    return getenv_brand_bool("TRAINING", False)


async def run_training_loop(
    interval_seconds: int = TRAINING_INTERVAL_SECONDS,
    runtime: TrainingRuntime | None = None,
):
    """Periodically retrain from universal data sources.

    Each run executes on the runtime's own daemon thread (never the loop's
    default executor) and stops when the runtime's stop event is set.
    """
    runtime = runtime or TrainingRuntime()
    await asyncio.sleep(TRAINING_INITIAL_DELAY_SECONDS)
    while not runtime.stop_event.is_set():
        try:
            from .training_agent import get_training_agent
            agent = get_training_agent()
            run = await runtime.run(agent.run_full_training, False, False, stop_event=runtime.stop_event)
            logger.info(
                "Training agent run %s: %s (%s examples)",
                run.id,
                run.status,
                run.metrics.get("conversation_examples", 0),
            )
        except Exception as e:
            logger.warning("Training agent loop error: %s", e)
        await asyncio.sleep(interval_seconds)


async def run_background_training(runtime: TrainingRuntime) -> None:
    """One-off training run shortly after API warm-up (started by the lifespan)."""
    await asyncio.sleep(BACKGROUND_TRAINING_DELAY_SECONDS)
    try:
        from .training_agent import get_training_agent
        training = get_training_agent()
        run = await runtime.run(training.run_full_training, False, False, stop_event=runtime.stop_event)
        ready = run.metrics.get("copilot_evaluation", {}).get("ready", False)
        examples = run.metrics.get("conversation_examples", 0)
        print(f"[+] Copilot Training Agent: {run.status} ({examples} examples, ready={ready})")
    except Exception as te:
        print(f"[!] Copilot training warning: {te}")


def _admin_run(include_embedding_tune: bool, force: bool, stop_event):
    from .training_agent import get_training_agent

    return get_training_agent().run_full_training(
        include_embedding_tune=include_embedding_tune, force=force, stop_event=stop_event,
    )


async def run_admin_training(runtime: TrainingRuntime, *, include_embedding_tune: bool, force: bool):
    """An admin-requested run on the runtime's thread; await its ``TrainingRun``.

    The event loop stays free, and lifespan shutdown stops the run.
    """
    return await runtime.run(_admin_run, include_embedding_tune, force, runtime.stop_event)


def start_admin_training(runtime: TrainingRuntime, *, include_embedding_tune: bool, force: bool) -> asyncio.Task:
    """Start an admin-requested run in the background (not tied to the request)."""

    async def _background() -> None:
        try:
            run = await run_admin_training(
                runtime, include_embedding_tune=include_embedding_tune, force=force,
            )
            logger.info("Admin training run %s: %s", run.id, run.status)
        except Exception as exc:  # noqa: BLE001 - nobody awaits this task: log, never raise
            logger.warning("Admin training run failed: %s", exc)

    return runtime.start_task(_background(), name="training-admin")


def schedule_training_on_transfer(filename: str, columns: list[str], row_count: int, samples: dict | None = None):
    """Ingest a completed transfer into training data (sync, lightweight)."""
    try:
        from .training_agent import get_training_agent
        get_training_agent().ingest_from_transfer(filename, columns, row_count, samples or {})
    except Exception as e:
        logger.warning("Post-transfer training ingest failed: %s", e)
