"""
DataTransfer.space — Training Agent Scheduler

Background loop that keeps AI models trained on universal data.
Runs independently from the customer-facing Copilot.
"""

from __future__ import annotations
import asyncio
import logging

logger = logging.getLogger("datatransfer.training_agent")

TRAINING_INTERVAL_SECONDS = 30 * 60  # retrain every 30 minutes


async def run_training_loop(interval_seconds: int = TRAINING_INTERVAL_SECONDS):
    """Periodically retrain from universal data sources."""
    await asyncio.sleep(60)
    while True:
        try:
            from .training_agent import get_training_agent
            agent = get_training_agent()
            run = await asyncio.to_thread(agent.run_full_training, False, False)
            logger.info(
                "Training agent run %s: %s (%s examples)",
                run.id,
                run.status,
                run.metrics.get("conversation_examples", 0),
            )
        except Exception as e:
            logger.warning("Training agent loop error: %s", e)
        await asyncio.sleep(interval_seconds)


def schedule_training_on_transfer(filename: str, columns: list[str], row_count: int, samples: dict | None = None):
    """Ingest a completed transfer into training data (sync, lightweight)."""
    try:
        from .training_agent import get_training_agent
        get_training_agent().ingest_from_transfer(filename, columns, row_count, samples or {})
    except Exception as e:
        logger.warning("Post-transfer training ingest failed: %s", e)
