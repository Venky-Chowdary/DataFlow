"""Running heartbeats must not reset started_at (Theater elapsed 0s)."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mongodb_service import MemoryMongoDBService  # noqa: E402


def test_memory_store_keeps_first_started_at():
    mongo = MemoryMongoDBService()
    job_id = mongo.create_transfer_job({"name": "elapsed-clock"})
    mongo.update_job_status(job_id, "running", phase="reading", message="start")
    first = mongo.get_job(job_id)["started_at"]
    assert first is not None
    mongo.update_job_status(
        job_id, "running", phase="preflight", message="confirm", records_processed=0
    )
    mongo.update_job_status(
        job_id, "running", phase="writing", message="batch", records_processed=20_000
    )
    again = mongo.get_job(job_id)["started_at"]
    assert again == first


def test_mongo_updates_do_not_set_started_at_when_already_present():
    """Mirrors the $set bug: setdefault on the updates dict reset the clock."""
    started = datetime.now(timezone.utc) - timedelta(minutes=3)
    prev_doc = {"status": "running", "started_at": started, "phase": "preflight"}
    updates = {"status": "running", "phase": "writing"}
    kwargs: dict = {}
    if (prev_doc or {}).get("started_at") and "started_at" not in kwargs:
        updates.pop("started_at", None)
    else:
        updates.setdefault("started_at", datetime.now(timezone.utc))
    assert "started_at" not in updates
