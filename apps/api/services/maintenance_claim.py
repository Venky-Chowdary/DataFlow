"""Once-per-period claims for scheduler housekeeping (retention, sandbox expiry).

The beat runs on every scheduler replica, as often as once a second. A step
that scans stores calls :func:`claim_run` first: an atomic advance of one
document in Mongo (``maintenance_runs``, ``_id = <name>``) — the first replica
past ``next_due_at`` wins the period, every other call returns ``False``.

Without a durable store (file-backed deployments) there is nothing replicas
could share, so :func:`claim_run` returns ``None`` and the caller decides:
scheduled retention refuses to run (and its setting refuses to turn on);
the sandbox sweep runs on its own, unclaimed, at most once per period per
process.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

COLLECTION = "maintenance_runs"
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_local_due: dict[str, datetime] = {}
_local_lock = threading.Lock()


def durable_store() -> Any:
    """The control-plane database replicas share, or ``None``."""
    from services.metadata_backend import mongo_database

    return mongo_database()


def claim_run(name: str, interval: timedelta, *, now: datetime | None = None, force: bool = False) -> bool | None:
    """Claim this period of ``name``. ``None`` = no durable store to claim in."""
    db = durable_store()
    if db is None:
        return None
    current = now or datetime.now(timezone.utc)
    coll = db[COLLECTION]
    coll.update_one({"_id": name}, {"$setOnInsert": {"next_due_at": _EPOCH}}, upsert=True)
    due = {} if force else {"next_due_at": {"$lte": current}}
    won = coll.find_one_and_update(
        {"_id": name, **due},
        {"$set": {"next_due_at": current + interval, "claimed_at": current}},
    )
    return won is not None


def claim_local(name: str, interval: timedelta, *, now: datetime | None = None) -> bool:
    """Per-process throttle for a step that may run without a durable store."""
    current = now or datetime.now(timezone.utc)
    with _local_lock:
        if _local_due.get(name, _EPOCH) > current:
            return False
        _local_due[name] = current + interval
        return True


def claim_status(name: str) -> dict[str, Any]:
    db = durable_store()
    if db is None:
        return {}
    return db[COLLECTION].find_one({"_id": name}) or {}
