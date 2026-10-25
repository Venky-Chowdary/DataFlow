"""Durable mail outbox.

A row is persisted *before* the serialized send lane picks it up, so a
deploy or restart can never silently lose a queued reset/OTP mail: on boot
``drain_queued`` re-queues every row still marked ``queued``, and the
sender's idempotency key keeps a repeat hand-off safe at the provider.

Storage follows the same durable-store pair as users/challenges:
MongoDB (``mail_outbox`` collection) when configured — shared across
replicas — else ``mail_outbox.json`` under ``data_dir()``.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from services.metadata_backend import (
    json_doc_transaction, load_json_doc, mongo_database)
from services.platform_config import data_dir

logger = logging.getLogger(__name__)

COLLECTION = "mail_outbox"
_FILE = "mail_outbox.json"
_KEEP_SENT_FOR = timedelta(days=7)


def _database():
    db = mongo_database()
    if db is None:
        return None
    try:
        db[COLLECTION].create_index("status")
        db[COLLECTION].create_index("expires_at", expireAfterSeconds=0)
    except Exception:
        return None
    return db


def _path() -> Path:
    return data_dir() / _FILE


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _expiry_iso() -> str:
    return (datetime.now(UTC) + _KEEP_SENT_FOR).isoformat()


def enqueue(*, to: str, subject: str, text: str, html: str,
            idempotency_key: str, purpose: str) -> dict[str, Any]:
    """Persist a queued outbox row and return it."""
    row = {
        "id": f"mo_{uuid.uuid4().hex}",
        "purpose": purpose,
        "to": to,
        "subject": subject,
        "text": text,
        "html": html,
        "idempotency_key": idempotency_key,
        "status": "queued",
        "created_at": _now_iso(),
        "expires_at": _expiry_iso(),
    }
    db = _database()
    if db is not None:
        db[COLLECTION].insert_one({**row, "_id": row["id"]})
        return row

    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        doc.setdefault("rows", {})[row["id"]] = row
    return row


def mark(row_id: str, status: str) -> None:
    """Record the terminal status (``sent`` | ``failed``) for a row."""
    db = _database()
    if db is not None:
        db[COLLECTION].update_one(
            {"_id": row_id},
            {"$set": {"status": status, "finished_at": _now_iso()}})
        return
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if row is not None:
            row["status"] = status
            row["finished_at"] = _now_iso()


def queued() -> list[dict[str, Any]]:
    """Rows persisted but never marked — crashed mid-flight or restart."""
    db = _database()
    if db is not None:
        return [{k: v for k, v in r.items() if k != "_id"}
                for r in db[COLLECTION].find({"status": "queued"})]
    doc = load_json_doc(_path(), {"rows": {}})
    return [r for r in doc.get("rows", {}).values()
            if r.get("status") == "queued"]


def counts() -> dict[str, int]:
    """Status histogram for admin metrics — surfaces 'queued but never
    sent' explicitly."""
    out = {"queued": 0, "sent": 0, "failed": 0}
    db = _database()
    if db is not None:
        for row in db[COLLECTION].aggregate(
                [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]):
            if row["_id"] in out:
                out[row["_id"]] = int(row["n"])
        return out
    doc = load_json_doc(_path(), {"rows": {}})
    for row in doc.get("rows", {}).values():
        status = row.get("status")
        if status in out:
            out[status] += 1
    return out


def drain_queued(submit) -> int:
    """Re-submit every persisted 'queued' row through the lane. Called at
    startup so a restart cannot silently drop mail; returns the count
    re-queued. ``submit(row)`` hands one row back to the send lane."""
    rows = queued()
    for row in rows:
        try:
            submit(row)
        except Exception:
            logger.error("mail outbox drain failed for %s",
                         row.get("id"), exc_info=True)
    if rows:
        logger.info("mail outbox: re-queued %d persisted row(s)", len(rows))
    return len(rows)
