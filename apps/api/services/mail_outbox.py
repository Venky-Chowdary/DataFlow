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
# Terminal statuses drop the body — nothing readable stays at rest longer
# than the delivery needs it.
_TERMINAL = ("sent", "failed", "expired", "superseded")
_BODY_KEYS = ("text", "html", "text_enc", "html_enc", "enc")


def _seal(plain: str) -> str:
    """Encrypt a rendered body with the platform secret vault — reset links
    and OTP codes must never sit at rest in plaintext (the reset store keeps
    only the token's HMAC; the outbox must not undo that)."""
    from services.secret_vault import _get_vault

    return _get_vault().encrypt(plain or "")


def _unseal(stored: str) -> str:
    if not stored:
        return ""
    from services.secret_vault import _get_vault

    return _get_vault().decrypt(stored)


def body(row: dict[str, Any]) -> dict[str, str]:
    """Decrypted ``{'text','html'}`` for delivery. Rows written before
    sealing still carry plaintext ``text``/``html`` — read them so an
    in-flight queue from the previous deploy still drains."""
    if row.get("enc"):
        return {
            "text": _unseal(row.get("text_enc", "")),
            "html": _unseal(row.get("html_enc", "")),
        }
    return {"text": row.get("text") or "", "html": row.get("html") or ""}


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
        "text_enc": _seal(text),
        "html_enc": _seal(html),
        "enc": "secret_vault",
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
    """Record the terminal status (``sent`` | ``failed`` | ``expired`` |
    ``superseded``) for a row; terminal rows lose the body entirely."""
    db = _database()
    if db is not None:
        update: dict[str, Any] = {
            "$set": {"status": status, "finished_at": _now_iso()}}
        if status in _TERMINAL:
            update["$unset"] = {k: "" for k in _BODY_KEYS}
        db[COLLECTION].update_one({"_id": row_id}, update)
        return
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if row is not None:
            row["status"] = status
            row["finished_at"] = _now_iso()
            if status in _TERMINAL:
                for k in _BODY_KEYS:
                    row.pop(k, None)


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
    out = {"queued": 0, "sent": 0, "failed": 0,
           "expired": 0, "superseded": 0}
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


def drain_queued(submit, *, skip_reason=None) -> int:
    """Re-submit every persisted 'queued' row through the lane. Called at
    startup so a restart cannot silently drop mail; returns the count
    re-queued. ``submit(row)`` hands one row back to the send lane.

    ``skip_reason(row)`` may return a terminal status ('expired',
    'superseded') for a row whose payload is already dead — e.g. a reset
    link whose token expired, was consumed, or was superseded by a newer
    request. Such rows are marked, never sent, and counted in metrics."""
    submitted = 0
    skipped = 0
    for row in queued():
        if skip_reason is not None:
            try:
                reason = skip_reason(row)
            except Exception:
                reason = None
                logger.error("mail outbox skip check failed for %s",
                             row.get("id"), exc_info=True)
            if reason:
                mark(row["id"], reason)
                skipped += 1
                continue
        try:
            submit(row)
            submitted += 1
        except Exception:
            logger.error("mail outbox drain failed for %s",
                         row.get("id"), exc_info=True)
    if submitted or skipped:
        logger.info("mail outbox: re-queued %d row(s), skipped %d dead",
                    submitted, skipped)
    return submitted
