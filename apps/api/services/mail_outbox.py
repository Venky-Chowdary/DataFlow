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
_LEASE_SEC = 60


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


_INDEXES_READY = False


def _database():
    """Mongo tier when configured. Index setup is one-time per process; a
    failure raises — silently falling back to the file tier would split
    the queue across replicas."""
    global _INDEXES_READY
    db = mongo_database()
    if db is None:
        return None
    if not _INDEXES_READY:
        try:
            db[COLLECTION].create_index("status")
            db[COLLECTION].create_index("expires_at", expireAfterSeconds=0)
        except Exception:
            logger.error("mail_outbox: Mongo index setup failed; refusing "
                         "the file fallback (queue state would split "
                         "across replicas)", exc_info=True)
            raise
        _INDEXES_READY = True
    return db


def _path() -> Path:
    return data_dir() / _FILE


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _prune_expired(rows: dict[str, Any], now: str) -> int:
    """Drop file-tier rows past expires_at — the Mongo TTL index does this
    there, but nothing bounded the file otherwise. Only rows that carry an
    expires_at are pruned (legacy rows without one are left alone)."""
    dead = [k for k, r in rows.items()
            if r.get("expires_at") and str(r["expires_at"]) <= now]
    for k in dead:
        rows.pop(k, None)
    return len(dead)


def _expiry_iso() -> str:
    """ISO string on the file/returned row; the Mongo copy stores a real
    datetime so the TTL index actually fires."""
    return (datetime.now(UTC) + _KEEP_SENT_FOR).isoformat()


def enqueue(*, to: str, subject: str, text: str, html: str,
            idempotency_key: str, purpose: str,
            quota_counted: bool = False,
            reply_to: str = "") -> dict[str, Any]:
    """Persist a queued outbox row and return it. ``quota_counted`` marks
    rows whose send-quota slot was already consumed on the request path
    (signup/OTP mails keep the synchronous 429), so the lane doesn't
    count the same send twice."""
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
        "attempts": 0,
        "quota_counted": quota_counted,
        "reply_to": reply_to,
        "created_at": _now_iso(),
        "expires_at": _expiry_iso(),
    }
    db = _database()
    if db is not None:
        # BSON datetime — the expires_at TTL index only fires on dates.
        db[COLLECTION].insert_one(
            {**row, "_id": row["id"],
             "expires_at": datetime.now(UTC) + _KEEP_SENT_FOR})
        return row

    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        rows = doc.setdefault("rows", {})
        _prune_expired(rows, _now_iso())
        rows[row["id"]] = row
    return row


def mark(row_id: str, status: str, *, claim_token: str | None = None) -> None:
    """Record the terminal status (``sent`` | ``failed`` | ``expired`` |
    ``superseded``) for a row; terminal rows lose the body entirely.
    ``claim_token`` restricts the write to the lease we hold — a stale
    claimer must not stamp its outcome over the replica that now owns
    the row."""
    db = _database()
    if db is not None:
        update: dict[str, Any] = {
            "$set": {"status": status, "finished_at": _now_iso()}}
        if status in _TERMINAL:
            update["$unset"] = {k: "" for k in _BODY_KEYS}
        flt: dict[str, Any] = {"_id": row_id}
        if claim_token is not None:
            flt["claim_token"] = claim_token
        db[COLLECTION].update_one(flt, update)
        return
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if row is None:
            return
        if claim_token is not None and row.get("claim_token") != claim_token:
            return
        row["status"] = status
        row["finished_at"] = _now_iso()
        if status in _TERMINAL:
            for k in _BODY_KEYS:
                row.pop(k, None)


def reschedule(row_id: str, delay_sec: float, reason: str = "",
               *, claim_token: str | None = None) -> None:
    """Return a claimed row to 'queued' after a transient failure, gated by
    ``next_attempt_at`` — it is not claimable again until the backoff elapses.
    The attempt count lives on the row so retries are bounded across
    replicas and restarts, not just within one process. ``claim_token``
    restricts the write to the lease we hold."""
    next_at = (datetime.now(UTC)
               + timedelta(seconds=max(0.05, delay_sec))).isoformat()
    db = _database()
    if db is not None:
        flt: dict[str, Any] = {"_id": row_id}
        if claim_token is not None:
            flt["claim_token"] = claim_token
        db[COLLECTION].update_one(flt, {
            "$set": {"status": "queued", "next_attempt_at": next_at,
                     "last_error": reason},
            "$inc": {"attempts": 1},
            "$unset": {"claimed_at": "", "lease_until": "",
                       "claim_token": ""}})
        return
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if row is None:
            return
        if claim_token is not None and row.get("claim_token") != claim_token:
            return
        row["status"] = "queued"
        row["next_attempt_at"] = next_at
        row["last_error"] = reason
        row["attempts"] = int(row.get("attempts") or 0) + 1
        row.pop("claimed_at", None)
        row.pop("lease_until", None)
        row.pop("claim_token", None)


def queued() -> list[dict[str, Any]]:
    """Rows persisted but never marked — crashed mid-flight or restart."""
    db = _database()
    if db is not None:
        return [{k: v for k, v in r.items() if k != "_id"}
                for r in db[COLLECTION].find({"status": "queued"})]
    doc = load_json_doc(_path(), {"rows": {}})
    return [r for r in doc.get("rows", {}).values()
            if r.get("status") == "queued"]


def _due_clause(now: str) -> dict[str, Any]:
    """A queued row is due when it has no backoff gate, or it has passed."""
    return {"$or": [{"next_attempt_at": {"$exists": False}},
                    {"next_attempt_at": None},
                    {"next_attempt_at": {"$lte": now}}]}


def _claimable() -> list[dict[str, Any]]:
    """queued-and-due rows plus 'sending' rows whose lease expired —
    the only rows a drain or delivery may try to claim."""
    now = _now_iso()
    db = _database()
    if db is not None:
        docs = db[COLLECTION].find({"$or": [
            {"status": "queued", **_due_clause(now)},
            {"status": "sending", "lease_until": {"$lte": now}},
        ]})
        return [{k: v for k, v in r.items() if k != "_id"} for r in docs]
    doc = load_json_doc(_path(), {"rows": {}})
    out = []
    for row in doc.get("rows", {}).values():
        status = row.get("status")
        due = str(row.get("next_attempt_at") or "") <= now
        stale = status == "sending" and str(row.get("lease_until") or "") <= now
        if (status == "queued" and due) or stale:
            out.append(row)
    return out


def claim(row_id: str, *, lease_sec: int = _LEASE_SEC) -> str | None:
    """Atomically move a due 'queued' row (or an expired 'sending' lease)
    to 'sending' and return a fencing token for it — exactly one caller
    across every replica wins, which is what makes two concurrent drains
    produce one send per row. The token identifies this claimer so a
    later :func:`extend_lease` / :func:`mark` / :func:`reschedule` cannot
    be mis-attributed to a replica that reclaimed the row in between."""
    now = datetime.now(UTC)
    now_s = now.isoformat()
    until = (now + timedelta(seconds=lease_sec)).isoformat()
    token = uuid.uuid4().hex
    db = _database()
    if db is not None:
        from pymongo import ReturnDocument

        doc = db[COLLECTION].find_one_and_update(
            {"_id": row_id, "$or": [
                {"status": "queued", **_due_clause(now_s)},
                {"status": "sending", "lease_until": {"$lte": now_s}},
            ]},
            {"$set": {"status": "sending", "claimed_at": now_s,
                      "lease_until": until, "claim_token": token}},
            return_document=ReturnDocument.AFTER)
        return token if doc is not None else None
    claimed = False
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if row is not None:
            due = str(row.get("next_attempt_at") or "") <= now_s
            stale = (row.get("status") == "sending"
                     and str(row.get("lease_until") or "") <= now_s)
            if (row.get("status") == "queued" and due) or stale:
                row["status"] = "sending"
                row["claimed_at"] = now_s
                row["lease_until"] = until
                row["claim_token"] = token
                claimed = True
    return token if claimed else None


def extend_lease(row_id: str, claim_token: str,
                 *, lease_sec: int = _LEASE_SEC) -> bool:
    """Push ``lease_until`` out while we still own the claim. Called right
    before the provider send so the lease always covers the whole call:
    a send that runs long can never be reclaimed and duplicated by a
    second replica mid-flight. False when the claim is already lost
    (expired and reclaimed, or terminal) — the caller must not send."""
    now = datetime.now(UTC)
    until = (now + timedelta(seconds=lease_sec)).isoformat()
    db = _database()
    if db is not None:
        res = db[COLLECTION].update_one(
            {"_id": row_id, "status": "sending",
             "claim_token": claim_token},
            {"$set": {"lease_until": until}})
        return res.modified_count == 1
    ok = False
    with json_doc_transaction(_path(), {"rows": {}}) as doc:
        row = doc.setdefault("rows", {}).get(row_id)
        if (row is not None and row.get("status") == "sending"
                and row.get("claim_token") == claim_token):
            row["lease_until"] = until
            ok = True
    return ok


def counts() -> dict[str, int]:
    """Status histogram for admin metrics — surfaces 'queued but never
    sent' explicitly."""
    out = {"queued": 0, "sending": 0, "sent": 0, "failed": 0,
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
    for row in _claimable():
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
