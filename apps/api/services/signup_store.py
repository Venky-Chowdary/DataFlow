"""pending_signups + otp_challenges stores (P1-1).

- ``pending_signups``: one row per canonical email; re-signup REPLACES the
  row (fresh password hash + challenge). Rows expire after 24 h (Mongo TTL
  index; the file fallback purges on access).
- ``otp_challenges``: hashed codes only (``code_hmac`` — the raw code never
  touches storage), attempt counting with a 5-attempt burn, per-purpose
  expiry; one active challenge per (subject, purpose) — creating a new one
  voids the previous.

Mongo when configured; ``pending_signups.json`` / ``otp_challenges.json``
under data_dir() otherwise.
"""

from __future__ import annotations

import threading
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from services.email_canonical import canonical_email
from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

PENDING_COLLECTION = "pending_signups"
CHALLENGES_COLLECTION = "otp_challenges"

_PENDING_FILE = "pending_signups.json"
_CHALLENGES_FILE = "otp_challenges.json"
_LOCK = threading.Lock()

PENDING_TTL = timedelta(hours=24)
OTP_PURPOSES = ("signup", "reset", "email_change", "login")

_PREPARED_DATABASES: set[str] = set()


def _database():
    db = mongo_database()
    if db is None:
        return None
    if db.name not in _PREPARED_DATABASES:
        db[PENDING_COLLECTION].create_index(
            [("canonical_email", 1)], unique=True, name="canonical_email_unique")
        db[PENDING_COLLECTION].create_index(
            [("expires_at", 1)], expireAfterSeconds=0, name="pending_ttl")
        db[CHALLENGES_COLLECTION].create_index(
            [("subject_id", 1), ("purpose", 1)])
        db[CHALLENGES_COLLECTION].create_index(
            [("expires_at", 1)], expireAfterSeconds=0, name="challenge_ttl")
        _PREPARED_DATABASES.add(db.name)
    return db


def _pending_path():
    return data_dir() / _PENDING_FILE


def _challenges_path():
    return data_dir() / _CHALLENGES_FILE


def _now() -> datetime:
    return datetime.now(UTC)


def _parse_ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


# ── pending_signups ──────────────────────────────────────────────────────────


def upsert_pending_signup(
    *,
    email: str,
    name: str,
    password_hash: str,
    ip: str = "",
    device_id: str = "",
    risk_score: float = 0.0,
    pending_id: str | None = None,
    ttl: timedelta = PENDING_TTL,
) -> dict[str, Any]:
    """Create-or-replace the pending row for this canonical email."""
    canonical = canonical_email(email)
    now = _now()
    pid = pending_id or f"psu_{uuid.uuid4().hex}"
    doc: dict[str, Any] = {
        "_id": pid,
        "id": pid,
        "email": email.strip(),
        "canonical_email": canonical,
        "name": (name or "").strip(),
        "password_hash": password_hash,
        "risk_score": float(risk_score),
        "ip": ip,
        "device_id": device_id,
        "created_at": now.isoformat(),
        "expires_at": (now + ttl).isoformat(),
    }
    db = _database()
    if db is not None:
        db[PENDING_COLLECTION].delete_many({"canonical_email": canonical})
        db[PENDING_COLLECTION].insert_one(doc)
        return {k: v for k, v in doc.items() if k != "_id"}
    with _LOCK:
        with json_doc_transaction(_pending_path(), {"pending_signups": []}) as data:
            rows = [
                r for r in data.get("pending_signups", [])
                if isinstance(r, dict)
                and r.get("canonical_email") != canonical
                and not _expired(r)
            ]
            rows.append({k: v for k, v in doc.items() if k != "_id"})
            data["pending_signups"] = rows
    return {k: v for k, v in doc.items() if k != "_id"}


def _expired(row: dict[str, Any]) -> bool:
    exp = _parse_ts(row.get("expires_at"))
    return exp is not None and exp <= _now()


def get_pending_signup(*, email: str | None = None, canonical: str | None = None,
                       pending_id: str | None = None) -> dict[str, Any] | None:
    key = canonical or (canonical_email(email) if email else None)
    db = _database()
    if db is not None:
        query: dict[str, Any] = {}
        if pending_id:
            query["_id"] = pending_id
        elif key:
            query["canonical_email"] = key
        else:
            return None
        row = db[PENDING_COLLECTION].find_one(query, {"_id": False})
        if row and not _expired(row):
            return dict(row)
        return None
    rows = load_json_doc(_pending_path(), {"pending_signups": []}).get("pending_signups", [])
    for row in rows:
        if not isinstance(row, dict) or _expired(row):
            continue
        if pending_id and row.get("id") == pending_id:
            return row
        if key and row.get("canonical_email") == key:
            return row
    return None


def delete_pending_signup(*, canonical: str | None = None, pending_id: str | None = None) -> None:
    db = _database()
    if db is not None:
        if pending_id:
            db[PENDING_COLLECTION].delete_one({"_id": pending_id})
        elif canonical:
            db[PENDING_COLLECTION].delete_many({"canonical_email": canonical})
        return
    with _LOCK:
        with json_doc_transaction(_pending_path(), {"pending_signups": []}) as data:
            data["pending_signups"] = [
                r for r in data.get("pending_signups", [])
                if not (
                    isinstance(r, dict)
                    and ((pending_id and r.get("id") == pending_id)
                         or (canonical and r.get("canonical_email") == canonical))
                )
            ]


def purge_expired_pending() -> int:
    """File fallback helper — Mongo handles expiry via the TTL index."""
    db = _database()
    if db is not None:
        return 0
    removed = 0
    with _LOCK:
        with json_doc_transaction(_pending_path(), {"pending_signups": []}) as data:
            before = data.get("pending_signups", [])
            data["pending_signups"] = [r for r in before if not _expired(r)]
            removed = len(before) - len(data["pending_signups"])
    return removed


# ── otp_challenges ───────────────────────────────────────────────────────────

MAX_ATTEMPTS = 5


def create_challenge(
    *,
    purpose: str,
    subject_id: str,
    code_hmac: str,
    ttl: timedelta,
    challenge_id: str | None = None,
) -> dict[str, Any]:
    """New challenge for (subject, purpose); prior active ones are voided."""
    if purpose not in OTP_PURPOSES:
        raise ValueError(f"purpose must be one of {OTP_PURPOSES}")
    cid = challenge_id or f"otc_{uuid.uuid4().hex}"
    now = _now()
    doc: dict[str, Any] = {
        "_id": cid,
        "id": cid,
        "purpose": purpose,
        "subject_id": subject_id,
        "code_hmac": code_hmac,
        "attempts": 0,
        "max_attempts": MAX_ATTEMPTS,
        "sent_count": 1,
        "last_sent_at": now.isoformat(),
        "created_at": now.isoformat(),
        "expires_at": (now + ttl).isoformat(),
        "consumed_at": None,
    }
    db = _database()
    if db is not None:
        db[CHALLENGES_COLLECTION].update_many(
            {"subject_id": subject_id, "purpose": purpose, "consumed_at": None},
            {"$set": {"consumed_at": now.isoformat()}},
        )
        db[CHALLENGES_COLLECTION].insert_one(doc)
        return {k: v for k, v in doc.items() if k != "_id"}
    with _LOCK:
        with json_doc_transaction(_challenges_path(), {"otp_challenges": []}) as data:
            for row in data.get("otp_challenges", []):
                if (
                    isinstance(row, dict)
                    and row.get("subject_id") == subject_id
                    and row.get("purpose") == purpose
                    and not row.get("consumed_at")
                ):
                    row["consumed_at"] = now.isoformat()
            data.setdefault("otp_challenges", []).append(
                {k: v for k, v in doc.items() if k != "_id"})
    return {k: v for k, v in doc.items() if k != "_id"}


def get_challenge(challenge_id: str) -> dict[str, Any] | None:
    db = _database()
    if db is not None:
        row = db[CHALLENGES_COLLECTION].find_one({"_id": challenge_id}, {"_id": False})
        return dict(row) if row and not _expired(row) else None
    rows = load_json_doc(_challenges_path(), {"otp_challenges": []}).get("otp_challenges", [])
    for row in rows:
        if isinstance(row, dict) and row.get("id") == challenge_id and not _expired(row):
            return row
    return None


def _update_challenge(cid: str, **fields: Any) -> None:
    db = _database()
    if db is not None:
        db[CHALLENGES_COLLECTION].update_one({"_id": cid}, {"$set": fields})
        return
    with _LOCK:
        with json_doc_transaction(_challenges_path(), {"otp_challenges": []}) as data:
            for row in data.get("otp_challenges", []):
                if isinstance(row, dict) and row.get("id") == cid:
                    row.update(fields)


def record_attempt(challenge_id: str) -> dict[str, Any] | None:
    """Atomically increment attempts; burn the challenge when the cap is hit.
    Returns the post-increment row so callers never report a stale
    attempts_remaining under concurrent wrong-code submissions."""
    db = _database()
    if db is not None:
        from pymongo import ReturnDocument
        row = db[CHALLENGES_COLLECTION].find_one_and_update(
            {"_id": challenge_id},
            {"$inc": {"attempts": 1}},
            return_document=ReturnDocument.AFTER,
            projection={"_id": False},
        )
        if row is None:
            return None
        if (
            int(row.get("attempts") or 0) >= int(row.get("max_attempts") or MAX_ATTEMPTS)
            and not row.get("consumed_at")
        ):
            consumed = _now().isoformat()
            db[CHALLENGES_COLLECTION].update_one(
                {"_id": challenge_id, "consumed_at": None},
                {"$set": {"consumed_at": consumed}},
            )
            row["consumed_at"] = consumed
        return dict(row)
    with _LOCK:
        with json_doc_transaction(_challenges_path(), {"otp_challenges": []}) as data:
            for row in data.get("otp_challenges", []):
                if (
                    isinstance(row, dict)
                    and row.get("id") == challenge_id
                    and not _expired(row)
                ):
                    attempts = int(row.get("attempts") or 0) + 1
                    row["attempts"] = attempts
                    if attempts >= int(row.get("max_attempts") or MAX_ATTEMPTS) and not row.get("consumed_at"):
                        row["consumed_at"] = _now().isoformat()
                    return dict(row)
            return None


def mark_sent(challenge_id: str, *, code_hmac: str, ttl: timedelta) -> dict[str, Any] | None:
    """Refresh a challenge for a resend: new hash, sent_count++, new expiry."""
    row = get_challenge(challenge_id)
    if row is None:
        return None
    now = _now()
    fields = {
        "code_hmac": code_hmac,
        "attempts": 0,
        "sent_count": int(row.get("sent_count") or 0) + 1,
        "last_sent_at": now.isoformat(),
        "expires_at": (now + ttl).isoformat(),
    }
    _update_challenge(challenge_id, **fields)
    row.update(fields)
    return row


def consume_challenge(challenge_id: str) -> None:
    _update_challenge(challenge_id, consumed_at=_now().isoformat())


def active_challenge(subject_id: str, purpose: str) -> dict[str, Any] | None:
    db = _database()
    if db is not None:
        row = db[CHALLENGES_COLLECTION].find_one(
            {"subject_id": subject_id, "purpose": purpose, "consumed_at": None},
            {"_id": False},
            sort=[("created_at", -1)],
        )
        return dict(row) if row and not _expired(row) else None
    rows = load_json_doc(_challenges_path(), {"otp_challenges": []}).get("otp_challenges", [])
    for row in sorted(rows, key=lambda r: str(r.get("created_at") or ""), reverse=True):
        if (
            isinstance(row, dict)
            and row.get("subject_id") == subject_id
            and row.get("purpose") == purpose
            and not row.get("consumed_at")
            and not _expired(row)
        ):
            return row
    return None


def pending_count() -> int:
    """Live (unexpired) pending signups — admin metrics.

    ``expires_at`` is a consistent ISO-8601 string, so a ``$gt`` string
    compare matches ``_expired`` and stays O(index) instead of scanning."""
    db = _database()
    if db is not None:
        return db[PENDING_COLLECTION].count_documents(
            {"expires_at": {"$gt": _now().isoformat()}})
    rows = load_json_doc(_pending_path(), {"pending_signups": []}).get(
        "pending_signups", [])
    return sum(1 for r in rows if isinstance(r, dict) and not _expired(r))


def active_challenge_count() -> int:
    """Live (unexpired, unconsumed) OTP challenges — admin metrics."""
    db = _database()
    if db is not None:
        return db[CHALLENGES_COLLECTION].count_documents(
            {"expires_at": {"$gt": _now().isoformat()},
             "consumed_at": None})
    rows = load_json_doc(_challenges_path(), {"otp_challenges": []}).get(
        "otp_challenges", [])
    return sum(1 for r in rows
               if isinstance(r, dict) and not _expired(r)
               and not r.get("consumed_at"))
