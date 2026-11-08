"""Workspace invitations — durable records with a hashed, single-use token.

One record per (workspace, invitee). Only ``HMAC-SHA256(pepper, token)`` is
stored; the raw token leaves the process only inside the invitation mail.
Re-sending rotates the token on the same record (the old link reads as
``superseded``) and pushes the expiry out. Accepting and revoking are
compare-and-set transitions from ``pending``, so a double click or two
replicas can never accept one invitation twice.

At most one *pending* invitation exists per (workspace, canonical email):
a partial unique index on Mongo, the exclusive file lock otherwise.

Statuses: ``pending`` → ``accepted`` | ``revoked``; a pending record past
``expires_at`` reads as ``expired``. Records are kept ``RETAIN_FOR`` after
they stop being pending (audit trail and the "this link was revoked"
answer), then the Mongo TTL index / file prune drops them.

Mongo (``workspace_invites``) when configured; ``workspace_invites.json``
under data_dir() otherwise — the same durable-store pair as the reset
tokens and the mail outbox.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

COLLECTION = "workspace_invites"
_FILE = "workspace_invites.json"
TOKEN_BYTES = 32  # secrets.token_urlsafe(32) -> 256 bits
RETAIN_FOR = timedelta(days=30)
_SUPERSEDED_KEEP = 5

_PREPARED: set[str] = set()


class InviteAlreadyPending(Exception):
    """A pending invitation for this address already exists in the workspace."""

    def __init__(self, existing: dict[str, Any] | None = None) -> None:
        super().__init__("an invitation for this address is already pending")
        self.existing = existing


def _pepper() -> str:
    from services import otp_service

    return otp_service._pepper()


def hash_token(token: str) -> str:
    return hmac.new(_pepper().encode(), token.encode(), hashlib.sha256).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat()


def effective_status(doc: dict[str, Any], now: datetime | None = None) -> str:
    """``pending`` past its expiry reads as ``expired``; else the stored status."""
    status = str(doc.get("status") or "pending")
    if status == "pending" and str(doc.get("expires_at") or "") <= _iso(now or _now()):
        return "expired"
    return status


def _database():
    db = mongo_database()
    if db is None:
        return None
    if db.name not in _PREPARED:
        col = db[COLLECTION]
        col.create_index("token_hash", unique=True, name="invite_token_unique")
        col.create_index("superseded_hashes", name="invite_superseded")
        col.create_index([("workspace_id", 1), ("status", 1)], name="invite_ws_status")
        col.create_index(
            [("workspace_id", 1), ("canonical_email", 1)], unique=True,
            name="invite_one_pending", partialFilterExpression={"status": "pending"})
        col.create_index("purge_at", expireAfterSeconds=0, name="invite_purge_ttl")
        _PREPARED.add(db.name)
    return db


def _path():
    return data_dir() / _FILE


def _clean(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    if doc is None:
        return None
    return {k: v for k, v in doc.items() if k not in ("_id", "purge_at")}


def _purge_at(expires_at: datetime) -> datetime:
    return expires_at + RETAIN_FOR


def _prune(rows: dict[str, Any], now: datetime) -> None:
    cutoff = _iso(now - RETAIN_FOR)
    dead = [k for k, r in rows.items()
            if str(r.get("finished_at") or r.get("expires_at") or "") < cutoff]
    for k in dead:
        rows.pop(k, None)


def _expire_stale_pending(db, workspace_id: str, canonical: str, now: datetime) -> None:
    """A lapsed pending record must not hold the one-pending slot."""
    flt = {"workspace_id": workspace_id, "canonical_email": canonical,
           "status": "pending", "expires_at": {"$lte": _iso(now)}}
    db[COLLECTION].update_many(flt, {"$set": {"status": "expired",
                                             "finished_at": _iso(now)}})


# ── public API ────────────────────────────────────────────────────────────


def create(*, workspace_id: str, email: str, canonical: str, role: str,
           name: str, invited_by: str, invited_by_name: str,
           ttl: timedelta) -> tuple[str, dict[str, Any]]:
    """Mint a pending invitation. Returns ``(raw_token, record)``.

    Raises :class:`InviteAlreadyPending` when one is already pending for this
    address in this workspace (resend that one instead)."""
    token = secrets.token_urlsafe(TOKEN_BYTES)
    now = _now()
    expires = now + ttl
    doc = {
        "id": f"inv_{uuid.uuid4().hex[:24]}",
        "workspace_id": workspace_id,
        "email": email,
        "canonical_email": canonical,
        "role": role,
        "name": name,
        "invited_by": invited_by,
        "invited_by_name": invited_by_name,
        "status": "pending",
        "token_hash": hash_token(token),
        "superseded_hashes": [],
        "sent_count": 1,
        "created_at": _iso(now),
        "last_sent_at": _iso(now),
        "expires_at": _iso(expires),
    }
    db = _database()
    if db is not None:
        from pymongo.errors import DuplicateKeyError

        _expire_stale_pending(db, workspace_id, canonical, now)
        try:
            db[COLLECTION].insert_one({**doc, "_id": doc["id"], "purge_at": _purge_at(expires)})
        except DuplicateKeyError as exc:
            existing = db[COLLECTION].find_one(
                {"workspace_id": workspace_id, "canonical_email": canonical, "status": "pending"})
            raise InviteAlreadyPending(_clean(existing)) from exc
        return token, doc
    with json_doc_transaction(_path(), {"invites": {}}) as data:
        rows = data.setdefault("invites", {})
        _prune(rows, now)
        for row in rows.values():
            if (row.get("workspace_id") == workspace_id
                    and row.get("canonical_email") == canonical
                    and row.get("status") == "pending"):
                if effective_status(row, now) == "expired":
                    row["status"] = "expired"
                    row["finished_at"] = _iso(now)
                    continue
                raise InviteAlreadyPending(dict(row))
        rows[doc["id"]] = doc
    return token, dict(doc)


def get(invite_id: str) -> dict[str, Any] | None:
    db = _database()
    if db is not None:
        return _clean(db[COLLECTION].find_one({"_id": invite_id}))
    row = load_json_doc(_path(), {"invites": {}}).get("invites", {}).get(invite_id)
    return dict(row) if row else None


def find_by_token(token: str) -> tuple[dict[str, Any] | None, bool]:
    """``(record, superseded)`` for a raw token. ``superseded`` is True when
    the token is an older link of a record that was re-sent since."""
    if not token:
        return None, False
    digest = hash_token(token)
    db = _database()
    if db is not None:
        row = db[COLLECTION].find_one({"token_hash": digest})
        if row is not None:
            return _clean(row), False
        row = db[COLLECTION].find_one({"superseded_hashes": digest})
        return (_clean(row), True) if row is not None else (None, False)
    rows = load_json_doc(_path(), {"invites": {}}).get("invites", {})
    for row in rows.values():
        if hmac.compare_digest(str(row.get("token_hash") or ""), digest):
            return dict(row), False
    for row in rows.values():
        if any(hmac.compare_digest(str(h), digest) for h in row.get("superseded_hashes") or []):
            return dict(row), True
    return None, False


def list_for_workspace(workspace_id: str) -> list[dict[str, Any]]:
    """Pending (unexpired or expired-but-unswept) records, newest first."""
    db = _database()
    if db is not None:
        rows = [_clean(r) for r in db[COLLECTION].find(
            {"workspace_id": workspace_id, "status": "pending"})]
    else:
        rows = [dict(r) for r in load_json_doc(_path(), {"invites": {}}).get("invites", {}).values()
                if r.get("workspace_id") == workspace_id and r.get("status") == "pending"]
    rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
    return [r for r in rows if r is not None]


def pending_count(workspace_id: str) -> int:
    now = _now()
    return sum(1 for r in list_for_workspace(workspace_id) if effective_status(r, now) == "pending")


def rotate_token(invite_id: str, *, ttl: timedelta) -> tuple[str, dict[str, Any]] | None:
    """Re-send: a new token on the same pending record; the old link reads as
    superseded. Also re-opens an invitation whose link lapsed. None when the
    record is no longer pending (accepted or revoked meanwhile)."""
    token = secrets.token_urlsafe(TOKEN_BYTES)
    now = _now()
    expires = now + ttl
    digest = hash_token(token)
    db = _database()
    if db is not None:
        from pymongo import ReturnDocument

        current = db[COLLECTION].find_one({"_id": invite_id, "status": "pending"})
        if current is None:
            return None
        older = ([current["token_hash"]] + list(current.get("superseded_hashes") or []))[:_SUPERSEDED_KEEP]
        row = db[COLLECTION].find_one_and_update(
            {"_id": invite_id, "status": "pending", "token_hash": current["token_hash"]},
            {"$set": {"token_hash": digest, "superseded_hashes": older,
                      "last_sent_at": _iso(now), "expires_at": _iso(expires),
                      "purge_at": _purge_at(expires)},
             "$inc": {"sent_count": 1}},
            return_document=ReturnDocument.AFTER)
        return (token, _clean(row)) if row is not None else None
    with json_doc_transaction(_path(), {"invites": {}}) as data:
        row = data.setdefault("invites", {}).get(invite_id)
        if row is None or row.get("status") != "pending":
            return None
        row["superseded_hashes"] = ([row["token_hash"]]
                                    + list(row.get("superseded_hashes") or []))[:_SUPERSEDED_KEEP]
        row["token_hash"] = digest
        row["sent_count"] = int(row.get("sent_count") or 1) + 1
        row["last_sent_at"] = _iso(now)
        row["expires_at"] = _iso(expires)
        out = dict(row)
    return token, out


def transition(invite_id: str, *, to_status: str, require_unexpired: bool = False,
               token_hash: str = "", **fields: Any) -> dict[str, Any] | None:
    """Compare-and-set ``pending`` → ``to_status``. Exactly one caller wins;
    the loser (or a record that is no longer pending) gets None.

    ``token_hash`` pins the CAS to the link being used, so a link superseded
    between inspect and accept cannot accept the re-sent invitation."""
    now = _now()
    update = {"status": to_status, "finished_at": _iso(now), **fields}
    db = _database()
    if db is not None:
        from pymongo import ReturnDocument

        flt: dict[str, Any] = {"_id": invite_id, "status": "pending"}
        if require_unexpired:
            flt["expires_at"] = {"$gt": _iso(now)}
        if token_hash:
            flt["token_hash"] = token_hash
        row = db[COLLECTION].find_one_and_update(
            flt, {"$set": {**update, "purge_at": now + RETAIN_FOR}},
            return_document=ReturnDocument.AFTER)
        return _clean(row)
    with json_doc_transaction(_path(), {"invites": {}}) as data:
        row = data.setdefault("invites", {}).get(invite_id)
        if row is None or row.get("status") != "pending":
            return None
        if require_unexpired and effective_status(row, now) != "pending":
            return None
        if token_hash and not hmac.compare_digest(str(row.get("token_hash") or ""), token_hash):
            return None
        row.update(update)
        return dict(row)


def reopen(invite_id: str, *, from_status: str) -> bool:
    """Compensation: put a just-accepted record back to pending when the
    membership write that follows it failed."""
    db = _database()
    if db is not None:
        res = db[COLLECTION].update_one(
            {"_id": invite_id, "status": from_status},
            {"$set": {"status": "pending"},
             "$unset": {"finished_at": "", "accepted_at": "", "accepted_by": ""}})
        return res.modified_count == 1
    with json_doc_transaction(_path(), {"invites": {}}) as data:
        row = data.setdefault("invites", {}).get(invite_id)
        if row is None or row.get("status") != from_status:
            return False
        row["status"] = "pending"
        for key in ("finished_at", "accepted_at", "accepted_by"):
            row.pop(key, None)
        return True


def delete(invite_id: str) -> None:
    """Compensation for an invitation whose mail could not be queued."""
    db = _database()
    if db is not None:
        db[COLLECTION].delete_one({"_id": invite_id})
        return
    with json_doc_transaction(_path(), {"invites": {}}) as data:
        data.setdefault("invites", {}).pop(invite_id, None)
