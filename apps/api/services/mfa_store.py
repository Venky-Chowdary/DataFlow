"""Where second factors live: one document per sign-in account, keyed by email.

Kept apart from ``services.user_store`` on purpose: the environment-provisioned
bootstrap admin has no stored account row, and it must be able to enrol too.

Every change is a read-modify-write guarded by a version number, so two API
workers verifying codes for the same account cannot both accept the same code
or lose a failure count: MongoDB replaces only the version that was read (and
the loser retries); the JSON fallback runs the whole cycle under the file lock.

Document shape (no plaintext secret, no plaintext recovery code)::

    {"email", "v",
     "active":  {"factor_id", "secret_enc", "enrolled_at", "last_step"} | None,
     "pending": {"factor_id", "secret_enc", "created_at"} | None,
     "recovery": [{"salt", "hash", "used_at"}],
     "failures", "failures_since", "locked_until",
     "challenges_used": {nonce: expires_at}}
"""

from __future__ import annotations

import copy
import logging
from pathlib import Path
from typing import Any, Callable, TypeVar

from pymongo.errors import DuplicateKeyError

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

logger = logging.getLogger(__name__)

COLLECTION = "auth_mfa_factors"
_EMPTY_FILE: dict[str, Any] = {"factors": {}}
_MAX_RETRIES = 8

T = TypeVar("T")


class MfaStoreConflict(RuntimeError):
    """Concurrent writers kept winning; the caller should fail closed."""


def _path() -> Path:
    return data_dir() / "mfa_factors.json"


def _key(email: str) -> str:
    return (email or "").strip().lower()


def empty_doc(email: str) -> dict[str, Any]:
    return {
        "email": _key(email),
        "v": 0,
        "active": None,
        "pending": None,
        "recovery": [],
        "failures": 0,
        "failures_since": 0,
        "locked_until": 0,
        "challenges_used": {},
    }


def get(email: str) -> dict[str, Any] | None:
    key = _key(email)
    if not key:
        return None
    db = mongo_database()
    if db is not None:
        doc = db[COLLECTION].find_one({"_id": key}, {"_id": False})
        return dict(doc) if doc else None
    row = load_json_doc(_path(), _EMPTY_FILE).get("factors", {}).get(key)
    return dict(row) if isinstance(row, dict) else None


def emails_with_active_factor(emails: list[str]) -> set[str]:
    """Which of ``emails`` hold a confirmed factor (posture counts)."""
    keys = sorted({_key(e) for e in emails if _key(e)})
    if not keys:
        return set()
    db = mongo_database()
    if db is not None:
        rows = db[COLLECTION].find({"_id": {"$in": keys}, "active": {"$ne": None}}, {"_id": True})
        return {str(r["_id"]) for r in rows}
    factors = load_json_doc(_path(), _EMPTY_FILE).get("factors", {})
    return {k for k in keys if isinstance(factors.get(k), dict) and factors[k].get("active")}


def mutate(email: str, change: Callable[[dict[str, Any]], tuple[dict[str, Any] | None, T]]) -> T:
    """Apply ``change(doc) -> (new_doc | None, result)`` atomically.

    ``new_doc=None`` means "nothing to write". ``doc`` is a fresh copy (an
    empty document when the account has none yet).
    """
    key = _key(email)
    if not key:
        raise ValueError("email required")
    db = mongo_database()
    if db is None:
        with json_doc_transaction(_path(), _EMPTY_FILE) as data:
            factors = data.setdefault("factors", {})
            current = factors.get(key)
            doc = copy.deepcopy(current) if isinstance(current, dict) else empty_doc(key)
            new_doc, result = change(doc)
            if new_doc is not None:
                new_doc["v"] = int(doc.get("v") or 0) + 1
                factors[key] = new_doc
        return result

    coll = db[COLLECTION]
    for _attempt in range(_MAX_RETRIES):
        current = coll.find_one({"_id": key}, {"_id": False})
        doc = copy.deepcopy(current) if current else empty_doc(key)
        version = int(doc.get("v") or 0)
        new_doc, result = change(doc)
        if new_doc is None:
            return result
        new_doc["v"] = version + 1
        if current is None:
            try:
                coll.insert_one({"_id": key, **new_doc})
                return result
            except DuplicateKeyError:
                continue
        replaced = coll.replace_one({"_id": key, "v": version}, {"_id": key, **new_doc})
        if replaced.matched_count == 1:
            return result
    logger.warning("mfa store: optimistic write kept conflicting")
    raise MfaStoreConflict("concurrent second-factor update; try again")


def delete(email: str) -> bool:
    key = _key(email)
    db = mongo_database()
    if db is not None:
        return db[COLLECTION].delete_one({"_id": key}).deleted_count > 0
    with json_doc_transaction(_path(), _EMPTY_FILE) as data:
        return data.setdefault("factors", {}).pop(key, None) is not None


__all__ = [
    "COLLECTION",
    "MfaStoreConflict",
    "delete",
    "emails_with_active_factor",
    "empty_doc",
    "get",
    "mutate",
]
