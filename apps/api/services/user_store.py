"""User accounts an operator can actually create.

Before this store, logins existed only in ``DATAFLOW_ADMIN_EMAIL`` /
``DATAFLOW_AUTH_USERS``: a client admin could add an email to a workspace but
that person could never sign in, because creating an account meant editing
environment variables and restarting the API. Accounts now live in the metadata
database — one document per account, keyed by normalized email — and login reads
them alongside the environment-provisioned bootstrap admin.

Platform roles here answer "may this person administer the deployment": ``admin``
or ``member``. What a person may do inside one workspace is a workspace role and
belongs to ``services.team_store``.
"""

from __future__ import annotations

import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo.errors import DuplicateKeyError

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.password_hash import hash_password
from services.platform_config import data_dir

COLLECTION = "platform_users"

_EMPTY_FILE: dict[str, Any] = {"users": []}

PLATFORM_ROLES = ("admin", "member")
USER_STATUSES = ("active", "disabled")

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]{2,}$")
_MIN_PASSWORD_LEN = 12

_PUBLIC_FIELDS = (
    "email",
    "name",
    "role",
    "status",
    "created_at",
    "created_by",
    "updated_at",
    "last_login_at",
    "must_change_password",
)


class UserStoreError(ValueError):
    """A caller asked for something the account store must refuse."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    return data_dir() / "users.json"


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _validate_email(email: str) -> str:
    normalized = normalize_email(email)
    if not _EMAIL_RE.match(normalized) or len(normalized) > 254:
        raise UserStoreError(f"{email!r} is not a valid email address")
    return normalized


def _validate_role(role: str) -> str:
    value = (role or "").strip().lower()
    if value not in PLATFORM_ROLES:
        raise UserStoreError(f"role must be one of {', '.join(PLATFORM_ROLES)}")
    return value


def _validate_status(status: str) -> str:
    value = (status or "").strip().lower()
    if value not in USER_STATUSES:
        raise UserStoreError(f"status must be one of {', '.join(USER_STATUSES)}")
    return value


def _validate_password(password: str) -> str:
    if len(password or "") < _MIN_PASSWORD_LEN:
        raise UserStoreError(f"password must be at least {_MIN_PASSWORD_LEN} characters")
    return password


def generate_password() -> str:
    """A one-time password strong enough to hand to a new account holder."""
    return secrets.token_urlsafe(15)


def public_user(doc: dict[str, Any]) -> dict[str, Any]:
    """Strip the hash: nothing outside this module may see stored credentials."""
    return {field: doc.get(field) for field in _PUBLIC_FIELDS}


def _read_all() -> list[dict[str, Any]]:
    db = mongo_database()
    if db is not None:
        return [dict(doc) for doc in db[COLLECTION].find({}, {"_id": False})]
    raw = load_json_doc(_store_path(), _EMPTY_FILE)
    return [dict(u) for u in raw.get("users", []) if isinstance(u, dict)]


def _write_one(doc: dict[str, Any]) -> None:
    db = mongo_database()
    if db is not None:
        db[COLLECTION].replace_one({"_id": doc["email"]}, {"_id": doc["email"], **doc}, upsert=True)
        return
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        users = [
            u
            for u in data.get("users", [])
            if isinstance(u, dict) and normalize_email(u.get("email", "")) != doc["email"]
        ]
        data["users"] = [*users, doc]


def _insert_one(doc: dict[str, Any]) -> None:
    """Create an account, refusing a duplicate at the storage layer.

    Checking "does this email exist?" before writing would let two concurrent
    admins both pass the check and the second write clobber the first account's
    password. The unique ``_id`` (and the file lock) decide instead.
    """
    db = mongo_database()
    if db is not None:
        try:
            db[COLLECTION].insert_one({"_id": doc["email"], **doc})
        except DuplicateKeyError as e:
            raise UserStoreError(f"{doc['email']} already has an account") from e
        return
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        users = [u for u in data.get("users", []) if isinstance(u, dict)]
        if any(normalize_email(u.get("email", "")) == doc["email"] for u in users):
            raise UserStoreError(f"{doc['email']} already has an account")
        data["users"] = [*users, doc]


def _delete_one(email: str) -> bool:
    db = mongo_database()
    if db is not None:
        return db[COLLECTION].delete_one({"_id": email}).deleted_count > 0
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        users = [u for u in data.get("users", []) if isinstance(u, dict)]
        kept = [u for u in users if normalize_email(u.get("email", "")) != email]
        data["users"] = kept
        removed = len(kept) != len(users)
    return removed


def _read_one(email: str) -> dict[str, Any] | None:
    db = mongo_database()
    if db is not None:
        doc = db[COLLECTION].find_one({"_id": email}, {"_id": False})
        return dict(doc) if doc else None
    for user in _read_all():
        if normalize_email(user.get("email", "")) == email:
            return user
    return None


def list_users() -> list[dict[str, Any]]:
    """Every stored account, newest last, without credentials."""
    users = [public_user(u) for u in _read_all()]
    return sorted(users, key=lambda u: (str(u.get("created_at") or ""), str(u.get("email") or "")))


def get_user(email: str) -> dict[str, Any] | None:
    doc = _read_one(normalize_email(email))
    return public_user(doc) if doc else None


def credentials_for_auth() -> list[dict[str, Any]]:
    """Login records for active accounts — ``auth_service`` is the only caller.

    Disabled accounts are withheld rather than deleted so an operator can revoke
    access without losing who did what in the audit trail.
    """
    return [
        {
            "email": doc.get("email", ""),
            "name": doc.get("name") or doc.get("email", ""),
            "role": doc.get("role", "member"),
            "password_hash": doc.get("password_hash", ""),
        }
        for doc in _read_all()
        if doc.get("status", "active") == "active" and doc.get("password_hash")
    ]


def create_user(
    *,
    email: str,
    name: str = "",
    role: str = "member",
    password: str | None = None,
    created_by: str = "",
) -> tuple[dict[str, Any], str | None]:
    """Create an account. Returns the public record and a one-time password.

    When the caller supplies no password we mint one and return it exactly once —
    there is no mail transport in a self-hosted deployment, so the admin has to
    be able to read it off the screen and hand it over.
    """
    normalized = _validate_email(email)
    role_value = _validate_role(role)
    issued: str | None = None
    if password is None:
        issued = generate_password()
        secret = issued
    else:
        secret = _validate_password(password)
    doc = {
        "email": normalized,
        "name": (name or "").strip()[:128] or normalized.split("@")[0],
        "role": role_value,
        "status": "active",
        "password_hash": hash_password(secret),
        "must_change_password": issued is not None,
        "created_at": _now(),
        "created_by": created_by,
        "updated_at": _now(),
        "last_login_at": None,
    }
    _insert_one(doc)
    return public_user(doc), issued


def update_user(
    *,
    email: str,
    name: str | None = None,
    role: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    if name is not None:
        doc["name"] = name.strip()[:128] or doc.get("name") or normalized
    if role is not None:
        doc["role"] = _validate_role(role)
    if status is not None:
        doc["status"] = _validate_status(status)
    doc["updated_at"] = _now()
    _write_one(doc)
    return public_user(doc)


def set_password(*, email: str, password: str) -> dict[str, Any]:
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    doc["password_hash"] = hash_password(_validate_password(password))
    doc["must_change_password"] = False
    doc["updated_at"] = _now()
    _write_one(doc)
    return public_user(doc)


def reset_password(*, email: str) -> tuple[dict[str, Any], str]:
    """Issue a new one-time password and require a change at next sign-in."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    issued = generate_password()
    doc["password_hash"] = hash_password(issued)
    doc["must_change_password"] = True
    doc["updated_at"] = _now()
    _write_one(doc)
    return public_user(doc), issued


def delete_user(*, email: str) -> bool:
    return _delete_one(normalize_email(email))


def record_login(email: str) -> None:
    """Stamp a successful sign-in; silent when the account is env-provisioned."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        return
    doc["last_login_at"] = _now()
    _write_one(doc)
