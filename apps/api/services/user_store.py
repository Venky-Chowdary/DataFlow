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

import logging
import re
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pymongo.errors import DuplicateKeyError

from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.password_hash import hash_password
from services.platform_config import data_dir
from services.email_canonical import canonical_email

logger = logging.getLogger(__name__)

COLLECTION = "platform_users"

_EMPTY_FILE: dict[str, Any] = {"users": []}

PLATFORM_ROLES = ("admin", "member")
# ``banned`` = abuse hold from the signup/admin surface: cannot authenticate,
# cannot re-verify, sessions revoked (banned sign-ins stay neutral).
USER_STATUSES = ("active", "disabled", "banned")

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
    "email_verified_at",
)

# Password hash algorithms this store may carry. ``bcrypt`` rows are
# transparently rehashed to argon2id on the next successful login (P0-4).
PASSWORD_ALGOS = ("bcrypt", "argon2id")


class UserStoreError(ValueError):
    """A caller asked for something the account store must refuse."""


class EmailTakenError(UserStoreError):
    """Raised when an address is already claimed by another account."""


class IndexUnavailableError(UserStoreError):
    """The canonical-claim index could not be ensured — fail closed."""


class SameAddressError(UserStoreError):
    """The new address canonicalizes to the account's own current one."""


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


def validate_email(email: str) -> str:
    """The normalized address, or the reason it is not one.

    Exported because membership is keyed by email as well: an invitation has to
    refuse a malformed address before it writes a member nobody can ever sign in
    as, and there is one spelling of "valid email" in the product.
    """
    return _validate_email(email)


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
    password. The unique ``_id`` (and the file lock) decide instead — and the
    canonical claim is held by the unique index (Mongo, ensured here so a
    fresh database can't run two variant-canonical creates without it) or by
    the same-claim check inside the file lock.
    """
    db = _canonical_indexed_db()
    if db is not None:
        try:
            db[COLLECTION].insert_one({"_id": doc["email"], **doc})
        except DuplicateKeyError as e:
            raise EmailTakenError(f"{doc['email']} already has an account") from e
        return
    canonical = doc.get("canonical_email")
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        users = [u for u in data.get("users", []) if isinstance(u, dict)]
        if any(normalize_email(u.get("email", "")) == doc["email"]
               or (canonical
                   and (u.get("canonical_email")
                        or canonical_email(u.get("email", ""))) == canonical)
               for u in users):
            raise EmailTakenError(f"{doc['email']} already has an account")
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
    ensure_backfilled()
    users = [public_user(u) for u in _read_all()]
    return sorted(users, key=lambda u: (str(u.get("created_at") or ""), str(u.get("email") or "")))


def get_user(email: str) -> dict[str, Any] | None:
    doc = _read_one(normalize_email(email))
    return public_user(doc) if doc else None


def get_user_record(email: str) -> dict[str, Any] | None:
    """The raw stored doc — internal fields (rekey_pending, rekey_lease,
    canonical_email) included. For store-adjacent consumers like the auth
    middleware; never return it over HTTP."""
    return _read_one(normalize_email(email))


def credentials_for_auth() -> list[dict[str, Any]]:
    """Login records for active accounts — ``auth_service`` is the only caller.

    Disabled accounts are withheld rather than deleted so an operator can revoke
    access without losing who did what in the audit trail.
    """
    ensure_backfilled()
    return [
        {
            "email": doc.get("email", ""),
            "name": doc.get("name") or doc.get("email", ""),
            "role": doc.get("role", "member"),
            "password_hash": doc.get("password_hash", ""),
            "password_algo": doc.get("password_algo", "bcrypt"),
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
    email_verified: bool = True,
    signup_ip: str = "",
    signup_risk: float = 0.0,
) -> tuple[dict[str, Any], str | None]:
    """Create an account. Returns the public record and a one-time password.

    When the caller supplies no password we mint one and return it exactly once —
    there is no mail transport in a self-hosted deployment, so the admin has to
    be able to read it off the screen and hand it over.

    Admin-created accounts are trusted: ``email_verified`` defaults True so a
    Team-settings invite still signs in without an OTP round-trip. The signup
    flow passes an already-hashed secret through :func:`create_signup_user`.
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
        "password_algo": "bcrypt",
        "canonical_email": normalized,
        "email_verified_at": _now() if email_verified else None,
        "signup_ip": signup_ip,
        "signup_risk": float(signup_risk),
        "must_change_password": issued is not None,
        "created_at": _now(),
        "created_by": created_by,
        "updated_at": _now(),
        "last_login_at": None,
    }
    _insert_one(doc)
    return public_user(doc), issued


def create_signup_user(
    *,
    email: str,
    name: str,
    password_hash: str,
    password_algo: str,
    canonical: str,
    signup_ip: str = "",
    signup_risk: float = 0.0,
) -> dict[str, Any]:
    """Insert the verified account after a successful OTP challenge.

    The caller owns the hash (argon2id from the pending row) — this path never
    re-hashes. Verified at insert: a user row only exists after the code proved
    the inbox.
    """
    normalized = _validate_email(email)
    if password_algo not in PASSWORD_ALGOS:
        raise UserStoreError(f"password_algo must be one of {', '.join(PASSWORD_ALGOS)}")
    doc = {
        "email": normalized,
        "name": (name or "").strip()[:128] or normalized.split("@")[0],
        "role": "member",
        "status": "active",
        "password_hash": password_hash,
        "password_algo": password_algo,
        "canonical_email": canonical or normalized,
        "email_verified_at": _now(),
        "signup_ip": signup_ip,
        "signup_risk": float(signup_risk),
        "must_change_password": False,
        "created_at": _now(),
        "created_by": "signup",
        "updated_at": _now(),
        "last_login_at": None,
    }
    _insert_one(doc)
    return public_user(doc)


def find_by_canonical(canonical: str) -> dict[str, Any] | None:
    """Auth-credential row for a canonical email (enumeration-safe lookup)."""
    for doc in _read_all():
        if doc.get("canonical_email") == canonical:
            return doc
    return None


def mark_email_verified(email: str) -> dict[str, Any]:
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    if not doc.get("email_verified_at"):
        doc["email_verified_at"] = _now()
        doc["updated_at"] = _now()
        _write_one(doc)
    return public_user(doc)


def update_password_hash(*, email: str, password_hash: str, password_algo: str) -> None:
    """Swap the stored hash — used by the transparent bcrypt→argon2id rehash."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        return
    doc["password_hash"] = password_hash
    doc["password_algo"] = password_algo
    doc["updated_at"] = _now()
    _write_one(doc)


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


_CANONICAL_INDEXED: set[str] = set()


def _canonical_indexed_db():
    """Mongo with the sparse unique canonical_email index ensured (once).

    Sparse because rows written before the backfill lack the field; unique so
    the canonical-claim below is decided by the database, not by a
    check-then-write race. Fails CLOSED: when the ensure fails the guarantee
    is not there (e.g. duplicate legacy canonicals blocking creation), so we
    raise rather than pretend the claim is atomic."""
    db = mongo_database()
    if db is not None and db.name not in _CANONICAL_INDEXED:
        try:
            db[COLLECTION].create_index(
                [("canonical_email", 1)], unique=True, sparse=True,
                name="canonical_email_unique")
        except Exception as exc:
            logger.error("canonical_email index ensure failed", exc_info=True)
            raise IndexUnavailableError(
                "canonical_email unique index unavailable") from exc
        _CANONICAL_INDEXED.add(db.name)
    return db


def change_email(*, email: str, new_email: str,
                 canonical: str | None = None) -> dict[str, Any]:
    """Rekey an account to a new address (email-change flow after the new
    address is verified). The account's storage key IS the email, so this
    deletes the old row and inserts the updated one — callers move
    memberships, orgs and sessions in the same step. The canonical claim is
    atomic: the unique index (Mongo) or the file lock decides it, so two
    racing rekeys cannot both land the same canonical address."""
    normalized = normalize_email(email)
    new_normalized = normalize_email(validate_email(new_email))
    canonical = canonical or new_normalized
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    if _read_one(new_normalized) is not None:
        raise EmailTakenError(f"{new_normalized} already has an account")
    if canonical == doc.get("canonical_email"):
        # A same-canonical "change" (case-only, or a variant folding to the
        # account's own key) has nothing to claim — refuse it up front.
        raise SameAddressError(
            f"{new_normalized} canonicalizes to the current address")
    doc["email"] = new_normalized
    doc["canonical_email"] = canonical
    doc["updated_at"] = _now()
    db = _canonical_indexed_db()
    if db is not None:
        # Insert BEFORE delete: a lost canonical race must leave the
        # original account intact, not deleted.
        try:
            db[COLLECTION].insert_one({"_id": new_normalized, **doc})
        except DuplicateKeyError as e:
            raise EmailTakenError(
                f"{canonical} already belongs to an account") from e
        try:
            db[COLLECTION].delete_one({"_id": normalized})
        except Exception:
            # Compensation: without it the account exists under both keys.
            # Remove the just-inserted row so the original stays intact.
            try:
                db[COLLECTION].delete_one({"_id": new_normalized})
            except Exception:
                logger.error(
                    "email rekey compensation failed", exc_info=True)
            raise
    else:
        with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
            users = [
                u for u in data.get("users", [])
                if isinstance(u, dict)
                and normalize_email(u.get("email", "")) != normalized
            ]
            # Legacy rows predate the field — derive it instead of skipping.
            if any((u.get("canonical_email")
                    or canonical_email(u.get("email", ""))) == canonical
                   for u in users):
                raise EmailTakenError(
                    f"{canonical} already belongs to an account")
            data["users"] = [*users, doc]
    return public_user(doc)


def mark_rekey_pending(*, email: str, old_email: str) -> None:
    """Flag a moved account whose follow-on rekeys still need to run."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is not None:
        doc["rekey_pending"] = normalize_email(old_email)
        doc["updated_at"] = _now()
        _write_one(doc)


def rekey_pending(email: str) -> str | None:
    """The pre-rekey address when follow-on rekeys are outstanding."""
    doc = _read_one(normalize_email(email))
    return doc.get("rekey_pending") if doc else None


REKEY_LEASE_SECONDS = 60


def claim_rekey_pending(email: str, *, lease_seconds: int = REKEY_LEASE_SECONDS) -> str | None:
    """Single-flight claim on the rekey flag.

    Returns the pre-rekey address for exactly one caller; others see None
    until the lease lapses or release_rekey_claim frees it. The flag itself
    stays set until the heal succeeds and clear_rekey_pending removes it,
    so two parallel requests can never double-move memberships.
    """
    normalized = normalize_email(email)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat()
    lease_until = (now_dt + timedelta(seconds=lease_seconds)).isoformat()
    db = mongo_database()
    if db is not None:
        from pymongo import ReturnDocument
        row = db[COLLECTION].find_one_and_update(
            {"_id": normalized,
             "rekey_pending": {"$exists": True, "$nin": [None, ""]},
             "$or": [{"rekey_lease": {"$exists": False}},
                     {"rekey_lease": None},
                     {"rekey_lease": {"$lte": now}}]},
            {"$set": {"rekey_lease": lease_until,
                      "updated_at": now}},
            return_document=ReturnDocument.AFTER,
            projection={"_id": False},
        )
        return (row or {}).get("rekey_pending") or None
    claimed: list[str] = []
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        for u in data.get("users", []):
            if not (isinstance(u, dict)
                    and normalize_email(u.get("email", "")) == normalized):
                continue
            old = u.get("rekey_pending")
            if not old:
                break
            lease = u.get("rekey_lease")
            if lease and str(lease) > now:
                break
            u["rekey_lease"] = lease_until
            u["updated_at"] = now
            claimed.append(old)
            break
    return claimed[0] if claimed else None


def release_rekey_claim(email: str) -> None:
    """Free the claim lease after a failed heal so the next request retries."""
    normalized = normalize_email(email)
    db = mongo_database()
    if db is not None:
        db[COLLECTION].update_one(
            {"_id": normalized}, {"$unset": {"rekey_lease": ""}})
        return
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        for u in data.get("users", []):
            if isinstance(u, dict) and normalize_email(u.get("email", "")) == normalized:
                u.pop("rekey_lease", None)
                break


def clear_rekey_pending(*, email: str) -> None:
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is not None:
        removed = doc.pop("rekey_pending", None) is not None
        doc.pop("rekey_lease", None)
        if removed:
            doc["updated_at"] = _now()
            _write_one(doc)


def mark_onboarded(email: str) -> dict[str, Any]:
    """Stamp onboarding completion on the account (P4-4)."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    doc["onboarded_at"] = _now()
    doc["updated_at"] = _now()
    _write_one(doc)
    return public_user(doc)


def set_password(*, email: str, password: str, algo: str = "bcrypt") -> dict[str, Any]:
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        raise UserStoreError(f"{normalized} has no account")
    if algo == "argon2id":
        from services.password_hash import hash_password_argon2
        doc["password_hash"] = hash_password_argon2(_validate_password(password))
        doc["password_algo"] = "argon2id"
    else:
        doc["password_hash"] = hash_password(_validate_password(password))
        doc["password_algo"] = "bcrypt"
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


_BACKFILLED = False


_CANONICAL_DUPLICATES: list[str] = []


def canonical_email_duplicates() -> list[str]:
    """Canonical keys claimed by more than one account (backfill report)."""
    return list(_CANONICAL_DUPLICATES)


def backfill_signup_fields() -> int:
    """P1-7 migration: existing accounts predate email verification.

    Every pre-signup account was provisioned by an admin or env var — trusted,
    so backfill marks them verified (``email_verified_at`` = ``created_at``),
    stamps the canonical key, and records ``password_algo=bcrypt`` so the
    transparent rehash knows what it's reading. Idempotent; runs once per
    process from ``_read_all``. Duplicate canonical keys (which would block
    the unique index) are reported via ``canonical_email_duplicates``.
    """
    updated = 0
    seen: dict[str, str] = {}
    _CANONICAL_DUPLICATES.clear()
    for doc in _read_all():
        changed = False
        if not doc.get("email"):
            # No address => nothing meaningful to stamp or dedupe on.
            continue
        if not doc.get("canonical_email"):
            doc["canonical_email"] = canonical_email(doc.get("email") or "")
            changed = True
        claimer = doc.get("canonical_email")
        if claimer:
            if seen.get(claimer, doc.get("email")) != doc.get("email"):
                if claimer not in _CANONICAL_DUPLICATES:
                    _CANONICAL_DUPLICATES.append(claimer)
            seen[claimer] = doc.get("email") or ""
        if not doc.get("email_verified_at"):
            doc["email_verified_at"] = doc.get("created_at") or _now()
            changed = True
        if not doc.get("password_algo") and doc.get("password_hash"):
            doc["password_algo"] = "bcrypt"
            changed = True
        if changed:
            _write_one(doc)
            updated += 1
    if _CANONICAL_DUPLICATES:
        logger.error("backfill: %d duplicate canonical_email key(s)",
                     len(_CANONICAL_DUPLICATES))
    return updated


def ensure_backfilled() -> None:
    """Cheap once-per-process guard called from reads."""
    global _BACKFILLED
    if not _BACKFILLED:
        backfill_signup_fields()
        _BACKFILLED = True


def record_login(email: str) -> None:
    """Stamp a successful sign-in; silent when the account is env-provisioned."""
    normalized = normalize_email(email)
    doc = _read_one(normalized)
    if doc is None:
        return
    doc["last_login_at"] = _now()
    _write_one(doc)
