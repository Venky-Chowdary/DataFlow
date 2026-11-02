"""TOTP second factor for password sign-in accounts (QA S7, round 11).

What this module owns:

* enrolment — a server-generated secret, stored only through
  ``services.secret_vault``; it becomes active only after a valid code;
* ten one-time recovery codes, stored as salted SHA-256 hashes and shown once;
* verification — ±1 time step of drift, a code is accepted at most once (the
  last accepted step is remembered per factor), and repeated bad codes lock
  the account's second factor for a while;
* the login challenge — a short-lived, HMAC-signed token minted after the
  password check, exchanged exactly once (with a code) for a session.

Routes live in ``src.routers.mfa_router``; the workspace "require MFA" gate in
``services.mfa_enforcement``. Nothing here logs a secret, a code, or a
recovery code.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from services import mfa_store, totp
from services.brand import PRODUCT_NAME
from services.brand_env import getenv_brand

RECOVERY_CODE_COUNT = 10
_RECOVERY_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz"  # no 0/o, 1/l/i
_RECOVERY_GROUPS = 4
_RECOVERY_GROUP_LEN = 4  # 16 symbols of 31 ≈ 79 bits each
_SECRET_LABEL = "mfa-totp"
_CHALLENGE_PURPOSE = "mfa-login"


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        return max(minimum, int(getenv_brand(name, str(default)) or default))
    except ValueError:
        return default


def max_failures() -> int:
    """Bad codes tolerated inside :func:`failure_window_sec` before lockout."""
    return _env_int("MFA_MAX_FAILURES", 5)


def lockout_sec() -> int:
    return _env_int("MFA_LOCKOUT_SEC", 900)


def failure_window_sec() -> int:
    return _env_int("MFA_FAILURE_WINDOW_SEC", 900)


def challenge_ttl_sec() -> int:
    return _env_int("MFA_CHALLENGE_TTL_SEC", 300, minimum=30)


def pending_ttl_sec() -> int:
    """How long an unconfirmed enrolment secret stays usable."""
    return _env_int("MFA_PENDING_TTL_SEC", 900, minimum=60)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── secrets at rest ─────────────────────────────────────────────────────────


def _seal(secret_b32: str) -> str:
    from services.secret_vault import encrypt_secret

    return encrypt_secret(secret_b32, label=_SECRET_LABEL)


def _open(sealed: str) -> bytes:
    from services.secret_vault import decrypt_secret

    return totp.secret_bytes(decrypt_secret(sealed))


# ── recovery codes ──────────────────────────────────────────────────────────


def normalize_recovery_code(code: str) -> str:
    return "".join(ch for ch in str(code or "").lower() if ch.isalnum())


def _recovery_hash(salt_hex: str, code: str) -> str:
    return hashlib.sha256(bytes.fromhex(salt_hex) + normalize_recovery_code(code).encode("utf-8")).hexdigest()


def _new_recovery_codes() -> tuple[list[str], list[dict[str, Any]]]:
    plain: list[str] = []
    stored: list[dict[str, Any]] = []
    for _ in range(RECOVERY_CODE_COUNT):
        groups = [
            "".join(secrets.choice(_RECOVERY_ALPHABET) for _ in range(_RECOVERY_GROUP_LEN))
            for _ in range(_RECOVERY_GROUPS)
        ]
        code = "-".join(groups)
        salt = secrets.token_hex(16)
        plain.append(code)
        stored.append({"salt": salt, "hash": _recovery_hash(salt, code), "used_at": None})
    return plain, stored


def _match_recovery(entries: list[dict[str, Any]], code: str) -> int | None:
    """Index of the unused entry matching ``code`` (every entry compared)."""
    found: int | None = None
    supplied = normalize_recovery_code(code)
    for index, entry in enumerate(entries):
        try:
            candidate = _recovery_hash(str(entry.get("salt") or ""), supplied)
        except ValueError:
            continue
        same = hmac.compare_digest(candidate, str(entry.get("hash") or ""))
        if same and not entry.get("used_at") and found is None:
            found = index
    return found if supplied else None


def recovery_remaining(doc: dict[str, Any] | None) -> int:
    return sum(1 for e in (doc or {}).get("recovery") or [] if not e.get("used_at"))


# ── status ──────────────────────────────────────────────────────────────────


def is_enrolled(email: str) -> bool:
    doc = mfa_store.get(email)
    return bool(doc and doc.get("active"))


def status(email: str) -> dict[str, Any]:
    doc = mfa_store.get(email) or {}
    active = doc.get("active") or {}
    now = int(time.time())
    locked_until = int(doc.get("locked_until") or 0)
    return {
        "enrolled": bool(active),
        "method": "totp" if active else None,
        "enrolled_at": active.get("enrolled_at") if active else None,
        "pending_enrollment": bool(doc.get("pending")) and not _pending_expired(doc.get("pending"), now),
        "recovery_codes_remaining": recovery_remaining(doc) if active else 0,
        "locked": locked_until > now,
        "locked_until": locked_until if locked_until > now else None,
    }


def _pending_expired(pending: dict[str, Any] | None, now: int) -> bool:
    if not pending:
        return True
    return now - int(pending.get("created_at") or 0) > pending_ttl_sec()


# ── enrolment ───────────────────────────────────────────────────────────────


def begin_enrollment(email: str) -> dict[str, str]:
    """Generate a fresh secret (pending until confirmed). An active factor stays
    active until the new one is confirmed, so re-enrolment never leaves the
    account without a factor halfway through."""
    secret = totp.generate_secret()
    sealed = _seal(secret)
    factor_id = uuid.uuid4().hex

    def change(doc: dict[str, Any]):
        doc["pending"] = {"factor_id": factor_id, "secret_enc": sealed, "created_at": int(time.time())}
        return doc, None

    mfa_store.mutate(email, change)
    account = (email or "").strip().lower()
    return {
        "secret": secret,
        "otpauth_uri": totp.provisioning_uri(secret, account=account, issuer=PRODUCT_NAME),
        "issuer": PRODUCT_NAME,
        "account": account,
        "digits": str(totp.DIGITS),
        "period": str(totp.STEP_SECONDS),
        "algorithm": "SHA1",
    }


@dataclass
class CodeResult:
    ok: bool
    reason: str = ""  # bad_code | replayed | locked | not_enrolled | no_pending | challenge_used | challenge_stale
    method: str = ""  # totp | recovery
    newly_locked: bool = False
    retry_after: int = 0
    recovery_remaining: int = 0
    recovery_codes: list[str] | None = None
    reauth: bool = False


def _locked(doc: dict[str, Any], now: int) -> CodeResult | None:
    until = int(doc.get("locked_until") or 0)
    if until > now:
        return CodeResult(ok=False, reason="locked", retry_after=until - now)
    return None


def _fail(doc: dict[str, Any], now: int, reason: str) -> tuple[dict[str, Any], CodeResult]:
    since = int(doc.get("failures_since") or 0)
    if now - since > failure_window_sec():
        doc["failures"], doc["failures_since"] = 0, now
    doc["failures"] = int(doc.get("failures") or 0) + 1
    if doc["failures"] >= max_failures():
        doc["locked_until"] = now + lockout_sec()
        doc["failures"], doc["failures_since"] = 0, 0
        return doc, CodeResult(ok=False, reason=reason, newly_locked=True, retry_after=lockout_sec())
    return doc, CodeResult(ok=False, reason=reason)


def _succeed(doc: dict[str, Any]) -> None:
    doc["failures"], doc["failures_since"], doc["locked_until"] = 0, 0, 0


def confirm_enrollment(email: str, code: str) -> CodeResult:
    """Activate the pending secret with a valid code; issue recovery codes."""
    now = int(time.time())

    def change(doc: dict[str, Any]):
        locked = _locked(doc, now)
        if locked:
            return None, locked
        pending = doc.get("pending")
        if _pending_expired(pending, now):
            return None, CodeResult(ok=False, reason="no_pending")
        step = totp.matching_step(_open(pending["secret_enc"]), code, at=now)
        if step is None:
            return _fail(doc, now, "bad_code")
        plain, stored = _new_recovery_codes()
        doc["active"] = {
            "factor_id": pending["factor_id"],
            "secret_enc": pending["secret_enc"],
            "enrolled_at": _now_iso(),
            # The code that confirmed enrolment may not be replayed at sign-in.
            "last_step": step,
        }
        doc["pending"] = None
        doc["recovery"] = stored
        doc["challenges_used"] = {}
        _succeed(doc)
        return doc, CodeResult(ok=True, method="totp", recovery_codes=plain,
                               recovery_remaining=len(plain))

    return mfa_store.mutate(email, change)


def verify(
    email: str,
    *,
    code: str = "",
    recovery_code: str = "",
    challenge: dict[str, Any] | None = None,
) -> CodeResult:
    """Check a TOTP or recovery code for the account's active factor.

    With ``challenge`` (an opened login challenge) the challenge's nonce is
    consumed in the same atomic write, so a challenge buys one session.
    """
    now = int(time.time())

    def change(doc: dict[str, Any]):
        active = doc.get("active")
        if not active:
            return None, CodeResult(ok=False, reason="not_enrolled")
        locked = _locked(doc, now)
        if locked:
            return None, locked
        used = {n: x for n, x in (doc.get("challenges_used") or {}).items() if int(x) > now}
        if challenge is not None:
            if challenge.get("f") != active.get("factor_id"):
                return None, CodeResult(ok=False, reason="challenge_stale")
            if challenge.get("n") in used:
                return None, CodeResult(ok=False, reason="challenge_used")
        if recovery_code and not code:
            index = _match_recovery(doc.get("recovery") or [], recovery_code)
            if index is None:
                return _fail(doc, now, "bad_code")
            doc["recovery"][index]["used_at"] = _now_iso()
            method = "recovery"
        else:
            step = totp.matching_step(_open(active["secret_enc"]), code, at=now)
            if step is None:
                return _fail(doc, now, "bad_code")
            last = active.get("last_step")
            if last is not None and step <= int(last):
                return _fail(doc, now, "replayed")
            doc["active"]["last_step"] = step
            method = "totp"
        if challenge is not None:
            used[str(challenge.get("n"))] = int(challenge.get("x") or now)
        doc["challenges_used"] = used
        _succeed(doc)
        return doc, CodeResult(ok=True, method=method, recovery_remaining=recovery_remaining(doc))

    return mfa_store.mutate(email, change)


def regenerate_recovery_codes(email: str) -> list[str]:
    plain, stored = _new_recovery_codes()

    def change(doc: dict[str, Any]):
        if not doc.get("active"):
            return None, None
        doc["recovery"] = stored
        return doc, plain

    result = mfa_store.mutate(email, change)
    return list(result or [])


def remove_factor(email: str, *, clear_lockout: bool = False) -> bool:
    """Disable (self) or reset (platform admin): factor, pending secret and
    recovery codes go; outstanding login challenges die with the factor id."""

    def change(doc: dict[str, Any]):
        had = bool(doc.get("active") or doc.get("pending"))
        doc["active"], doc["pending"], doc["recovery"] = None, None, []
        doc["challenges_used"] = {}
        if clear_lockout:
            _succeed(doc)
        return doc, had

    return bool(mfa_store.mutate(email, change))


# ── login challenge ─────────────────────────────────────────────────────────


def _challenge_key() -> bytes:
    from src.services.auth_service import _token_secret

    # Domain-separated from the session-token MAC so neither can stand in for the other.
    return hmac.new(_token_secret().encode("utf-8"), b"datawrap.mfa.challenge.v1", hashlib.sha256).digest()


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def mint_challenge(email: str) -> tuple[str, int] | None:
    """A signed, short-lived challenge for an account with an active factor."""
    doc = mfa_store.get(email)
    active = (doc or {}).get("active")
    if not active:
        return None
    expires = int(time.time()) + challenge_ttl_sec()
    payload = {
        "p": _CHALLENGE_PURPOSE,
        "e": (email or "").strip().lower(),
        "f": active["factor_id"],
        "n": secrets.token_hex(16),
        "x": expires,
    }
    body = _b64(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    sig = _b64(hmac.new(_challenge_key(), body.encode("ascii"), hashlib.sha256).digest())
    return f"{body}.{sig}", expires


def open_challenge(token: str) -> dict[str, Any] | None:
    """The payload of a well-signed, unexpired challenge; ``None`` otherwise."""
    if not token or token.count(".") != 1 or len(token) > 2048:
        return None
    body, sig = token.split(".", 1)
    expected = _b64(hmac.new(_challenge_key(), body.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(expected, sig):
        return None
    try:
        payload = json.loads(_unb64(body))
    except (ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("p") != _CHALLENGE_PURPOSE:
        return None
    try:
        expires = int(payload.get("x") or 0)
    except (TypeError, ValueError):
        return None
    if expires <= int(time.time()) or not payload.get("e"):
        return None
    return payload


__all__ = [
    "CodeResult",
    "RECOVERY_CODE_COUNT",
    "begin_enrollment",
    "confirm_enrollment",
    "is_enrolled",
    "mint_challenge",
    "open_challenge",
    "regenerate_recovery_codes",
    "remove_factor",
    "status",
    "verify",
]
