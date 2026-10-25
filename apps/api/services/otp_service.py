"""OTP service (P1-2): issue, HMAC-hash, verify, burn.

- 6 digits from ``secrets.randbelow``, zero-padded.
- Only ``HMAC-SHA256(pepper, challenge_id|purpose|code)`` is stored — the raw
  code exists in memory and the mail body, nowhere else. Never logged.
- 10-minute expiry (the challenge row carries the TTL).
- 5 wrong attempts burn the challenge: the 6th try fails even with the
  right code (it's already consumed).
- Verification compares with ``hmac.compare_digest``.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
from datetime import timedelta
from typing import Any, Literal

from services import signup_store
from services.brand_env import getenv_brand
from services.platform_config import is_production

logger = logging.getLogger(__name__)

OTP_TTL = timedelta(minutes=10)
CODE_LENGTH = 6
MAX_ATTEMPTS = signup_store.MAX_ATTEMPTS

VerifyResult = Literal["ok", "invalid", "expired", "consumed", "not_found"]

_DEV_PEPPER = "datawrap-dev-otp-pepper"


def _pepper() -> str:
    pepper = (getenv_brand("OTP_PEPPER") or "").strip()
    if pepper:
        return pepper
    # Fail closed: in production a missing pepper must refuse to issue or
    # verify — logging and continuing with a derived key is an oracle.
    if is_production():
        raise RuntimeError(
            "DATAWRAP_OTP_PEPPER is unset in production — refusing to issue or verify")
    return (getenv_brand("AUTH_SECRET") or _DEV_PEPPER).strip()


def generate_code() -> str:
    """6 digits, uniformly drawn, zero-padded."""
    return f"{secrets.randbelow(10**CODE_LENGTH):0{CODE_LENGTH}d}"


def code_hmac(challenge_id: str, purpose: str, code: str) -> str:
    """The only persisted form of a code."""
    msg = f"{challenge_id}|{purpose}|{code}".encode("utf-8")
    return hmac.new(_pepper().encode("utf-8"), msg, hashlib.sha256).hexdigest()


def issue(subject_id: str, purpose: str) -> tuple[dict[str, Any], str]:
    """Create a challenge and return (row, raw_code) — caller mails the code.

    Creating a challenge voids the previous active one for the same
    (subject, purpose) — one live code at a time.
    """
    challenge = signup_store.create_challenge(
        purpose=purpose,
        subject_id=subject_id,
        code_hmac="pending",
        ttl=OTP_TTL,
    )
    code = generate_code()
    signup_store._update_challenge(
        challenge["id"], code_hmac=code_hmac(challenge["id"], purpose, code)
    )
    challenge["code_hmac"] = code_hmac(challenge["id"], purpose, code)
    return challenge, code


def reissue(challenge_id: str) -> tuple[dict[str, Any] | None, str | None]:
    """New code on an existing challenge (resend path): fresh hash, attempts
    reset, expiry extended, sent_count incremented."""
    row = signup_store.get_challenge(challenge_id)
    if row is None:
        return None, None
    code = generate_code()
    updated = signup_store.mark_sent(
        challenge_id,
        code_hmac=code_hmac(challenge_id, row["purpose"], code),
        ttl=OTP_TTL,
    )
    return updated, code


def verify(challenge_id: str, code: str) -> VerifyResult:
    """Constant-time check. A wrong code counts; the cap burns the challenge."""
    result, _row = verify_code(challenge_id, code)
    return result


def verify_code(challenge_id: str, code: str) -> tuple[VerifyResult, dict[str, Any] | None]:
    """verify + the post-increment challenge row on a wrong code, so callers
    report attempts_remaining from the same atomic read that counted the
    attempt — never a second, racy fetch."""
    row = signup_store.get_challenge(challenge_id)
    if row is None:
        return "not_found", None
    if row.get("consumed_at"):
        return "consumed", row
    if int(row.get("attempts") or 0) >= int(row.get("max_attempts") or MAX_ATTEMPTS):
        return "consumed", row  # already burned

    candidate = code_hmac(challenge_id, str(row.get("purpose") or ""), (code or "").strip())
    expected = str(row.get("code_hmac") or "")
    if hmac.compare_digest(candidate, expected):
        signup_store.consume_challenge(challenge_id)
        return "ok", row

    updated = signup_store.record_attempt(challenge_id)
    if updated is None:
        return "consumed", row
    # The submission that burns the challenge still reports invalid_code with
    # attempts_remaining=0; submissions landing past the cap observe the
    # already-burned challenge instead of a misleading fresh wrong-code verdict.
    if int(updated.get("attempts") or 0) > int(updated.get("max_attempts") or MAX_ATTEMPTS):
        return "consumed", updated
    return "invalid", updated
