"""Workspace authentication — server-side only; never expose password hashes to clients."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import time
from typing import Any, Optional

from services.platform_config import is_production

logger = logging.getLogger("dataflow.auth")

_REAUTH_SECRET = os.getenv("DATAFLOW_AUTH_SECRET", "dev-change-me-before-production")
_REQUIRE_AUTH = os.getenv("DATAFLOW_REQUIRE_AUTH", "1" if is_production() else "0").lower() in ("1", "true", "yes")
_TOKEN_TTL_SEC = int(os.getenv("DATAFLOW_TOKEN_TTL_SEC", "86400"))
_ALLOW_DEV_USER = os.getenv("DATAFLOW_ALLOW_DEV_USER", "0").lower() in ("1", "true", "yes")

# bcrypt hash of "password123" for test@gmail.com (dev/staging only, never production)
_DEV_USER = {
    "email": "test@gmail.com",
    "password_hash": "$2b$12$II.e7tCoYPLs2Pv8/dWEVeOMl3GOwsiUnSteHd6Twq3juXLiLsO9e",
    "name": "Test User",
    "role": "Workspace tester",
}

# Legacy unsalted SHA-256 hashes are exactly 64 hex characters.
_LEGACY_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Cache admin bcrypt hash so we do not re-salt on every login request.
_ADMIN_USER_CACHE: dict[str, str] | None = None
_ADMIN_CACHE_KEY: tuple[str, str] | None = None


def _token_secret() -> str:
    """Return the signing secret after validating it is not the dev default in production."""
    if is_production() and _REAUTH_SECRET in ("", "dev-change-me-before-production"):
        raise RuntimeError("DATAFLOW_AUTH_SECRET must be set to a strong random value in production")
    return _REAUTH_SECRET


def auth_required() -> bool:
    return _REQUIRE_AUTH


def _normalize_secret(value: str) -> str:
    """Normalize Railway/shell-set secrets.

    - Strip wrapping quotes (``"..."`` / ``'...'``)
    - Treat ``$$`` as a literal ``$`` (common escape when `$` would expand)
    """
    text = (value or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        text = text[1:-1]
    # Railway/shell often expand `$Ui` away; operators escape as `$$Ui`.
    text = text.replace("$$", "$")
    return text


def _admin_user_from_env() -> dict[str, str] | None:
    global _ADMIN_USER_CACHE, _ADMIN_CACHE_KEY
    admin_email = _normalize_secret(os.getenv("DATAFLOW_ADMIN_EMAIL", ""))
    admin_password = _normalize_secret(os.getenv("DATAFLOW_ADMIN_PASSWORD", ""))
    if not admin_email or not admin_password:
        return None
    key = (admin_email.lower(), admin_password)
    if _ADMIN_USER_CACHE is not None and _ADMIN_CACHE_KEY == key:
        return dict(_ADMIN_USER_CACHE)
    user = {
        "email": admin_email,
        "password_hash": hash_password(admin_password),
        "name": "Admin",
        "role": "admin",
    }
    _ADMIN_USER_CACHE = user
    _ADMIN_CACHE_KEY = key
    return dict(user)


def _users_from_auth_users_env() -> list[dict[str, str]]:
    raw = os.getenv("DATAFLOW_AUTH_USERS", "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("DATAFLOW_AUTH_USERS is set but is not valid JSON — ignoring")
        return []
    if not isinstance(data, list):
        return []
    users: list[dict[str, str]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        email = str(row.get("email") or "").strip()
        if not email:
            continue
        # Prefer bcrypt/legacy hash; allow plaintext "password" for bootstrap (hashed at load).
        password_hash = str(row.get("password_hash") or "").strip()
        plaintext = _normalize_secret(str(row.get("password") or ""))
        if not password_hash and plaintext:
            password_hash = hash_password(plaintext)
        if not password_hash:
            continue
        users.append({
            "email": email,
            "password_hash": password_hash,
            "name": str(row.get("name") or email),
            "role": str(row.get("role") or "member"),
        })
    return users


def _load_users() -> list[dict[str, str]]:
    """Load workspace users.

    Priority:
    1. DATAFLOW_ADMIN_EMAIL + DATAFLOW_ADMIN_PASSWORD (always included when set)
    2. DATAFLOW_AUTH_USERS JSON list (merged; admin email wins on conflict)
    3. Dev user (non-production / DATAFLOW_ALLOW_DEV_USER only)
    """
    users: list[dict[str, str]] = []
    seen: set[str] = set()

    admin = _admin_user_from_env()
    if admin:
        users.append(admin)
        seen.add(admin["email"].strip().lower())

    for user in _users_from_auth_users_env():
        key = user["email"].strip().lower()
        if key in seen:
            continue
        users.append(user)
        seen.add(key)

    if users:
        return users

    if _ALLOW_DEV_USER or not is_production():
        return [_DEV_USER]
    return []


def auth_bootstrap_status() -> dict[str, Any]:
    """Safe diagnostics for operators (no secrets)."""
    admin_email = _normalize_secret(os.getenv("DATAFLOW_ADMIN_EMAIL", ""))
    admin_password = _normalize_secret(os.getenv("DATAFLOW_ADMIN_PASSWORD", ""))
    users = _load_users()
    return {
        "auth_required": auth_required(),
        "user_count": len(users),
        "admin_email_configured": bool(admin_email),
        "admin_password_configured": bool(admin_password),
        "admin_password_length": len(admin_password) if admin_password else 0,
        "auth_users_configured": bool(os.getenv("DATAFLOW_AUTH_USERS", "").strip()),
        "emails": [u.get("email") for u in users],
    }


def hash_password(password: str) -> str:
    """Hash a password with bcrypt (adaptive, salted, slow)."""
    import bcrypt

    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def _legacy_verify(password: str, password_hash: str) -> bool:
    """Verify a legacy unsalted SHA-256 hash."""
    expected = hashlib.sha256(password.encode("utf-8")).hexdigest()
    return hmac.compare_digest(password_hash, expected)


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a bcrypt hash.

    Legacy unsalted SHA-256 is still accepted in development for backwards
    compatibility, but it is rejected in production because it is not suitable
    for regulated deployments.
    """
    if not password_hash:
        return False
    if _LEGACY_SHA256_RE.match(password_hash):
        if is_production():
            return False
        return _legacy_verify(password, password_hash)
    try:
        import bcrypt
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False


def authenticate(email: str, password: str) -> Optional[dict[str, str]]:
    normalized = email.strip().lower()
    users = _load_users()
    if not users:
        logger.error("Login rejected — no auth users configured (set DATAFLOW_ADMIN_EMAIL/PASSWORD)")
        return None
    for user in users:
        if user.get("email", "").strip().lower() != normalized:
            continue
        if verify_password(password, user.get("password_hash", "")):
            return {
                "email": user["email"],
                "name": user.get("name", user["email"]),
                "role": user.get("role", "member"),
            }
        logger.info("Login failed for %s — password mismatch", normalized)
        return None
    logger.info("Login failed — unknown email %s (configured: %s)", normalized, [u.get("email") for u in users])
    return None


def create_token(email: str) -> tuple[str, int]:
    expires = int(time.time()) + _TOKEN_TTL_SEC
    payload = f"{email.strip().lower()}:{expires}"
    sig = hmac.new(_token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}", expires


def lookup_user(email: str) -> Optional[dict[str, str]]:
    normalized = email.strip().lower()
    for user in _load_users():
        if user.get("email", "").strip().lower() == normalized:
            return {
                "email": user["email"],
                "name": user.get("name", user["email"]),
                "role": user.get("role", "member"),
            }
    return None


def verify_token(token: str) -> Optional[str]:
    if not token or ":" not in token:
        return None
    try:
        email, expires_s, sig = token.rsplit(":", 2)
        expires = int(expires_s)
    except (ValueError, TypeError):
        return None
    if expires < int(time.time()):
        return None
    payload = f"{email}:{expires_s}"
    expected = hmac.new(_token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    return email


def public_user(user: dict[str, str]) -> dict[str, Any]:
    return {"email": user["email"], "name": user["name"], "role": user["role"]}
