"""Workspace authentication — server-side only; never expose password hashes to clients."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re

from services.brand_env import getenv_brand
import time
from typing import Any, Optional

from services.platform_config import is_production

logger = logging.getLogger("dataflow.auth")

# bcrypt hash of "password123" for test@gmail.com (dev/staging only, never production)
_DEV_USER = {
    "email": "test@gmail.com",
    "password_hash": "$2b$12$II.e7tCoYPLs2Pv8/dWEVeOMl3GOwsiUnSteHd6Twq3juXLiLsO9e",  # nosec B105
    "name": "Test User",
    "role": "Workspace tester",
}

# Legacy unsalted SHA-256 hashes are exactly 64 hex characters.
_LEGACY_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Cache admin bcrypt hash so we do not re-salt on every login request.
_ADMIN_USER_CACHE: dict[str, str] | None = None
_ADMIN_CACHE_KEY: tuple[str, str] | None = None

# Import-time snapshot kept only for shim re-exports / backward-compat tests.
# Runtime paths MUST call the lazy readers below (audit §1.3 D2 — suite order).
_REAUTH_SECRET = getenv_brand("AUTH_SECRET", "dev-change-me-before-production")
_REQUIRE_AUTH = getenv_brand("REQUIRE_AUTH", "1" if is_production() else "0").lower() in (
    "1",
    "true",
    "yes",
)
_TOKEN_TTL_SEC = int(getenv_brand("TOKEN_TTL_SEC", "86400") or "86400")
_ALLOW_DEV_USER = getenv_brand("ALLOW_DEV_USER", "0").lower() in ("1", "true", "yes")


def _read_auth_secret() -> str:
    return getenv_brand("AUTH_SECRET", "dev-change-me-before-production")


def _read_require_auth() -> bool:
    return getenv_brand("REQUIRE_AUTH", "1" if is_production() else "0").lower() in (
        "1",
        "true",
        "yes",
    )


def _read_token_ttl_sec() -> int:
    try:
        return int(getenv_brand("TOKEN_TTL_SEC", "86400") or "86400")
    except ValueError:
        return 86400


def _read_allow_dev_user() -> bool:
    return getenv_brand("ALLOW_DEV_USER", "0").lower() in ("1", "true", "yes")


def _token_secret() -> str:
    """Return the signing secret after validating it is not the dev default in production."""
    secret = _read_auth_secret()
    if is_production() and secret in ("", "dev-change-me-before-production"):
        raise RuntimeError("DATAFLOW_AUTH_SECRET must be set to a strong random value in production")
    return secret


def auth_required() -> bool:
    """Whether auth is required — read env at call time (never import-frozen)."""
    return _read_require_auth()


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
    admin_email = _normalize_secret(getenv_brand("ADMIN_EMAIL", ""))
    admin_password = _normalize_secret(getenv_brand("ADMIN_PASSWORD", ""))
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


_AUTH_USERS_JSON_WARNED = False


def _users_from_auth_users_env() -> list[dict[str, str]]:
    global _AUTH_USERS_JSON_WARNED
    raw = getenv_brand("AUTH_USERS", "").strip()
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        if not _AUTH_USERS_JSON_WARNED:
            logger.warning(
                "DATAFLOW_AUTH_USERS is set but is not valid JSON — ignoring "
                "(fix once: use a JSON array like "
                '[{\"email\":\"a@b.c\",\"password\":\"…\",\"role\":\"admin\"}] '
                "or clear the variable and rely on DATAFLOW_ADMIN_*)"
            )
            _AUTH_USERS_JSON_WARNED = True
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
        entry: dict[str, Any] = {
            "email": email,
            "password_hash": password_hash,
            "name": str(row.get("name") or email),
            "role": str(row.get("role") or "member"),
        }
        tenant_id = str(row.get("tenant_id") or "").strip()
        if tenant_id:
            entry["tenant_id"] = tenant_id
        raw_ids = row.get("tenant_ids")
        if isinstance(raw_ids, list):
            entry["tenant_ids"] = [str(x).strip() for x in raw_ids if str(x).strip()]
        elif isinstance(raw_ids, str) and raw_ids.strip():
            entry["tenant_ids"] = [p.strip() for p in raw_ids.split(",") if p.strip()]
        users.append(entry)
    return users


def _load_users() -> list[dict[str, Any]]:
    """Load workspace users.

    Priority:
    1. DATAFLOW_ADMIN_EMAIL + DATAFLOW_ADMIN_PASSWORD (always included when set)
    2. DATAFLOW_AUTH_USERS JSON list (merged; admin email wins on conflict)
    3. Dev user — **only** when DATAFLOW_ALLOW_DEV_USER=1 and not production (audit §6.9)
    """
    users: list[dict[str, Any]] = []
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

    # Never expose the hard-coded test@gmail.com account in production — even
    # when ALLOW_DEV_USER is mistakenly set (validate_production_config rejects it).
    if is_production():
        return []

    # Phase D5: staging/non-prod must opt in — never auto-enable password123.
    if _read_allow_dev_user():
        return [dict(_DEV_USER)]
    return []

def auth_bootstrap_status(*, include_sensitive: bool = False) -> dict[str, Any]:
    """Public-safe auth diagnostics (audit ITEM 3 / §6.1).

    Unauthenticated callers receive **only** ``auth_required`` and ``has_users``
    (whether ``user_count > 0``). Never account emails, password lengths, or
    configuration detail that aids enumeration / brute-force.

    Authenticated operators may pass ``include_sensitive=True`` for deploy
    diagnostics: boolean config flags and a count — still no emails and no
    password length.
    """
    admin_email = _normalize_secret(getenv_brand("ADMIN_EMAIL", ""))
    admin_password = _normalize_secret(getenv_brand("ADMIN_PASSWORD", ""))
    raw_users = getenv_brand("AUTH_USERS", "").strip()
    auth_users_json_valid: bool | None = None
    if raw_users:
        try:
            parsed = json.loads(raw_users)
            auth_users_json_valid = isinstance(parsed, list)
        except json.JSONDecodeError:
            auth_users_json_valid = False
    users = _load_users()
    # Public payload — exact audit contract: nothing else.
    public: dict[str, Any] = {
        "auth_required": auth_required(),
        "has_users": len(users) > 0,
    }
    if include_sensitive:
        public.update(
            {
                "user_count": len(users),
                "admin_email_configured": bool(admin_email),
                "admin_password_configured": bool(admin_password),
                "auth_users_configured": bool(raw_users) and auth_users_json_valid is True,
                "auth_users_json_valid": auth_users_json_valid,
                # Never expose emails or password length — both aid attackers.
            }
        )
    return public


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
    # Never dump the configured user directory — aids account discovery via logs.
    logger.info("Login failed — unknown email (users_configured=%s)", len(users))
    return None


def _legacy_tokens_allowed() -> bool:
    """Allow pre-jti ``email:expires:sig`` tokens (transition). Default off in production."""
    raw = getenv_brand("AUTH_LEGACY_TOKENS", "").strip().lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no"):
        return False
    return not is_production()


def create_token(email: str) -> tuple[str, int]:
    """Mint Bearer token with server-side session (``jti``) — Phase D3."""
    from services.auth_sessions import create_session

    expires = int(time.time()) + _read_token_ttl_sec()
    email_n = email.strip().lower()
    jti = create_session(email_n, expires_at=expires)
    payload = f"{email_n}:{expires}:{jti}"
    sig = hmac.new(_token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}:{sig}", expires


def lookup_user(email: str) -> Optional[dict[str, Any]]:
    normalized = email.strip().lower()
    for user in _load_users():
        if user.get("email", "").strip().lower() == normalized:
            out: dict[str, Any] = {
                "email": user["email"],
                "name": user.get("name", user["email"]),
                "role": user.get("role", "member"),
            }
            if user.get("tenant_id"):
                out["tenant_id"] = user["tenant_id"]
            if user.get("tenant_ids"):
                out["tenant_ids"] = list(user["tenant_ids"])
            return out
    return None


def verify_token(token: str) -> Optional[str]:
    """Return email when HMAC + expiry + (session jti) are valid."""
    if not token or ":" not in token:
        return None
    parts = token.rsplit(":", 3)
    # New form: email:expires:jti:sig  (4 segments after rsplit limit 3 → 4 parts)
    if len(parts) == 4:
        email, expires_s, jti, sig = parts
        try:
            expires = int(expires_s)
        except (ValueError, TypeError):
            return None
        if expires < int(time.time()):
            return None
        payload = f"{email}:{expires_s}:{jti}"
        expected = hmac.new(
            _token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        from services.auth_sessions import session_active

        if not session_active(jti, email):
            return None
        return email

    # Legacy form: email:expires:sig (no revocation)
    if len(parts) == 3 and _legacy_tokens_allowed():
        email, expires_s, sig = parts
        try:
            expires = int(expires_s)
        except (ValueError, TypeError):
            return None
        if expires < int(time.time()):
            return None
        payload = f"{email}:{expires_s}"
        expected = hmac.new(
            _token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        return email
    return None


def token_jti(token: str) -> Optional[str]:
    """Extract jti from a v2 token; None for legacy / invalid."""
    if not token or token.count(":") < 3:
        return None
    parts = token.rsplit(":", 3)
    if len(parts) != 4:
        return None
    return parts[2] or None


def revoke_token(token: str) -> bool:
    """Revoke the session for a Bearer token (logout)."""
    from services.auth_sessions import revoke_session

    jti = token_jti(token)
    if not jti:
        return False
    return revoke_session(jti)


def revoke_sessions_for_email(email: str) -> int:
    """Invalidate all sessions for an email (password change / force logout)."""
    from services.auth_sessions import revoke_all_for_email

    return revoke_all_for_email(email)


def public_user(user: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "email": user["email"],
        "name": user["name"],
        "role": user["role"],
    }
    if user.get("tenant_id"):
        out["tenant_id"] = user["tenant_id"]
    if user.get("tenant_ids"):
        out["tenant_ids"] = list(user["tenant_ids"])
    return out