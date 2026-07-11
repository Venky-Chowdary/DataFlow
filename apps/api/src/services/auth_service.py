"""Workspace authentication — server-side only; never expose password hashes to clients."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
from typing import Any, Optional

from services.platform_config import is_production

_AUTH_SECRET = os.getenv("DATAFLOW_AUTH_SECRET", "dev-change-me-before-production")
_REQUIRE_AUTH = os.getenv("DATAFLOW_REQUIRE_AUTH", "1" if is_production() else "0").lower() in ("1", "true", "yes")
_TOKEN_TTL_SEC = int(os.getenv("DATAFLOW_TOKEN_TTL_SEC", "86400"))
_ALLOW_DEV_USER = os.getenv("DATAFLOW_ALLOW_DEV_USER", "0").lower() in ("1", "true", "yes")

# SHA-256 of "password123" for test@gmail.com (dev/staging only)
_DEV_USER = {
    "email": "test@gmail.com",
    "password_hash": "527ebe0507adc1c8d2260420e4f70e1ae6e61f24ec6bcf54e827c1afba8b2810",
    "name": "Test User",
    "role": "Workspace tester",
}


def auth_required() -> bool:
    return _REQUIRE_AUTH


def _load_users() -> list[dict[str, str]]:
    raw = os.getenv("DATAFLOW_AUTH_USERS", "").strip()
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass
    if _ALLOW_DEV_USER or not is_production():
        return [_DEV_USER]
    return []


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def authenticate(email: str, password: str) -> Optional[dict[str, str]]:
    normalized = email.strip().lower()
    digest = hash_password(password)
    for user in _load_users():
        if user.get("email", "").strip().lower() == normalized and user.get("password_hash") == digest:
            return {
                "email": user["email"],
                "name": user.get("name", user["email"]),
                "role": user.get("role", "member"),
            }
    return None


def create_token(email: str) -> tuple[str, int]:
    expires = int(time.time()) + _TOKEN_TTL_SEC
    payload = f"{email.strip().lower()}:{expires}"
    sig = hmac.new(_AUTH_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
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
    expected = hmac.new(_AUTH_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return None
    return email


def public_user(user: dict[str, str]) -> dict[str, Any]:
    return {"email": user["email"], "name": user["name"], "role": user["role"]}
