"""Compatibility shim: canonical auth_service lives in src.services.auth_service.

Attributes are resolved on the canonical module at *access* time. A star-style
``from src.services.auth_service import auth_required`` would copy the binding
into this module, so a later rebinding of the canonical symbol (a feature flag
flip, a patched dependency) left two live answers to "is auth required?" in one
process — the middleware imported through this shim kept enforcing while the
canonical module said otherwise.
"""

from __future__ import annotations

from typing import Any

from src.services import auth_service as _canonical

__all__ = [
    "_REAUTH_SECRET",
    "_REQUIRE_AUTH",
    "_TOKEN_TTL_SEC",
    "_ALLOW_DEV_USER",
    "_DEV_USER",
    "_LEGACY_SHA256_RE",
    "_token_secret",
    "_normalize_secret",
    "auth_bootstrap_status",
    "auth_required",
    "_load_users",
    "hash_password",
    "_legacy_verify",
    "verify_password",
    "authenticate",
    "create_token",
    "lookup_user",
    "verify_token",
    "public_user",
]


def __getattr__(name: str) -> Any:
    return getattr(_canonical, name)


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(dir(_canonical)))
