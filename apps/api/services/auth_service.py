"""Compatibility shim: canonical auth_service lives in src.services.auth_service."""
from __future__ import annotations

from src.services.auth_service import (
    _ALLOW_DEV_USER,
    _DEV_USER,
    _LEGACY_SHA256_RE,
    _REAUTH_SECRET,
    _REQUIRE_AUTH,
    _TOKEN_TTL_SEC,
    _legacy_verify,
    _load_users,
    _token_secret,
    auth_required,
    authenticate,
    create_token,
    hash_password,
    lookup_user,
    public_user,
    verify_password,
    verify_token,
)

__all__ = ['_REAUTH_SECRET', '_REQUIRE_AUTH', '_TOKEN_TTL_SEC', '_ALLOW_DEV_USER', '_DEV_USER', '_LEGACY_SHA256_RE', '_token_secret', 'auth_required', '_load_users', 'hash_password', '_legacy_verify', 'verify_password', 'authenticate', 'create_token', 'lookup_user', 'verify_token', 'public_user']
