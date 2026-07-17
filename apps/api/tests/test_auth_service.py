"""Tests for the authentication service primitives."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def auth_env(monkeypatch):
    """Isolate auth-service module state for each test."""
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "unit-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")


def test_hash_password_and_verify_bcrypt(auth_env):
    from src.services.auth_service import hash_password, verify_password

    h = hash_password("my-password")
    assert h.startswith("$2")
    assert verify_password("my-password", h) is True
    assert verify_password("wrong-password", h) is False


def test_verify_legacy_sha256_still_works(auth_env):
    from src.services.auth_service import verify_password

    legacy_hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
    assert verify_password("password", legacy_hash) is True
    assert verify_password("wrong", legacy_hash) is False


def test_token_create_and_verify(auth_env):
    from src.services.auth_service import create_token, verify_token

    token, expires = create_token("user@example.com")
    assert isinstance(token, str)
    assert expires > int(time.time())
    assert verify_token(token) == "user@example.com"
    assert verify_token("invalid-token") is None


def test_expired_token_is_rejected(auth_env):
    from src.services.auth_service import create_token, verify_token

    token, _ = create_token("user@example.com")
    # Fast-forward past the TTL.
    token = token.replace(str(int(time.time()) + 86400), str(int(time.time()) - 1))
    assert verify_token(token) is None


def test_production_rejects_default_secret(monkeypatch):
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "is_production", lambda: True)
    monkeypatch.setattr(auth_mod, "_REAUTH_SECRET", "dev-change-me-before-production")

    with pytest.raises(RuntimeError):
        auth_mod._token_secret()


def test_authenticate_with_env_credentials(auth_env):
    from src.services.auth_service import authenticate

    assert authenticate("admin@example.com", "strong-password-123") is not None
    assert authenticate("admin@example.com", "wrong-password") is None
