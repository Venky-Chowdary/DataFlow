"""Tests for the authentication service primitives."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    """Isolate auth-service module state for each test."""
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "unit-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_AUTH_LEGACY_TOKENS", "0")


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


def test_verify_legacy_sha256_rejected_in_production(auth_env, monkeypatch):
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "is_production", lambda: True)
    legacy_hash = "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
    assert auth_mod.verify_password("password", legacy_hash) is False


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
    # Rewrite expires segment (email:expires:jti:sig) to the past and re-sign.
    email, _expires_s, jti, _sig = token.rsplit(":", 3)
    past = str(int(time.time()) - 1)
    payload = f"{email}:{past}:{jti}"
    from src.services.auth_service import _token_secret
    import hashlib
    import hmac

    sig = hmac.new(_token_secret().encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    assert verify_token(f"{payload}:{sig}") is None


def test_production_rejects_default_secret(monkeypatch):
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "is_production", lambda: True)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "dev-change-me-before-production")
    monkeypatch.setenv("DATAWRAP_AUTH_SECRET", "dev-change-me-before-production")

    with pytest.raises(RuntimeError):
        auth_mod._token_secret()

def test_authenticate_with_env_credentials(auth_env):
    from src.services.auth_service import authenticate

    assert authenticate("admin@example.com", "strong-password-123") is not None
    assert authenticate("admin@example.com", "wrong-password") is None


def test_normalize_secret_handles_dollar_escape(auth_env, monkeypatch):
    """Railway/shell often expand $FOO — operators escape as $$FOO."""
    import src.services.auth_service as auth_mod

    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@dataflow.app")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "p@ss$$word")
    # Reset admin cache between env mutations
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    user = auth_mod.authenticate("admin@dataflow.app", "p@ss$word")
    assert user is not None


def test_auth_bootstrap_status_reports_admin(auth_env):
    from src.services.auth_service import auth_bootstrap_status

    # Public payload — exact ITEM 3 contract; no enumeration aids.
    public = auth_bootstrap_status()
    assert public == {
        "auth_required": public["auth_required"],
        "has_users": True,
    }
    assert set(public.keys()) == {"auth_required", "has_users"}
    assert "emails" not in public
    assert "user_count" not in public
    assert "admin_password_length" not in public

    sensitive = auth_bootstrap_status(include_sensitive=True)
    assert sensitive["admin_email_configured"] is True
    assert sensitive["admin_password_configured"] is True
    assert sensitive["user_count"] >= 1
    assert "emails" not in sensitive
    assert "admin_password_length" not in sensitive
