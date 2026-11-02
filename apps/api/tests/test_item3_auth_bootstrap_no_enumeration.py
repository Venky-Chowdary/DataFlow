"""ITEM 3 — unauthenticated /auth/bootstrap must not enumerate accounts.

Reproduced audit fact: public bootstrap returned every configured email and
``admin_password_length``. The public payload must be exactly
``{auth_required, has_users}`` (has_users ≡ user_count > 0). Nothing else.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

_FORBIDDEN_PUBLIC_KEYS = frozenset(
    {
        "emails",
        "admin_password_length",
        "user_count",
        "admin_email_configured",
        "admin_password_configured",
        "auth_users_configured",
        "auth_users_json_valid",
    }
)


@pytest.fixture
def auth_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "item3-bootstrap-secret-value")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv(
        "DATAFLOW_AUTH_USERS",
        '[{"email":"ops@example.com","password":"other-strong-pass-456","role":"viewer"}]',
    )

    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None


def _bootstrap_client(auth_env):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router

    app = FastAPI()
    app.add_middleware(AuthMiddleware)
    # Mirror src/main.py: router already carries prefix="/auth".
    app.include_router(auth_router, prefix="/api/v1")
    app.include_router(auth_router)
    return TestClient(app)


def test_unauthenticated_bootstrap_payload_is_exactly_public_contract(auth_env):
    client = _bootstrap_client(auth_env)
    for path in ("/api/v1/auth/bootstrap", "/auth/bootstrap"):
        response = client.get(path)
        assert response.status_code == 200, path
        body = response.json()
        assert set(body.keys()) == {"auth_required", "has_users"}, (path, body)
        assert body["auth_required"] is True
        assert body["has_users"] is True
        assert not (_FORBIDDEN_PUBLIC_KEYS & set(body.keys())), body
        assert "admin@example.com" not in response.text
        assert "ops@example.com" not in response.text
        assert "admin_password_length" not in response.text


def test_auth_bootstrap_status_public_never_lists_emails(auth_env):
    from src.services.auth_service import auth_bootstrap_status

    public = auth_bootstrap_status()
    assert set(public.keys()) == {"auth_required", "has_users"}
    assert public["has_users"] is True

    sensitive = auth_bootstrap_status(include_sensitive=True)
    assert "emails" not in sensitive
    assert "admin_password_length" not in sensitive
    assert sensitive["user_count"] >= 2
    assert "admin@example.com" not in str(sensitive)
    assert "ops@example.com" not in str(sensitive)


def test_authenticated_bootstrap_still_omits_emails(auth_env):
    from src.services.auth_service import create_token

    client = _bootstrap_client(auth_env)
    token, _ = create_token("admin@example.com")
    response = client.get(
        "/api/v1/auth/bootstrap",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["has_users"] is True
    assert "emails" not in body
    assert "admin_password_length" not in body
    assert "admin@example.com" not in response.text
    assert "ops@example.com" not in response.text
    # Authenticated may see non-secret deploy flags — never account list.
    assert body.get("admin_email_configured") is True
