"""Password-login audit events contain outcomes, never credentials."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def login_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-login-audit-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_AUTH_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.test")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Strong-admin-password-123")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_SEC", "120")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_ENV", "development")

    from services import audit_log, auth_rate_limit, user_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(auth_service, "_ADMIN_USER_CACHE", None)
    monkeypatch.setattr(auth_service, "_ADMIN_CACHE_KEY", None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    auth_rate_limit.reset_auth_rate_limits()

    from src.main import app

    with TestClient(app) as client:
        yield {"client": client, "audit_log": audit_log, "user_store": user_store}


def _events(context):
    path = context["audit_log"].STORE_PATH
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_bad_password_audit_uses_normalized_truncated_actor_and_no_password(login_app):
    email = "A" * 242 + "@EXAMPLE.TEST"
    password = "never-store-this-password"
    response = login_app["client"].post(
        "/api/v1/auth/login", json={"email": email, "password": password}
    )
    assert response.status_code == 401

    event = next(row for row in _events(login_app) if row["action"] == "auth.login.failure")
    assert event["actor"] == email.strip().lower()[:254]
    assert len(event["actor"]) == 254
    assert event["details"]["reason"] == "bad_credentials"
    assert password not in login_app["audit_log"].STORE_PATH.read_text(encoding="utf-8")


def test_login_lockout_event_is_written_once(login_app):
    client = login_app["client"]
    for _ in range(3):
        response = client.post(
            "/api/v1/auth/login",
            json={"email": "admin@example.test", "password": "wrong-password-123"},
        )
        assert response.status_code in (401, 429)

    rows = _events(login_app)
    locked = [row for row in rows if row["action"] == "auth.login.locked"]
    assert len(locked) == 1
    failures = [row for row in rows if row["action"] == "auth.login.failure"]
    assert failures
    assert any(row["details"]["reason"] == "locked" for row in failures)
    assert all(row["actor"] == "admin@example.test" for row in locked + failures)


def test_disabled_account_login_audit_has_disabled_reason(login_app):
    email = "disabled@example.test"
    account, _ = login_app["user_store"].create_user(
        email=email,
        password="Disabled-password-123",
        created_by="admin@example.test",
    )
    login_app["user_store"].update_user(email=email, status="disabled")

    response = login_app["client"].post(
        "/api/v1/auth/login",
        json={"email": account["email"], "password": "Disabled-password-123"},
    )
    assert response.status_code == 401
    event = next(row for row in _events(login_app) if row["action"] == "auth.login.failure")
    assert event["actor"] == email
    assert event["details"]["reason"] == "disabled"


def test_successful_login_survives_audit_failure_with_typed_warning(
    login_app, monkeypatch, caplog
):
    def fail_append(**_kwargs):
        raise OSError("storage offline")

    monkeypatch.setattr(login_app["audit_log"], "append_audit_event", fail_append)
    response = login_app["client"].post(
        "/api/v1/auth/login",
        json={
            "email": "admin@example.test",
            "password": "Strong-admin-password-123",
        },
    )
    assert response.status_code == 200
    assert any(
        "Login success audit write failed (OSError)" in record.getMessage()
        for record in caplog.records
    )
