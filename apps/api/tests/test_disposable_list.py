"""P2-2 — disposable list: seed + feed sync + team denylist/allowlist."""

from __future__ import annotations

import hashlib

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services import denylist_store, disposable_list, signup_store, user_store
from services.email_sender import reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setattr(denylist_store, "_path", lambda: tmp_path / "deny.json")
    disposable_list._reset_for_tests()
    yield
    disposable_list._reset_for_tests()
    reset_email_sender()


def test_seed_domain_refused():
    assert disposable_list.is_disposable("a@mailinator.com")
    assert disposable_list.is_disposable("a@guerrillamail.com")
    assert not disposable_list.is_disposable("a@corp.example")


def _payload():
    return b"newburner.example\nsecond-burner.example\n" + b"".join(
        f"x{i}.example\n".encode() for i in range(12))


def test_feed_sync_checksum_pinned():
    payload = _payload()
    digest = hashlib.sha256(payload).hexdigest()
    state = disposable_list.sync_feed(
        fetch_fn=lambda url: payload, expected_sha256=digest)
    assert state["error"] == ""
    assert disposable_list.is_disposable("a@newburner.example")


def test_feed_checksum_mismatch_keeps_last_good():
    payload = _payload()
    digest = hashlib.sha256(payload).hexdigest()
    disposable_list.sync_feed(fetch_fn=lambda u: payload, expected_sha256=digest)
    state = disposable_list.sync_feed(
        fetch_fn=lambda u: b"tampered.example\n", expected_sha256=digest)
    assert "mismatch" in state["error"]
    assert disposable_list.is_disposable("a@newburner.example")
    assert not disposable_list.is_disposable("a@tampered.example")


def test_feed_failure_keeps_last_good():
    def boom(url):
        raise ConnectionError("nope")
    state = disposable_list.sync_feed(fetch_fn=boom)
    assert state["error"]
    assert disposable_list.is_disposable("a@mailinator.com")  # seed floor


def test_team_denylist_and_allowlist():
    denylist_store.add_entry(
        value="corp-disposable.example", kind="domain",
        source="team", reason="burner", added_by="ops")
    assert disposable_list.is_disposable("a@corp-disposable.example")
    denylist_store.add_entry(
        value="mailinator.com", kind="domain",
        source="feed", reason="allow", added_by="ops")
    assert not disposable_list.is_disposable("a@mailinator.com")


def test_signup_refuses_seed_domain():
    app = FastAPI()
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    resp = TestClient(app).post("/api/v1/auth/signup", json={
        "email": "a@mailinator.com", "password": "a-very-long-password"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "blocked_address"
