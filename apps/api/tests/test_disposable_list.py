"""P2-2 — disposable list: vendored snapshot, optional feed, allow_domain."""

from __future__ import annotations

import hashlib

import services.form_token as form_token_mod
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services import denylist_store, disposable_list, mx_check
from services.email_sender import reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.delenv("DATAWRAP_DISPOSABLE_FEED_ENABLED", raising=False)
    monkeypatch.setattr(denylist_store, "_path", lambda: tmp_path / "deny.json")
    import services.signup_service as signup_service
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    disposable_list._reset_for_tests()
    yield
    disposable_list._reset_for_tests()
    reset_email_sender()


def test_vendored_snapshot_loaded_and_checksum_matches():
    """The vendored file's header checksum verifies its own payload."""
    domains = disposable_list.vendored_domains()
    assert len(domains) > 1000
    assert "mailinator.com" in domains
    recorded = disposable_list.vendored_sha256()
    assert recorded
    assert disposable_list.vendored_payload_sha256() == recorded


def test_blocking_works_with_no_network():
    """No fetch_fn, no flag: the vendored set alone refuses burners."""
    assert disposable_list.is_disposable("a@mailinator.com")
    assert disposable_list.is_disposable("a@guerrillamail.com")
    assert not disposable_list.is_disposable("a@corp.example")


def test_feed_disabled_by_default():
    state = disposable_list.sync_feed(fetch_fn=lambda u: b"x\n" * 20)
    assert "disabled" in state["error"]
    assert disposable_list.feed_state()["synced_at"] is None


_SHA = "e4f0910a1a05b6dff2ff9299471a9cddeadbddb8"
_PINNED_URL = ("https://raw.githubusercontent.com/disposable-email-domains/"
               f"disposable-email-domains/{_SHA}/"
               "disposable_email_blocklist.conf")


@pytest.mark.parametrize("url", [
    # branch names
    "https://raw.githubusercontent.com/o/r/master/list.conf",
    "https://raw.githubusercontent.com/o/r/main/list.conf",
    "https://raw.githubusercontent.com/o/r/refs/heads/main/list.conf",
    # tags
    "https://raw.githubusercontent.com/o/r/v1.2.3/list.conf",
    "https://raw.githubusercontent.com/o/r/refs/tags/v1/list.conf",
    # short SHAs
    f"https://raw.githubusercontent.com/o/r/{_SHA[:7]}/list.conf",
    f"https://raw.githubusercontent.com/o/r/{_SHA[:39]}/list.conf",
    # non-https
    f"http://raw.githubusercontent.com/o/r/{_SHA}/list.conf",
    f"ftp://raw.githubusercontent.com/o/r/{_SHA}/list.conf",
    # other hosts
    f"https://example.com/o/r/{_SHA}/list.conf",
    f"https://github.com/o/r/blob/{_SHA}/list.conf",
    f"https://raw.githubusercontent.com.evil.com/o/r/{_SHA}/list.conf",
    # malformed
    "not-a-url",
    f"https://raw.githubusercontent.com/{_SHA}",
    f"https://raw.githubusercontent.com/o/r/{_SHA}",
])
def test_feed_url_pin_validation_rejects(url):
    with pytest.raises(ValueError):
        disposable_list._validate_feed_url(url)


@pytest.mark.parametrize("url", [
    _PINNED_URL,
    # uppercase hex SHA is still a full commit pin
    ("https://raw.githubusercontent.com/o/r/"
     f"{_SHA.upper()}/list.conf"),
])
def test_feed_url_pin_validation_accepts(url):
    disposable_list._validate_feed_url(url)


def test_feed_refuses_branch_urls_and_missing_checksum(monkeypatch):
    monkeypatch.setenv("DATAWRAP_DISPOSABLE_FEED_ENABLED", "1")
    state = disposable_list.sync_feed(
        fetch_fn=lambda u: b"x\n" * 20,
        url="https://raw.githubusercontent.com/o/r/master/list.conf",
        expected_sha256="abc")
    assert "commit" in state["error"]
    state = disposable_list.sync_feed(
        fetch_fn=lambda u: b"x\n" * 20,
        url=_PINNED_URL)
    assert "checksum pin required" in state["error"]


def test_feed_wrong_checksum_keeps_old_set(monkeypatch):
    monkeypatch.setenv("DATAWRAP_DISPOSABLE_FEED_ENABLED", "1")
    payload = b"fresh-burner.example\n" + b"".join(
        f"x{i}.example\n".encode() for i in range(12))
    digest = hashlib.sha256(payload).hexdigest()
    url = _PINNED_URL
    state = disposable_list.sync_feed(
        fetch_fn=lambda u: payload, url=url, expected_sha256=digest)
    assert state["error"] == ""
    assert disposable_list.is_disposable("a@fresh-burner.example")
    state = disposable_list.sync_feed(
        fetch_fn=lambda u: b"tampered.example\n" * 20,
        url=url, expected_sha256=digest)
    assert "mismatch" in state["error"]
    assert disposable_list.is_disposable("a@fresh-burner.example")


def test_allow_domain_beats_every_deny_source():
    denylist_store.add_entry(
        value="mailinator.com", kind="allow_domain",
        source="team", reason="real customer", added_by="ops")
    assert not disposable_list.is_disposable("a@mailinator.com")


def test_allow_domain_cannot_come_from_feed():
    with pytest.raises(ValueError):
        denylist_store.add_entry(
            value="mailinator.com", kind="allow_domain",
            source="feed", reason="nope")


def test_team_denylist_entry_refused():
    denylist_store.add_entry(
        value="corp-burner.example", kind="domain",
        source="team", reason="burner", added_by="ops")
    assert disposable_list.is_disposable("a@corp-burner.example")


def test_signup_refuses_vendored_domain():
    app = FastAPI()
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router, prefix="/api/v1")
    resp = TestClient(app).post("/api/v1/auth/signup", json={
        "email": "a@mailinator.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "blocked_address"
