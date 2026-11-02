"""P2-5 — Turnstile: replay/wrong-action/wrong-host/missing refused."""

from __future__ import annotations

import pytest

from services import turnstile


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "test-secret")


def _post(payload):
    def fn(**kw):
        return payload
    return fn


def test_missing_token_refused():
    assert not turnstile.verify("", post_fn=_post({}))["ok"]


def test_clean_token_ok():
    r = turnstile.verify(
        "tok", expected_action="signup", expected_hostname="datawrap.io",
        post_fn=_post({"success": True, "hostname": "datawrap.io",
                       "action": "signup"}))
    assert r["ok"]


def test_replay_refused():
    r = turnstile.verify("tok", post_fn=_post(
        {"success": False, "error-codes": ["timeout-or-duplicate"]}))
    assert not r["ok"] and r["reason"] == "rejected"


def test_wrong_action_refused():
    r = turnstile.verify("tok", expected_action="signup",
                         post_fn=_post({"success": True,
                                        "hostname": "datawrap.io",
                                        "action": "login"}))
    assert not r["ok"] and r["reason"] == "action_mismatch"


def test_wrong_hostname_refused():
    r = turnstile.verify("tok", expected_hostname="datawrap.io",
                         post_fn=_post({"success": True,
                                        "hostname": "evil.example"}))
    assert not r["ok"]


def test_unreachable_fails_closed():
    def boom(**kw):
        raise ConnectionError("down")
    assert turnstile.verify("tok", post_fn=boom)["reason"] == "verify_unreachable"


def test_unconfigured_refuses(monkeypatch):
    monkeypatch.delenv("DATAWRAP_TURNSTILE_SECRET", raising=False)
    monkeypatch.delenv("DATAFLOW_TURNSTILE_SECRET", raising=False)
    assert turnstile.verify("tok", post_fn=_post({}))["reason"] == "not_configured"
