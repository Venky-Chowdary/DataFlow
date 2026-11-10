"""D322 - signup from https://www.datawrap.io always failed the Turnstile check.

QA: POST /api/v1/auth/signup from www.datawrap.io answered 400
``turnstile_required`` with a valid token; the same flow on the apex host
reached the OTP step.

Root cause: ``signup_service.request_signup`` pinned
``expected_hostname="datawrap.io"`` and ``turnstile.verify`` compares it
exactly, so a token Cloudflare minted on ``www.datawrap.io`` came back
``hostname_mismatch`` although ``www.datawrap.io`` is in the served-host
allow-list.

Route: ``POST /api/v1/auth/signup`` with Cloudflare siteverify answered by a
stub at the ``httpx`` layer (no network here).
"""

from __future__ import annotations

import logging

import pytest

from tests.test_signup_request import _body, _client, _isolate  # noqa: F401 - fixture
from services import turnstile


class _SiteVerify:
    def __init__(self, hostname: str) -> None:
        self.hostname = hostname

    def __call__(self, url, data=None, timeout=None):  # httpx.post signature
        hostname = self.hostname

        class _Resp:
            @staticmethod
            def json():
                return {"success": True, "hostname": hostname, "action": "signup"}

        return _Resp()


@pytest.fixture
def siteverify(monkeypatch):
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "test-secret")

    def _install(hostname: str) -> None:
        monkeypatch.setattr(turnstile.httpx, "post", _SiteVerify(hostname))

    return _install


@pytest.mark.parametrize("host", ["www.datawrap.io", "datawrap.io"])
def test_signup_from_www_and_apex_reaches_the_otp_step(siteverify, host):
    siteverify(host)
    body = {**_body(email=f"ada-{host.split('.')[0]}@example.com"), "turnstile_token": "tok"}
    resp = _client().post("/api/v1/auth/signup", json=body)
    assert resp.status_code == 202, resp.text
    assert resp.json()["challenge_id"].startswith("otc_")


def test_unknown_host_is_refused_and_the_reason_is_logged(siteverify, caplog):
    siteverify("evil.example")
    body = {**_body(email="mallory@example.com"), "turnstile_token": "tok"}
    with caplog.at_level(logging.INFO):
        resp = _client().post("/api/v1/auth/signup", json=body)
    assert resp.status_code == 400, resp.text
    assert resp.json()["error"]["code"] == "turnstile_required"
    # The client gets the generic message; the server log names why.
    assert "evil.example" not in resp.text
    reasons = [getattr(r, "reason_code", "") for r in caplog.records]
    assert "turnstile:hostname_not_allowed" in reasons, reasons


@pytest.mark.parametrize("host", ["www.datawrap.io", "datawrap.io", "WWW.DATAWRAP.IO"])
def test_verify_accepts_every_served_hostname(monkeypatch, host):
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "test-secret")
    verdict = turnstile.verify(
        "tok", expected_action="signup",
        post_fn=lambda **_kw: {"success": True, "hostname": host, "action": "signup"},
    )
    assert verdict["ok"], verdict
