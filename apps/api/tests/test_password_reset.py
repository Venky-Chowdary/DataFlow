"""Forgot/reset via single-use emailed LINK: uniform 202, HMAC-only storage,
30-minute TTL, supersede-on-new, atomic consume, sessions revoked."""

from __future__ import annotations

import re
import time
from datetime import timedelta, UTC
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
import services.signup_service as signup_service
from services.api_errors import ApiError
from services import auth_state, email_quota, password_reset_store, signup_store, user_store
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    # Deterministic mail: deliveries run inline unless a test restores the
    # background lane; the timing floor stays on (tests assert it).
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    monkeypatch.setattr(signup_service, "FORGOT_FLOOR_SEC", 0.02)
    monkeypatch.setenv("DATAWRAP_APP_URL", "https://www.datawrap.io")
    monkeypatch.setattr(signup_store, "_pending_path",
                        lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path",
                        lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(password_reset_store, "_path",
                        lambda: tmp_path / "reset_tokens.json")
    monkeypatch.setattr(user_store, "_store_path",
                        lambda: tmp_path / "users.json")
    monkeypatch.setattr(signup_service.mx_check, "check_domain",
                        lambda d: {"ok": True})
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "reset-link-test-secret-value")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_LIMIT", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_BURST", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_RATE_QPS", "1000")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setattr(signup_service, "RESEND_COOLDOWN_SEC", 0)
    import src.services.auth_service as auth_mod
    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)
    auth_mod._ADMIN_USER_CACHE = None
    auth_mod._ADMIN_CACHE_KEY = None
    from services.auth_rate_limit import reset_auth_rate_limits
    reset_auth_rate_limits()
    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler

    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_middleware(AuthMiddleware)
    app.include_router(router, prefix="/api/v1")
    return TestClient(app, base_url="https://testserver")


def _verified_user(c: TestClient, email: str = "ada@example.com") -> dict:
    baseline = len([m for m in ConsoleEmailSender.OUTBOX
                    if re.search(r"\b\d{6}\b", m.text)])
    r = c.post("/api/v1/auth/signup", json={
        "email": email, "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})
    code = _code_mail(after=baseline)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200
    return resp.json()


def _code_mail(after: int = 0) -> str:
    """Wait for the (after+1)-th verification-code mail — signup_code rides
    the same outbox lane now, so a previous user's code mail may be the
    newest when we look."""
    deadline = time.monotonic() + 5
    while True:
        mails = [m for m in ConsoleEmailSender.OUTBOX
                 if re.search(r"\b\d{6}\b", m.text)]
        if len(mails) > after:
            return re.search(r"\b(\d{6})\b", mails[-1].text).group(1)
        if time.monotonic() > deadline:
            raise AssertionError("no verification-code mail in outbox")
        time.sleep(0.05)


def _reset_statuses(tmp_path) -> list:
    """Outbox statuses for password.reset rows only — signup/OTP mails
    share the outbox since item 7 and must not skew reset assertions."""
    import json
    doc = json.loads((tmp_path / "mail_outbox.json").read_text())
    return sorted(r["status"] for r in doc.get("rows", {}).values()
                  if r.get("purpose") == "password.reset")


def _forgot(c: TestClient, email: str = "ada@example.com"):
    return c.post("/api/v1/auth/password/forgot",
                  json={"email": email,
                        "form_token": form_token_mod.issue("reset")})


def _link_token() -> str:
    m = re.search(r"reset-password#token=([A-Za-z0-9_\-]+)",
                  ConsoleEmailSender.OUTBOX[-1].text)
    assert m, ConsoleEmailSender.OUTBOX[-1].text
    return m.group(1)


def test_forgot_returns_202_for_unknown_and_known_alike():
    c = _client()
    _verified_user(c)
    r_unknown = _forgot(c, "ghost@example.com")
    r_known = _forgot(c, "ada@example.com")
    assert r_unknown.status_code == r_known.status_code == 202
    for resp in (r_unknown, r_known):
        body = resp.json()
        assert set(body) == {"ok", "challenge_id", "resend_after_sec",
                             "expires_in_sec"}
        assert body["ok"] is True


def test_forgot_unknown_sends_no_mail():
    c = _client()
    _verified_user(c)
    before = len(ConsoleEmailSender.OUTBOX)
    _forgot(c, "nobody@example.com")
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_link_goes_to_account_address_only_and_has_no_crlf():
    c = _client()
    _verified_user(c)
    _forgot(c, "ada@example.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada@example.com"] or mail.to == "ada@example.com"
    assert "www.datawrap.io/reset-password#token=" in mail.text
    # injection-shaped input is refused before any mail work
    before = len(ConsoleEmailSender.OUTBOX)
    _forgot(c, "ada@example.com\r\nBCC: evil@example.com")
    assert len(ConsoleEmailSender.OUTBOX) == before


def test_token_is_256bit_and_only_hmac_is_stored():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    assert len(token) >= 40  # token_urlsafe(32) -> 43 chars
    doc = password_reset_store._load()["tokens"]
    row = next(iter(doc.values()))
    assert row["token_hash"] != token
    assert len(row["token_hash"]) == 64  # sha256 hex
    assert row["expires_at"] and row["used_at"] is None
    assert row["request_ip"] is not None


def test_inspect_then_reset_revokes_sessions_and_notifies():
    c = _client()
    _verified_user(c)
    state = auth_state.get_auth_state()
    assert state.list_sessions("ada@example.com")
    _forgot(c)
    token = _link_token()
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.status_code == 200 and probe.json()["ok"] is True
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert resp.status_code == 200
    creds = [u for u in user_store.credentials_for_auth()
             if u["email"] == "ada@example.com"]
    assert creds[0]["password_algo"] == "argon2id"
    assert state.list_sessions("ada@example.com") == []
    notice = ConsoleEmailSender.OUTBOX[-1]
    assert "password was changed" in notice.subject


def test_reused_link_is_rejected():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    ok = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert ok.status_code == 200
    again = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "third-password-here"})
    assert again.status_code == 410
    assert again.json()["error"]["code"] == "link_expired"
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["status"] == "used"


def test_expired_link_is_rejected(monkeypatch):
    monkeypatch.setattr(password_reset_store, "TOKEN_TTL",
                        timedelta(seconds=-1))
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "an-even-better-password"})
    assert resp.status_code == 410
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["status"] == "expired"


def test_new_request_invalidates_older_link():
    c = _client()
    _verified_user(c)
    _forgot(c)
    old = _link_token()
    _forgot(c)
    new = _link_token()
    assert new != old
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": old, "new_password": "an-even-better-password"})
    assert resp.status_code == 410
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": new})
    assert probe.json()["ok"] is True


def test_concurrent_double_reset_exactly_one_wins():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    body = {"token": token, "new_password": "an-even-better-password"}
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(
            lambda _: c.post("/api/v1/auth/password/reset", json=body),
            range(2)))
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 410]


def test_garbage_token_is_410_not_500():
    c = _client()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": "totally-bogus-token", "new_password": "a-long-enough-password"})
    assert resp.status_code == 410
    assert resp.json()["error"]["code"] == "link_expired"


def test_reset_rejects_short_password_before_consume():
    c = _client()
    _verified_user(c)
    _forgot(c)
    token = _link_token()
    resp = c.post("/api/v1/auth/password/reset", json={
        "token": token, "new_password": "short"})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_password"
    # token still usable — the password check ran before consume
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": token})
    assert probe.json()["ok"] is True


def test_raw_token_never_logged(caplog):
    c = _client()
    _verified_user(c)
    import logging
    with caplog.at_level(logging.DEBUG):
        _forgot(c)
        token = _link_token()
        c.post("/api/v1/auth/password/reset", json={
            "token": token, "new_password": "an-even-better-password"})
    for record in caplog.records:
        assert token not in record.getMessage()


def test_email_change_velocity_429s_with_retry_after(monkeypatch):
    """forgot stays uniform-202 by design; the 429 + Retry-After contract
    lives on the authenticated email-change route sharing the helper."""
    monkeypatch.setenv("DATAWRAP_EMCHG_EMAIL_DAILY", "1")
    c = _client()
    _verified_user(c)
    csrf = c.cookies["dw_csrf"]
    last = None
    for _ in range(3):
        last = c.post("/api/v1/auth/email/change",
                      headers={"x-csrf-token": csrf},
                      json={"new_email": "grace@example.com"})
    assert last.status_code == 429
    assert last.json()["error"]["retry_after_sec"] > 0


def test_variant_request_mails_only_the_stored_address():
    """A canonical-equivalent variant (case/dots/+tag) must not route the
    reset link to the typed mailbox — the link goes to the stored,
    verified account address only."""
    c = _client()
    _verified_user(c, "ada.signup@gmail.com")
    _forgot(c, "ADASIGNUP@gmail.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada.signup@gmail.com"] or mail.to == "ada.signup@gmail.com"
    _forgot(c, "ada.signup+alternate@gmail.com")
    mail = ConsoleEmailSender.OUTBOX[-1]
    assert mail.to == ["ada.signup@gmail.com"] or mail.to == "ada.signup@gmail.com"


def test_variant_requests_supersede_one_token_set():
    """Tokens are keyed to the account, not the typed string: a second
    request through a different variant kills the first link."""
    c = _client()
    _verified_user(c, "ada.signup@gmail.com")
    _forgot(c, "ada.signup@gmail.com")
    first = _link_token()
    _forgot(c, "adasignup@gmail.com")
    second = _link_token()
    assert first != second
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": first})
    assert probe.json()["status"] == "used"
    probe = c.post("/api/v1/auth/password/reset/inspect", json={"token": second})
    assert probe.json()["ok"] is True


def test_forgot_pads_both_branches_to_floor(monkeypatch):
    """Fake (no account) and real (verified account) replies are padded to
    the same floor — elapsed time reveals nothing about existence."""
    monkeypatch.setattr(signup_service, "FORGOT_FLOOR_SEC", 0.30)
    c = _client()
    t0 = time.monotonic()
    _forgot(c, "ghost@example.com")
    fake_dt = time.monotonic() - t0
    _verified_user(c)
    t0 = time.monotonic()
    _forgot(c, "ada@example.com")
    real_dt = time.monotonic() - t0
    assert fake_dt >= 0.30
    assert real_dt >= 0.30


def test_mailer_latency_never_blocks_the_request(monkeypatch):
    """The request path returns without awaiting the provider — a slow
    Resend call cannot become an existence oracle."""
    monkeypatch.setattr(
        signup_service, "_deliver_later",
        lambda fn: signup_service._MAIL_LANE.submit(fn))
    import services.email_sender as sender_mod
    orig_send = sender_mod.ConsoleEmailSender.send

    def slow_send(self, *a, **kw):
        time.sleep(0.5)
        return orig_send(self, *a, **kw)
    monkeypatch.setattr(sender_mod.ConsoleEmailSender, "send", slow_send)
    monkeypatch.setattr(signup_service, "FORGOT_FLOOR_SEC", 0.0)
    c = _client()
    _verified_user(c)
    t0 = time.monotonic()
    resp = _forgot(c)
    dt = time.monotonic() - t0
    assert resp.status_code == 202
    assert dt < 0.45, f"request awaited the mailer ({dt:.2f}s)"
    # the delivery still lands once the lane drains
    deadline = time.monotonic() + 5
    while not ConsoleEmailSender.OUTBOX and time.monotonic() < deadline:
        time.sleep(0.05)
    assert ConsoleEmailSender.OUTBOX


def test_missing_pepper_fails_closed_in_production(monkeypatch):
    """In production a missing OTP_PEPPER refuses to issue or verify — the
    dev-pepper fallback must not fail open (reset store and OTP share it)."""
    monkeypatch.delenv("DATAWRAP_OTP_PEPPER", raising=False)
    monkeypatch.delenv("DATAFLOW_OTP_PEPPER", raising=False)
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    from services import platform_config
    assert platform_config.is_production()
    errs = platform_config.validate_production_config()
    assert any("OTP_PEPPER" in e for e in errs)
    with pytest.raises(RuntimeError):
        password_reset_store.issue(email="ada@example.com", ip="", ua="")
    from services import otp_service
    with pytest.raises(RuntimeError):
        otp_service.code_hmac("cid", "signup", "123456")


def test_reset_link_uses_fragment_not_query():
    """The token rides `#token=` — never `?token=` — so it cannot reach
    server/CDN/proxy access logs or a Referer header."""
    c = _client()
    _verified_user(c)
    _forgot(c)
    mail = ConsoleEmailSender.OUTBOX[-1]
    body = mail.text + (mail.html or "")
    assert "reset-password?token=" not in body
    assert "reset-password#token=" in body


def test_delivery_exception_is_audited_and_rerequest_works(monkeypatch):
    """A non-mail exception inside _deliver must not be swallowed by the
    Future: it logs, records password.reset.send_failed, and the account
    holder can immediately re-request a fresh link."""
    events = []
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: events.append((event, kw)))
    calls = {"n": 0}
    real_sender = signup_service.get_email_sender

    class _FlakySender:
        def send(self, mail):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("provider exploded")
            return real_sender().send(mail)

    c = _client()
    _verified_user(c)
    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: _FlakySender())
    r1 = _forgot(c)
    assert r1.status_code == 202  # the reply never reveals the failure
    assert any(e == "password.reset.send_failed" for e, _ in events), events
    # Re-request works right away and a fresh link actually goes out.
    before = len(ConsoleEmailSender.OUTBOX)
    r2 = _forgot(c)
    assert r2.status_code == 202
    assert len(ConsoleEmailSender.OUTBOX) == before + 1
    assert _link_token()


def test_send_failed_event_is_registered():
    from services.audit_coverage import SECURITY_EVENTS
    assert "password.reset.send_failed" in SECURITY_EVENTS


def test_forgot_fake_and_real_replies_share_one_shape():
    """Enumeration parity in the body: fake and real 202s are byte-identical
    in shape — same keys and the same challenge_id form (prt-<24 hex>)."""
    c = _client()
    _verified_user(c)
    fake = _forgot(c, "ghost@example.com").json()
    real = _forgot(c).json()
    assert set(fake) == set(real) == {"ok", "challenge_id",
                                    "resend_after_sec", "expires_in_sec"}
    for body in (fake, real):
        assert re.fullmatch(r"prt-[0-9a-f]{24}", body["challenge_id"]), body
        assert body["ok"] is True
    assert fake["challenge_id"] != real["challenge_id"]


def test_real_branch_stays_under_the_floor(monkeypatch):
    """The fake branch can only be padded DOWN to the floor if the real
    branch finishes its work (issue + store write, mail mocked) under it;
    measure the real path with padding disabled."""
    monkeypatch.setattr(signup_service, "FORGOT_FLOOR_SEC", 0.0)
    real_send = signup_service._send
    monkeypatch.setattr(signup_service, "_send",
                        lambda kind, *a, **kw: None if kind == "reset_link"
                        else real_send(kind, *a, **kw))  # mail already off-path
    c = _client()
    _verified_user(c)
    started = time.monotonic()
    r = _forgot(c)
    elapsed = time.monotonic() - started
    assert r.status_code == 202
    # Generous headroom vs the 0.25s production floor.
    assert elapsed < 0.2, f"real branch took {elapsed:.3f}s"


def test_forgot_floor_parks_coroutines_not_workers(monkeypatch):
    """The pacing floor must be awaited on the event loop, not slept on a
    pool worker: a flood of concurrent forgot requests (more than the
    default executor's workers) all finish near the floor, and unrelated
    to_thread work is not queued behind sleeping pads."""
    import asyncio
    import httpx

    monkeypatch.setattr(signup_service, "FORGOT_FLOOR_SEC", 0.3)

    from src.routers.signup_router import router
    from services.api_errors import ApiError, api_error_handler
    from src.middleware.auth_middleware import AuthMiddleware

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.add_middleware(AuthMiddleware)
    app.include_router(router, prefix="/api/v1")

    async def run():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
                transport=transport, base_url="http://t") as ac:
            n = 64  # well over any default-executor worker count
            tokens = [form_token_mod.issue("reset") for _ in range(n)]
            t0 = time.monotonic()
            reqs = [ac.post("/api/v1/auth/password/forgot", json={
                "email": f"u{i}@example.com",
                "form_token": tk}) for i, tk in enumerate(tokens)]

            async def probe():
                # unrelated to_thread work launched mid-flood: if the pads
                # were pool sleeps, this queues behind waves of them.
                await asyncio.sleep(0.05)
                t1 = time.monotonic()
                await asyncio.to_thread(lambda: None)
                return time.monotonic() - t1

            rs = await asyncio.gather(*reqs, probe(), probe(), probe())
            total = time.monotonic() - t0
            # Min over probes: suite-load event-loop stalls inflate any single
            # sample, but true worker pinning delays EVERY probe behind the
            # pad-length flood (≥ FORGOT_FLOOR_SEC).
            probe_dt = min(rs[-3:])
            assert all(r.status_code == 202 for r in rs[:-3])
            assert probe_dt < 0.25, (
                f"unrelated to_thread waited {probe_dt:.2f}s — pads are "
                "pinning pool workers")
            assert total < 2.5, (
                f"{n} concurrent pads took {total:.2f}s — pads are not "
                "concurrent coroutine sleeps")
    asyncio.run(run())


def test_provider_timeout_is_bounded_and_send_failed_audited(monkeypatch):
    """The serialized mail lane (max_workers=1) can head-of-line-block: a
    stalled provider call must be bounded by a short timeout, the NEXT mail
    must still go out, and the failure must audit password.reset.send_failed."""
    import concurrent.futures
    from services.email_sender import (
        EmailProviderError, _RESEND_TIMEOUT_SEC)

    # Provider calls are bounded well under any human-perceivable delay so one
    # hung Resend socket can't stall every reset/OTP mail behind it.
    assert _RESEND_TIMEOUT_SEC <= 10.0

    events = []
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: events.append((event, kw)))
    futures = []
    lane = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(
        signup_service, "_deliver_later",
        lambda fn: futures.append(lane.submit(fn)))

    sends: list[str] = []

    class _StallOnce:
        def send(self, mail):
            sends.append(mail.to)
            if len(sends) == 1:
                # Simulates urllib's timeout firing on a hung provider socket.
                time.sleep(0.2)
                raise EmailProviderError("resend timeout", retryable=True)

    c = _client()
    _verified_user(c, "one@example.com")
    _verified_user(c, "two@example.com")
    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: _StallOnce())
    r1 = _forgot(c, "one@example.com")
    r2 = _forgot(c, "two@example.com")
    assert r1.status_code == 202 and r2.status_code == 202
    concurrent.futures.wait(futures, timeout=10)
    lane.shutdown()
    # Serialized lane drained: the stalled send failed, the next mail went out.
    assert sends == ["one@example.com", "two@example.com"]
    kinds = [e for e, _ in events]
    assert kinds.count("password.reset.send_failed") == 1, events
    assert kinds.count("password.reset.sent") == 1, events


def test_outbox_survives_restart_and_drains(monkeypatch, tmp_path):
    """Mail lost on restart is no longer silent: the row is persisted BEFORE
    the lane sees it, so a crash leaves status 'queued' and the startup drain
    re-sends it (idempotency key dedupes at the provider)."""
    from services import mail_outbox

    # Per-test outbox: the module-scoped data dir is shared across this file,
    # so rows queued by earlier tests would skew the absolute counts below.
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    c = _client()
    _verified_user(c)

    # Simulate a process dying between enqueue and send: hold the lane.
    real_deliver = signup_service._deliver_later
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    r = _forgot(c)
    assert r.status_code == 202
    rows = mail_outbox.queued()
    assert len(rows) == 1 and rows[0]["purpose"] == "password.reset", rows
    assert rows[0]["status"] == "queued"

    # "Restart": the drain re-submits queued rows through the lane; the
    # _isolate fixture's inline runner completes the send synchronously.
    monkeypatch.setattr(signup_service, "_deliver_later", real_deliver)
    before = len(ConsoleEmailSender.OUTBOX)
    assert signup_service.recover_mail_outbox() == 1
    assert len(ConsoleEmailSender.OUTBOX) == before + 1
    cts = mail_outbox.counts()
    assert cts["queued"] == 0, cts
    assert _reset_statuses(tmp_path) == ["sent"]
    # 'queued but never sent' is visible in admin metrics.
    assert signup_service.signup_metrics()["mail_outbox"] == cts


def test_outbox_row_marked_failed_on_send_error(monkeypatch, tmp_path):
    """A permanent send failure marks the persisted row failed (not queued)
    and audits send_failed — the admin surface can tell the difference."""
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    events = []
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: events.append((event, kw)))

    class _AlwaysBroke:
        def send(self, mail):
            raise EmailProviderError("permanent 4xx", retryable=False)

    from services.email_sender import EmailProviderError

    c = _client()
    _verified_user(c)
    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: _AlwaysBroke())
    r = _forgot(c)
    assert r.status_code == 202
    cts = mail_outbox.counts()
    assert cts["queued"] == 0 and cts["failed"] == 1, cts
    assert any(e == "password.reset.send_failed" for e, _ in events)


def test_transient_send_retries_then_terminal_failed(monkeypatch, tmp_path):
    """Item 4: a transient provider error retries with backoff; once the
    retry bound is hit the row goes terminal failed — never silently lost
    and never retried forever."""
    import time
    from services import mail_outbox
    from services.email_sender import EmailProviderError

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(signup_service, "_RETRY_BASE_SEC", 0.05)
    monkeypatch.setattr(signup_service, "_MAX_SEND_RETRIES", 2)
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())

    events = []
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: events.append((event, kw)))
    calls = []

    class _AlwaysTransient:
        def send(self, mail):
            calls.append(mail.to)
            raise EmailProviderError("timeout", retryable=True)

    c = _client()
    _verified_user(c)
    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: _AlwaysTransient())
    assert _forgot(c).status_code == 202
    time.sleep(0.4)  # let the timer-driven retries run
    # initial + 2 retries = 3 send attempts, then terminal failed
    assert len(calls) == 3, calls
    cts = mail_outbox.counts()
    assert cts["failed"] == 1 and cts["queued"] == 0 and cts["sending"] == 0, cts
    kinds = [e for e, _ in events]
    assert kinds.count("password.reset.send_failed") == 3


def test_transient_retry_can_recover(monkeypatch, tmp_path):
    """A transient failure that clears on retry ends 'sent' — the whole
    point of retrying instead of marking failed on the first timeout."""
    import time
    from services import mail_outbox
    from services.email_sender import EmailProviderError

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(signup_service, "_RETRY_BASE_SEC", 0.05)
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())

    calls = []

    class _FailOnce:
        def send(self, mail):
            calls.append(mail.to)
            if len(calls) == 1:
                raise EmailProviderError("timeout", retryable=True)

    c = _client()
    _verified_user(c)
    monkeypatch.setattr(signup_service, "get_email_sender", lambda: _FailOnce())
    assert _forgot(c).status_code == 202
    time.sleep(0.3)
    assert len(calls) == 2, calls
    assert _reset_statuses(tmp_path) == ["sent"]


def test_quota_error_honors_retry_after(monkeypatch, tmp_path):
    """EmailQuotaExceeded carries retry_after_sec; the backoff must honor
    it (the row is gated by next_attempt_at, not retried immediately)."""
    from services import mail_outbox
    from services.email_sender import EmailQuotaExceeded

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    c = _client()
    _verified_user(c)
    # Force the provider's own hint path: sender raises quota with a hint.
    class _Quota:
        def send(self, mail):
            raise EmailQuotaExceeded("day cap", retry_after_sec=120)

    monkeypatch.setattr(signup_service, "get_email_sender", lambda: _Quota())
    # consume_send_slot runs before send; reset quota so the raise is the
    # sender's, not the slot checker's.
    from services import email_quota
    email_quota._reset_backends()
    assert _forgot(c).status_code == 202
    row = mail_outbox.queued()[0]
    import json
    doc = json.loads((tmp_path / "mail_outbox.json").read_text())
    stored = doc["rows"][row["id"]]
    assert stored["status"] == "queued" and stored["attempts"] == 1
    # next_attempt_at sits ~120s out — gated, not immediate
    assert stored["next_attempt_at"] > stored["created_at"]


def test_outbox_never_stores_the_raw_token(monkeypatch, tmp_path):
    """Item 1 (security): the reset link contains the raw token; persisting the
    rendered body in mail_outbox.json/Mongo would undo the HMAC-only design.
    At rest the body is vault-sealed, and a terminal mark drops it entirely."""
    import json
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    c = _client()
    _verified_user(c)
    sent_before = len(ConsoleEmailSender.OUTBOX)
    r = _forgot(c)
    assert r.status_code == 202

    # The delivered mail carries the link; the stored row must not.
    assert len(ConsoleEmailSender.OUTBOX) == sent_before + 1
    mail = ConsoleEmailSender.OUTBOX[-1]
    token = mail.text.split("#token=", 1)[-1].split()[0].rstrip(".)")
    assert len(token) >= 32

    store_file = tmp_path / "mail_outbox.json"
    raw = store_file.read_text() if store_file.exists() else ""
    assert token not in raw
    doc = json.loads(raw)
    rows = doc.get("rows", {})
    assert rows, "outbox row expected"
    row = next(iter(rows.values()))
    assert row["status"] == "sent"
    # Terminal row: no body fields at all — nothing left at rest.
    for key in ("text", "html", "text_enc", "html_enc", "enc"):
        assert key not in row

    # A still-queued row keeps only ciphertext.
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    r2 = _forgot(c, email="ada@example.com")
    assert r2.status_code == 202
    raw2 = store_file.read_text()
    assert token not in raw2
    queued_rows = [v for v in json.loads(raw2)["rows"].values()
                   if v.get("status") == "queued"]
    assert queued_rows
    for q in queued_rows:
        assert q.get("enc") == "secret_vault"
        token_q = q.get("text") or q.get("html")
        assert token_q is None  # no plaintext body fields on the row


def test_drain_never_sends_dead_reset_links(monkeypatch, tmp_path):
    """Item 2: a queued reset mail whose token was superseded or expired
    before the drain must be marked (not sent) and counted in metrics."""
    import json
    from services import mail_outbox, password_reset_store

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    c = _client()
    _verified_user(c)

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    assert _forgot(c).status_code == 202   # row A queued, token A live
    assert _forgot(c).status_code == 202   # row B queued, token A superseded

    rows = [r for r in mail_outbox.queued()
            if r["purpose"] == "password.reset"]
    assert len(rows) == 2

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    before = len(ConsoleEmailSender.OUTBOX)
    assert signup_service.recover_mail_outbox() == 1   # only the live row sent
    assert len(ConsoleEmailSender.OUTBOX) == before + 1

    assert _reset_statuses(tmp_path) == ["sent", "superseded"]


def test_drain_marks_expired_reset_links(monkeypatch, tmp_path):
    """An expired-token row drains as 'expired', never leaves the outbox."""
    from services import mail_outbox, password_reset_store
    from datetime import UTC, datetime, timedelta

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    c = _client()
    _verified_user(c)

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    assert _forgot(c).status_code == 202
    assert len(mail_outbox.queued()) == 1

    real_now = password_reset_store._now
    monkeypatch.setattr(
        password_reset_store, "_now",
        lambda: real_now() + password_reset_store.TOKEN_TTL + timedelta(seconds=1))

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    before = len(ConsoleEmailSender.OUTBOX)
    assert signup_service.recover_mail_outbox() == 0
    assert len(ConsoleEmailSender.OUTBOX) == before  # nothing sent
    assert _reset_statuses(tmp_path) == ["expired"]


def test_concurrent_drains_send_each_row_once(monkeypatch, tmp_path):
    """Item 3 (multi-replica): two drains racing the same queued row must
    produce exactly one send — the queued->sending claim is atomic."""
    import threading
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    # Fresh send quota — earlier tests in this module already consumed slots.
    from services import email_quota
    email_quota._reset_backends()

    c = _client()
    _verified_user(c)

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    assert _forgot(c).status_code == 202
    rows = mail_outbox.queued()
    assert len(rows) == 1


    def deliver(r):
        signup_service._deliver_outbox_row(r)

    # Two "replicas" racing the same row through the real claim path.
    before = len(ConsoleEmailSender.OUTBOX)
    t1 = threading.Thread(target=deliver, args=(rows[0],))
    t2 = threading.Thread(target=deliver, args=(rows[0],))
    t1.start(); t2.start(); t1.join(); t2.join()

    # Exactly one send total (both threads raced; claim admitted one).
    assert len(ConsoleEmailSender.OUTBOX) == before + 1
    cts = mail_outbox.counts()
    assert cts["queued"] == 0 and cts["sending"] == 0, cts
    assert _reset_statuses(tmp_path) == ["sent"]


def test_stale_lease_is_reclaimed(monkeypatch, tmp_path):
    """A row stuck 'sending' past its lease (replica died mid-send) becomes
    claimable again so the next drain can finish it."""
    import json
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    row = mail_outbox.enqueue(
        to="a@x.io", subject="s", text="t", html="h",
        idempotency_key="k", purpose="password.reset")
    assert mail_outbox.claim(row["id"]) is not None
    # Second immediate claim loses (live lease).
    assert mail_outbox.claim(row["id"]) is None
    # Expire the lease in place — simulates a dead replica.
    f = tmp_path / "mail_outbox.json"
    doc = json.loads(f.read_text())
    doc["rows"][row["id"]]["lease_until"] = "2000-01-01T00:00:00+00:00"
    f.write_text(json.dumps(doc))
    assert mail_outbox.claim(row["id"]) is not None


def test_mongo_index_setup_runs_once_and_fails_loud(monkeypatch):
    """Item 5: create_index ran on EVERY outbox call (two round trips per
    op) and a failure silently fell back to the file tier — splitting the
    queue across replicas. Index setup is now one-time, and a failure
    raises instead of quietly changing tiers."""
    from services import mail_outbox

    calls = []

    class _Coll:
        def create_index(self, *a, **kw):
            calls.append((a, kw))

    class _Db:
        def __getitem__(self, name):
            return _Coll()

    monkeypatch.setattr(mail_outbox, "_INDEXES_READY", False)
    monkeypatch.setattr(mail_outbox, "mongo_database", lambda: _Db())
    mail_outbox._database()
    mail_outbox._database()
    mail_outbox._database()
    # two indexes, created exactly once across the three calls
    assert len(calls) == 2, calls

    class _BadColl:
        def create_index(self, *a, **kw):
            raise RuntimeError("auth failed")

    class _BadDb:
        def __getitem__(self, name):
            return _BadColl()

    monkeypatch.setattr(mail_outbox, "_INDEXES_READY", False)
    monkeypatch.setattr(mail_outbox, "mongo_database", lambda: _BadDb())
    try:
        mail_outbox._database()
        assert False, "expected raise, not silent file fallback"
    except RuntimeError:
        pass


def test_file_outbox_prunes_expired_rows_on_write(monkeypatch, tmp_path):
    """Item 6: the file tier never expired anything — the Mongo TTL was the
    only bound. enqueue now prunes rows past expires_at on write."""
    import json
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    f = tmp_path / "mail_outbox.json"
    f.write_text(json.dumps({"rows": {
        "old-sent": {"id": "old-sent", "status": "sent",
                     "expires_at": "2020-01-01T00:00:00+00:00"},
        "old-queued": {"id": "old-queued", "status": "queued",
                       "expires_at": "2020-01-01T00:00:00+00:00"},
        "live": {"id": "live", "status": "sent",
                 "expires_at": "2999-01-01T00:00:00+00:00"},
        "legacy": {"id": "legacy", "status": "sent"},  # no expires_at
    }}))
    mail_outbox.enqueue(to="a@x.io", subject="s", text="t", html="h",
                        idempotency_key="k", purpose="password.reset")
    doc = json.loads(f.read_text())
    ids = set(doc["rows"])
    assert "old-sent" not in ids and "old-queued" not in ids
    assert "live" in ids and "legacy" in ids
    assert len(ids) == 3  # live + legacy + the new row


def test_signup_code_rides_the_outbox(monkeypatch, tmp_path):
    """Item 7: every auth mail — not just reset links — is persisted sealed
    and delivered through the outbox lane, so restart durability, atomic
    claim and bounded retries are uniform."""
    import json
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    events = []
    monkeypatch.setattr(signup_service, "record_security_event",
                        lambda event, **kw: events.append((event, kw)))

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)
    c = _client()
    c.post("/api/v1/auth/signup", json={
        "email": "be@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})

    rows = mail_outbox.queued()
    assert len(rows) == 1 and rows[0]["purpose"] == "signup_code", rows
    assert rows[0]["quota_counted"] is True   # slot taken on request path
    assert "text" not in rows[0] and rows[0]["enc"] == "secret_vault"
    # the raw 6-digit code is sealed, never plaintext at rest
    doc_text = (tmp_path / "mail_outbox.json").read_text()
    assert not re.search(r"\b\d{6}\b", json.loads(doc_text)["rows"][rows[0]["id"]]["text_enc"])

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    assert signup_service.recover_mail_outbox() == 1
    assert [e for e, _ in events].count("mail.sent") == 1
    assert all("password.reset" not in e for e, _ in events)
    assert _code_mail()  # the code actually delivered


def test_dead_otp_code_row_is_skipped(monkeypatch, tmp_path):
    """A queued OTP mail whose challenge was consumed or superseded by a
    resend drains as superseded — the user never gets a dead code."""
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)

    c = _client()
    r = c.post("/api/v1/auth/signup", json={
        "email": "bo@example.com", "password": "a-very-long-password",
        "form_token": form_token_mod.issue("signup"), "device_id": "d1"})

    rows = mail_outbox.queued()
    assert len(rows) == 1 and rows[0]["purpose"] == "signup_code"
    # Extract the code from the sealed row itself, then consume the
    # challenge — the queued mail is now a dead code.
    code = re.search(r"\b(\d{6})\b",
                     mail_outbox.body(rows[0])["text"]).group(1)
    resp = c.post("/api/v1/auth/verify-email",
                  json={"challenge_id": r.json()["challenge_id"],
                        "code": code, "device_id": "d1"})
    assert resp.status_code == 200

    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    before = len(ConsoleEmailSender.OUTBOX)
    assert signup_service.recover_mail_outbox() == 0
    assert len(ConsoleEmailSender.OUTBOX) == before
    cts = mail_outbox.counts()
    assert cts["superseded"] == len(rows), cts


def test_status_by_id_mongo_looks_up_id_field_not_objectid(monkeypatch):
    """Mongo docs keep the token id under ``id`` (the store sets no ``_id``),
    so a ``{"_id": token_id}`` query misses every real row — which would mark
    every live reset row 'invalid'/dead on the Mongo tier."""
    captured = {}

    class _Coll:
        def find_one(self, filt, *a, **k):
            captured["filter"] = filt
            return None

    class _Db:
        name = "datatransfer"

        def __getitem__(self, name):
            return _Coll()

    monkeypatch.setattr(password_reset_store, "_database", lambda: _Db())
    assert password_reset_store.status_by_id("prt-abc") == "invalid"
    assert captured["filter"] == {"id": "prt-abc"}


def test_mongo_naive_bson_expires_at_never_crashes_inspect(monkeypatch):
    """pymongo returns BSON datetimes as naive (UTC by convention); the
    active-doc compare used to raise TypeError: can't compare offset-naive
    and offset-aware datetimes — the reset-inspect route 500'd on Mongo."""
    from datetime import datetime, timedelta

    naive_future = datetime.now(UTC).replace(tzinfo=None) + timedelta(days=1)
    token, doc = password_reset_store.issue(
        email="ada@example.com", ip="127.0.0.1", ua="pytest")
    stored = dict(doc)
    stored["expires_at"] = naive_future  # the shape pymongo hands back

    monkeypatch.setattr(password_reset_store, "_find",
                        lambda _h: stored)
    assert password_reset_store.inspect(token) == "ok"
    parsed = password_reset_store._parse_ts(naive_future)
    assert parsed is not None and parsed.tzinfo is not None


def test_resend_cooldown_handles_naive_bson_last_sent(monkeypatch):
    """A challenge whose last_sent_at arrives as a naive BSON datetime
    (Mongo tier) must not crash the resend cooldown and must still enforce
    it — naive values are UTC, never host-local time."""
    from datetime import datetime, timedelta

    naive_recent = (datetime.now(UTC).replace(tzinfo=None)
                    - timedelta(seconds=5))

    # The file-level fixture zeroes the cooldown for speed — restore it so
    # the naive-datetime path is exercised at the real threshold.
    monkeypatch.setattr(signup_service, "RESEND_COOLDOWN_SEC", 60)
    monkeypatch.setattr(signup_service.signup_store, "get_challenge",
                        lambda _cid: {"subject_id": "s-1",
                                      "last_sent_at": naive_recent})
    with pytest.raises(ApiError) as exc:
        signup_service.resend_code(challenge_id="c-1")
    assert exc.value.status_code == 429
    assert exc.value.code == "resend_too_soon"


def test_send_renews_lease_so_second_claim_never_wins(monkeypatch, tmp_path):
    """Review-12 item 1: a provider call slower than the original 60s
    claim window must not let a second replica reclaim and re-send the
    row. The lane renews the lease under its fencing token immediately
    before send(), so claim() loses for the whole send — and forever
    after, because the row lands terminal."""
    import threading
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: None)

    row = mail_outbox.enqueue(
        to="slow@x.io", subject="s", text="t", html="h",
        idempotency_key="k", purpose="password.reset",
        quota_counted=True)

    started = threading.Event()
    release = threading.Event()

    class _SlowSender:
        name = "slow"

        def send(self, email):
            started.set()
            release.wait(5)
            return {"ok": True, "provider": "slow"}

    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: _SlowSender())

    t = threading.Thread(target=signup_service._deliver_outbox_row,
                         args=(row,))
    t.start()
    try:
        assert started.wait(5), "send never entered the provider call"
        # The send is in flight under a renewed lease — a racing drain
        # cannot take the row, no matter how long the provider stalls
        # inside its own timeout.
        assert mail_outbox.claim(row["id"]) is None
    finally:
        release.set()
        t.join(10)
    # Terminal now — never claimable again.
    assert mail_outbox.claim(row["id"]) is None
    assert mail_outbox.counts()["sent"] == 1


def test_lease_fencing_blocks_a_stale_claimer(monkeypatch, tmp_path):
    """The fencing token stops a stale claimer whose lease lapsed and was
    reclaimed: its extend_lease/mark/reschedule writes must not apply to
    the row the second replica now owns."""
    import json
    from datetime import datetime
    from services import mail_outbox

    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)

    row = mail_outbox.enqueue(
        to="a@x.io", subject="s", text="t", html="h",
        idempotency_key="k", purpose="password.reset")
    stale = mail_outbox.claim(row["id"], lease_sec=60)
    assert stale is not None

    # Replica A's lease lapses; replica B claims and gets a new token.
    f = tmp_path / "mail_outbox.json"
    doc = json.loads(f.read_text())
    doc["rows"][row["id"]]["lease_until"] = "2000-01-01T00:00:00+00:00"
    f.write_text(json.dumps(doc))
    live = mail_outbox.claim(row["id"])
    assert live is not None and live != stale

    # The stale claimer cannot extend its old claim…
    assert mail_outbox.extend_lease(row["id"], stale) is False
    # …nor stamp its outcome over B's in-flight claim.
    mail_outbox.mark(row["id"], "failed", claim_token=stale)
    mail_outbox.reschedule(row["id"], 5, "stale", claim_token=stale)
    doc = json.loads(f.read_text())["rows"][row["id"]]
    assert doc["status"] == "sending"
    assert doc["claim_token"] == live
    assert datetime.fromisoformat(doc["lease_until"]) > datetime.now(UTC)

    # The live claimer can extend and complete normally.
    assert mail_outbox.extend_lease(row["id"], live) is True
    mail_outbox.mark(row["id"], "sent", claim_token=live)
    doc = json.loads(f.read_text())["rows"][row["id"]]
    assert doc["status"] == "sent"


def test_provider_timeouts_stay_under_the_lease():
    """Review-12 item 1: one provider call must fit inside the claim lease
    with margin — otherwise a send could outlive its own claim. Covers
    both transactional drivers; if a timeout is ever raised past the
    lease this fails loudly instead of double-sending in production."""
    from services import mail_outbox
    from services.email_sender import _RESEND_TIMEOUT_SEC
    from services.notification_service import _SMTP_TIMEOUT_SEC

    assert _RESEND_TIMEOUT_SEC < mail_outbox._LEASE_SEC / 2
    assert _SMTP_TIMEOUT_SEC < mail_outbox._LEASE_SEC


def test_forgot_with_turnstile_configured_verifies_instead_of_crashing(monkeypatch):
    """With a Turnstile secret set, forgot-password passed turnstile.verify()
    an unknown ``user_agent`` keyword: TypeError, a 500 for every reset."""
    c = _client()
    _verified_user(c)
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "ts-secret")

    class _Resp:
        def json(self):
            return {"success": True, "hostname": "datawrap.io", "action": ""}

    seen = []
    monkeypatch.setattr("services.turnstile.httpx.post",
                        lambda url, data=None, timeout=None: seen.append(data) or _Resp())
    resp = c.post("/api/v1/auth/password/forgot",
                  json={"email": "ada@example.com",
                        "form_token": form_token_mod.issue("reset"),
                        "turnstile_token": "tok-1"})
    assert resp.status_code == 202, resp.text
    assert seen and seen[0]["response"] == "tok-1"
    from tests.helpers.mail_wait import await_outbox

    mails = await_outbox("ada@example.com", min_count=2)  # signup code + reset link
    assert any("reset-password#token=" in m.text for m in mails)
