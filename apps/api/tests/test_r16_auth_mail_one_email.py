"""QA 2026-11-10: signup OTP / reset mail must be robust and arrive once.

Observed: two "Your Datawrap verification code" mails a minute apart, after the
first did not show up (it landed late or in spam) and the user pressed Resend.
The second carried a new code, so the first one was dead.

- A resend counted twice against the daily send cap (resend_code consumed a
  slot, then queue_mail(quota="request") consumed another), so a busy day hit
  the cap early and every mail was refused until midnight.
- A delivery that failed and was retried consumed a second quota slot: the
  "counted" flag lived only in memory.
- A code email still waiting in the lane when the user pressed Resend was sent
  anyway: two mails for one request.
- The sealed copy of a code (kept so a resend can repeat it) must go once the
  code is used.
- Resend dropped the message headers on the floor.
"""

from __future__ import annotations

import json

import pytest

import services.signup_service as signup_service
from services import mail_outbox, otp_service, signup_store


class _Recorder:
    def __init__(self) -> None:
        self.sent: list = []

    def send(self, message) -> dict:
        self.sent.append(message)
        return {"ok": True, "provider": "recorder"}


@pytest.fixture
def lane(monkeypatch, tmp_path):
    """File outbox, a recording sender, a counted quota and a lane the test runs."""
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setattr(mail_outbox, "_database", lambda: None)
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    sender = _Recorder()
    monkeypatch.setattr(signup_service, "get_email_sender", lambda: sender)
    slots: list[int] = []
    monkeypatch.setattr(signup_service, "consume_send_slot", lambda: slots.append(1))
    tasks: list = []
    monkeypatch.setattr(signup_service, "_deliver_later", tasks.append)
    signup_service.stop_mail_outbox_sweeper()
    yield sender, slots, tasks
    signup_service.stop_mail_outbox_sweeper()


def _status(row_id: str) -> str:
    doc = mail_outbox.load_json_doc(mail_outbox._path(), {"rows": {}})
    return str(doc["rows"][row_id]["status"])


def test_a_code_mail_overtaken_by_a_resend_is_never_sent(lane):
    sender, _slots, tasks = lane
    challenge, code = otp_service.issue("psu_1", "signup")
    first = signup_service.queue_mail(
        "signup_code", "ada@example.com",
        {"name": "Ada", "code": code, "expires_minutes": 10},
        idempotency_key=f"otp:{challenge['id']}:{challenge['sent_count']}",
        quota="prepaid")
    # The user presses Resend while the first mail still waits in the lane.
    updated, same = otp_service.reissue(challenge["id"])
    assert same == code
    second = signup_service.queue_mail(
        "signup_code", "ada@example.com",
        {"name": "Ada", "code": same, "expires_minutes": 10},
        idempotency_key=f"otp:{updated['id']}:{updated['sent_count']}",
        quota="prepaid")
    for task in list(tasks):
        task()
    assert len(sender.sent) == 1, [m.subject for m in sender.sent]
    assert _status(first["id"]) == "superseded"
    assert _status(second["id"]) == "sent"


def test_a_retried_delivery_counts_one_quota_slot(lane, monkeypatch):
    sender, slots, tasks = lane
    from services.email_sender import EmailProviderError

    calls = {"n": 0}

    def _flaky(message):
        calls["n"] += 1
        if calls["n"] == 1:
            raise EmailProviderError("transport", retryable=True)
        return sender.send(message)

    monkeypatch.setattr(signup_service, "get_email_sender",
                        lambda: type("S", (), {"send": staticmethod(_flaky)})())
    monkeypatch.setattr(signup_service, "_retry_delay_sec", lambda exc, attempt: 0.01)
    row = signup_service.queue_mail(
        "welcome", "ada@example.com",
        {"name": "Ada", "workspace_name": "Ada's workspace", "app_url": "https://x/",
         "joined": False, "role": "admin"},
        idempotency_key="welcome:ada@example.com", purpose="welcome",
        quota="lane", once=True)
    tasks.pop(0)()  # first attempt: counted, fails, reschedules
    # The retry, as the sweeper would offer it after a restart: from the store.
    import time

    time.sleep(0.05)
    stored = mail_outbox.load_json_doc(mail_outbox._path(), {"rows": {}})["rows"][row["id"]]
    assert stored.get("quota_counted") is True
    signup_service._deliver_outbox_row(dict(stored))
    assert _status(row["id"]) == "sent"
    assert len(slots) == 1, slots


def test_using_a_code_drops_its_sealed_copy(lane):
    challenge, code = otp_service.issue("psu_1", "signup")
    assert signup_store.get_challenge(challenge["id"])["code_enc"]
    assert otp_service.verify(challenge["id"], code) == "ok"
    rows = signup_store.load_json_doc(signup_store._challenges_path(), {"otp_challenges": []})
    row = next(r for r in rows["otp_challenges"] if r.get("id") == challenge["id"])
    assert row.get("code_enc") == ""
    assert otp_service.live_code(row) is None


def test_a_challenge_from_before_this_change_gets_a_new_code(lane):
    """No sealed code on the row (issued by the previous release): resend issues
    a new code rather than failing."""
    challenge, code = otp_service.issue("psu_1", "signup")
    signup_store._update_challenge(challenge["id"], code_enc="")
    updated, new = otp_service.reissue(challenge["id"])
    assert updated is not None and new and len(new) == 6
    assert otp_service.verify(challenge["id"], new) == "ok"


def test_resend_counts_one_quota_slot(monkeypatch, tmp_path):
    """resend_code takes the slot itself; the send it queues must not take another."""
    from tests.test_resend import _age_last_sent, _client, _signup
    from services import email_quota

    counted: list[int] = []
    real = email_quota.consume_send_slot

    def _count():
        counted.append(1)
        return real()

    monkeypatch.setattr(signup_service, "consume_send_slot", _count)
    # test_resend's isolation fixture is autouse in its own module only.
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_OTP_PEPPER", "test-pepper")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    import services.form_token as form_token_mod
    from services import user_store

    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    monkeypatch.setattr(signup_service.mx_check, "check_domain", lambda d: {"ok": True})
    monkeypatch.setattr(signup_store, "_pending_path", lambda: tmp_path / "pending.json")
    monkeypatch.setattr(signup_store, "_challenges_path", lambda: tmp_path / "challenges.json")
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(mail_outbox, "_database", lambda: None)
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    from services import auth_state
    from services.email_sender import reset_email_sender

    auth_state.reset_auth_state()
    email_quota._reset_backends()
    reset_email_sender()
    c = _client()
    cid = _signup(c, email="quota@example.com")
    before = len(counted)
    _age_last_sent(cid, 61)
    resp = c.post("/api/v1/auth/resend", json={"challenge_id": cid})
    assert resp.status_code == 202, resp.text
    signup_service.drain_mail_lane()
    assert len(counted) - before == 1, counted


def test_resend_api_forwards_message_headers(monkeypatch):
    from services import email_sender

    captured: dict = {}

    def _post(url, payload, headers=None, timeout=0):
        captured.update(payload=payload)
        return {"ok": True, "status": 200, "body": json.dumps({"id": "m1"})}

    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_test")
    import services.notification_service as ns

    monkeypatch.setattr(ns, "_http_post", _post)
    email_sender.ResendEmailSender().send(email_sender.OutboundEmail(
        to="ada@example.com", subject="s", text="t",
        headers={"X-Entity-Ref-ID": "mo_1"}))
    assert captured["payload"]["headers"] == {"X-Entity-Ref-ID": "mo_1"}


def test_a_reset_link_dead_by_its_turn_in_the_lane_is_not_sent(lane, monkeypatch, tmp_path):
    """A reset mail whose link expired (or was replaced) before the lane reached
    it is marked, not sent: the user would only get a link that cannot work."""
    from datetime import timedelta

    from services import password_reset_store

    sender, _slots, tasks = lane
    monkeypatch.setattr(password_reset_store, "_database", lambda: None)
    monkeypatch.setattr(password_reset_store, "_path", lambda: tmp_path / "resets.json")
    monkeypatch.setattr(password_reset_store, "TOKEN_TTL", timedelta(seconds=-1))
    _token, stored = password_reset_store.issue(email="ada@example.com")
    row = mail_outbox.enqueue(
        to="ada@example.com", subject="Reset your password", text="link",
        html="<p>link</p>", idempotency_key=f"reset-link:{stored['id']}",
        purpose="password.reset", quota_counted=True)
    signup_service._deliver_outbox_row(row)
    assert sender.sent == []
    assert _status(row["id"]) == "expired"
