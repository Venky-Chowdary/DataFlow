"""QA1 email findings on cleanup-stories (QA_REPORT_EMAILS_371).

F1  a row in retry backoff when the API restarted was never sent: the drain ran
    once at boot and only claims due rows.
F2  invite copy read "invited you to the Sam Signup's workspace workspace".
F3  the SMTP sender refused a loopback sink even when DATAFLOW_EGRESS_ALLOWLIST
    admitted it.
F4  the email said "7 days" while the Team page said "Expires in 6 days".
F6  the signup response must carry the entered name the sidebar shows.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

import services.signup_service as signup_service
from services import mail_outbox
from services.email_templates import render_email


class _Recorder:
    def __init__(self) -> None:
        self.sent: list = []

    def send(self, message) -> None:
        self.sent.append(message)


@pytest.fixture
def file_outbox(monkeypatch, tmp_path):
    monkeypatch.setattr(mail_outbox, "_database", lambda: None)
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    sender = _Recorder()
    monkeypatch.setattr(signup_service, "get_email_sender", lambda: sender)
    monkeypatch.setattr(signup_service, "consume_send_slot", lambda: None)
    monkeypatch.setattr(signup_service, "_deliver_later", lambda fn: fn())
    # An app started by an earlier test leaves its 30 s sweeper running.
    signup_service.stop_mail_outbox_sweeper()
    yield sender
    signup_service.stop_mail_outbox_sweeper()


def _row_status(row_id: str) -> str:
    rows = {r["id"]: r for r in mail_outbox.queued()}
    if row_id in rows:
        return "queued"
    doc = mail_outbox.load_json_doc(mail_outbox._path(), {"rows": {}})
    return str(doc["rows"][row_id]["status"])


def test_f1_row_in_backoff_at_restart_is_sent_once_due(file_outbox):
    row = mail_outbox.enqueue(
        to="qasignup371@example.com", subject="Welcome", text="hi",
        html="<p>hi</p>", idempotency_key="welcome:qasignup371@example.com",
        purpose="welcome", quota_counted=True)
    token = mail_outbox.claim(row["id"])
    assert token
    # A transient SMTP failure put the row in backoff; then the API restarted,
    # so the in-memory retry timer is gone.
    mail_outbox.reschedule(row["id"], 0.6, "SMTP send failed", claim_token=token)

    assert signup_service.recover_mail_outbox() == 0  # boot drain: not due yet
    signup_service.start_mail_outbox_sweeper(interval_sec=0.1)

    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and _row_status(row["id"]) != "sent":
        time.sleep(0.05)
    assert _row_status(row["id"]) == "sent"
    assert [m.to for m in file_outbox.sent] == ["qasignup371@example.com"]


def test_f1_sweeper_never_double_sends_a_due_row(file_outbox):
    row = mail_outbox.enqueue(
        to="once@example.com", subject="s", text="t", html="<p>t</p>",
        idempotency_key="welcome:once@example.com", purpose="welcome",
        quota_counted=True)
    signup_service.start_mail_outbox_sweeper(interval_sec=0.05)
    assert signup_service.recover_mail_outbox() in (0, 1)
    time.sleep(0.4)
    assert _row_status(row["id"]) == "sent"
    assert len(file_outbox.sent) == 1


def test_f1_repeated_sweeps_do_not_pile_duplicate_lane_tasks(file_outbox, monkeypatch):
    row = mail_outbox.enqueue(
        to="slow@example.com", subject="s", text="t", html="<p>t</p>",
        idempotency_key="welcome:slow@example.com", purpose="welcome",
        quota_counted=True)
    lane: list = []  # a slow provider: queued tasks have not run yet
    monkeypatch.setattr(signup_service, "_deliver_later", lane.append)
    signup_service.recover_mail_outbox()
    signup_service.recover_mail_outbox()
    signup_service.recover_mail_outbox()
    assert len(lane) == 1
    lane.pop()()
    assert _row_status(row["id"]) == "sent" and len(file_outbox.sent) == 1


def _invite(workspace: str) -> dict:
    return render_email("workspace_invite", {
        "inviter_name": "Sam Signup", "inviter_email": "sam@example.com",
        "workspace_name": workspace, "role": "editor",
        "accept_url": "https://app.example.com/accept-invite#token=t",
        "expires_days": 7})


@pytest.mark.parametrize("part", ["html", "text"])
def test_f2_name_ending_in_workspace_is_not_doubled(part):
    mail = _invite("Sam Signup's workspace")
    body = getattr(mail, part) if not isinstance(mail, dict) else mail[part]
    assert "workspace workspace" not in body
    assert "invited you to Sam Signup" in body.replace("&#x27;", "'").replace("&#39;", "'")


def test_f2_plain_name_keeps_the_noun():
    mail = _invite("Acme Analytics")
    text = mail.text if not isinstance(mail, dict) else mail["text"]
    assert "invited you to the Acme Analytics workspace." in text


class _FakeSMTP:
    sent: list = []

    def __init__(self, host, port, timeout=None):
        self.host = host

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def starttls(self):
        return None

    def login(self, *_a):
        return None

    def send_message(self, msg):
        _FakeSMTP.sent.append((self.host, msg["To"]))


def _smtp(host: str) -> dict:
    from services.notification_service import _smpt_send

    return _smpt_send(["qa@example.com"], "hi", "body",
                      smtp_cfg={"host": host, "port": 1025, "from": "x@example.com",
                                "use_tls": False})


def test_f3_loopback_sink_allowed_only_through_the_shared_allowlist(monkeypatch):
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    _FakeSMTP.sent = []
    monkeypatch.delenv("DATAFLOW_EGRESS_ALLOWLIST", raising=False)
    monkeypatch.delenv("DATAWRAP_EGRESS_ALLOWLIST", raising=False)
    refused = _smtp("127.0.0.1")
    assert refused["ok"] is False and "not allowed" in refused["error"]

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    assert _smtp("127.0.0.1") == {"ok": True}
    assert _FakeSMTP.sent == [("127.0.0.1", "qa@example.com")]


def test_f3_metadata_endpoint_stays_refused_even_if_listed(monkeypatch):
    monkeypatch.setattr("smtplib.SMTP", _FakeSMTP)
    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "169.254.0.0/16")
    assert _smtp("169.254.169.254")["ok"] is False


def test_f4_days_round_up_like_the_team_page():
    from services.workspace_invites import invite_days_left

    now = datetime(2026, 11, 8, 12, tzinfo=UTC)
    fresh = now + timedelta(days=7) - timedelta(milliseconds=5)
    assert invite_days_left(fresh, now) == 7
    assert invite_days_left(fresh.isoformat(), now) == 7
    assert invite_days_left((now + timedelta(days=2, hours=1)).isoformat(), now) == 3
    assert invite_days_left(now + timedelta(minutes=5), now) == 1


def test_f4_resent_invite_email_counts_from_its_own_expiry(monkeypatch):
    import services.workspace_invites as wi

    seen: dict = {}
    monkeypatch.setattr(signup_service, "queue_mail",
                        lambda template, to, ctx, **kw: seen.update(ctx))
    left = datetime.now(UTC) + timedelta(days=3) - timedelta(seconds=30)
    wi._queue_invite_mail({"id": "inv_1", "email": "x@example.com", "role": "viewer",
                           "expires_at": left.isoformat(), "sent_count": 2},
                          "tok", "Acme")
    assert seen["expires_days"] == 3
