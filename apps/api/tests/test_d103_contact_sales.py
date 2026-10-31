"""D103 — Contact Sales form must deliver, not console.log.

Repro (prod c6c16128): the marketing form showed "sent" while no request
left the browser and nothing reached sales@datawrap.io. Then (ad9cb344) the
form POSTed, but the route sat behind the session gate (401 for every
visitor) and mailed through its own Resend-HTTP/Zoho stack instead of the
configured ``DATAWRAP_EMAIL_PROVIDER``. The lead now goes through the same
durable outbox + sender as every auth mail. No real mail is sent: the
console driver captures, or the SMTP transport is mocked.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_rate_store():
    from src.routers import contact_router

    contact_router._RATE_STORE.clear()
    yield
    contact_router._RATE_STORE.clear()


@pytest.fixture(autouse=True)
def _mail_capture(monkeypatch, tmp_path):
    """Console mail capture on a private file-tier outbox; Turnstile off."""
    from services import email_quota, mail_outbox
    from services.email_sender import ConsoleEmailSender, reset_email_sender

    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setattr(mail_outbox, "data_dir", lambda: tmp_path)
    monkeypatch.setattr(mail_outbox, "mongo_database", lambda: None)
    for name in ("TURNSTILE_SECRET", "TURNSTILE_SITE_KEY"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()


def _persisted(store: list, lead_id: str = "lead-1"):
    def _persist(lead):
        rec = {**lead, "lead_id": lead_id, "created_at": "2026-10-25T00:00:00+00:00"}
        store.append(rec)
        return rec
    return _persist


def _sales_mail():
    from tests.helpers.mail_wait import await_outbox, drain_mail_lane

    drain_mail_lane()
    return await_outbox("sales@datawrap.io")


def _payload(**over):
    base = {
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "company": "Acme Corp",
        "message": "We need a migration pilot.",
        "website": "",
    }
    base.update(over)
    return base


def test_honeypot_accepts_and_drops(client, monkeypatch):
    persisted = []
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted(persisted))

    res = client.post("/api/v1/contact-sales", json=_payload(website="http://spam.example"))
    assert res.status_code == 200 and res.json()["ok"] is True
    from services import mail_outbox

    assert not persisted and not mail_outbox.queued()


def test_valid_lead_persists_and_mails_sales_through_outbox(client, monkeypatch):
    persisted = []
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted(persisted))

    res = client.post("/api/v1/contact-sales", json=_payload(sources=["Oracle"]))
    assert res.status_code == 200, res.text
    body = res.json()
    assert body == {"ok": True, "lead_id": "lead-1", "queued": True}
    assert persisted[0]["email"] == "ada@example.com"

    mails = _sales_mail()
    assert len(mails) == 1
    mail = mails[0]
    assert mail.reply_to == "ada@example.com"
    assert "Acme Corp" in mail.subject
    assert "We need a migration pilot." in mail.text
    assert "Oracle" in mail.text and "lead-1" in mail.text
    assert mail.idempotency_key == "sales-lead:lead-1"


def test_lead_uses_configured_provider_not_a_second_stack(client, monkeypatch):
    """Prod runs DATAWRAP_EMAIL_PROVIDER=smtp with a Resend key also set; the
    lead must follow the provider setting like every other mail."""
    from services.email_sender import reset_email_sender

    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "smtp")
    monkeypatch.setenv("DATAWRAP_RESEND_API_KEY", "re_test_key")
    reset_email_sender()
    smtp_calls = []
    monkeypatch.setattr(
        "services.notification_service._smpt_send",
        lambda recipients, subject, body, cfg=None, **kw: smtp_calls.append(
            {"to": recipients, "subject": subject, **kw}) or {"ok": True},
    )
    monkeypatch.setattr(
        "services.notification_service._http_post",
        lambda *a, **k: pytest.fail("Resend HTTP must not be used when provider=smtp"),
    )
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([]))

    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200, res.text
    from tests.helpers.mail_wait import drain_mail_lane

    drain_mail_lane()
    assert smtp_calls and smtp_calls[0]["to"] == ["sales@datawrap.io"]
    assert smtp_calls[0]["reply_to"] == "ada@example.com"


def test_send_failure_is_recorded_on_the_outbox_and_never_loses_the_lead(client, monkeypatch):
    from services import mail_outbox
    from services.email_sender import EmailProviderError

    class _Refusing:
        name = "refusing"

        def send(self, email):
            raise EmailProviderError("Resend send rejected (status=403)", retryable=False, status=403)

    monkeypatch.setattr("services.signup_service.get_email_sender", lambda: _Refusing())
    persisted = []
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted(persisted, "lead-2"))

    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200 and res.json()["ok"] is True
    assert persisted and persisted[0]["lead_id"] == "lead-2"
    from tests.helpers.mail_wait import drain_mail_lane

    drain_mail_lane()
    assert mail_outbox.counts().get("failed") == 1


def test_queue_outage_still_returns_the_persisted_lead(client, monkeypatch):
    def _down(**kw):
        raise RuntimeError("outbox store down")

    monkeypatch.setattr("services.mail_outbox.enqueue", _down)
    persisted = []
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted(persisted, "lead-3"))
    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200
    assert res.json() == {"ok": True, "lead_id": "lead-3", "queued": False}
    assert persisted


def test_anonymous_visitor_is_not_refused_by_the_session_gate(client, monkeypatch):
    """The form is on the public site: with auth enforced (production) a
    visitor without a session must still reach the route."""
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: True)
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([]))
    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200, res.text
    assert client.get("/api/v1/contact-sales/config").status_code == 200


def test_subject_strips_crlf(client, monkeypatch):
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([], "h1"))
    res = client.post(
        "/api/v1/contact-sales",
        json=_payload(company="Acme\r\nBcc: attacker@evil.example"),
    )
    assert res.status_code == 200
    mails = _sales_mail()
    assert mails and "\r" not in mails[0].subject and "\n" not in mails[0].subject


def _turnstile_on(monkeypatch, verdict):
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "ts-secret")
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SITE_KEY", "ts-site-key")
    calls = []

    def _verify(token, **kw):
        calls.append({"token": token, **kw})
        return verdict
    monkeypatch.setattr("services.turnstile.verify", _verify)
    return calls


def test_turnstile_enforced_when_configured(client, monkeypatch):
    calls = _turnstile_on(monkeypatch, {"ok": False, "reason": "missing_token"})
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([]))
    assert client.get("/api/v1/contact-sales/config").json() == {
        "turnstile_site_key": "ts-site-key"}
    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 400
    assert res.json()["error"] == "turnstile_required"
    assert calls[0]["expected_action"] == "contact_sales"
    from services import mail_outbox

    assert not mail_outbox.queued()


def test_turnstile_pass_lets_the_lead_through(client, monkeypatch):
    calls = _turnstile_on(monkeypatch, {"ok": True, "reason": "ok"})
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([]))
    res = client.post("/api/v1/contact-sales", json=_payload(turnstile_token="tok-1"))
    assert res.status_code == 200, res.text
    assert calls[0]["token"] == "tok-1"


def test_turnstile_secret_without_site_key_is_not_enforced(client, monkeypatch):
    """No published site key means the form cannot render the widget; the
    secret alone must not refuse every visitor."""
    monkeypatch.setenv("DATAWRAP_TURNSTILE_SECRET", "ts-secret")
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted([]))
    assert client.get("/api/v1/contact-sales/config").json() == {"turnstile_site_key": ""}
    assert client.post("/api/v1/contact-sales", json=_payload()).status_code == 200


def test_validation_rejects_missing_and_bad_email(client):
    res = client.post("/api/v1/contact-sales", json=_payload(email="not-an-email"))
    assert res.status_code == 422
    res = client.post("/api/v1/contact-sales", json=_payload(company=""))
    assert res.status_code == 422
    res = client.post("/api/v1/contact-sales", json=_payload(first_name=""))
    assert res.status_code == 422


def test_rate_limit_per_email(client, monkeypatch):
    monkeypatch.setattr(
        "src.routers.contact_router.persist_lead",
        lambda l: {**l, "lead_id": "x", "created_at": "t"},
    )
    from src.routers import contact_router

    cap = int(contact_router._EMAIL_CAPACITY)
    for i in range(cap):
        r = client.post("/api/v1/contact-sales", json=_payload())
        assert r.status_code == 200, r.text
    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 429
    assert res.json()["error"] == "rate_limited"


def test_rate_limit_per_ip(client, monkeypatch):
    monkeypatch.setattr(
        "src.routers.contact_router.persist_lead",
        lambda l: {**l, "lead_id": "x", "created_at": "t"},
    )
    from src.routers import contact_router

    cap = int(contact_router._IP_CAPACITY)
    for i in range(cap):
        r = client.post(
            "/api/v1/contact-sales",
            json=_payload(email=f"user{i}@example.com"),
        )
        assert r.status_code == 200, r.text
    res = client.post("/api/v1/contact-sales", json=_payload(email="another@example.com"))
    assert res.status_code == 429


def _flood_with_rotating_xff(client, contact_router, *, xff_prefix: str):
    statuses = []
    for i in range(int(contact_router._IP_CAPACITY) + 1):
        r = client.post(
            "/api/v1/contact-sales",
            json=_payload(email=f"spoof{i}@example.com"),
            headers={"X-Forwarded-For": f"{xff_prefix}.{i}"},
        )
        statuses.append(r.status_code)
    return statuses


def test_rate_limit_per_ip_ignores_spoofed_xff_without_trusted_proxy(client, monkeypatch):
    monkeypatch.setattr(
        "src.routers.contact_router.persist_lead",
        lambda l: {**l, "lead_id": "x", "created_at": "t"},
    )
    monkeypatch.setenv("DATAFLOW_TRUSTED_PROXY_COUNT", "0")
    monkeypatch.delenv("DATAWRAP_TRUSTED_PROXY_COUNT", raising=False)
    monkeypatch.delenv("TRUSTED_PROXY_COUNT", raising=False)
    from src.routers import contact_router

    statuses = _flood_with_rotating_xff(client, contact_router, xff_prefix="1.2.3")
    assert statuses[:-1] == [200] * int(contact_router._IP_CAPACITY)
    assert statuses[-1] == 429


def test_rate_limit_per_ip_spoofed_leftmost_xff_shares_bucket_behind_proxy(
    client, monkeypatch
):
    monkeypatch.setattr(
        "src.routers.contact_router.persist_lead",
        lambda l: {**l, "lead_id": "x", "created_at": "t"},
    )
    monkeypatch.setenv("DATAFLOW_TRUSTED_PROXY_COUNT", "2")
    monkeypatch.delenv("DATAWRAP_TRUSTED_PROXY_COUNT", raising=False)
    monkeypatch.delenv("TRUSTED_PROXY_COUNT", raising=False)
    from src.routers import contact_router

    statuses = []
    for i in range(int(contact_router._IP_CAPACITY) + 1):
        r = client.post(
            "/api/v1/contact-sales",
            json=_payload(email=f"proxied{i}@example.com"),
            # attacker-forged leftmost hop; our edge appended the real client and
            # the next proxy appended the edge (10.0.0.1): client = hops[-2]
            headers={"X-Forwarded-For": f"9.9.9.{i}, 203.0.113.7, 10.0.0.1"},
        )
        statuses.append(r.status_code)
    assert statuses[-1] == 429


def test_client_ip_uses_trusted_proxy_resolution(monkeypatch):
    from starlette.requests import Request

    from src.routers import contact_router

    def _req(xff: str):
        return Request(
            {
                "type": "http",
                "headers": [(b"x-forwarded-for", xff.encode())],
                "client": ("198.51.100.4", 1234),
            }
        )

    monkeypatch.delenv("DATAWRAP_TRUSTED_PROXY_COUNT", raising=False)
    monkeypatch.setenv("DATAFLOW_TRUSTED_PROXY_COUNT", "0")
    assert contact_router._client_ip(_req("1.2.3.4")) == "198.51.100.4"
    # One proxy appends the client it saw: hops[-1] (ProxyFix semantics).
    monkeypatch.setenv("DATAFLOW_TRUSTED_PROXY_COUNT", "1")
    assert contact_router._client_ip(_req("1.2.3.4")) == "1.2.3.4"
    assert contact_router._client_ip(_req("6.6.6.6, 1.2.3.4")) == "1.2.3.4"
    # Two proxies: the second appended the first's address.
    monkeypatch.setenv("DATAFLOW_TRUSTED_PROXY_COUNT", "2")
    assert contact_router._client_ip(_req("6.6.6.6, 1.2.3.4, 203.0.113.7")) == "1.2.3.4"
