"""D103 — Contact Sales form must deliver, not console.log.

Repro (prod c6c16128): the marketing form showed "sent" while no request
left the browser and nothing reached sales@datawrap.io.
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
    from services import contact_leads, smtp_mailer

    persisted = []
    sent = []
    monkeypatch.setattr(contact_leads, "persist_lead", lambda l: persisted.append(l) or l)
    monkeypatch.setattr(
        "src.routers.contact_router.persist_lead",
        lambda l: persisted.append(l) or {**l, "lead_id": "x", "created_at": "t"},
    )
    monkeypatch.setattr(smtp_mailer, "send_mail", lambda **kw: sent.append(kw) or {"sent": True})
    monkeypatch.setattr("src.routers.contact_router.send_mail", lambda **kw: sent.append(kw) or {"sent": True})
    monkeypatch.setattr("src.routers.contact_router.smtp_configured", lambda: True)

    res = client.post("/api/v1/contact-sales", json=_payload(website="http://spam.example"))
    assert res.status_code == 200 and res.json()["ok"] is True
    assert not persisted and not sent


def test_valid_lead_persists_and_mails(client, monkeypatch):
    persisted = []
    mailed = []

    def _persist(lead):
        rec = {**lead, "lead_id": "lead-1", "created_at": "2026-10-25T00:00:00+00:00"}
        persisted.append(rec)
        return rec

    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persist)
    monkeypatch.setattr("src.routers.contact_router.smtp_configured", lambda: True)
    monkeypatch.setattr(
        "src.routers.contact_router.send_mail",
        lambda **kw: mailed.append(kw) or {"sent": True},
    )

    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is True and body["lead_id"] == "lead-1" and body["delivered"] is True
    # persist happened before the mail attempt (nothing lost if mail fails)
    assert persisted[0]["email"] == "ada@example.com"
    assert mailed[0]["to"] == "sales@datawrap.io"


def test_mail_failure_still_persists_and_returns_ok(client, monkeypatch):
    """Persist-first: a mail outage records mail_error, never loses the lead."""
    persisted = []

    def _persist(lead):
        rec = {**lead, "lead_id": "lead-2", "created_at": "2026-10-25T00:00:00+00:00"}
        persisted.append(rec)
        return rec

    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persist)
    monkeypatch.setattr("src.routers.contact_router.smtp_configured", lambda: True)
    monkeypatch.setattr(
        "src.routers.contact_router.send_mail",
        lambda **kw: {"sent": False, "error": "SMTPRecipientsRefused"},
    )
    # Mongo update of mail_error will fail under test mongo — tolerated.
    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200
    assert res.json()["delivered"] is False
    assert persisted[0]["mail_sent"] is False
    assert "SMTPRecipientsRefused" in persisted[0]["mail_error"]


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
    monkeypatch.setattr("src.routers.contact_router.smtp_configured", lambda: False)
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
    monkeypatch.setattr("src.routers.contact_router.smtp_configured", lambda: False)
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
