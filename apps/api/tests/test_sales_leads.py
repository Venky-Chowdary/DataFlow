"""P4-5 — sales leads: form-token wall, durable store, internal +
acknowledgement mails."""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.form_token as form_token_mod
from services import email_quota, sales_leads
from services.email_sender import ConsoleEmailSender, reset_email_sender


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    monkeypatch.setenv("DATAWRAP_EMAIL_PROVIDER", "console")
    monkeypatch.setenv("DATAWRAP_ENV", "test")
    monkeypatch.setenv("DATAWRAP_SALES_INBOX", "sales@datawrap.io")
    monkeypatch.setattr(sales_leads, "_leads_path",
                        lambda: tmp_path / "leads.json")
    monkeypatch.setattr(form_token_mod, "MIN_FILL_MS", 0)
    email_quota._reset_backends()
    reset_email_sender()
    ConsoleEmailSender.OUTBOX.clear()
    yield
    reset_email_sender()


def _client() -> TestClient:
    from src.routers.signup_router import public_router
    from services.api_errors import ApiError, api_error_handler

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(public_router, prefix="/api/v1")
    return TestClient(app)


def test_lead_stored_and_two_mails_sent():
    c = _client()
    resp = c.post("/api/v1/public/sales-leads", json={
        "first_name": "Ada", "last_name": "Lovelace",
        "email": "ada@bigcorp.example", "company": "BigCorp",
        "volume": "10TB", "sources": ["postgres"],
        "destinations": ["snowflake"],
        "form_token": form_token_mod.issue("sales")})
    assert resp.status_code == 202
    assert resp.json() == {"ok": True}
    leads = sales_leads.list_leads()
    assert len(leads) == 1
    assert leads[0]["company"] == "BigCorp"
    assert leads[0]["status"] == "new"
    recipients = {m.to for m in ConsoleEmailSender.OUTBOX}
    assert recipients == {"sales@datawrap.io", "ada@bigcorp.example"}


def test_honeypot_gets_fake_202_and_nothing_stored():
    c = _client()
    resp = c.post("/api/v1/public/sales-leads", json={
        "email": "bot@spam.example", "form_token": "",
        "honeypot": "totally-a-human-field"})
    assert resp.status_code == 202
    assert sales_leads.list_leads() == []
    assert ConsoleEmailSender.OUTBOX == []


def test_missing_form_token_gets_fake_202_and_nothing_stored():
    c = _client()
    resp = c.post("/api/v1/public/sales-leads",
                  json={"email": "x@y.example"})
    assert resp.status_code == 202
    assert sales_leads.list_leads() == []


def test_wrong_purpose_token_refused():
    c = _client()
    resp = c.post("/api/v1/public/sales-leads", json={
        "email": "ada@bigcorp.example",
        "form_token": form_token_mod.issue("signup")})
    assert resp.status_code == 202
    assert sales_leads.list_leads() == []


def test_flag_off_route_404s(monkeypatch):
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "0")
    c = _client()
    resp = c.post("/api/v1/public/sales-leads",
                  json={"email": "a@b.example"})
    assert resp.status_code == 404
