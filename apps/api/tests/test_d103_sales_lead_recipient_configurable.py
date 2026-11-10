"""D103 - the Contact Sales lead mail goes to a configured sales inbox.

The form now delivers (route, durable outbox, configured provider:
tests/test_d103_contact_sales.py), but the recipient was a constant in
``signup_service`` (``SALES_ADDRESS``): no deployment could route leads to
another inbox, and QA could not retest without mailing the real sales team.
``DATAWRAP_SALES_EMAIL`` (``DATAFLOW_SALES_EMAIL`` legacy) now names it, read
at send time like ``EMAIL_FROM`` / ``EMAIL_REPLY_TO``; unset, the published
sales address is used. No real mail is sent: the console driver captures.
"""
from __future__ import annotations

import pytest

from tests.test_d103_contact_sales import (  # noqa: F401 - fixtures
    _mail_capture,
    _payload,
    _persisted,
    _reset_rate_store,
    client,
)


def _mails_to(address: str):
    from tests.helpers.mail_wait import await_outbox, drain_mail_lane

    drain_mail_lane()
    return await_outbox(address)


@pytest.mark.parametrize("env", ["DATAWRAP_SALES_EMAIL", "DATAFLOW_SALES_EMAIL"])
def test_lead_mail_goes_to_the_configured_sales_inbox(client, monkeypatch, env):
    monkeypatch.delenv("DATAWRAP_SALES_EMAIL", raising=False)
    monkeypatch.delenv("DATAFLOW_SALES_EMAIL", raising=False)
    monkeypatch.setenv(env, "  qa-leads@example.com ")
    persisted: list = []
    monkeypatch.setattr("src.routers.contact_router.persist_lead", _persisted(persisted))

    res = client.post("/api/v1/contact-sales", json=_payload())
    assert res.status_code == 200 and res.json()["queued"] is True, res.text
    mails = _mails_to("qa-leads@example.com")
    assert len(mails) == 1 and mails[0].reply_to == "ada@example.com"
    from services.email_sender import ConsoleEmailSender

    assert all("sales@datawrap.io" not in m.to for m in ConsoleEmailSender.OUTBOX)


def test_unset_falls_back_to_the_published_sales_address(monkeypatch):
    monkeypatch.delenv("DATAWRAP_SALES_EMAIL", raising=False)
    monkeypatch.delenv("DATAFLOW_SALES_EMAIL", raising=False)
    from services.signup_service import sales_address

    assert sales_address() == "sales@datawrap.io"
