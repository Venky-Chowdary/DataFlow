"""Public contact-sales intake (D103).

``POST /api/v1/contact-sales`` — unauthenticated by design (the form is on the
public landing page) but defended:

- **honeypot** — a filled ``website`` field means a bot; we accept-and-drop it
  silently so the bot learns nothing (no persistence, no mail).
- **rate limits** — independent per-IP and per-email buckets via the canonical
  ``TokenBucketStore`` (no second bucket implementation).
- **server-side validation** — pydantic field lengths + an email shape check;
  the client validation is UX, this is the gate.
- **persist before mail** — the lead lands in ``contact_sales_leads`` (with a
  JSONL fallback) before SMTP is touched; a mail outage records
  ``mail_error`` on the lead but still returns the lead id.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services.contact_leads import persist_lead
from services.mcp_rate_limit import TokenBucketStore
from services.smtp_mailer import send_mail, smtp_configured

LOG = logging.getLogger(__name__)

router = APIRouter()

_SALES_ADDRESS = "sales@datawrap.io"
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_MAX_LIST = 12
_MAX_ITEM_LEN = 120

# ~5 submissions/hour per IP, ~2/hour per email — generous for humans,
# useless for spray. Tunable via env for ops.
import os


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


_IP_CAPACITY = _env_float("DATAFLOW_CONTACT_IP_CAPACITY", 5.0)
_IP_REFILL = _env_float("DATAFLOW_CONTACT_IP_REFILL_PER_SEC", 5.0 / 3600.0)
_EMAIL_CAPACITY = _env_float("DATAFLOW_CONTACT_EMAIL_CAPACITY", 2.0)
_EMAIL_REFILL = _env_float("DATAFLOW_CONTACT_EMAIL_REFILL_PER_SEC", 2.0 / 3600.0)

_RATE_STORE = TokenBucketStore()


class ContactSalesIn(BaseModel):
    first_name: str = Field(default="", max_length=120)
    last_name: str = Field(default="", max_length=120)
    email: str = Field(default="", max_length=254)
    company: str = Field(default="", max_length=200)
    role: str = Field(default="", max_length=120)
    phone: str = Field(default="", max_length=40)
    message: str = Field(default="", max_length=4000)
    sources: list[str] = Field(default_factory=list, max_length=_MAX_LIST)
    destinations: list[str] = Field(default_factory=list, max_length=_MAX_LIST)
    volume: str = Field(default="", max_length=_MAX_ITEM_LEN)
    region: str = Field(default="", max_length=_MAX_ITEM_LEN)
    timeframe: str = Field(default="", max_length=_MAX_ITEM_LEN)
    # Honeypot: real users never fill it (it is invisible in the UI).
    website: str = Field(default="", max_length=200)


def _client_ip(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    if forwarded:
        return forwarded[:64]
    return str(getattr(request.client, "host", "") or "unknown")[:64]


def _clean_list(values: list[str]) -> list[str]:
    return [str(v).strip()[:_MAX_ITEM_LEN] for v in values if str(v).strip()][
        :_MAX_LIST
    ]


def _lead_text(lead: dict[str, Any]) -> str:
    lines = [
        "New Datawrap sales lead",
        "",
        f"Name:    {lead['name']}",
        f"Email:   {lead['email']}",
        f"Company: {lead['company']}",
        f"Role:    {lead.get('role') or '-'}",
        f"Phone:   {lead.get('phone') or '-'}",
        f"Sources: {', '.join(lead.get('sources') or []) or '-'}",
        f"Destinations: {', '.join(lead.get('destinations') or []) or '-'}",
        f"Volume:  {lead.get('volume') or '-'}",
        f"Region:  {lead.get('region') or '-'}",
        f"Timeframe: {lead.get('timeframe') or '-'}",
        "",
        "Message:",
        lead.get("message") or "-",
        "",
        f"Lead ID: {lead['lead_id']}",
        f"Received: {lead['created_at']}",
        f"Client IP: {lead.get('client_ip') or '-'}",
    ]
    return "\n".join(lines)


@router.post("/contact-sales")
async def contact_sales(body: ContactSalesIn, request: Request):
    # Honeypot filled → bot. Return a normal success shape and do nothing.
    if body.website.strip():
        return JSONResponse({"ok": True, "lead_id": None, "delivered": False})

    email = body.email.strip().lower()
    first = body.first_name.strip()
    last = body.last_name.strip()
    company = body.company.strip()
    if not first or not last or not company or not email:
        return JSONResponse(
            {"ok": False, "error": "name, work email and company are required"},
            status_code=422,
        )
    if not _EMAIL_RE.match(email):
        return JSONResponse(
            {"ok": False, "error": "invalid email address"}, status_code=422
        )

    client_ip = _client_ip(request)
    for key, cap, refill in (
        (f"ip:{client_ip}", _IP_CAPACITY, _IP_REFILL),
        (f"email:{email}", _EMAIL_CAPACITY, _EMAIL_REFILL),
    ):
        verdict = _RATE_STORE.consume(key, capacity=cap, refill_per_sec=refill)
        if not verdict["allowed"]:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "rate_limited",
                    "retry_after_sec": verdict["retry_after_sec"],
                },
                status_code=429,
            )

    lead = {
        "name": f"{first} {last}".strip(),
        "first_name": first,
        "last_name": last,
        "email": email,
        "company": company,
        "role": body.role.strip(),
        "phone": body.phone.strip(),
        "message": body.message.strip(),
        "sources": _clean_list(body.sources),
        "destinations": _clean_list(body.destinations),
        "volume": body.volume.strip(),
        "region": body.region.strip(),
        "timeframe": body.timeframe.strip(),
        "client_ip": client_ip,
        "user_agent": str(request.headers.get("user-agent") or "")[:300],
    }
    stored = persist_lead(lead)

    mail_result: dict[str, Any] = {"sent": False, "error": "smtp_not_configured"}
    if smtp_configured():
        mail_result = send_mail(
            to=_SALES_ADDRESS,
            subject=f"Datawrap pilot request — {company} ({stored['name']})",
            text_body=_lead_text(stored),
            reply_to=email,
        )
    if mail_result.get("sent"):
        stored["mail_sent"] = True
    else:
        stored["mail_sent"] = False
        stored["mail_error"] = str(mail_result.get("error") or "")[:300]
        LOG.warning("contact lead %s: mail not sent: %s", stored["lead_id"], stored["mail_error"])
        try:
            from services.mongodb_service import get_mongodb_service

            get_mongodb_service().get_database()["contact_sales_leads"].update_one(
                {"lead_id": stored["lead_id"]},
                {"$set": {"mail_sent": False, "mail_error": stored["mail_error"]}},
            )
        except Exception:
            pass

    return JSONResponse(
        {
            "ok": True,
            "lead_id": stored["lead_id"],
            "delivered": bool(mail_result.get("sent")),
        }
    )
