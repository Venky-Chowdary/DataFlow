"""Public contact-sales intake (D103).

``POST /api/v1/contact-sales`` — unauthenticated by design (the form is on the
public landing page) but defended:

- **honeypot** — a filled ``website`` field means a bot; we accept-and-drop it
  silently so the bot learns nothing (no persistence, no mail).
- **rate limits** — independent per-IP and per-email buckets via the canonical
  ``TokenBucketStore`` (no second bucket implementation). The IP key comes from
  ``services.client_ip`` so a spoofed ``X-Forwarded-For`` cannot mint buckets.
- **server-side validation** — pydantic field lengths + an email shape check;
  the client validation is UX, this is the gate.
- **Turnstile** — when the deployment configures both the Turnstile secret
  and the public site key (so the form can render the widget), a missing or
  refused token is a 400 ``turnstile_required``.
- **signed form token** — ``GET /contact-sales/config`` also issues an
  HMAC-signed, one-use form token (``services.form_token``) with a minimum
  fill time: a post with no/forged/tampered token is accepted-and-dropped
  like the honeypot, while ``expired``/``replayed`` answer 400
  ``form_expired`` so a real visitor's stale page can retry. This is the
  zero-config bot wall that works before Turnstile keys are set.
- **persist before mail** — the lead lands in ``contact_sales_leads`` (with a
  JSONL fallback) before any mail is queued. The mail to sales@ goes through
  the same durable outbox and ``DATAWRAP_EMAIL_PROVIDER`` driver as every
  auth mail (bounded retries, failure audited as ``mail.send_failed``); the
  outbox row's idempotency key ``sales-lead:<lead_id>`` ties the two.
"""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from services import form_token as form_token_mod
from services import turnstile
from services.client_ip import client_ip_from_request
from services.contact_leads import persist_lead
from services.mcp_rate_limit import TokenBucketStore
from services.signup_config import turnstile_site_key

LOG = logging.getLogger(__name__)

router = APIRouter()

_TURNSTILE_ACTION = "contact_sales"
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
    turnstile_token: str = Field(default="", max_length=4096)
    form_token: str = Field(default="", max_length=4096)


def _client_ip(request: Request) -> str:
    return (client_ip_from_request(request) or "unknown")[:64]


def _clean_list(values: list[str]) -> list[str]:
    return [str(v).strip()[:_MAX_ITEM_LEN] for v in values if str(v).strip()][
        :_MAX_LIST
    ]


def _single_line(text: str) -> str:
    # Header-injection guard: lead fields end up in the Subject header, so
    # they must never carry CR/LF.
    return " ".join(str(text or "").split())


def _turnstile_enforced() -> str:
    """The site key when the form must carry a Turnstile token, else "".

    Enforced only when the widget can actually be rendered: a secret without
    a published site key would refuse every visitor."""
    site_key = turnstile_site_key()
    return site_key if site_key and turnstile.configured() else ""


@router.get("/contact-sales/config")
async def contact_sales_config():
    return {
        "turnstile_site_key": _turnstile_enforced(),
        "form_token": form_token_mod.issue("contact_sales"),
    }


@router.post("/contact-sales")
async def contact_sales(body: ContactSalesIn, request: Request):
    # Honeypot filled → bot. Return a normal success shape and do nothing.
    if body.website.strip():
        return JSONResponse({"ok": True, "lead_id": None, "delivered": False})

    # Signed one-use form token: missing/forged/too-fast posts are bots —
    # accept-and-drop like the honeypot. Expired/replayed are real visitors
    # with a stale page; the 400 lets the form refetch config and retry.
    verdict_form = form_token_mod.check(
        body.form_token, purpose="contact_sales")
    if not verdict_form["ok"]:
        if verdict_form["reason"] in ("expired", "replayed"):
            return JSONResponse(
                {"ok": False, "error": "form_expired"}, status_code=400
            )
        LOG.info("contact lead refused: form_token %s", verdict_form["reason"])
        return JSONResponse({"ok": True, "lead_id": None, "delivered": False})

    email = body.email.strip().lower()
    first = _single_line(body.first_name)
    last = _single_line(body.last_name)
    company = _single_line(body.company)
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
    if _turnstile_enforced():
        verdict = turnstile.verify(
            body.turnstile_token, remote_ip=client_ip,
            expected_action=_TURNSTILE_ACTION)
        if not verdict["ok"]:
            LOG.info("contact lead refused: turnstile %s", verdict["reason"])
            return JSONResponse(
                {"ok": False, "error": "turnstile_required"}, status_code=400
            )
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
        "role": _single_line(body.role),
        "phone": _single_line(body.phone),
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
    try:
        from services.signup_service import send_sales_lead

        send_sales_lead(stored)
        queued = True
    except Exception:  # noqa: BLE001 — the lead is persisted; never fail the visitor
        LOG.error("contact lead %s: mail not queued", stored["lead_id"], exc_info=True)
        queued = False

    return JSONResponse(
        {"ok": True, "lead_id": stored["lead_id"], "queued": queued}
    )
