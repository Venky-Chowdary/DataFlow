"""Signup orchestration (P1-3/4/5): request, verify, resend.

Enumeration rules baked in here, not at the route:

- ``request_signup`` returns the same 202 body for new, pending and existing
  emails; constant-work hashing runs on every path; the existing-account
  path mails the owner a notice instead of a code and returns a decoy
  challenge id.
- Quota is consumed on every path *before* sending, so an over-quota error
  looks identical regardless of whether the address exists.
- ``verify_email`` creates user + org + workspace + membership in one unit
  (Mongo txn where supported, ordered writes + compensating deletes
  elsewhere — the same guarantee team_store already gives).
- Codes and passwords never appear in logs.
"""

from __future__ import annotations

import ipaddress
import logging
import secrets
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from services import otp_service, password_reset_store, signup_store, team_store, user_store
from services.api_errors import ApiError
from services.audit_coverage import record_security_event
from services.auth_state import get_auth_state
from services.email_canonical import canonical_email
from services import denylist_store, disposable_list, mx_check, turnstile
from services import form_token as form_token_mod
from services.brand_env import getenv_brand
from services.email_policy import role_assessment
from services import risk_scorer
from services import email_verifier, email_quota, mail_outbox
from services.email_quota import consume_send_slot
from services.email_sender import (
    EmailQuotaExceeded,
    EmailSendError,
    OutboundEmail,
    get_email_sender,
)
from services.email_templates import render_email
from datetime import UTC, datetime

from services.org_store import create_organization
from services import org_store
from services.password_hash import hash_password_argon2

logger = logging.getLogger(__name__)

RESEND_COOLDOWN_SEC = 60
RESEND_HOURLY_CAP = 5
RESEND_DAILY_CAP = 10
MIN_PASSWORD_LEN = 12
MAX_PASSWORD_LEN = 128

# Mail delivery runs off the request path (one serialized lane) so provider
# latency can't time-leak account existence; every /password/forgot reply —
# fake or real — is also padded to the same floor.
_MAIL_LANE = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dw-mail")
FORGOT_FLOOR_SEC = 0.25


def _deliver_later(fn) -> None:
    """Fire-and-forget mail delivery; the request path never awaits it."""
    try:
        _MAIL_LANE.submit(fn)
    except RuntimeError:  # executor shut down (interpreter exit/tests)
        fn()


def _forgot_reply(body: dict[str, Any],
                  started: float) -> tuple[dict[str, Any], float]:
    """Uniform pacing: fake and real branches get the same remaining pad
    to FORGOT_FLOOR_SEC. The pad is RETURNED, not slept here — the async
    route awaits it with asyncio.sleep, so a flood of forgot requests
    parks coroutines instead of pinning pool workers."""
    elapsed = time.monotonic() - started
    pad = FORGOT_FLOOR_SEC - elapsed
    if pad <= 0:
        # A real branch that overruns the floor makes the fake branch a
        # timing oracle again — surface it instead of silently leaking.
        logger.warning("password-reset floor overrun: %.3fs > %.3fs", elapsed, FORGOT_FLOOR_SEC)
    return body, max(0.0, pad)



def _env_int(name: str, default: int) -> int:
    try:
        return int(getenv_brand(name, str(default)))
    except (TypeError, ValueError):
        return default


def _subnet_key(ip: str) -> str:
    """/24 for IPv4, /48 for IPv6; 'unknown' when unparseable."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return "unknown"
    if addr.version == 4:
        net = ipaddress.ip_network(f"{ip}/24", strict=False)
    else:
        net = ipaddress.ip_network(f"{ip}/48", strict=False)
    return str(net.network_address)


def _check_address_deliverable(normalized: str, *,
                               ip: str = "") -> tuple[int, list[str]]:
    """Shared anti-abuse address gate (P2): role-based block, denylist email
    kind + disposable (team ``allow_domain`` wins inside is_disposable), MX,
    and the optional external verifier. Returns the verifier's risk bump and
    reasons for the risk scorer; raises ApiError on a hard refusal."""
    role = role_assessment(normalized)
    if role == "blocked":
        record_security_event("signup.blocked", outcome="failed",
                              reason_code="role_address",
                              details={"ip": ip})
        raise ApiError(400, "blocked_address",
                       "this address can't be used for signup")
    domain = normalized.split("@", 1)[-1]
    if denylist_store.is_denied(canonical_email(normalized), kind="email") or \
            disposable_list.is_disposable(normalized):
        record_security_event("signup.blocked", outcome="failed",
                              reason_code="denied_domain",
                              details={"ip": ip})
        raise ApiError(400, "blocked_address",
                       "this address can't be used for signup")
    mx = mx_check.check_domain(domain)
    if not mx["ok"]:
        record_security_event("signup.blocked", outcome="failed",
                              reason_code=f"mx:{mx['reason']}",
                              details={"ip": ip})
        raise ApiError(400, "undeliverable_domain",
                       "that email domain can't receive mail")

    # P2-9 external verifier (optional, fail-open): undeliverable is refused
    # like an MX failure; a "risky" verdict feeds the risk scorer.
    bump, reasons = 0, []
    if email_verifier.configured():
        verdict = email_verifier.verify(normalized)
        if verdict["verdict"] == "undeliverable":
            record_security_event("signup.blocked", outcome="failed",
                                  reason_code="verifier:undeliverable",
                                  details={"ip": ip})
            raise ApiError(400, "undeliverable_domain",
                           "that email domain can't receive mail")
        if verdict["verdict"] == "risky":
            bump, reasons = 20, ["verifier:risky"]
    return bump, reasons


def _velocity_bucket(key: str, *, limit: int, window_sec: int) -> int:
    allowed, retry_after = get_auth_state().rate_limit_hit(
        key, limit=limit, window_sec=window_sec)
    return 0 if allowed else (retry_after or window_sec)


def _check_signup_velocity(*, ip: str, device_id: str, canonical: str) -> int:
    """Per-IP, per-subnet, per-device and per-address caps (P2-6).

    Returns the retry_after seconds when a bucket is exhausted, else 0.
    Limits configurable: DATAWRAP_SIGNUP_IP_HOURLY (10),
    DATAWRAP_SIGNUP_SUBNET_HOURLY (50), DATAWRAP_SIGNUP_DEVICE_DAILY (10),
    DATAWRAP_SIGNUP_EMAIL_DAILY (5).
    """
    buckets = (
        (f"signup:ip:{ip or 'unknown'}", _env_int("SIGNUP_IP_HOURLY", 10), 3600),
        (f"signup:net:{_subnet_key(ip)}", _env_int("SIGNUP_SUBNET_HOURLY", 50), 3600),
        (f"signup:dev:{device_id or 'unknown'}", _env_int("SIGNUP_DEVICE_DAILY", 10), 86400),
        (f"signup:em:{canonical}", _env_int("SIGNUP_EMAIL_DAILY", 5), 86400),
    )
    for key, limit, window in buckets:
        allowed, retry_after = get_auth_state().rate_limit_hit(
            key, limit=limit, window_sec=window)
        if not allowed:
            return retry_after or window
    return 0


def _deliver_outbox_row(row: dict[str, Any], *, actor_ip: str = "") -> None:
    """Send one persisted outbox row and mark it sent/failed. Every failed
    attempt audits password.reset.send_failed (reset links) or
    mail.send_failed (all other purposes); terminal state also marks the
    row failed — silent loss is forbidden. A fresh link/code can always be
    requested (each new issue supersedes the last)."""
    token = mail_outbox.claim(row["id"])
    if not token:
        # Another replica claimed this row first (concurrent drain) or it is
        # already terminal — exactly one send per row, no double send.
        return
    try:
        if not row.get("quota_counted"):
            consume_send_slot()
        payload = mail_outbox.body(row)  # sealed at rest, decrypted on send
        # Renew under our fencing token right before the provider call so
        # the lease covers the whole send: a slow provider can never let a
        # second replica reclaim and re-send this row mid-flight.
        if not mail_outbox.extend_lease(row["id"], token):
            logger.warning(
                "mail outbox lease lost before send (%s); not sending",
                row.get("purpose"))
            return
        get_email_sender().send(OutboundEmail(
            to=row["to"], subject=row["subject"], text=payload["text"],
            html=payload["html"],
            reply_to=row.get("reply_to") or None,
            idempotency_key=row["idempotency_key"]))
        mail_outbox.mark(row["id"], "sent", claim_token=token)
        details = {"ip": actor_ip, "purpose": row.get("purpose")}
        # Literal event names: the security-event registry audit requires
        # every call site to name its event as a string constant.
        if row.get("purpose") == "password.reset":
            record_security_event("password.reset.sent", outcome="succeeded",
                                  actor=row["to"], session_id=row["id"],
                                  details=details)
        else:
            record_security_event("mail.sent", outcome="succeeded",
                                  actor=row["to"], session_id=row["id"],
                                  details=details)
    except Exception as exc:
        _outbox_send_failed(row, exc, actor_ip=actor_ip, claim_token=token)


_MAX_SEND_RETRIES = 3
_RETRY_BASE_SEC = 30.0
_RETRY_MAX_SEC = 3600.0


def _retry_delay_sec(exc: Exception, attempt: int) -> float:
    """Backoff for a transient failure: the provider's own retry_after when
    it gives one (e.g. send-quota), else bounded exponential."""
    hint = getattr(exc, "retry_after_sec", None)
    if hint:
        try:
            return min(float(hint), _RETRY_MAX_SEC)
        except (TypeError, ValueError):
            pass
    return min(_RETRY_BASE_SEC * (2 ** (attempt - 1)), _RETRY_MAX_SEC)


def _outbox_send_failed(row: dict[str, Any], exc: Exception,
                        *, actor_ip: str = "",
                        claim_token: str | None = None) -> None:
    """One failed send: transient errors reschedule with bounded backoff
    (the row's attempts count survives restarts and replicas); permanent
    ones — and exhausted retries — mark the row failed for good. Every
    failed attempt audits password.reset.send_failed."""
    retryable = isinstance(exc, EmailSendError) and exc.retryable
    attempts = int(row.get("attempts") or 0)
    if retryable and attempts < _MAX_SEND_RETRIES:
        delay = _retry_delay_sec(exc, attempts + 1)
        mail_outbox.reschedule(row["id"], delay, type(exc).__name__,
                               claim_token=claim_token)
        row["attempts"] = attempts + 1
        timer = threading.Timer(
            delay,
            lambda: _deliver_later(
                lambda: _deliver_outbox_row(row, actor_ip=actor_ip)))
        timer.daemon = True
        timer.start()
        logger.warning("mail outbox transient failure (%s); retry %d in %.0fs",
                       type(exc).__name__, attempts + 1, delay)
    else:
        mail_outbox.mark(row["id"], "failed", claim_token=claim_token)
        if isinstance(exc, EmailSendError):
            logger.warning("mail send failed terminally (%s)",
                           row.get("purpose"))
        else:
            logger.error("mail delivery failed (%s)", row.get("purpose"),
                         exc_info=True)
    try:
        details = {"ip": actor_ip, "purpose": row.get("purpose"),
                   "attempt": attempts + 1}
        if row.get("purpose") == "password.reset":
            record_security_event("password.reset.send_failed",
                                  outcome="failed", actor=row["to"],
                                  session_id=row["id"], details=details)
        else:
            record_security_event("mail.send_failed", outcome="failed",
                                  actor=row["to"], session_id=row["id"],
                                  details=details)
    except Exception:
        logger.error("mail send_failed audit event failed", exc_info=True)


def _outbox_dead_link_status(row: dict[str, Any]) -> str | None:
    """Terminal status for a queued reset-mail row whose link is already
    dead — expired, consumed, superseded by a newer request, or the token
    doc is gone. The drain marks the row instead of sending a dead link."""
    key = str(row.get("idempotency_key") or "")
    purpose = row.get("purpose")
    if purpose == "password.reset":
        token_id = (key.split("reset-link:", 1)[-1]
                    if "reset-link:" in key else "")
        if not token_id:
            return None
        status = password_reset_store.status_by_id(token_id)
        if status == "expired":
            return "expired"
        if status in ("used", "superseded", "invalid"):
            return "superseded"
        return None
    if key.startswith("otp:"):
        parts = key.split(":")
        if len(parts) < 3:
            return None
        ch = signup_store.get_challenge(parts[1])
        if ch is None:
            return "expired"  # challenge TTL already deleted it
        if ch.get("consumed_at"):
            return "superseded"  # code already used
        if int(ch.get("attempts") or 0) >= int(ch.get("max_attempts") or 1):
            return "superseded"  # burned by wrong-code attempts
        if int(ch.get("sent_count") or 1) > int(parts[2]):
            return "superseded"  # a resend replaced this code
        if str(ch.get("expires_at") or "") <= datetime.now(UTC).isoformat():
            return "expired"
    return None


def recover_mail_outbox() -> int:
    """Startup hook: re-queue persisted 'queued' rows so a restart cannot
    silently drop mail. Rows whose reset link is already dead are marked
    expired/superseded and never sent. Returns how many rows re-queued."""
    return mail_outbox.drain_queued(
        lambda row: _deliver_later(lambda: _deliver_outbox_row(row)),
        skip_reason=_outbox_dead_link_status)


def _send(template: str, to: str, context: dict[str, Any], *, idempotency_key: str) -> None:
    """Sync quota check (raises → caller maps to the 429 envelope) → render
    → persist a sealed outbox row → serialized-lane delivery. Every auth
    mail — signup code, reset link, email-change code, notices — shares the
    same restart durability, atomic claim, and bounded retries. The quota
    slot is consumed here on the request path (the user-facing 429 contract
    stays synchronous); the row is flagged so the lane doesn't count twice.
    Provider failures never reach the request — they mark the row and audit."""
    consume_send_slot()  # raises EmailQuotaExceeded — caller maps to envelope
    mail = render_email(template, context)
    row = mail_outbox.enqueue(
        to=to, subject=mail.subject, text=mail.text, html=mail.html,
        idempotency_key=idempotency_key, purpose=template,
        quota_counted=True)
    _deliver_later(lambda: _deliver_outbox_row(row))


SALES_ADDRESS = "sales@datawrap.io"


def send_sales_lead(lead: dict[str, Any]) -> str:
    """Queue the internal Contact Sales lead mail to sales@ on the same
    durable outbox + provider as every auth mail (D103) and return the
    outbox row id. Reply-To is the prospect, so sales answers them directly.
    The send-quota slot is taken on the lane, not the request: the lead is
    already persisted, and a quota refusal reschedules instead of failing
    the visitor's submission."""
    context = {
        "name": lead.get("name"),
        "email": lead.get("email"),
        "company": lead.get("company"),
        "role": lead.get("role"),
        "phone": lead.get("phone"),
        "sources": ", ".join(lead.get("sources") or []),
        "destinations": ", ".join(lead.get("destinations") or []),
        "volume": lead.get("volume"),
        "region": lead.get("region"),
        "timeframe": lead.get("timeframe"),
        "message": lead.get("message"),
        "lead_id": lead.get("lead_id"),
        "received": lead.get("created_at"),
    }
    mail = render_email("sales_lead_internal", context)
    row = mail_outbox.enqueue(
        to=SALES_ADDRESS, subject=mail.subject, text=mail.text, html=mail.html,
        idempotency_key=f"sales-lead:{lead['lead_id']}",
        purpose="sales_lead_internal",
        reply_to=str(lead.get("email") or ""))
    _deliver_later(lambda: _deliver_outbox_row(row))
    return row["id"]


def _decoy_challenge() -> str:
    return f"otc_{uuid.uuid4().hex}"


def request_signup(
    *,
    email: str,
    name: str,
    password: str,
    ip: str = "",
    device_id: str = "",
    ua: str = "",
    turnstile_token: str = "",
    form_token: str = "",
    honeypot: str = "",
) -> dict[str, Any]:
    """Accept a signup. Same 202 body for new/pending/existing addresses."""
    # Bot wall: honeypot filled or a bad/expired/fast form token gets a fake
    # 202 — nothing stored, nothing mailed, nothing logged beyond the event.
    verdict_form = form_token_mod.check(
        form_token, purpose="signup", honeypot=honeypot)
    if not verdict_form["ok"]:
        record_security_event("signup.blocked", outcome="failed",
                              reason_code=f"form:{verdict_form['reason']}",
                              details={"ip": ip})
        return form_token_mod.fake_accept_body()
    # Turnstile: enforced when a secret is configured; unconfigured just
    # raises the risk floor (P2-8 wires the full scorer).
    if turnstile.configured():
        verdict = turnstile.verify(
            turnstile_token, remote_ip=ip,
            expected_action="signup", expected_hostname="datawrap.io")
        if not verdict["ok"]:
            record_security_event("signup.blocked", outcome="failed",
                                  reason_code=f"turnstile:{verdict['reason']}",
                                  details={"ip": ip})
            raise ApiError(400, "turnstile_required",
                           "couldn't verify you're human — try again")
    normalized = user_store.validate_email(email)
    retry = _check_signup_velocity(
        ip=ip, device_id=device_id, canonical=canonical_email(normalized))
    if retry:
        record_security_event("signup.blocked", outcome="failed",
                              reason_code="rate_limited",
                              details={"ip": ip, "retry_after_sec": retry})
        raise ApiError(429, "signup_rate_limited",
                       "too many attempts — try again shortly",
                       retry_after_sec=retry)
    if not (MIN_PASSWORD_LEN <= len(password or "") <= MAX_PASSWORD_LEN):
        # Validation is fine to surface — it reveals nothing about the account.
        raise ApiError(400, "invalid_password",
                       f"password must be {MIN_PASSWORD_LEN}-{MAX_PASSWORD_LEN} characters")
    _refuse_pwned_password(password)
    verifier_bump, verifier_reasons = _check_address_deliverable(
        normalized, ip=ip)
    role_flagged = role_assessment(normalized) == "flagged"
    canonical = canonical_email(normalized)

    # P2-8 risk scorer — deny drops with a decoy 202 (nothing stored/mailed);
    # hold stores the pending row with its score but skips the OTP mail.
    risk = risk_scorer.score_signals(
        ip=ip, ua=ua, device_id=device_id,
        turnstile_ok=True if turnstile.configured() else None,
        extra_score=verifier_bump, extra_reasons=verifier_reasons)
    if risk["action"] == "deny":
        record_security_event("signup.blocked", outcome="failed",
                              reason_code="risk:deny",
                              details={"ip": ip, "score": risk["score"],
                                       "signals": risk["reasons"]})
        return form_token_mod.fake_accept_body()

    # Constant-work: hash on every path so timing doesn't reveal whether the
    # hash is stored (new/pending) or discarded (existing).
    password_hash = hash_password_argon2(password)

    existing = user_store.find_by_canonical(canonical)
    pending = signup_store.get_pending_signup(canonical=canonical)

    # Quota applies identically to every path — over-quota is the same error
    # whether or not the address exists.
    try:
        consume_send_slot()
    except EmailQuotaExceeded as exc:
        raise ApiError(503, "email_quota_exceeded",
                       "we couldn't send your code right now — try again later",
                       retry_after_sec=exc.retry_after_sec) from exc

    if existing is not None:
        # Already-verified owner: notice mail, decoy challenge, same 202.
        try:
            _send("existing_account_notice", existing["email"],
                  {"name": existing.get("name") or "there"},
                  idempotency_key=f"signup-notice:{canonical}")
        except EmailSendError:
            # The notice is best-effort — a failed notice must not change the
            # response (enumeration) and must not confuse the user.
            logger.warning("existing-account notice send failed")
        record_security_event(
            "signup.requested", outcome="succeeded",
            details={"kind": "existing_account_notice", "ip": ip},
        )
        return {
            "challenge_id": _decoy_challenge(),
            "resend_after_sec": RESEND_COOLDOWN_SEC,
            "expires_in_sec": int(otp_service.OTP_TTL.total_seconds()),
        }

    row = signup_store.upsert_pending_signup(
        email=normalized,
        name=name,
        password_hash=password_hash,
        ip=ip,
        device_id=device_id,
        risk_score=max(float(risk["score"]),
                       10.0 if role_flagged else 0.0),
    )
    if risk["action"] == "hold":
        record_security_event("admin.signup_held", outcome="skipped",
                              details={"ip": ip, "score": risk["score"],
                                       "signals": risk["reasons"],
                                       "pending_id": row["id"]})
        return form_token_mod.fake_accept_body()
    challenge, code = otp_service.issue(row["id"], "signup")
    try:
        _send("signup_code", normalized,
              {"name": name, "code": code,
               "expires_minutes": int(otp_service.OTP_TTL.total_seconds() // 60)},
              idempotency_key=f"otp:{challenge['id']}:{challenge['sent_count']}")
    except EmailSendError as exc:
        # Challenge row stays resendable — tell the client plainly.
        retryable = getattr(exc, "retryable", False)
        raise ApiError(
            503 if retryable else 502,
            "email_send_failed",
            "we couldn't send your code right now — use resend to try again",
            retry_after_sec=60 if retryable else None,
        ) from exc

    record_security_event(
        "signup.requested", outcome="succeeded",
        details={"ip": ip},
    )
    record_security_event("otp.sent", outcome="succeeded", session_id=challenge["id"],
                          details={"purpose": "signup"})
    return {
        "challenge_id": challenge["id"],
        "resend_after_sec": RESEND_COOLDOWN_SEC,
        "expires_in_sec": int(otp_service.OTP_TTL.total_seconds()),
    }


def _provision_verified_user(pending: dict[str, Any]) -> tuple[dict, dict, object]:
    """user + org + workspace + membership. Ordered writes with compensation —
    the same transaction envelope team_store gives workspace creation."""
    user = None
    org = None
    try:
        user = user_store.create_signup_user(
            email=pending["email"],
            name=pending.get("name") or "",
            password_hash=pending["password_hash"],
            password_algo="argon2id",
            canonical=pending["canonical_email"],
            signup_ip=pending.get("ip", ""),
            signup_risk=pending.get("risk_score", 0.0),
        )
        org = create_organization(
            name=f"{pending.get('name') or pending['email'].split('@')[0]}'s workspace",
            type="individual",
            created_by=user["email"],
        )
        workspace = team_store.create_workspace(
            name=org["name"], created_by=user["email"], org_id=org["id"]
        )
        return user, org, workspace
    except Exception:
        # Compensate in reverse so a partial signup never leaves a half-org.
        try:
            if user is not None:
                user_store.delete_user(email=user["email"])
        except Exception:  # noqa: BLE001
            logger.error("signup compensation delete_user failed")
        raise


def _raise_invalid_code(attempt_row: dict[str, Any] | None) -> None:
    """Wrong code — include attempts_remaining from the post-increment row
    so concurrent submissions can't race a second, stale read."""
    row = attempt_row or {}
    left = max(0, int(row.get("max_attempts") or signup_store.MAX_ATTEMPTS)
               - int(row.get("attempts") or 0))
    raise ApiError(400, "invalid_code",
                   "That code doesn't match — try again.",
                   extra={"attempts_remaining": left})


def verify_email(
    *,
    challenge_id: str,
    code: str,
    ip: str = "",
    device_id: str = "",
    ua: str = "",
    idle_seconds: int = 12 * 3600,
    absolute_seconds: int = 24 * 3600,
) -> dict[str, Any]:
    """Verify the code; on success create everything and open the session."""
    result, attempt_row = otp_service.verify_code(challenge_id, code)
    challenge = signup_store.get_challenge(challenge_id)
    if result != "ok":
        record_security_event(
            "otp.verify_failed", outcome="failed",
            session_id=challenge_id, reason_code=result,
        )
        if result in ("consumed", "not_found"):
            raise ApiError(410, "code_expired",
                           "this code is no longer valid — request a new one")
        _raise_invalid_code(attempt_row)
    if challenge is None or challenge.get("purpose") != "signup":
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")

    pending = signup_store.get_pending_signup(pending_id=challenge["subject_id"])
    if pending is None:
        raise ApiError(410, "code_expired",
                       "this signup has expired — sign up again")

    # One-account-per-inbox guard — re-check canonical at verify time so a
    # raced double-signup can't create a second account.
    existing = user_store.find_by_canonical(pending["canonical_email"])
    if existing is not None:
        signup_store.delete_pending_signup(pending_id=pending["id"])
        raise ApiError(409, "account_exists", "this email already has an account")

    try:
        user, org, workspace = _provision_verified_user(pending)
    except user_store.IndexUnavailableError as exc:
        raise ApiError(503, "unavailable",
                       "signup is temporarily unavailable") from exc
    except user_store.EmailTakenError as exc:
        signup_store.delete_pending_signup(pending_id=pending["id"])
        raise ApiError(409, "account_exists",
                       "this email already has an account") from exc
    signup_store.delete_pending_signup(pending_id=pending["id"])

    session = get_auth_state().create_session(
        user_id=user["email"],
        device_id=device_id,
        ip=ip,
        ua=ua,
        idle_seconds=idle_seconds,
        absolute_seconds=absolute_seconds,
        csrf_token=secrets.token_urlsafe(32),
    )
    record_security_event(
        "signup.verified", outcome="succeeded",
        actor=user["email"], session_id=session["jti"],
        details={"org_id": org["id"], "workspace_id": workspace.id},
    )
    record_security_event("session.created", outcome="succeeded",
                          actor=user["email"], session_id=session["jti"])
    return {
        "user": user,
        "org": org,
        "workspace": {"id": workspace.id, "name": workspace.name},
        "session": session,
        "next": "onboarding",
    }


def resend_code(*, challenge_id: str) -> dict[str, Any]:
    """Re-send a code: 60 s cooldown + 5/h + 10/day per subject."""
    challenge = signup_store.get_challenge(challenge_id)
    if challenge is None or challenge.get("consumed_at"):
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    subject = str(challenge.get("subject_id") or "")

    last = challenge.get("last_sent_at")
    if last:
        from datetime import UTC, datetime

        try:
            last_dt = datetime.fromisoformat(str(last))
            if last_dt.tzinfo is None:
                # BSON datetimes read back naive — they are UTC, not local.
                last_dt = last_dt.replace(tzinfo=UTC)
            last_ts = last_dt.timestamp()
            import time as _t

            elapsed = _t.time() - last_ts
            if elapsed < RESEND_COOLDOWN_SEC:
                raise ApiError(429, "resend_too_soon",
                               "wait before requesting another code",
                               retry_after_sec=int(RESEND_COOLDOWN_SEC - elapsed) + 1)
        except ValueError:
            pass

    state = get_auth_state()
    ok, retry = state.rate_limit_hit(
        f"resend:h:{subject}", limit=RESEND_HOURLY_CAP, window_sec=3600)
    if not ok:
        raise ApiError(429, "resend_limit", "too many codes — try later",
                       retry_after_sec=retry)
    ok, retry = state.rate_limit_hit(
        f"resend:d:{subject}", limit=RESEND_DAILY_CAP, window_sec=86400)
    if not ok:
        raise ApiError(429, "resend_limit", "too many codes today",
                       retry_after_sec=retry)

    updated, code = otp_service.reissue(challenge_id)
    if updated is None:
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")

    # Find the recipient from the pending signup or the user store.
    recipient, ctx = _challenge_recipient(updated)
    try:
        consume_send_slot()
    except EmailQuotaExceeded as exc:
        raise ApiError(503, "email_quota_exceeded",
                       "we couldn't send your code right now — try again later",
                       retry_after_sec=exc.retry_after_sec) from exc
    try:
        _send(
            "reset_code" if updated["purpose"] == "reset" else "signup_code",
            recipient,
            {**ctx, "code": code,
             "expires_minutes": int(otp_service.OTP_TTL.total_seconds() // 60)},
            idempotency_key=f"otp:{updated['id']}:{updated['sent_count']}",
        )
    except EmailSendError as exc:
        raise ApiError(503, "email_send_failed",
                       "we couldn't send your code right now — try again",
                       retry_after_sec=60) from exc

    record_security_event("otp.resent", outcome="succeeded", session_id=challenge_id)
    return {
        "challenge_id": challenge_id,
        "resend_after_sec": RESEND_COOLDOWN_SEC,
        "expires_in_sec": int(otp_service.OTP_TTL.total_seconds()),
    }


def _forgot_response(challenge_id: str) -> dict[str, Any]:
    return {
        "ok": True,
        "challenge_id": challenge_id,
        "resend_after_sec": RESEND_COOLDOWN_SEC,
        "expires_in_sec": int(otp_service.OTP_TTL.total_seconds()),
    }


def _refuse_pwned_password(password: str) -> None:
    """P4-3: flag-gated HIBP check. ``-1`` (unknown / outage) is fail-open
    by design — a HIBP outage must never block a real signup or reset."""
    from services import hibp

    if hibp.pwned_count(password) > 0:
        raise ApiError(400, "pwned_password",
                       "that password appears in a public breach — "
                       "choose a different one")


def request_password_reset(*, email: str, ip: str = "",
                           device_id: str = "", ua: str = "",
                           turnstile_token: str = "", form_token: str = "",
                           honeypot: str = "") -> dict[str, Any]:
    """P4-1 forgot-password: uniform 202 with a challenge handle — a fake
    id when the address has no usable account (enumeration parity).
    Bot/abuse checks (form-token, Turnstile, IP+address velocity) never
    change the 202: throttling a bomber must not become an oracle.
    Returns (reply_body, pad_seconds) — the route awaits the pad with
    asyncio.sleep so the pacing floor costs no worker thread."""
    started = time.monotonic()
    # Same id shape as a real stored row (prt-<24 hex>) — the reply body
    # must not reveal whether the account exists.
    fake = _forgot_response(f"prt-{uuid.uuid4().hex[:24]}")
    verdict_form = form_token_mod.check(
        form_token, purpose="reset", honeypot=honeypot)
    if not verdict_form["ok"]:
        record_security_event("password.reset.requested", outcome="failed",
                              reason_code=f"form:{verdict_form['reason']}",
                              details={"ip": ip})
        return _forgot_reply(fake, started)
    if turnstile.configured():
        verdict = turnstile.verify(
            turnstile_token, remote_ip=ip)
        if not verdict["ok"]:
            record_security_event("password.reset.requested", outcome="failed",
                                  reason_code=f"turnstile:{verdict['reason']}",
                                  details={"ip": ip})
            return _forgot_reply(fake, started)
    try:
        normalized = user_store.validate_email(email)
    except (ApiError, user_store.UserStoreError):
        return _forgot_reply(fake, started)
    canonical = canonical_email(normalized)
    # Per-IP and per-target-address velocity: exceeded -> same 202 shape.
    for key, limit, window in (
            (f"forgot:ip:{ip or 'unknown'}", _env_int("FORGOT_IP_HOURLY", 10), 3600),
            (f"forgot:em:{canonical}", _env_int("FORGOT_EMAIL_DAILY", 5), 86400)):
        if _velocity_bucket(key, limit=limit, window_sec=window):
            record_security_event("password.reset.requested", outcome="held",
                                  reason_code="rate_limited",
                                  details={"ip": ip})
            return _forgot_reply(fake, started)
    user = user_store.find_by_canonical(canonical)
    if user is None or user.get("status", "active") != "active" \
            or not user.get("email_verified_at"):
        return _forgot_reply(fake, started)
    # Secure reset LINK (not a code): 256-bit single-use token, only the
    # HMAC is stored, and issuing it invalidates every earlier link.
    # Everything is keyed to the STORED account address — never the typed
    # variant — so a canonical alias can't route the link to a different
    # mailbox and every variant supersedes the same token set.
    account_email = user["email"]
    token, stored = password_reset_store.issue(
        email=account_email, ip=ip, ua=ua)
    base = (getenv_brand("APP_URL") or "https://www.datawrap.io").rstrip("/")
    # The token rides the URL fragment, not the query string — it can never
    # reach a server, CDN or proxy access log or a Referer header.
    reset_url = f"{base}/reset-password#token={token}"

    # Persist the mail BEFORE the lane sees it: a deploy/restart can no
    # longer silently drop a queued reset link — drain_queued() re-submits
    # it on the next boot and the idempotency key dedupes at the provider.
    mail = render_email(
        "reset_link",
        {"name": user.get("name") or "there",
         "reset_url": reset_url,
         "expires_minutes": int(password_reset_store.TOKEN_TTL.total_seconds() // 60)})
    row = mail_outbox.enqueue(
        to=account_email, subject=mail.subject, text=mail.text,
        html=mail.html, idempotency_key=f"reset-link:{stored['id']}",
        purpose="password.reset")
    _deliver_later(lambda: _deliver_outbox_row(row, actor_ip=ip))
    record_security_event("password.reset.requested", outcome="succeeded",
                          actor=account_email, session_id=stored["id"],
                          details={"ip": ip, "requested_as": normalized})
    return _forgot_reply(_forgot_response(stored["id"]), started)


def inspect_reset_token(*, token: str) -> dict[str, Any]:
    """Validate a reset link without consuming it (the landing page calls
    this before showing the password form). Status mirrors the store."""
    status = password_reset_store.inspect(token)
    if status != "ok":
        record_security_event("password.reset.failed", outcome="failed",
                              reason_code=f"inspect:{status}")
    return {"ok": status == "ok", "status": status}


def confirm_password_reset(*, token: str,
                           new_password: str, ip: str = "") -> dict[str, Any]:
    """Consume a reset-link token, set the argon2id hash, kill sessions."""
    if not (MIN_PASSWORD_LEN <= len(new_password or "") <= MAX_PASSWORD_LEN):
        raise ApiError(400, "invalid_password",
                       f"password must be {MIN_PASSWORD_LEN}-{MAX_PASSWORD_LEN} characters")
    _refuse_pwned_password(new_password)
    status, doc = password_reset_store.consume(token)
    if status != "ok" or doc is None:
        # 'used' also covers links superseded by a newer request.
        record_security_event("password.reset.failed", outcome="failed",
                              reason_code=status, details={"ip": ip})
        if status == "expired":
            record_security_event("password.reset.expired", outcome="failed",
                                  details={"ip": ip})
        raise ApiError(410, "link_expired",
                       "This link has expired or was already used. Send a new one.")
    email = doc["email"]
    user = user_store.get_user(email)
    if user is None:
        record_security_event("password.reset.failed", outcome="failed",
                              reason_code="account_gone", actor=email)
        raise ApiError(410, "link_expired", "this account no longer exists")
    user_store.set_password(email=email, password=new_password,
                            algo="argon2id")
    get_auth_state().revoke_all_for_user(email)
    record_security_event("password.reset.used", outcome="succeeded",
                          actor=email, session_id=doc["id"],
                          details={"ip": ip})
    record_security_event("password.reset.completed", outcome="succeeded",
                          actor=email, session_id=doc["id"],
                          details={"ip": ip})
    try:
        _send("password_changed_notice", email,
              {"name": user.get("name") or "there"},
              idempotency_key=f"reset-notice:{doc['id']}")
    except (EmailSendError, EmailQuotaExceeded):
        logger.warning("password-changed notice send failed")
    return {"ok": True}


def request_email_change(*, user_email: str, new_email: str,
                         ip: str = "") -> dict[str, Any]:
    """P4-2: send an OTP to the NEW address — proving ownership — plus a
    notice to the current address."""
    user = user_store.get_user(user_email)
    if user is None:
        raise ApiError(401, "unauthorized", "sign in to change your email")
    # Mail-bomb/velocity: this route sends mail to an arbitrary new address,
    # so cap per-IP, per-requester and per-target before any address work.
    for key, limit, window in (
            (f"emchg:ip:{ip or 'unknown'}", _env_int("EMCHG_IP_HOURLY", 10), 3600),
            (f"emchg:user:{user_email}", _env_int("EMCHG_USER_DAILY", 5), 86400),
            (f"emchg:em:{canonical_email(new_email)}",
             _env_int("EMCHG_EMAIL_DAILY", 3), 86400)):
        retry = _velocity_bucket(key, limit=limit, window_sec=window)
        if retry:
            record_security_event("email_change.requested", outcome="failed",
                                  reason_code="rate_limited",
                                  actor=user_email, details={"ip": ip})
            raise ApiError(429, "rate_limited",
                           "too many requests — try again later",
                           retry_after_sec=retry)
    try:
        normalized = user_store.validate_email(new_email)
    except (ApiError, user_store.UserStoreError):
        raise ApiError(400, "invalid_email", "enter a valid email address")
    # Same address gate as signup: role-based/blocklist, disposable
    # (allow_domain wins inside is_disposable), MX, external verifier.
    bump, reasons = _check_address_deliverable(normalized, ip=ip)
    risk = risk_scorer.score_signals(
        ip=ip, ua="", device_id="",
        turnstile_ok=None, extra_score=bump, extra_reasons=reasons)
    if risk["action"] in ("deny", "hold"):
        record_security_event("email_change.requested", outcome="failed",
                              reason_code=f"risk:{risk['action']}",
                              actor=user_email,
                              details={"ip": ip, "score": risk["score"]})
        raise ApiError(400, "blocked_address",
                       "that address can't be used for an account")
    canonical_new = canonical_email(normalized)
    if canonical_new == canonical_email(user_email):
        raise ApiError(400, "same_address",
                       "that address is already yours")
    if user_store.find_by_canonical(canonical_new) is not None:
        raise ApiError(400, "email_taken",
                       "that address already belongs to an account")
    subject = f"{user_email}|{normalized}"
    challenge, code = otp_service.issue(subject, "email_change")
    try:
        _send("email_change_code", normalized,
              {"name": user.get("name") or "there", "code": code,
               "expires_minutes": int(otp_service.OTP_TTL.total_seconds() // 60)},
              idempotency_key=f"otp:{challenge['id']}:{challenge['sent_count']}")
    except (EmailSendError, EmailQuotaExceeded):
        logger.warning("email-change code send failed")
    record_security_event("email_change.requested", outcome="succeeded",
                          actor=user_email, session_id=challenge["id"],
                          details={"ip": ip})
    return {
        "ok": True,
        "challenge_id": challenge["id"],
        "resend_after_sec": RESEND_COOLDOWN_SEC,
        "expires_in_sec": int(otp_service.OTP_TTL.total_seconds()),
    }


def confirm_email_change(*, challenge_id: str, code: str,
                         requester: str = "",
                         ip: str = "") -> dict[str, Any]:
    """P4-2: verify the new-address code, rekey the account, memberships
    and orgs, revoke every session, notify the old address."""
    result, attempt_row = otp_service.verify_code(challenge_id, code)
    if result != "ok":
        record_security_event("otp.verify_failed", outcome="failed",
                              session_id=challenge_id, reason_code=result)
        if result in ("consumed", "not_found"):
            raise ApiError(410, "code_expired",
                           "this code is no longer valid — request a new one")
        _raise_invalid_code(attempt_row)
    challenge = signup_store.get_challenge(challenge_id)
    if challenge is None or challenge.get("purpose") != "email_change":
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    old_email, _, new_email = str(challenge["subject_id"]).partition("|")
    if not old_email or not new_email:
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    # Defense in depth: the mailed code proves new-address ownership; the
    # request must ALSO be authenticated as the account being rekeyed, so a
    # leaked code alone cannot hijack the account.
    if not requester:
        raise ApiError(401, "unauthorized", "sign in to confirm the change")
    if requester.strip().lower() != old_email.strip().lower():
        raise ApiError(403, "forbidden",
                       "this challenge belongs to a different account")
    canonical_new = canonical_email(new_email)
    if canonical_new == canonical_email(old_email):
        raise ApiError(400, "same_address",
                       "that address is already yours")
    if user_store.find_by_canonical(canonical_new) is not None:
        raise ApiError(400, "email_taken",
                       "that address already belongs to an account")
    try:
        user_store.change_email(
            email=old_email, new_email=new_email, canonical=canonical_new)
    except user_store.EmailTakenError as exc:
        # The store's atomic canonical claim is the real guard; the race
        # loser lands here.
        raise ApiError(400, "email_taken",
                       "that address already belongs to an account") from exc
    except user_store.SameAddressError as exc:
        raise ApiError(400, "same_address",
                       "that address is already yours") from exc
    except user_store.IndexUnavailableError as exc:
        raise ApiError(503, "unavailable",
                       "account rekey is temporarily unavailable") from exc
    # Follow-on rekeys run after the row move. A partial failure leaves a
    # consistent, retryable state: the account carries ``rekey_pending``
    # and ``finish_pending_rekeys`` completes the move.
    try:
        team_store.rekey_memberships(old_email, new_email)
        org_store.rekey_creator(old_email, new_email)
    except Exception:
        logger.error("email-change follow-on rekey failed", exc_info=True)
        try:
            user_store.mark_rekey_pending(email=new_email, old_email=old_email)
        except Exception:
            # The flag IS the retry path — if it can't be written the account
            # stays half-moved with no way to heal, so surface loudly.
            logger.error("email-change rekey flag write failed", exc_info=True)
            record_security_event(
                "email_change.rekey_flag_failed", outcome="failed",
                actor=new_email, session_id=challenge_id,
                details={"ip": ip})
    get_auth_state().revoke_all_for_user(old_email)
    try:
        _send("email_change_old_notice", old_email,
              {"new_email": new_email},
              idempotency_key=f"email-change-notice:{challenge_id}")
    except (EmailSendError, EmailQuotaExceeded):
        logger.warning("email-change old-address notice failed")
    record_security_event("email_change.completed", outcome="succeeded",
                          actor=new_email, session_id=challenge_id,
                          details={"ip": ip})
    return {"ok": True, "email": new_email}


def finish_pending_rekeys(*, email: str) -> bool:
    """Retry the follow-on rekeys for an account flagged ``rekey_pending``.

    Memberships and org-creator rows move by email; when a partial failure
    flagged the account, re-running these completes the move and clears the
    flag. Safe to call repeatedly and concurrently — the claim is
    single-flight (a lease inside the store), the rekeys are rewrites not
    increments, and a failed run frees the lease while keeping the flag.
    """
    old_email = user_store.claim_rekey_pending(email)
    if not old_email:
        return False
    try:
        team_store.rekey_memberships(old_email, email)
        org_store.rekey_creator(old_email, email)
    except Exception:
        user_store.release_rekey_claim(email)
        raise
    user_store.clear_rekey_pending(email=email)
    return True


def complete_onboarding(*, user_email: str, org_name: str = "",
                        profile: dict[str, Any] | None = None,
                        ip: str = "") -> dict[str, Any]:
    """P4-4: persist the onboarding answers on the user's org and stamp
    the account as onboarded."""
    user = user_store.get_user(user_email)
    if user is None:
        raise ApiError(401, "unauthorized", "sign in to finish onboarding")
    orgs = org_store.organizations_for_user(user_email)
    org_id = None
    if orgs:
        org = orgs[0]
        patch: dict[str, Any] = {
            "onboarding_profile": {**(profile or {}),
                                   "completed_at": datetime.now(UTC)
                                   .isoformat()}}
        if org_name.strip():
            patch["name"] = org_name.strip()[:128]
        org_store.update_organization(org["id"], **patch)
        org_id = org["id"]
    user_store.mark_onboarded(user_email)
    return {"ok": True, "org_id": org_id}


def _challenge_recipient(challenge: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Resolve (to, template-context) for a challenge's subject."""
    subject = str(challenge.get("subject_id") or "")
    pending = signup_store.get_pending_signup(pending_id=subject)
    if pending:
        return pending["email"], {"name": pending.get("name") or "there"}
    user = user_store.get_user(subject)
    if user:
        return user["email"], {"name": user.get("name") or "there"}
    return subject, {"name": "there"}


def challenge_for_pending_login(email: str, request: Any | None = None) -> str | None:
    """Login attempted on a pending (unverified) signup: mail a fresh code on
    the active challenge (or issue one) and return the challenge id for the
    403 envelope. Returns None when no pending signup exists — the caller
    falls back to the neutral 401. Send failures still return the id: the
    challenge stays resendable via /auth/resend."""
    canonical = canonical_email(email)
    row = signup_store.get_pending_signup(canonical=canonical)
    if row is None or signup_store._expired(row):
        return None
    challenge = signup_store.active_challenge(row["id"], "signup")
    if challenge is None:
        challenge, code = otp_service.issue(row["id"], "signup")
    else:
        challenge, code = otp_service.reissue(challenge["id"])
    try:
        _send(
            "signup_code",
            row["email"],
            {"name": row.get("name") or "there",
             "code": code,
             "expires_minutes": int(otp_service.OTP_TTL.total_seconds() // 60)},
            idempotency_key=f"otp:{challenge['id']}:{challenge['sent_count']}",
        )
    except EmailSendError:
        logger.warning("login-code resend failed (challenge=%s)", challenge["id"])
    record_security_event("otp.resent", outcome="succeeded",
                          session_id=challenge["id"], request=request)
    return challenge["id"]


def signup_metrics() -> dict[str, Any]:
    """P5: admin metrics for the signup surface — pending work, live
    challenges and the shared send-quota position."""
    return {
        "pending_signups": signup_store.pending_count(),
        "active_challenges": signup_store.active_challenge_count(),
        "email_quota": email_quota.quota_status(),
        # queued > 0 for more than a moment means mail is stuck — the
        # persisted outbox makes 'queued but never sent' visible here.
        "mail_outbox": mail_outbox.counts(),
    }
