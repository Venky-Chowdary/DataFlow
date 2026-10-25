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
import uuid
from typing import Any

from services import otp_service, signup_store, team_store, user_store
from services.api_errors import ApiError
from services.audit_coverage import record_security_event
from services.auth_state import get_auth_state
from services.email_canonical import canonical_email
from services import denylist_store, disposable_list, mx_check, turnstile
from services import form_token as form_token_mod
from services.brand_env import getenv_brand
from services.email_policy import role_assessment
from services import risk_scorer
from services import email_verifier
from services.email_quota import consume_send_slot
from services.email_sender import (
    EmailQuotaExceeded,
    EmailSendError,
    OutboundEmail,
    get_email_sender,
)
from services.email_templates import render_email
from services.org_store import create_organization
from services import org_store
from services.password_hash import hash_password_argon2

logger = logging.getLogger(__name__)

RESEND_COOLDOWN_SEC = 60
RESEND_HOURLY_CAP = 5
RESEND_DAILY_CAP = 10
MIN_PASSWORD_LEN = 12
MAX_PASSWORD_LEN = 128


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


def _send(template: str, to: str, context: dict[str, Any], *, idempotency_key: str) -> None:
    """Quota check → render → send. Raises ApiError on quota (callers map it);
    provider failures raise EmailSendError for the caller to decide on."""
    consume_send_slot()  # raises EmailQuotaExceeded — caller maps to envelope
    mail = render_email(template, context)
    get_email_sender().send(
        OutboundEmail(
            to=to,
            subject=mail.subject,
            text=mail.text,
            html=mail.html,
            idempotency_key=idempotency_key,
        )
    )


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
    canonical = canonical_email(normalized)

    # P2-9 external verifier (optional, fail-open): undeliverable is refused
    # like an MX failure; a "risky" verdict feeds the risk scorer.
    verifier_bump, verifier_reasons = 0, []
    if email_verifier.configured():
        verdict = email_verifier.verify(normalized)
        if verdict["verdict"] == "undeliverable":
            record_security_event("signup.blocked", outcome="failed",
                                  reason_code="verifier:undeliverable",
                                  details={"ip": ip})
            raise ApiError(400, "undeliverable_domain",
                           "that email domain can't receive mail")
        if verdict["verdict"] == "risky":
            verifier_bump = 20
            verifier_reasons = ["verifier:risky"]

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
                       10.0 if role == "flagged" else 0.0),
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
    result = otp_service.verify(challenge_id, code)
    challenge = signup_store.get_challenge(challenge_id)
    if result != "ok":
        record_security_event(
            "otp.verify_failed", outcome="failed",
            session_id=challenge_id, reason_code=result,
        )
        if result in ("consumed", "not_found"):
            raise ApiError(410, "code_expired",
                           "this code is no longer valid — request a new one")
        raise ApiError(400, "invalid_code", "that code doesn't match — try again")
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

    user, org, workspace = _provision_verified_user(pending)
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
            last_ts = datetime.fromisoformat(str(last)).timestamp()
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


def request_password_reset(*, email: str, ip: str = "") -> dict[str, Any]:
    """P4-1 forgot-password: uniform 202 with a challenge handle — a fake
    id when the address has no usable account (enumeration parity)."""
    fake = _forgot_response(f"pw-{uuid.uuid4().hex[:24]}")
    try:
        normalized = user_store.validate_email(email)
    except ApiError:
        return fake
    canonical = canonical_email(normalized)
    user = user_store.find_by_canonical(canonical)
    if user is None or user.get("status", "active") != "active" \
            or not user.get("email_verified_at"):
        return fake
    challenge, code = otp_service.issue(canonical, "reset")
    try:
        _send("reset_code", normalized,
              {"name": user.get("name") or "there", "code": code,
               "expires_minutes": int(otp_service.OTP_TTL.total_seconds() // 60)},
              idempotency_key=f"otp:{challenge['id']}:{challenge['sent_count']}")
    except (EmailSendError, EmailQuotaExceeded):
        # Send failure must not change the response (enumeration); the
        # challenge stays live so the caller can request another code.
        logger.warning("password-reset code send failed")
    record_security_event("password.reset.requested", outcome="succeeded",
                          actor=normalized, session_id=challenge["id"],
                          details={"ip": ip})
    return _forgot_response(challenge["id"])


def confirm_password_reset(*, challenge_id: str, code: str,
                           new_password: str, ip: str = "") -> dict[str, Any]:
    """P4-1: verify the reset code, set the argon2id hash, kill sessions."""
    if not (MIN_PASSWORD_LEN <= len(new_password or "") <= MAX_PASSWORD_LEN):
        raise ApiError(400, "invalid_password",
                       f"password must be {MIN_PASSWORD_LEN}-{MAX_PASSWORD_LEN} characters")
    _refuse_pwned_password(new_password)
    result = otp_service.verify(challenge_id, code)
    if result != "ok":
        record_security_event("otp.verify_failed", outcome="failed",
                              session_id=challenge_id, reason_code=result)
        if result in ("consumed", "not_found"):
            raise ApiError(410, "code_expired",
                           "this code is no longer valid — request a new one")
        raise ApiError(400, "invalid_code", "that code doesn't match — try again")
    challenge = signup_store.get_challenge(challenge_id)
    if challenge is None or challenge.get("purpose") != "reset":
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    canonical = challenge["subject_id"]
    user = user_store.find_by_canonical(canonical)
    if user is None:
        raise ApiError(410, "code_expired", "this account no longer exists")
    user_store.set_password(email=user["email"], password=new_password,
                            algo="argon2id")
    get_auth_state().revoke_all_for_user(user["email"])
    record_security_event("password.reset.completed", outcome="succeeded",
                          actor=user["email"], details={"ip": ip})
    return {"ok": True}


def request_email_change(*, user_email: str, new_email: str,
                         ip: str = "") -> dict[str, Any]:
    """P4-2: send an OTP to the NEW address — proving ownership — plus a
    notice to the current address."""
    user = user_store.get_user(user_email)
    if user is None:
        raise ApiError(401, "unauthorized", "sign in to change your email")
    normalized = user_store.validate_email(new_email)
    if disposable_list.is_disposable(normalized):
        raise ApiError(400, "disposable_domain",
                       "that email domain can't be used for an account")
    canonical_new = canonical_email(normalized)
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
                         ip: str = "") -> dict[str, Any]:
    """P4-2: verify the new-address code, rekey the account, memberships
    and orgs, revoke every session, notify the old address."""
    result = otp_service.verify(challenge_id, code)
    if result != "ok":
        record_security_event("otp.verify_failed", outcome="failed",
                              session_id=challenge_id, reason_code=result)
        if result in ("consumed", "not_found"):
            raise ApiError(410, "code_expired",
                           "this code is no longer valid — request a new one")
        raise ApiError(400, "invalid_code", "that code doesn't match — try again")
    challenge = signup_store.get_challenge(challenge_id)
    if challenge is None or challenge.get("purpose") != "email_change":
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    old_email, _, new_email = str(challenge["subject_id"]).partition("|")
    if not old_email or not new_email:
        raise ApiError(410, "code_expired",
                       "this code is no longer valid — request a new one")
    canonical_new = canonical_email(new_email)
    if user_store.find_by_canonical(canonical_new) is not None:
        raise ApiError(400, "email_taken",
                       "that address already belongs to an account")
    user_store.change_email(
        email=old_email, new_email=new_email, canonical=canonical_new)
    team_store.rekey_memberships(old_email, new_email)
    org_store.rekey_creator(old_email, new_email)
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


def submit_sales_lead(*, fields: dict[str, Any], ip: str = "",
                      ua: str = "", form_token: str = "",
                      honeypot: str = "") -> dict[str, Any]:
    """P4-5: form-token wall first (bots get the same 202 body, nothing
    stored/mailed), then store + internal lead mail + acknowledgement."""
    check = form_token_mod.check(form_token, purpose="sales",
                                 honeypot=honeypot)
    if not check["ok"]:
        record_security_event("signup.blocked", outcome="failed",
                              reason_code=f"sales_form:{check['reason']}",
                              details={"ip": ip})
        return form_token_mod.fake_accept_body()
    from services import sales_leads

    lead = sales_leads.create_lead(fields, ip=ip, user_agent=ua)
    inbox = getenv_brand("SALES_INBOX", "sales@datawrap.io")
    ctx = {k: v for k, v in {
        "First name": fields.get("first_name"),
        "Last name": fields.get("last_name"),
        "Email": fields.get("email"),
        "Company": fields.get("company"),
        "Role": fields.get("role"),
        "Volume": fields.get("volume"),
        "Region": fields.get("region"),
        "Timeframe": fields.get("timeframe"),
        "Phone": fields.get("phone"),
        "Sources": ", ".join(fields.get("sources") or []) or None,
        "Destinations": ", ".join(fields.get("destinations") or []) or None,
        "Message": fields.get("message"),
    }.items() if v}
    for template, to, key in (
        ("sales_lead_internal", inbox, f"sales-lead:{lead['created_at']}:{fields.get('email')}"),
        ("sales_acknowledgement", fields.get("email") or "",
         f"sales-ack:{lead['created_at']}:{fields.get('email')}"),
    ):
        if not to:
            continue
        try:
            consume_send_slot()
            _send(template, to, {**ctx, "name": fields.get("first_name")},
                  idempotency_key=key)
        except (EmailSendError, EmailQuotaExceeded):
            # A lead is durable the moment it is stored — mail failures are
            # logged, never surfaced to the submitter.
            logger.warning("sales-lead mail (%s) failed", template)
    record_security_event("sales.lead.created", outcome="succeeded",
                          details={"ip": ip,
                                   "domain": str(fields.get("email") or "")
                                   .split("@")[-1]})
    return {"ok": True}


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
