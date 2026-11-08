"""Workspace invitations by email — the enterprise "invite a teammate" flow.

A workspace admin names an address and a role; the invitee receives a branded
mail with a single-use link (token in the URL fragment, like the reset link)
and joins by accepting it — signing in to an existing account, or creating
one, in which case the link itself proves the inbox. Nothing is granted until
the invitee acts.

Rules, all enforced here so every caller (routes, future SCIM/Pilot tools)
gets them:

- Authority: the exact membership rule ``team_store`` applies to a direct
  add (``authorize_member_grant``): ``member.invite`` to invite, never above
  the actor's own role, ``workspace.manage`` for admin. Checked at create,
  resend and revoke, and again for the *inviter* at accept — an inviter who
  lost the right since cannot still bring someone in.
- One pending invitation per address per workspace; an existing member is a
  conflict, not a silent re-role.
- Resend rotates the token (the old link reads ``superseded``) and pushes the
  expiry out; per-actor, per-workspace and per-address caps plus a cooldown.
- Accept is a compare-and-set on the link being used; membership write
  failures compensate (the invitation is reopened, a just-created account is
  removed).
- Every step is a named security event; tokens never reach logs or audit.
- Mail goes through ``signup_service.queue_mail`` (sealed outbox, retries,
  ``mail.sent``/``mail.send_failed``), and the outbox drain skips a queued
  invitation whose link is already dead (``outbox_skip_reason``).
"""

from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from services import team_store, user_store
from services import workspace_invite_store as store
from services.api_errors import ApiError
from services.audit_coverage import record_security_event
from services.brand_env import getenv_brand
from services.email_canonical import canonical_email

logger = logging.getLogger(__name__)

RESEND_COOLDOWN_SEC = 60
MAX_PENDING_PER_WORKSPACE = 500
_PURPOSE = "team.invite"


def _env_int(name: str, default: int, *, lo: int, hi: int) -> int:
    try:
        value = int(getenv_brand(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(lo, min(hi, value))


def invite_ttl() -> timedelta:
    """``DATAWRAP_INVITE_TTL_DAYS`` (default 7, clamped 1..30)."""
    return timedelta(days=_env_int("INVITE_TTL_DAYS", 7, lo=1, hi=30))


def _rate_limited(key: str, *, limit: int, window_sec: int) -> int:
    from services.auth_state import get_auth_state

    allowed, retry_after = get_auth_state().rate_limit_hit(key, limit=limit, window_sec=window_sec)
    return 0 if allowed else (retry_after or window_sec)


def _now() -> datetime:
    return datetime.now(UTC)


def public_view(doc: dict[str, Any]) -> dict[str, Any]:
    """What an admin may see: never the token hash or superseded hashes."""
    return {
        "id": doc["id"],
        "workspace_id": doc.get("workspace_id"),
        "email": doc.get("email"),
        "name": doc.get("name") or "",
        "role": doc.get("role"),
        "status": store.effective_status(doc),
        "invited_by": doc.get("invited_by"),
        "invited_by_name": doc.get("invited_by_name") or "",
        "created_at": doc.get("created_at"),
        "last_sent_at": doc.get("last_sent_at"),
        "expires_at": doc.get("expires_at"),
        "sent_count": int(doc.get("sent_count") or 1),
    }


def _accept_url(token: str) -> str:
    from services.signup_service import app_url

    # Fragment, not query: the token never reaches a server, CDN or proxy
    # access log, nor a Referer header.
    return f"{app_url()}/accept-invite#token={token}"


def _inviter_display(email: str) -> str:
    account = user_store.get_user(email)
    if account and str(account.get("name") or "").strip():
        return str(account["name"]).strip()
    from src.services.auth_service import lookup_user

    env_user = lookup_user(email) if email else None
    name = str((env_user or {}).get("name") or "").strip()
    return "" if name == email else name


def _deny(principal: Any, *, operation: str, workspace_id: str, target: str,
          role: str, error: Exception, ip: str) -> None:
    record_security_event(
        "team.invite.denied", outcome="failed", actor=principal.email,
        workspace_id=workspace_id, reason_code=getattr(error, "reason", "refused"),
        details={"operation": operation, "target": target, "attempted_role": role,
                 "detail": str(error), "ip": ip, "principal": principal.describe()})


def _authorize(principal: Any, *, operation: str, workspace_id: str, target: str,
               role: str, ip: str) -> None:
    try:
        team_store.authorize_member_grant(workspace_id=workspace_id, role=role, principal=principal)
    except team_store.WorkspaceNotFound as exc:
        raise ApiError(404, "workspace_not_found", "Workspace not found") from exc
    except team_store.WorkspaceClosing as exc:
        raise ApiError(409, "workspace_closing", str(exc)) from exc
    except team_store.PermissionDenied as exc:
        _deny(principal, operation=operation, workspace_id=workspace_id, target=target,
              role=role, error=exc, ip=ip)
        raise ApiError(403, "forbidden", str(exc)) from exc


def _check_rate(principal: Any, *, workspace_id: str, canonical: str, operation: str, ip: str) -> None:
    buckets = (
        (f"invite:actor:{principal.email}", _env_int("INVITE_ACTOR_HOURLY", 30, lo=1, hi=10_000), 3600),
        (f"invite:ws:{workspace_id}", _env_int("INVITE_WORKSPACE_DAILY", 200, lo=1, hi=100_000), 86400),
        (f"invite:to:{workspace_id}:{canonical}", _env_int("INVITE_ADDRESS_DAILY", 5, lo=1, hi=1_000), 86400),
    )
    for key, limit, window in buckets:
        retry = _rate_limited(key, limit=limit, window_sec=window)
        if retry:
            _deny(principal, operation=operation, workspace_id=workspace_id, target=canonical,
                  role="", error=PermissionError("rate_limited"), ip=ip)
            raise ApiError(429, "rate_limited", "Too many invitations — try again later.",
                           retry_after_sec=retry)


def _consume_quota() -> None:
    from services.email_quota import consume_send_slot
    from services.email_sender import EmailQuotaExceeded

    try:
        consume_send_slot()
    except EmailQuotaExceeded as exc:
        raise ApiError(503, "email_quota_exceeded",
                       "We couldn't send the invitation right now — try again later.",
                       retry_after_sec=exc.retry_after_sec) from exc


def _queue_invite_mail(doc: dict[str, Any], token: str, workspace_name: str) -> None:
    from services.signup_service import queue_mail

    queue_mail(
        "workspace_invite", doc["email"],
        {"inviter_name": doc.get("invited_by_name") or "", "inviter_email": doc.get("invited_by") or "",
         "workspace_name": workspace_name, "role": doc.get("role"), "name": doc.get("name") or "",
         "accept_url": _accept_url(token), "expires_days": invite_ttl().days},
        idempotency_key=f"invite:{doc['id']}:{int(doc.get('sent_count') or 1)}",
        purpose=_PURPOSE, quota="prepaid")


def _workspace_name(workspace_id: str) -> str:
    ws = team_store.get_workspace(workspace_id)
    return ws.name if ws else ""


# ── admin operations ─────────────────────────────────────────────────────


def create_invite(*, workspace_id: str, email: str, role: str, name: str,
                  principal: Any, ip: str = "") -> dict[str, Any]:
    try:
        normalized = user_store.validate_email(email)
    except user_store.UserStoreError as exc:
        raise ApiError(400, "invalid_email", str(exc)) from exc
    role = team_store.normalize_role(role)
    if role not in team_store.ROLES:
        raise ApiError(400, "invalid_role", f"role must be one of {', '.join(team_store.ROLES)}")
    canonical = canonical_email(normalized)
    _authorize(principal, operation="create", workspace_id=workspace_id, target=normalized,
               role=role, ip=ip)
    account = user_store.find_by_canonical(canonical)
    member_email = str(account["email"]) if account else normalized
    current = team_store.get_workspace_role(workspace_id=workspace_id, email=member_email)
    if current:
        raise ApiError(409, "already_member",
                       f"{member_email} is already a member of this workspace as {current} — "
                       "change their role instead of inviting them again")
    from services import denylist_store

    if denylist_store.is_denied(canonical, kind="email"):
        raise ApiError(400, "blocked_address", "This address can't be invited.")
    if store.pending_count(workspace_id) >= MAX_PENDING_PER_WORKSPACE:
        raise ApiError(409, "too_many_pending",
                       "This workspace has too many pending invitations — revoke some first.")
    _check_rate(principal, workspace_id=workspace_id, canonical=canonical, operation="create", ip=ip)
    # Quota before any write: a refusal leaves no invitation behind that
    # nobody was ever told about.
    _consume_quota()
    try:
        token, doc = store.create(
            workspace_id=workspace_id, email=normalized, canonical=canonical, role=role,
            name=team_store.clean_display_name(name), invited_by=principal.email,
            invited_by_name=_inviter_display(principal.email), ttl=invite_ttl())
    except store.InviteAlreadyPending as exc:
        raise ApiError(409, "invite_pending",
                       f"{normalized} already has a pending invitation — resend it instead",
                       extra={"invite_id": (exc.existing or {}).get("id")}) from exc
    try:
        _queue_invite_mail(doc, token, _workspace_name(workspace_id))
    except Exception:
        store.delete(doc["id"])
        logger.error("invitation mail could not be queued; invitation withdrawn", exc_info=True)
        raise ApiError(503, "email_send_failed", "We couldn't send the invitation — try again.")
    record_security_event(
        "team.invite.created", outcome="succeeded", actor=principal.email,
        session_id=doc["id"], workspace_id=workspace_id,
        details={"target": normalized, "role": role, "expires_at": doc["expires_at"],
                 "ip": ip, "principal": principal.describe()})
    return public_view(doc)


def list_invites(workspace_id: str) -> list[dict[str, Any]]:
    return [public_view(doc) for doc in store.list_for_workspace(workspace_id)]


def _invite_in(workspace_id: str, invite_id: str) -> dict[str, Any]:
    doc = store.get(invite_id)
    if doc is None or doc.get("workspace_id") != workspace_id:
        raise ApiError(404, "invite_not_found", "Invitation not found")
    return doc


def resend_invite(*, workspace_id: str, invite_id: str, principal: Any, ip: str = "") -> dict[str, Any]:
    doc = _invite_in(workspace_id, invite_id)
    _authorize(principal, operation="resend", workspace_id=workspace_id, target=doc["email"],
               role=str(doc.get("role")), ip=ip)
    if doc.get("status") != "pending":
        raise ApiError(409, "invite_closed", f"This invitation was already {doc.get('status')}.")
    last = str(doc.get("last_sent_at") or "")
    if last:
        elapsed = (_now() - datetime.fromisoformat(last)).total_seconds()
        if elapsed < RESEND_COOLDOWN_SEC:
            raise ApiError(429, "resend_too_soon", "Wait a minute before sending this invitation again.",
                           retry_after_sec=int(RESEND_COOLDOWN_SEC - elapsed) + 1)
    _check_rate(principal, workspace_id=workspace_id, canonical=str(doc.get("canonical_email")),
                operation="resend", ip=ip)
    _consume_quota()
    rotated = store.rotate_token(invite_id, ttl=invite_ttl())
    if rotated is None:
        raise ApiError(409, "invite_closed", "This invitation is no longer pending.")
    token, fresh = rotated
    try:
        _queue_invite_mail(fresh, token, _workspace_name(workspace_id))
    except Exception:
        logger.error("invitation resend mail could not be queued", exc_info=True)
        raise ApiError(503, "email_send_failed", "We couldn't send the invitation — try again.")
    record_security_event(
        "team.invite.resent", outcome="succeeded", actor=principal.email,
        session_id=invite_id, workspace_id=workspace_id,
        details={"target": fresh["email"], "role": fresh.get("role"), "sent_count": fresh.get("sent_count"),
                 "expires_at": fresh.get("expires_at"), "ip": ip, "principal": principal.describe()})
    return public_view(fresh)


def revoke_invite(*, workspace_id: str, invite_id: str, principal: Any, ip: str = "") -> dict[str, Any]:
    doc = _invite_in(workspace_id, invite_id)
    _authorize(principal, operation="revoke", workspace_id=workspace_id, target=doc["email"],
               role=str(doc.get("role")), ip=ip)
    done = store.transition(invite_id, to_status="revoked", revoked_by=principal.email,
                            revoked_at=_now().isoformat())
    if done is None:
        raise ApiError(409, "invite_closed", f"This invitation was already {doc.get('status')}.")
    record_security_event(
        "team.invite.revoked", outcome="succeeded", actor=principal.email,
        session_id=invite_id, workspace_id=workspace_id,
        details={"target": doc["email"], "role": doc.get("role"), "ip": ip,
                 "principal": principal.describe()})
    return public_view(done)


# ── invitee operations (public, token-authenticated) ─────────────────────


def _check_accept_rate(ip: str) -> None:
    retry = _rate_limited(f"invite:accept:ip:{ip or 'unknown'}",
                          limit=_env_int("INVITE_ACCEPT_IP_10MIN", 30, lo=1, hi=10_000), window_sec=600)
    if retry:
        raise ApiError(429, "rate_limited", "Too many attempts — try again shortly.",
                       retry_after_sec=retry)


def _resolve(token: str) -> tuple[str, dict[str, Any] | None]:
    """(status, record) for a link: ok | expired | revoked | accepted |
    superseded | invalid."""
    doc, superseded = store.find_by_token(token)
    if doc is None:
        return "invalid", None
    if superseded:
        return "superseded", doc
    status = store.effective_status(doc)
    if status != "pending":
        return status, doc
    ws = team_store.get_workspace(str(doc.get("workspace_id")))
    if ws is None or ws.deleting_at:
        return "invalid", doc
    return "ok", doc


def _identity(doc: dict[str, Any]) -> tuple[str, str]:
    """(account email or "", state) where state is new | existing | disabled."""
    canonical = str(doc.get("canonical_email") or canonical_email(doc["email"]))
    stored = user_store.find_by_canonical(canonical) or user_store.get_user_record(doc["email"])
    if stored is not None:
        email = str(stored.get("email"))
        if str(stored.get("status") or "active") != "active":
            return email, "disabled"
        return email, "existing"
    from services.sign_in_capability import can_sign_in

    if can_sign_in(doc["email"]):  # environment-provisioned or SSO identity
        return str(doc["email"]), "existing"
    return "", "new"


def _same_mailbox(a: str, b: str) -> bool:
    return bool(a) and bool(b) and canonical_email(a) == canonical_email(b)


def inspect_invite(*, token: str, signed_in_email: str = "", ip: str = "") -> dict[str, Any]:
    """Landing-page probe; never consumes the link. Details only for a live
    link — anything else answers its status alone."""
    _check_accept_rate(ip)
    status, doc = _resolve(token)
    if status != "ok" or doc is None:
        return {"ok": False, "status": status}
    account_email, account = _identity(doc)
    return {
        "ok": True,
        "status": "ok",
        "email": doc["email"],
        "workspace_id": doc["workspace_id"],
        "workspace_name": _workspace_name(doc["workspace_id"]),
        "role": doc.get("role"),
        "inviter_name": doc.get("invited_by_name") or "",
        "inviter_email": doc.get("invited_by") or "",
        "expires_at": doc.get("expires_at"),
        "account": account,
        "signed_in_as_invitee": _same_mailbox(signed_in_email, account_email or doc["email"]),
    }


def _fail_accept(doc: dict[str, Any] | None, reason: str, *, ip: str, actor: str = "") -> None:
    record_security_event(
        "team.invite.accept_failed", outcome="failed", actor=actor or (doc or {}).get("email"),
        session_id=(doc or {}).get("id"), workspace_id=(doc or {}).get("workspace_id"),
        reason_code=reason, details={"ip": ip})


def _inviter_still_authorized(doc: dict[str, Any]) -> tuple[bool, bool]:
    """(authorized, inviter_is_platform_admin) for the original inviter, now."""
    from services.tenant_scope import actor_is_unrestricted
    from src.services.auth_service import lookup_user

    inviter = str(doc.get("invited_by") or "")
    platform_admin = actor_is_unrestricted(lookup_user(inviter) if inviter else None)
    try:
        team_store.authorize_member_grant(
            workspace_id=doc["workspace_id"], role=str(doc.get("role")),
            actor_email=inviter, actor_is_platform_admin=platform_admin)
    except team_store.TeamStoreError:
        return False, platform_admin
    return True, platform_admin


def accept_invite(*, token: str, signed_in_email: str = "", name: str = "", password: str = "",
                  ip: str = "", ua: str = "", device_id: str = "") -> dict[str, Any]:
    """Accept a link. Existing identity → must be signed in as it. New
    address → ``name`` + ``password`` create the account (verified: the link
    proved the inbox) and a session is opened (returned as ``session``)."""
    _check_accept_rate(ip)
    status, doc = _resolve(token)
    if status != "ok" or doc is None:
        _fail_accept(doc, status, ip=ip)
        raise ApiError(410, f"invite_{status}", {
            "expired": "This invitation has expired. Ask the workspace admin to send a new one.",
            "revoked": "This invitation was withdrawn by the workspace admin.",
            "accepted": "This invitation was already accepted. Sign in to open the workspace.",
            "superseded": "A newer invitation was sent to you — use the link in the latest email.",
        }.get(status, "This invitation link isn't valid."))
    account_email, account = _identity(doc)
    if account == "disabled":
        _fail_accept(doc, "account_disabled", ip=ip)
        raise ApiError(403, "account_disabled", "Sign-in is disabled for this account. Contact your admin.")
    if signed_in_email and not _same_mailbox(signed_in_email, account_email or doc["email"]):
        _fail_accept(doc, "email_mismatch", ip=ip, actor=signed_in_email)
        raise ApiError(403, "invite_email_mismatch",
                       f"This invitation is for {doc['email']}. Sign out, then open the link again.")
    if account == "existing" and not signed_in_email:
        raise ApiError(401, "sign_in_required", f"Sign in as {doc['email']} to accept this invitation.")
    if account == "new":
        from services.signup_service import check_new_password

        check_new_password(password)
    authorized, inviter_pa = _inviter_still_authorized(doc)
    if not authorized:
        _fail_accept(doc, "inviter_lost_authority", ip=ip)
        raise ApiError(410, "invite_invalid",
                       "The person who invited you can no longer add members. Ask a workspace admin to invite you again.")
    claimed = store.transition(doc["id"], to_status="accepted", require_unexpired=True,
                               token_hash=store.hash_token(token), accepted_at=_now().isoformat(),
                               accepted_by=account_email or doc["email"])
    if claimed is None:
        _fail_accept(doc, "race_lost", ip=ip)
        raise ApiError(410, "invite_accepted", "This invitation was already used.")
    created_user: dict[str, Any] | None = None
    member_email = account_email or doc["email"]
    try:
        if account == "new":
            from services.password_hash import hash_password_argon2

            created_user = user_store.create_signup_user(
                email=doc["email"], name=name or doc.get("name") or "",
                password_hash=hash_password_argon2(password), password_algo="argon2id",
                canonical=str(doc.get("canonical_email")), signup_ip=ip,
                created_by=f"invite:{doc['id']}")
            member_email = created_user["email"]
        already = team_store.get_workspace_role(workspace_id=doc["workspace_id"], email=member_email)
        if not already:
            team_store.add_workspace_member(
                workspace_id=doc["workspace_id"], email=member_email, role=str(doc.get("role")),
                added_by=str(doc.get("invited_by")), actor_is_platform_admin=inviter_pa,
                name=doc.get("name") or (created_user or {}).get("name") or "")
    except Exception as exc:
        store.reopen(doc["id"], from_status="accepted")
        if created_user is not None:
            try:
                user_store.delete_user(email=created_user["email"])
            except Exception:  # noqa: BLE001
                logger.error("invite accept compensation delete_user failed", exc_info=True)
        if isinstance(exc, user_store.EmailTakenError):
            _fail_accept(doc, "account_exists", ip=ip)
            raise ApiError(409, "account_exists", "This email already has an account — sign in to accept.") from exc
        if isinstance(exc, team_store.TeamStoreError):
            _fail_accept(doc, getattr(exc, "reason", "membership_refused"), ip=ip)
            raise ApiError(409, "invite_invalid", str(exc)) from exc
        raise
    workspace_name = _workspace_name(doc["workspace_id"])
    record_security_event(
        "team.invite.accepted", outcome="succeeded", actor=member_email, session_id=doc["id"],
        workspace_id=doc["workspace_id"],
        details={"role": doc.get("role"), "invited_by": doc.get("invited_by"),
                 "account_created": created_user is not None, "already_member": bool(already), "ip": ip})
    result: dict[str, Any] = {
        "ok": True,
        "workspace": {"id": doc["workspace_id"], "name": workspace_name},
        "role": already or doc.get("role"),
        "already_member": bool(already),
        "account_created": created_user is not None,
    }
    if created_user is not None:
        from services.auth_state import get_auth_state
        from services.signup_service import queue_welcome

        session = get_auth_state().create_session(
            user_id=created_user["email"], device_id=device_id, ip=ip, ua=ua,
            csrf_token=secrets.token_urlsafe(32))
        record_security_event("session.created", outcome="succeeded",
                              actor=created_user["email"], session_id=session["jti"])
        queue_welcome(email=created_user["email"], name=created_user.get("name") or "",
                      workspace_name=workspace_name, joined=True, role=str(doc.get("role")), ip=ip)
        result["user"] = created_user
        result["session"] = session
    return result


def outbox_skip_reason(idempotency_key: str) -> str | None:
    """Terminal status for a queued invitation mail whose link is dead."""
    parts = str(idempotency_key or "").split(":")
    if len(parts) != 3 or parts[0] != "invite":
        return None
    doc = store.get(parts[1])
    if doc is None:
        return "expired"
    status = store.effective_status(doc)
    if status == "expired":
        return "expired"
    if status != "pending":
        return "superseded"
    try:
        if int(doc.get("sent_count") or 1) > int(parts[2]):
            return "superseded"
    except ValueError:
        return None
    return None
