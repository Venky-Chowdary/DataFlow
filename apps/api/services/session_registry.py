"""One registry of a person's sign-ins: list, revoke one, revoke the others (D338).

Every sign-in mints a Bearer session through ``auth_service.create_token``,
which registers it in ``services.auth_sessions`` (durable in Mongo, checked on
every request by ``verify_token``): password login, MFA step-up, SSO, and the
browser sign-ins that also set a cookie (verified signup, accepted invitation).
That store is the one "Active sessions" lists and revokes.

A browser sign-in additionally holds a cookie session in ``services.auth_state``;
its jti is recorded on the Bearer row (``cookie_jti``), so revoking the listed
session signs the cookie out too. Cookie sessions are never listed on their
own. "Sign out everywhere else" and a password change also revoke any cookie
session of the person except the caller's own, so one opened before sessions
were paired cannot outlive them.

The session making the request (Bearer jti or its paired cookie) is kept on a
password change and on "sign out everywhere else"; a password reset by e-mail
link revokes everything.
"""

from __future__ import annotations

import logging
from typing import Any

from services import auth_sessions

logger = logging.getLogger(__name__)


def current_session_id(request: Any) -> str:
    """The jti the request authenticated with: its cookie session, else its Bearer token."""
    jti = getattr(request.state, "cookie_session_jti", "") or ""
    if not jti:
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            from src.services.auth_service import token_jti

            jti = token_jti(auth[7:].strip()) or ""
    return jti


def _is_current(row: dict[str, Any], current: str) -> bool:
    return bool(current) and current in (row.get("jti"), row.get("cookie_jti"))


def _cookie_state():
    from services.auth_state import get_auth_state

    return get_auth_state()


def list_sessions(email: str, *, current: str = "") -> list[dict[str, Any]]:
    """The person's live sessions, newest activity first; an idled-out one is left out."""
    from services.session_policy import idle_timeout_seconds

    rows = auth_sessions.list_active(email)
    limit = idle_timeout_seconds(email) if rows else None
    if limit is not None:
        import time

        now = int(time.time())
        rows = [
            r for r in rows
            if _is_current(r, current)
            or now - int(r.get("last_seen_at") or r.get("created_at") or now) <= limit
        ]
    return [
        {
            "jti": r["jti"],
            "device_id": r.get("device_id", ""),
            "ip": r.get("ip", ""),
            "ua": r.get("ua", ""),
            "created_at": r.get("created_at"),
            "last_seen_at": r.get("last_seen_at") or r.get("created_at"),
            "absolute_expires_at": r.get("expires_at"),
            "current": _is_current(r, current),
        }
        for r in rows
    ]


def revoke_one(email: str, jti: str) -> bool:
    """Revoke one of the person's own sessions; False when it is not theirs."""
    normalized = email.strip().lower()
    row = auth_sessions.get_session(jti)
    if row and str(row.get("email") or "").strip().lower() == normalized:
        auth_sessions.revoke_session(jti)
        if row.get("cookie_jti"):
            _cookie_state().revoke_session(str(row["cookie_jti"]))
        return True
    state = _cookie_state()
    doc = state.get_session(jti)
    if doc and str(doc.get("user_id") or "").strip().lower() == normalized:
        state.revoke_session(jti)
        return True
    return False


def revoke_others(email: str, *, current: str) -> int:
    """Revoke every session of ``email`` except ``current``; returns how many listed ones died."""
    rows = auth_sessions.list_active(email)
    mine = next((r for r in rows if _is_current(r, current)), None)
    keep_bearer = mine["jti"] if mine else current
    keep_cookie = frozenset(
        j for j in (current, (mine or {}).get("cookie_jti")) if j
    )
    auth_sessions.revoke_all_for_email(email, keep_jti=keep_bearer)
    # A cookie session opened before sessions were paired is not listed but
    # still signs someone in: revoke it and count it; paired cookies die with
    # their listed session.
    paired = frozenset(r["cookie_jti"] for r in rows if r.get("cookie_jti"))
    unpaired = _revoke_cookies(email, keep=keep_cookie | paired)
    _revoke_cookies(email, keep=keep_cookie)
    return sum(1 for r in rows if r is not mine) + unpaired


def revoke_all(email: str) -> int:
    """Revoke every session of ``email`` (password reset by e-mail link)."""
    count = auth_sessions.revoke_all_for_email(email)
    _revoke_cookies(email)
    return count


def _revoke_cookies(email: str, *, keep: frozenset[str] = frozenset()) -> int:
    """Revoke the person's live cookie sessions not in ``keep``; returns how many."""
    state = _cookie_state()
    revoked = 0
    for user_id in {email, email.strip().lower()}:
        for doc in state.list_sessions(user_id):
            if doc["jti"] not in keep:
                state.revoke_session(doc["jti"])
                revoked += 1
    return revoked
