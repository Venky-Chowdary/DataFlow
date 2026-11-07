"""Role-address policy (P2-4).

Blocked prefixes can never sign up (no human behind them). Flagged
prefixes raise the risk score rather than refusing outright.
"""

from __future__ import annotations

_BLOCKED_LOCAL_PARTS = frozenset(
    {"noreply", "postmaster", "abuse", "mailer-daemon", "no-reply",
     "donotreply", "do-not-reply", "bounce", "hostmaster", "webmaster"}
)

_FLAGGED_LOCAL_PARTS = frozenset(
    {"info", "admin", "test", "support", "sales", "contact", "help",
     "office", "mail", "hello"}
)


def role_assessment(email: str) -> str | None:
    """``"blocked"`` | ``"flagged"`` | ``None`` for the local part."""
    local = (email or "").strip().lower().split("@", 1)[0]
    base = local.split("+", 1)[0]
    if base in _BLOCKED_LOCAL_PARTS:
        return "blocked"
    if base in _FLAGGED_LOCAL_PARTS:
        return "flagged"
    return None


def is_blocked_role(email: str) -> bool:
    return role_assessment(email) == "blocked"
