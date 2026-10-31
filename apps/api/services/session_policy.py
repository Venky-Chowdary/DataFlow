"""Workspace session idle-timeout policy (QA enterprise #7, round 8).

A tenant (the workspace's enterprise profile) records ``session_timeout_hours``.
When the tenant's ``enforce_session_timeout`` is on — the default since QA
round 11, and set whenever the hours are saved — a sign-in session that has
been idle longer than that many hours is revoked on its next request (401).
No tenant, or a tenant that explicitly records the hours as policy only,
enforces nothing beyond the token's own TTL. Session rows are durable in
Mongo (services.auth_sessions); a session with no activity on record is
recorded as seen now instead of skipping the check.

A session belongs to a person, not to one workspace, so the strictest
enforced policy among the workspaces they are a member of applies. API keys
are not sessions and are not affected.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

#: Do not rewrite the session row on every request.
TOUCH_INTERVAL_SEC = 60


def idle_timeout_seconds(email: str) -> int | None:
    """Strictest enforced idle timeout for this person, or ``None``."""
    try:
        from services.team_store import list_memberships_for_user
        from services.tenant_store import get_tenant_for_workspace

        hours: list[int] = []
        for row in list_memberships_for_user(email):
            tenant = get_tenant_for_workspace(str(row.get("workspace_id") or ""))
            if tenant and getattr(tenant, "enforce_session_timeout", False):
                hours.append(max(1, int(tenant.session_timeout_hours or 8)))
        return min(hours) * 3600 if hours else None
    except Exception:  # noqa: BLE001 — policy lookup must never lock everyone out
        logger.warning("session idle policy lookup failed", exc_info=True)
        return None


def enforce_idle(*, email: str, last_seen_at: int | None, now: int | None = None) -> bool:
    """True when the session may continue; False when it has idled out."""
    limit = idle_timeout_seconds(email)
    if limit is None or not last_seen_at:
        return True
    current = int(now if now is not None else time.time())
    return current - int(last_seen_at) <= limit
