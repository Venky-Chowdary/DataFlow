"""Throwaway workspaces for security retests (C18, QA r14).

QA could not retest MFA, session timeout or the IP allowlist without risking
the real owner account: those controls are a workspace's tenant policy, and
only a platform administrator may create a workspace. A platform admin now
provisions an isolated **sandbox workspace** for one named tester:

* ``POST /api/v1/team/sandbox-workspaces`` — platform administrators only
  (signed in, or a platform-wide admin key), with a written reason. Creates a
  workspace named ``[sandbox] <name>``, makes the tester its only workspace
  admin (the platform admin who created it is *not* a member, so a policy the
  tester turns on — MFA, a short session timeout, an allowlist — can lock out
  nobody but the tester, in that workspace only), and stamps
  ``sandbox_expires_at`` (default 24 h, at most 7 days). The tester may be an
  existing login, or a synthetic one created here (``create_account``) whose
  one-time password is returned once.
* ``GET`` lists sandboxes; ``DELETE /{id}`` tears one down: its tenant
  profile (MFA / session / IP policy), notification channels and API keys
  (revoked, so a leaked test key dies with it) and the workspace with its
  memberships. Only a workspace marked as a sandbox can be deleted this way.
* Expired sandboxes are torn down by the scheduler beat
  (:func:`sweep_expired_sandboxes`), audited as actor ``system:sandbox-expiry``.

Every step is an audit event (``team.sandbox.*``). Nothing here touches a
workspace that is not a sandbox. Test fixture: ``tests/r14_sandbox_world.py``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

NAME_PREFIX = "[sandbox] "
DEFAULT_TTL_HOURS = 24
MAX_TTL_HOURS = 168
EXPIRY_ACTOR = "system:sandbox-expiry"


class SandboxError(ValueError):
    status = 400


class SandboxNotFound(SandboxError):
    status = 404


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _parse(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def is_sandbox(ws: Any) -> bool:
    return bool(getattr(ws, "sandbox_expires_at", ""))


def describe(ws: Any, *, now: datetime | None = None) -> dict[str, Any]:
    from services.team_store import list_workspace_members

    expires = _parse(ws.sandbox_expires_at)
    return {
        **ws.to_dict(),
        "sandbox": True,
        "expired": bool(expires and expires <= (now or _now())),
        "members": [
            {"email": m["email"], "role": m["role"]} for m in list_workspace_members(ws.id)
        ],
    }


def create_sandbox(
    *,
    name: str,
    tester_email: str,
    reason: str,
    actor: str,
    ttl_hours: int = DEFAULT_TTL_HOURS,
    create_account: bool = False,
) -> tuple[dict[str, Any], str | None]:
    """Create the sandbox; returns its description and a one-time password (or None)."""
    from services import team_store, user_store
    from services.sign_in_capability import can_sign_in

    if not 1 <= int(ttl_hours) <= MAX_TTL_HOURS:
        raise SandboxError(f"ttl_hours must be between 1 and {MAX_TTL_HOURS}")
    tester = user_store.validate_email(tester_email)
    if tester == user_store.normalize_email(actor):
        raise SandboxError(
            "The tester must be another account: the platform administrator stays "
            "outside the sandbox so its policies cannot lock them out"
        )
    issued: str | None = None
    if not can_sign_in(tester):
        if not create_account:
            raise SandboxError(
                f"{tester} cannot sign in — pass create_account to create a synthetic test login"
            )
        _user, issued = user_store.create_user(
            email=tester, name="Security tester", role="member", created_by=actor
        )
    label = " ".join((name or "security test").split())[: 128 - len(NAME_PREFIX)]
    ws = team_store.create_workspace(name=NAME_PREFIX + label, created_by=actor)
    ws.sandbox_expires_at = (_now() + timedelta(hours=int(ttl_hours))).isoformat()
    team_store._write_workspace(ws)
    team_store.add_workspace_member(
        workspace_id=ws.id, email=tester, role="admin", added_by=actor, actor_is_platform_admin=True
    )
    team_store.remove_workspace_member(
        workspace_id=ws.id, email=actor, removed_by=actor, actor_is_platform_admin=True
    )
    _audit("team.sandbox.create", actor, ws.id, {
        "workspace_id": ws.id,
        "name": ws.name,
        "tester": tester,
        "tester_account_created": issued is not None,
        "expires_at": ws.sandbox_expires_at,
        "ttl_hours": int(ttl_hours),
        "reason": " ".join(reason.split()),
    })
    return describe(ws), issued


def list_sandboxes() -> list[dict[str, Any]]:
    from services.team_store import list_workspaces

    now = _now()
    return [describe(ws, now=now) for ws in list_workspaces() if is_sandbox(ws)]


def delete_sandbox(workspace_id: str, *, actor: str, reason: str = "") -> dict[str, Any]:
    """Tear a sandbox down. Refuses any workspace that is not a sandbox."""
    from services import integrations_store, notification_store, team_store, tenant_store

    ws = team_store.get_workspace(workspace_id)
    if ws is None or not is_sandbox(ws):
        raise SandboxNotFound("Sandbox workspace not found")
    tenants = [t.id for t in tenant_store.list_tenants() if t.workspace_id == workspace_id]
    for tenant_id in tenants:
        tenant_store.delete_tenant(tenant_id)
    channels = [c.id for c in notification_store.list_channels(workspace_id=workspace_id)]
    for channel_id in channels:
        notification_store.delete_channel(channel_id)
    keys = [
        str(r["id"])
        for r in integrations_store.load_api_key_records()
        if str(r.get("workspace_id") or "") == workspace_id
    ]
    revoked = [k for k in keys if integrations_store.revoke_api_key(k)]
    members = len(team_store.list_workspace_members(workspace_id))
    team_store.delete_workspace(workspace_id=workspace_id)
    summary = {
        "workspace_id": workspace_id,
        "name": ws.name,
        "tenants_removed": len(tenants),
        "channels_removed": len(channels),
        "api_keys_revoked": len(revoked),
        "members_removed": members,
        "expires_at": ws.sandbox_expires_at,
        **({"reason": " ".join(reason.split())} if reason else {}),
    }
    _audit("team.sandbox.delete", actor, workspace_id, summary)
    return summary


def sweep_expired_sandboxes(*, now: datetime | None = None) -> list[dict[str, Any]]:
    """Delete every sandbox past its expiry (scheduler beat). Never raises per sandbox."""
    from services.team_store import list_workspaces

    current = now or _now()
    removed: list[dict[str, Any]] = []
    for ws in list_workspaces():
        expires = _parse(ws.sandbox_expires_at) if is_sandbox(ws) else None
        if expires is None or expires > current:
            continue
        try:
            removed.append(delete_sandbox(ws.id, actor=EXPIRY_ACTOR, reason="expired"))
        except Exception:  # noqa: BLE001 — one sandbox must not stop the sweep
            logger.exception("Expired sandbox %s could not be removed", ws.id)
    return removed


def _audit(action: str, actor: str, workspace_id: str, details: dict[str, Any]) -> None:
    from services.audit_log import append_audit_event

    append_audit_event(
        action=action,
        resource=f"/team/sandbox-workspaces/{workspace_id}",
        actor=actor,
        level="warn",
        outcome="succeeded",
        workspace_id=workspace_id,
        details=details,
    )
