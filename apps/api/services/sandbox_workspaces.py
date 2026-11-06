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
  ``sandbox_expires_at`` (default 24 h, at most 7 days) on the workspace row
  as it is created, so a half-made sandbox is still recognisable and
  deletable as one; a failed set-up is rolled back.
* **The tester is always a synthetic login created here** (one-time
  password returned once). An existing account is refused: a sandbox IP
  allowlist would also govern that person's header-less requests in their
  real workspaces (``ip_allowlist_policy._request_workspaces``), and the
  teardown deletes the login.
* ``GET`` lists sandboxes; ``DELETE /{id}`` tears one down with the one
  workspace cascade (``services.workspace_teardown``: schedules, connectors
  and their credentials, jobs, quarantine, transforms, tenant policy,
  channels, API keys revoked, memberships), then deletes the synthetic
  tester's login and revokes its sessions. Only a workspace marked as a
  sandbox can be deleted this way; an active job makes it wait (409).
* Expired sandboxes are torn down by the scheduler beat
  (:func:`sweep_expired_sandboxes`, claimed once per ``SWEEP_INTERVAL`` per
  deployment), audited as actor ``system:sandbox-expiry``.

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


class SandboxBusy(SandboxError):
    status = 409


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
) -> tuple[dict[str, Any], str]:
    """Create the sandbox; returns its description and the tester's one-time password."""
    from services import team_store, user_store
    from services.sign_in_capability import can_sign_in

    if not 1 <= int(ttl_hours) <= MAX_TTL_HOURS:
        raise SandboxError(f"ttl_hours must be between 1 and {MAX_TTL_HOURS}")
    tester = user_store.validate_email(tester_email)
    if tester == user_store.normalize_email(actor) or can_sign_in(tester) or user_store.get_user(tester):
        raise SandboxError(
            f"{tester} already has a login. A sandbox tester is a new synthetic account: "
            "the sandbox's IP allowlist would also apply to an existing person's other "
            "workspaces, and the teardown deletes the login"
        )
    label = " ".join((name or "security test").split())[: 128 - len(NAME_PREFIX)]
    expires = (_now() + timedelta(hours=int(ttl_hours))).isoformat()
    ws = team_store.create_workspace(
        name=NAME_PREFIX + label, created_by=actor, sandbox_expires_at=expires, sandbox_tester=tester
    )
    minted_at = ""
    try:
        user, issued = user_store.create_user(
            email=tester, name="Security tester", role="member", created_by=actor
        )
        # From here on the login is this sandbox's: the fingerprint lets the
        # teardown (and only it) delete exactly this record (S2).
        minted_at = str(user.get("created_at") or "")
        ws.sandbox_tester_created_at = minted_at
        team_store._write_workspace(ws)
        team_store.add_workspace_member(
            workspace_id=ws.id, email=tester, role="admin", added_by=actor, actor_is_platform_admin=True
        )
        team_store.remove_workspace_member(
            workspace_id=ws.id, email=actor, removed_by=actor, actor_is_platform_admin=True
        )
    except Exception:
        # Roll back: no half-made sandbox. The login goes only if this call
        # created it — a create that lost a race to a real account recorded no
        # fingerprint, so that account is never deleted.
        _teardown(ws.id, actor=actor)
        if minted_at:
            # The workspace write that records the fingerprint may itself have
            # failed: delete the login this call minted from the in-memory copy.
            user_store.delete_user_if_created_at(email=tester, created_at=minted_at)
        raise
    _audit("team.sandbox.create", actor, ws.id, {
        "workspace_id": ws.id,
        "name": ws.name,
        "tester": tester,
        "tester_account_created": True,
        "expires_at": expires,
        "ttl_hours": int(ttl_hours),
        "reason": " ".join(reason.split()),
    })
    return describe(team_store.get_workspace(ws.id) or ws), issued


def list_sandboxes() -> list[dict[str, Any]]:
    from services.team_store import list_workspaces

    now = _now()
    return [describe(ws, now=now) for ws in list_workspaces() if is_sandbox(ws)]


def _teardown(workspace_id: str, *, actor: str) -> dict[str, Any]:
    """The workspace cascade (it removes the sandbox's own tester login). Idempotent."""
    from services.workspace_teardown import teardown_workspace

    summary = teardown_workspace(workspace_id, actor=actor)
    return {**summary, "tester": None}


def delete_sandbox(workspace_id: str, *, actor: str, reason: str = "") -> dict[str, Any]:
    """Tear a sandbox down. Refuses any workspace that is not a sandbox."""
    from services import team_store
    from services.workspace_teardown import WorkspaceBusy

    ws = team_store.get_workspace(workspace_id)
    if ws is None or not is_sandbox(ws):
        raise SandboxNotFound("Sandbox workspace not found")
    try:
        summary = _teardown(workspace_id, actor=actor)
        summary["tester"] = ws.sandbox_tester or None
    except WorkspaceBusy as exc:
        raise SandboxBusy(str(exc)) from exc
    summary = {
        **summary,
        "name": ws.name,
        "expires_at": ws.sandbox_expires_at,
        **({"reason": " ".join(reason.split())} if reason else {}),
    }
    _audit("team.sandbox.delete", actor, workspace_id, summary)
    return summary


SWEEP_INTERVAL = timedelta(minutes=15)
SWEEP_CLAIM = "sandbox_expiry"


def sweep_expired_sandboxes(*, now: datetime | None = None, force: bool = False) -> list[dict[str, Any]]:
    """Delete every sandbox past its expiry — once per ``SWEEP_INTERVAL`` per deployment.

    Claimed in Mongo like scheduled retention (``services.maintenance_claim``);
    a file-backed deployment throttles per process instead. A sandbox with an
    active job is left for the next sweep. Never raises per sandbox.
    """
    from services.maintenance_claim import claim_local, claim_run
    from services.team_store import list_workspaces

    current = now or _now()
    if not force:
        claimed = claim_run(SWEEP_CLAIM, SWEEP_INTERVAL, now=current)
        if claimed is None:
            claimed = claim_local(SWEEP_CLAIM, SWEEP_INTERVAL, now=current)
        if not claimed:
            return []
    removed: list[dict[str, Any]] = []
    all_done = True
    for ws in list_workspaces():
        expires = _parse(ws.sandbox_expires_at) if is_sandbox(ws) else None
        if expires is None or expires > current:
            continue
        try:
            removed.append(delete_sandbox(ws.id, actor=EXPIRY_ACTOR, reason="expired"))
        except SandboxBusy:
            all_done = False
            logger.info("Expired sandbox %s has an active job; the next sweep retries", ws.id)
        except Exception:  # noqa: BLE001 — one sandbox must not stop the sweep
            all_done = False
            logger.exception("Expired sandbox %s could not be removed", ws.id)
    if all_done:
        # "Done" only when every expired sandbox went (review 4 L4).
        from services.maintenance_claim import complete_run

        complete_run(SWEEP_CLAIM)
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
