"""Which workspace a Pilot-confirmed one-shot transfer belongs to (D380).

The confirming caller's workspace is stamped on the job, so the job, its trust
report and its proof pack are readable there and 404 from any other workspace.

An approval staged in a workspace is confirmed only by a caller who can write
there. An approval staged with no workspace (the staging worker lost the
request stamp, or an older approval) is bound to the confirming caller's
workspace, but only when that cannot hand the caller another tenant's data:
the confirmer is the principal who staged it, or every saved connector the
transfer reads or writes belongs to that workspace (R11). Otherwise any member
could confirm someone else's unscoped approval and own a job that reads
another workspace's connectors.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

_CONNECTOR_REFUSAL = (
    "This transfer approval was staged without a workspace and uses a connector that "
    "does not belong to your workspace. Stage the transfer again from the workspace "
    "that owns its connectors, or ask the person who staged it to confirm it."
)


def _connector_ids(payload: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for side in ("source", "destination"):
        endpoint = payload.get(side)
        if isinstance(endpoint, dict):
            cid = str(endpoint.get("connector_id") or "").strip()
            if cid:
                ids.append(cid)
    return ids


def _inline_endpoint(payload: dict[str, Any]) -> bool:
    """A database endpoint carried by value (no saved connector to check)."""
    for side in ("source", "destination"):
        endpoint = payload.get(side)
        if not isinstance(endpoint, dict) or endpoint.get("connector_id"):
            continue
        if endpoint.get("host") or endpoint.get("connection_string"):
            return True
    return False


def confirm_transfer_workspace(
    payload: dict[str, Any], *, staged_by: str, http_request: Any
) -> dict[str, Any]:
    """The payload to run, stamped with the confirming caller's workspace.

    Raises ``HTTPException`` (403 / 400) with transfer wording (D380 AC-4).
    """
    from services.connector_store import connector_owner_workspace
    from services.team_store import require_workspace_isolation
    from services.tenant_scope import resolve_tenant_scope

    scope = resolve_tenant_scope(http_request)
    staged = str(payload.get("workspace_id") or "").strip()
    if staged:
        if not scope.can_write(staged):
            raise HTTPException(
                status_code=403,
                detail="This transfer approval belongs to a workspace you cannot write to.",
            )
        return payload
    try:
        target = scope.target_workspace_id(write=True)
    except HTTPException:
        target = ""
    if not target:
        if require_workspace_isolation():
            raise HTTPException(
                status_code=400,
                detail=(
                    "This transfer would belong to no workspace, so nobody but a platform "
                    "admin could see its job, trust report or proof pack. Confirm with "
                    "X-Workspace-Id set to the workspace that owns it."
                ),
            )
        return payload
    confirmer = str(getattr(scope, "actor", "") or "").strip()
    if not (staged_by and confirmer and staged_by == confirmer):
        if _inline_endpoint(payload):
            raise HTTPException(status_code=403, detail=_CONNECTOR_REFUSAL)
        for cid in _connector_ids(payload):
            exists, owner = connector_owner_workspace(cid)
            if not exists or owner != target:
                raise HTTPException(status_code=403, detail=_CONNECTOR_REFUSAL)
    return {**payload, "workspace_id": target}
