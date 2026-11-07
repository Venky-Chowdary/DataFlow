"""Continuous Fidelity — on-demand parallel-run parity checks.

The recurring-revenue answer to a migration or a managed pipeline: keep the old
system and the new one live, and prove continuously that each column still
carries the same population. One POST is a Dual Run *cycle*; when ``schedule_id``
is set, the cycle is recorded on that pipeline's campaign (consecutive clean
windows — Google Dual Run exit criterion is N consecutive Gate-8 checksum
cycles, not a one-shot green and not column-profile screening).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.continuous_fidelity import run_and_record_campaign, run_fidelity_check
from services.workspace_access import (
    assert_resource_workspace,
    resolve_read_workspace,
)

from ..transfer.models import EndpointConfig

router = APIRouter(prefix="/fidelity", tags=["Continuous Fidelity"])


class _FidelityCheckRequest(BaseModel):
    """Two endpoints to compare, and the mapping that pairs their columns."""

    source: dict[str, Any] = Field(default_factory=dict)
    destination: dict[str, Any] = Field(default_factory=dict)
    # Ordered ``{source, target[, target_type]}`` entries; an intentional omit is
    # honoured. When absent, ``column_types`` alone defines an identity mapping.
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    # Destination physical types keyed by target column; sharpen numeric/temporal
    # comparison and override any type inferred on the mapping.
    column_types: dict[str, str] = Field(default_factory=dict)
    # When set, persist this cycle on the schedule's Dual Run campaign.
    schedule_id: str = ""


def _deny_cross_workspace(request: Request) -> None:
    from services.audit_coverage import record_authz_denial

    record_authz_denial(
        request,
        required_permission="workspace.resource",
        effective_role=str(getattr(request.state, "effective_role", "") or "member"),
        reason="cross_workspace",
    )
    raise HTTPException(status_code=403, detail="Resource belongs to another workspace")


def _assert_resource_visible(request: Request, resource_workspace_id: str) -> None:
    try:
        assert_resource_workspace(request, resource_workspace_id)
    except HTTPException:
        _deny_cross_workspace(request)


def _assert_connector_workspace(connector_id: str, request: Request) -> None:
    from services.connector_store import connector_owner_workspace

    # The owner, not a scoped read: a scoped lookup hides a foreign connector,
    # which would let its id through as if it did not exist.
    exists, owner = connector_owner_workspace(connector_id)
    if exists:
        _assert_resource_visible(request, owner)


def _endpoints_from_schedule(
    schedule_id: str,
    workspace_id: str,
    request: Request,
):
    from services.schedule_runner import (
        _resolve_connector_for_schedule,
        build_schedule_request,
    )
    from services.schedule_store import get_schedule

    sched = get_schedule(schedule_id)
    if sched is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    _assert_resource_visible(request, str(getattr(sched, "workspace_id", "") or ""))
    _assert_connector_workspace(sched.source_connector_id, request)
    _assert_connector_workspace(sched.dest_connector_id, request)
    src = _resolve_connector_for_schedule(sched.source_connector_id, sched)
    dst = _resolve_connector_for_schedule(sched.dest_connector_id, sched)
    if not src or not dst:
        raise HTTPException(
            status_code=400,
            detail="Schedule source or destination connector is missing.",
        )
    request = build_schedule_request(sched, src, dst)
    return sched, request


@router.post("/check")
def check_fidelity(
    body: _FidelityCheckRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Compare two live datasets and return a column-level fidelity report."""
    ws = resolve_read_workspace(request, workspace_id)
    schedule_id = (body.schedule_id or "").strip()
    if schedule_id:
        sched, transfer = _endpoints_from_schedule(schedule_id, ws, request)
        report, campaign = run_and_record_campaign(
            dict(getattr(sched, "fidelity_campaign", None) or {}),
            source=transfer.source,
            destination=transfer.destination,
            mappings=list(body.mappings or getattr(transfer, "mappings", None) or []),
            column_types=body.column_types,
            workspace_id=ws or str(getattr(sched, "workspace_id", "") or ""),
        )
        try:
            from services.schedule_store import update_schedule

            update_schedule(schedule_id, {"fidelity_campaign": campaign})
        except Exception:
            pass
        payload = report.to_dict()
        payload["campaign"] = campaign
        payload["schedule_id"] = schedule_id
        return payload

    for endpoint in (body.source, body.destination):
        connector_id = str(endpoint.get("connector_id") or "").strip()
        if connector_id:
            _assert_connector_workspace(connector_id, request)

    source = EndpointConfig.from_dict(
        str(body.source.get("kind") or "database"), body.source
    )
    destination = EndpointConfig.from_dict(
        str(body.destination.get("kind") or "database"), body.destination
    )
    report = run_fidelity_check(
        source=source,
        destination=destination,
        mappings=body.mappings,
        column_types=body.column_types,
        workspace_id=ws,
    )
    return report.to_dict()
