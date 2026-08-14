"""Continuous Fidelity — on-demand parallel-run parity checks.

The recurring-revenue answer to a migration or a managed pipeline: keep the old
system and the new one live, and prove continuously that each column still
carries the same population. One POST is a Dual Run *cycle*; when ``schedule_id``
is set, the cycle is recorded on that pipeline's campaign (consecutive clean
windows — Google Dual Run exit criterion, not a one-shot green).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.continuous_fidelity import run_and_record_campaign, run_fidelity_check
from services.workspace_access import resolve_read_workspace

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


def _endpoints_from_schedule(schedule_id: str, workspace_id: str):
    from services.schedule_runner import (
        _resolve_connector,
        build_schedule_request,
    )
    from services.schedule_store import get_schedule

    sched = get_schedule(schedule_id)
    if sched is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if workspace_id and getattr(sched, "workspace_id", "") and sched.workspace_id != workspace_id:
        raise HTTPException(status_code=404, detail="Schedule not found")
    src = _resolve_connector(sched.source_connector_id)
    dst = _resolve_connector(sched.dest_connector_id)
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
        sched, transfer = _endpoints_from_schedule(schedule_id, ws)
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
