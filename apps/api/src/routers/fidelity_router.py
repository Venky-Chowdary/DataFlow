"""Continuous Fidelity — on-demand parallel-run parity checks.

The recurring-revenue answer to a migration or a managed pipeline: keep the old
system and the new one live, and prove continuously that each column still
carries the same population. This router exposes a single on-demand check; a
schedule wraps it by calling the same endpoint on an interval (the scheduler and
contract store already exist for that), and a Zero-ETL supervisor points it at a
source and the destination a managed pipeline writes, to catch the moment that
pipeline quietly changes the data.

The check reads both endpoints as they are now — there is no transfer in the
loop — and returns a tamper-evident report naming every divergence by column.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel, Field

from services.continuous_fidelity import run_fidelity_check
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


@router.post("/check")
def check_fidelity(
    body: _FidelityCheckRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
) -> dict[str, Any]:
    """Compare two live datasets and return a column-level fidelity report."""
    ws = resolve_read_workspace(request, workspace_id)
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
