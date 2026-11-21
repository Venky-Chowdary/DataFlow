"""Verified AI routes: the Mapping Witness and the AI decisions ledger.

``POST /transfer/mapping-witness`` judges a mapping against a bounded sample of
the data (``services.mapping_witness``) and keeps the report for that mapping
and destination; it never changes the mapping. ``POST
/transfer/mapping-witness/review`` records a person's answer to one flag.
``GET /jobs/{id}/mapping-witness`` returns the report for the mapping the job
ran with. A job of another workspace is a 404, exactly as ``GET /jobs/{id}``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.audit_log import actor_from_request

from .connectors_router import _can_access_job
from .transfer_router import EndpointDTO, _assert_endpoint_connectors

router = APIRouter(tags=["Verified AI"])


class WitnessKey(BaseModel):
    source: str = Field(default="", max_length=256)
    target: str = Field(default="", max_length=256)


class WitnessRequest(BaseModel):
    mappings: list[dict] = Field(default_factory=list, max_length=400)
    #: The sample Map already shows (file sources). Capped server-side at MAX_ROWS.
    source_rows: list[dict] = Field(default_factory=list, max_length=2000)
    source: Optional[EndpointDTO] = None
    destination: Optional[EndpointDTO] = None
    target_key_columns: list[str] = Field(default_factory=list, max_length=32)
    key: Optional[WitnessKey] = None
    destination_table_exists: Optional[bool] = None
    date_locale: str = Field(default="", max_length=8)


class ReviewRequest(BaseModel):
    mapping_digest: str = Field(..., min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    source: str = Field(..., min_length=1, max_length=256)
    decision: str = Field(..., pattern=r"^(kept|changed)$")
    new_target: str = Field(default="", max_length=256)


def _audit(action: str, digest: str, actor: str, details: dict[str, Any], workspace: str) -> None:
    """Names and counts only: a witness report never carries a cell value."""
    from services.audit_log import append_audit_event

    append_audit_event(action=action, resource=f"mapping:{digest[:16]}", actor=actor or "anonymous",
                       level="info", details=details, workspace_id=workspace or None)


def _endpoint(dto: EndpointDTO | None) -> Any:
    if dto is None:
        return None
    from ..transfer.models import EndpointConfig

    return EndpointConfig.from_dict(dto.kind, dto.model_dump(by_alias=True))


def _public_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if not record:
        return None
    return {
        "mapping_digest": record.get("mapping_digest"),
        "report": record.get("report"),
        "decisions": record.get("decisions") or [],
        "updated_at": record.get("updated_at"),
    }


@router.post("/transfer/mapping-witness")
async def mapping_witness(
    body: WitnessRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    from services.mapping_witness_service import key_pair, mapping_digest, run_witness, save_report
    from services.workspace_access import resolve_write_workspace

    readable = tuple(e for e in (body.source, body.destination) if e is not None)
    if readable:
        _assert_endpoint_connectors(request, read=readable)
    workspace = resolve_write_workspace(request, workspace_id)
    mappings = [m for m in body.mappings if isinstance(m, dict)]
    key = key_pair(mappings, body.target_key_columns, body.key.model_dump() if body.key else None)
    destination = body.destination.model_dump(by_alias=True) if body.destination else {}

    def _run() -> dict[str, Any]:
        report = run_witness(
            mappings=mappings,
            source_rows=body.source_rows,
            source_endpoint=_endpoint(body.source),
            destination_endpoint=_endpoint(body.destination),
            key=key,
            destination_table_exists=body.destination_table_exists,
            date_locale=body.date_locale,
            workspace_id=workspace,
        )
        digest = mapping_digest(mappings, destination)
        actor = actor_from_request(request)
        record = save_report(report, workspace_id=workspace, digest=digest, actor=actor)
        _audit("mapping_witness.recorded", digest, actor, {
            "mode": report.get("mode"), "summary": report.get("summary"),
            "inputs_sha256": report.get("inputs_sha256"),
        }, workspace)
        return _public_record(record) or {}

    return await asyncio.to_thread(_run)


@router.post("/transfer/mapping-witness/review")
async def review_witness_flag(
    body: ReviewRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    from services.mapping_witness_service import record_review
    from services.workspace_access import resolve_write_workspace

    workspace = resolve_write_workspace(request, workspace_id)
    try:
        entry = await asyncio.to_thread(
            record_review, workspace_id=workspace, digest=body.mapping_digest, source=body.source,
            decision=body.decision, actor=actor_from_request(request), new_target=body.new_target,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _audit("mapping_witness.reviewed", body.mapping_digest, entry["by"], {
        k: entry.get(k) for k in ("source", "target", "flag_kind", "decision", "new_target", "report_sha256")
    }, workspace)
    return {"decision": entry}


def _visible_job(job_id: str, request: Request) -> dict[str, Any]:
    from services.parity_state import job_store

    job = job_store().get_job(job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/mapping-witness")
async def job_mapping_witness(job_id: str, request: Request):
    from services.mapping_witness_service import record_for_job

    job = _visible_job(job_id, request)
    record = await asyncio.to_thread(record_for_job, job)
    return {"job_id": job_id, "witness": _public_record(record)}

