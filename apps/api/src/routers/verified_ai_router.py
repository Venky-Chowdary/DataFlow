"""Verified AI routes: the Mapping Witness and the AI decisions ledger.

``POST /transfer/mapping-witness`` judges a mapping against a bounded sample of
the data (``services.mapping_witness``) and appends the report for that mapping
and the *resolved* destination; it never changes the mapping, and it runs off
the event loop under the connector deadline. ``POST
/transfer/mapping-witness/review`` records a person's answer to one flag, and
``POST /transfer/mapping-witness/decisions`` who approved or changed each pair
in Map (always the authenticated caller). ``GET /jobs/{id}/mapping-witness``
returns the evidence snapshot the job was submitted with; ``GET
/jobs/{id}/ai-decisions`` the AI decisions ledger built from it
(``services.ai_decision_ledger``), the rows the signed packs carry. A job of
another workspace is a 404, exactly as ``GET /jobs/{id}``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.audit_log import actor_from_request

from .connectors_router import _can_access_job
from .transfer_router import EndpointDTO, _assert_endpoint_connectors

router = APIRouter(tags=["Verified AI"])

#: The witness reads at most two bounded samples; past this it answers 504 and the event loop stays free.
WITNESS_DEADLINE_SECONDS = 30.0


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
    #: Destination columns the witness may compare against (the catalog Map shows); bounded.
    candidate_columns: list[str] = Field(default_factory=list, max_length=200)
    key: Optional[WitnessKey] = None
    destination_table_exists: Optional[bool] = None
    date_locale: str = Field(default="", max_length=8)
    #: "explicit" = the operator chose Check mapping (the only way a warehouse is read).
    trigger: Literal["auto", "explicit"] = "auto"
    #: The Map session (the plan id, or the id Map minted); evidence without one never reaches a job.
    session_id: str = Field(default="", max_length=128)
    #: What the sample came from: {"kind": "file" | "database", "name": file name or table}.
    source_ref: dict[str, str] = Field(default_factory=dict)


class ReviewRequest(BaseModel):
    mapping_digest: str = Field(..., min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    session_id: str = Field(..., min_length=8, max_length=128)
    destination: Optional[EndpointDTO] = None
    source: str = Field(..., min_length=1, max_length=256)
    #: kept = "Keep this mapping"; changed = remapped; continued = went on past it unreviewed.
    decision: Literal["kept", "changed", "continued"]
    flag_id: str = Field(default="", max_length=64)
    new_target: str = Field(default="", max_length=128)


class MapDecision(BaseModel):
    source: str = Field(..., min_length=1, max_length=256)
    target: str = Field(default="", max_length=256)
    approved: bool = False
    changed: bool = False


class MapDecisionsRequest(BaseModel):
    session_id: str = Field(..., min_length=8, max_length=128)
    destination: Optional[EndpointDTO] = None
    mappings: list[MapDecision] = Field(default_factory=list, max_length=400)


def _audit(action: str, resource: str, actor: str, details: dict[str, Any], workspace: str) -> None:
    """Names and counts only: a witness report never carries a cell value."""
    from services.audit_log import append_audit_event

    append_audit_event(action=action, resource=resource, actor=actor or "anonymous",
                       level="info", details=details, workspace_id=workspace or None)


def _endpoint(dto: EndpointDTO | None) -> Any:
    if dto is None:
        return None
    from ..transfer.models import EndpointConfig

    return EndpointConfig.from_dict(dto.kind, dto.model_dump(by_alias=True))


async def _off_loop(fn: Any) -> Any:
    from ..ai.copilot.tool_deadline import run_off_event_loop

    def _timed_out(message: str) -> Any:
        raise HTTPException(status_code=504, detail="The mapping check did not finish in time.")

    return await run_off_event_loop("mapping_witness", fn, _timed_out, seconds=WITNESS_DEADLINE_SECONDS)


@router.post("/transfer/mapping-witness")
async def mapping_witness(
    body: WitnessRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    from services.mapping_witness_service import (
        destination_identity,
        key_pair,
        mapping_digest,
        run_witness,
        save_report,
        source_identity,
    )
    from services.workspace_access import resolve_write_workspace

    readable = tuple(e for e in (body.source, body.destination) if e is not None)
    if readable:
        _assert_endpoint_connectors(request, read=readable)
    workspace = resolve_write_workspace(request, workspace_id)
    mappings = [m for m in body.mappings if isinstance(m, dict)]
    key = key_pair(mappings, body.target_key_columns, body.key.model_dump() if body.key else None)
    actor = actor_from_request(request)

    def _run() -> dict[str, Any]:
        destination = _endpoint(body.destination)
        identity = destination_identity(destination, workspace)
        report = run_witness(
            mappings=mappings,
            source_rows=body.source_rows,
            source_endpoint=_endpoint(body.source),
            destination_endpoint=destination,
            key=key,
            candidate_columns=body.candidate_columns,
            destination_table_exists=body.destination_table_exists,
            date_locale=body.date_locale,
            workspace_id=workspace,
            trigger=body.trigger,
        )
        digest = mapping_digest(mappings, identity)
        saved = save_report(report, workspace_id=workspace, session=body.session_id, identity=identity,
                            digest=digest, source=source_identity(body.source_ref), actor=actor)
        _audit("mapping_witness.recorded", f"mapping:{digest[:16]}", actor, {
            "mode": report.get("mode"), "summary": report.get("summary"), "trigger": report.get("trigger"),
            "report_sha256": saved["report_sha256"],
        }, workspace)
        return {"mapping_digest": digest, "session": saved["session"] or None, "report": report,
                "report_sha256": saved["report_sha256"], "recorded_at": saved["recorded_at"],
                "requested_by": saved["requested_by"]}

    return await _off_loop(_run)


@router.post("/transfer/mapping-witness/review")
async def review_witness_flag(
    body: ReviewRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    from services.mapping_witness_service import destination_identity, record_review
    from services.workspace_access import resolve_write_workspace

    if body.destination is not None:
        _assert_endpoint_connectors(request, read=(body.destination,))
    workspace = resolve_write_workspace(request, workspace_id)
    actor = actor_from_request(request)

    def _run() -> dict[str, Any]:
        identity = destination_identity(_endpoint(body.destination), workspace)
        return record_review(workspace_id=workspace, session=body.session_id, identity=identity,
                             digest=body.mapping_digest, source=body.source, decision=body.decision, actor=actor,
                             flag_id=body.flag_id, new_target=body.new_target)

    try:
        entry = await _off_loop(_run)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    _audit("mapping_witness.reviewed", f"mapping:{body.mapping_digest[:16]}", entry["by"], {
        k: entry.get(k) for k in ("source", "flagged_target", "flag_kind", "flag_id", "decision", "new_target",
                                  "report_sha256")
    }, workspace)
    return {"decision": entry}


@router.post("/transfer/mapping-witness/decisions")
async def record_map_decisions_route(
    body: MapDecisionsRequest,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Who approved or changed each pair in Map: the authenticated caller, never a name in the body."""
    from services.mapping_witness_service import destination_identity, record_map_decisions
    from services.workspace_access import resolve_write_workspace

    if body.destination is not None:
        _assert_endpoint_connectors(request, read=(body.destination,))
    workspace = resolve_write_workspace(request, workspace_id)
    actor = actor_from_request(request)

    def _run() -> int:
        identity = destination_identity(_endpoint(body.destination), workspace)
        return record_map_decisions(workspace_id=workspace, session=body.session_id, identity=identity,
                                    mappings=[m.model_dump() for m in body.mappings], actor=actor)

    recorded = await _off_loop(_run)
    _audit("mapping.decisions_recorded", "mapping:decisions", actor, {
        "recorded": recorded,
        "approved": sum(1 for m in body.mappings if m.approved),
        "changed": sum(1 for m in body.mappings if m.changed),
    }, workspace)
    return {"recorded": recorded}


def _visible_job(job_id: str, request: Request) -> dict[str, Any]:
    from services.parity_state import job_store

    job = job_store().get_job(job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/mapping-witness")
async def job_mapping_witness(job_id: str, request: Request):
    """The evidence snapshot frozen onto the job at submit (``null`` for a job submitted before it existed)."""
    from services.mapping_witness_service import snapshot_of

    job = await asyncio.to_thread(_visible_job, job_id, request)
    return {"job_id": job_id, "witness": snapshot_of(job)}


@router.get("/jobs/{job_id}/ai-decisions")
async def job_ai_decisions(job_id: str, request: Request):
    """Every AI-originated proposal or explanation on the job and what became of it."""
    from services.ai_decision_ledger import build_ledger

    job = await asyncio.to_thread(_visible_job, job_id, request)
    return await asyncio.to_thread(build_ledger, job)
