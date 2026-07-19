"""Agentic repair + CDC incremental snapshot signals."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(tags=["repair"])


class ProposePreflightBody(BaseModel):
    preflight: dict[str, Any]
    coercion_report: dict[str, Any] = Field(default_factory=dict)
    job_id: str = ""


class ProposeQuarantineBody(BaseModel):
    rejected_details: list[dict[str, Any]] = Field(default_factory=list)
    job_id: str = ""


class DecideBody(BaseModel):
    approve: bool = True
    actor: str = "user"
    mappings: list[dict[str, Any]] = Field(default_factory=list)


class SnapshotBody(BaseModel):
    source_key: str
    table: str
    primary_key: str = "id"
    chunk_size: int = 1000


@router.post("/repair/propose/preflight")
async def propose_preflight(body: ProposePreflightBody) -> dict[str, Any]:
    from services.agentic_repair import propose_from_preflight

    p = propose_from_preflight(
        body.preflight,
        job_id=body.job_id,
        coercion_report=body.coercion_report,
    )
    return p.to_dict()


@router.post("/repair/propose/quarantine")
async def propose_quarantine(body: ProposeQuarantineBody) -> dict[str, Any]:
    from services.agentic_repair import propose_from_quarantine

    p = propose_from_quarantine(body.rejected_details, job_id=body.job_id)
    return p.to_dict()


@router.get("/repair/proposals")
async def list_repair_proposals(job_id: str = "", status: str = "") -> dict[str, Any]:
    from services.agentic_repair import list_proposals

    return {"proposals": [p.to_dict() for p in list_proposals(job_id=job_id, status=status)]}


@router.get("/repair/proposals/{proposal_id}")
async def get_repair_proposal(proposal_id: str) -> dict[str, Any]:
    from services.agentic_repair import get_proposal

    p = get_proposal(proposal_id)
    if p is None:
        raise HTTPException(404, "Proposal not found")
    return p.to_dict()


@router.post("/repair/proposals/{proposal_id}/decide")
async def decide_repair(proposal_id: str, body: DecideBody) -> dict[str, Any]:
    from services.agentic_repair import apply_actions_to_mappings, decide_proposal

    def _apply(actions: list[dict[str, Any]]) -> dict[str, Any]:
        updated = apply_actions_to_mappings(body.mappings, actions)
        return {"applied": True, "mappings": updated}

    try:
        p = decide_proposal(
            proposal_id,
            approve=body.approve,
            actor=body.actor,
            apply_fn=_apply if body.approve and body.mappings else (None if not body.approve else _apply),
        )
    except KeyError:
        raise HTTPException(404, "Proposal not found") from None
    return p.to_dict()


@router.post("/cdc/snapshots")
async def request_snapshot(body: SnapshotBody) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import request_incremental_snapshot

    sig = request_incremental_snapshot(
        body.source_key,
        body.table,
        primary_key=body.primary_key,
        chunk_size=body.chunk_size,
    )
    return sig.to_dict()


@router.get("/cdc/snapshots")
async def list_snapshots(source_key: str = "", status: str = "") -> dict[str, Any]:
    from services.cdc_incremental_snapshot import list_signals

    return {"signals": [s.to_dict() for s in list_signals(source_key, status=status)]}
