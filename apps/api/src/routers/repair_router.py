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
    from services.agentic_repair import apply_actions_with_report, decide_proposal

    def _apply(actions: list[dict[str, Any]]) -> dict[str, Any]:
        return apply_actions_with_report(body.mappings, actions)

    # Only apply when the caller supplies mappings — approve-without-mappings is audit-only.
    apply_fn = _apply if body.approve and body.mappings else None

    try:
        p = decide_proposal(
            proposal_id,
            approve=body.approve,
            actor=body.actor,
            apply_fn=apply_fn,
        )
    except KeyError:
        raise HTTPException(404, "Proposal not found") from None
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
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


@router.get("/cdc/snapshots/{signal_id}")
async def get_snapshot(signal_id: str) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import get_signal

    sig = get_signal(signal_id)
    if sig is None:
        raise HTTPException(404, "Snapshot signal not found")
    return sig.to_dict()


@router.post("/cdc/snapshots/{signal_id}/cancel")
async def cancel_snapshot(signal_id: str) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import cancel_signal

    sig = cancel_signal(signal_id)
    if sig is None:
        raise HTTPException(404, "Snapshot signal not found")
    return sig.to_dict()


@router.post("/cdc/signals/execute-snapshot")
async def execute_snapshot_signal(body: SnapshotBody) -> dict[str, Any]:
    """Debezium-compatible alias: enqueue incremental snapshot via signal API."""
    return await request_snapshot(body)


class EnsureSignalTableBody(BaseModel):
    dialect: str = "postgresql"
    table: str = "dataflow_signal"
    # Optional connection fields for live ensure (host/database/…).
    host: str = "localhost"
    port: int = 5432
    database: str = ""
    username: str = ""
    password: str = ""
    connection_string: str = ""
    ssl: bool = False


@router.post("/cdc/signals/ensure-table")
async def ensure_cdc_signal_table(body: EnsureSignalTableBody) -> dict[str, Any]:
    """Create Debezium-compatible ``dataflow_signal`` table on the source DB."""
    from services.cdc_signal_table import ensure_signal_table, signal_table_name

    dialect = (body.dialect or "postgresql").lower()
    table = signal_table_name({"signal_table": body.table})
    try:
        if dialect in {"postgresql", "postgres"}:
            from connectors.postgresql_conn import get_connection

            with get_connection(
                host=body.host,
                port=body.port or 5432,
                database=body.database or "postgres",
                username=body.username,
                password=body.password,
                connection_string=body.connection_string,
                ssl=body.ssl,
            ) as conn:
                ensure_signal_table(conn, table=table, dialect="postgresql")
        elif dialect in {"mysql", "mariadb"}:
            from connectors.mysql_conn import get_connection

            conn = get_connection(
                host=body.host,
                port=body.port or 3306,
                database=body.database,
                username=body.username,
                password=body.password,
                connection_string=body.connection_string,
                ssl=body.ssl,
            )
            try:
                ensure_signal_table(conn, table=table, dialect="mysql")
            finally:
                conn.close()
        else:
            raise HTTPException(400, f"Unsupported dialect: {dialect}")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"Could not ensure signal table: {exc}") from exc
    return {"ok": True, "table": table, "dialect": dialect}
