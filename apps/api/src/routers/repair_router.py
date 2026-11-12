"""Agentic repair + CDC incremental snapshot signals."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
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
    mappings: list[dict[str, Any]] = Field(default_factory=list)


def _actor_email(request: Request) -> str:
    return getattr(request.state, "user_email", None) or "anonymous"


class SnapshotBody(BaseModel):
    #: ``connector:<id>`` of a saved connector, or one of a job's source keys;
    #: resolved from ``connector_id`` / ``job_id`` when empty (round 2, M2).
    source_key: str = ""
    connector_id: str = ""
    job_id: str = ""
    table: str
    primary_key: str  # required — refuse inventing default "id"
    chunk_size: int = 1000
    row_filter: dict | list | None = None


@router.post("/repair/propose/preflight")
async def propose_preflight(body: ProposePreflightBody, request: Request) -> dict[str, Any]:
    from services.agentic_repair import propose_from_preflight
    from services.body_id_ownership import assert_body_ids

    assert_body_ids(request, jobs=(body.job_id,))  # review 3, Low

    p = propose_from_preflight(
        body.preflight,
        job_id=body.job_id,
        coercion_report=body.coercion_report,
    )
    return p.to_dict()


@router.post("/repair/propose/quarantine")
async def propose_quarantine(body: ProposeQuarantineBody, request: Request) -> dict[str, Any]:
    from services.agentic_repair import propose_from_quarantine
    from services.body_id_ownership import assert_body_ids

    assert_body_ids(request, jobs=(body.job_id,))  # review 3, Low

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
async def decide_repair(proposal_id: str, body: DecideBody, request: Request) -> dict[str, Any]:
    from services.agentic_repair import PARITY_SOURCE, apply_actions_with_report, decide_proposal, get_proposal

    found = get_proposal(proposal_id)
    if found is not None:
        # Deciding changes the proposal: an editor/admin role in its own
        # workspace, not merely a credential that may see it (audit H-3).
        from services.workspace_access import assert_resource_workspace

        assert_resource_workspace(
            request, found.workspace_id, write=True, not_found_detail="Proposal not found"
        )
    if found is not None and found.source == PARITY_SOURCE:
        # One workflow, one entry point per kind: a parity fix is applied through
        # the transfer path and re-checked on the job's Trust Report.
        fix_id = next((str(a.get("fix_id") or "") for a in found.actions), "")
        raise HTTPException(status_code=409, detail=(
            "This is a Parity Studio fix; decide it on the job's Trust Report "
            f"(POST /api/v1/jobs/{found.job_id}/parity-report/fixes/{fix_id}/approve or /dismiss)."
        ))

    def _apply(actions: list[dict[str, Any]]) -> dict[str, Any]:
        return apply_actions_with_report(body.mappings, actions)

    # Only apply when the caller supplies mappings — approve-without-mappings is audit-only.
    apply_fn = _apply if body.approve and body.mappings else None

    try:
        p = decide_proposal(
            proposal_id,
            approve=body.approve,
            actor=_actor_email(request),
            apply_fn=apply_fn,
        )
    except KeyError:
        raise HTTPException(404, "Proposal not found") from None
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    return p.to_dict()


def _visible_signal(request: Request, signal_id: str, *, write: bool = False):
    """The signal, or the same 404 for a missing id and another workspace's (D222).

    ``write=True`` for a route that changes the signal: the caller must hold
    write membership in the signal's own workspace (review M4)."""
    from services.cdc_incremental_snapshot import get_signal
    from services.workspace_access import assert_resource_workspace

    sig = get_signal(signal_id)
    if sig is None:
        raise HTTPException(404, "Snapshot signal not found")
    assert_resource_workspace(request, sig.workspace_id, write=write, not_found_detail="Snapshot signal not found")
    return sig


def _snapshot_source(request: Request, body: SnapshotBody) -> tuple[str, str]:
    """``(source_key, workspace_id)`` from a connector or job the caller may write.

    An arbitrary ``source_key`` let workspace B queue a signal on A's source,
    which A's CDC run then claimed and reported into B's signal (security
    review round 2, M2). The signal belongs to that resource's workspace.
    """
    from services.workspace_access import assert_resource_workspace

    key = (body.source_key or "").strip()
    if body.job_id.strip():
        from services.cdc_job_snapshots import resolve_job_cdc_snapshot_context
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(body.job_id.strip())
        if not job:
            raise HTTPException(404, "Job not found")
        workspace_id = str(job.get("workspace_id") or "")
        assert_resource_workspace(request, workspace_id, write=True, not_found_detail="Job not found")
        try:
            allowed = list(resolve_job_cdc_snapshot_context(job)["source_keys"])
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    else:
        from services.connector_store import connector_owner_workspace

        connector_id = body.connector_id.strip() or (key[len("connector:"):] if key.startswith("connector:") else "")
        if not connector_id:
            raise HTTPException(
                400,
                "Name the CDC source by connector_id or job_id (a source_key must be "
                "connector:<id> of a connector you can write)",
            )
        found, workspace_id = connector_owner_workspace(connector_id)
        if not found:
            raise HTTPException(404, "Connector not found")
        assert_resource_workspace(request, workspace_id, write=True, not_found_detail="Connector not found")
        allowed = [f"connector:{connector_id}"]
    if key and key not in allowed:
        raise HTTPException(400, "source_key is not a source key of that connector or job")
    return key or allowed[0], workspace_id


@router.post("/cdc/snapshots")
async def request_snapshot(body: SnapshotBody, request: Request) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import request_incremental_snapshot

    source_key, workspace_id = _snapshot_source(request, body)
    try:
        sig = request_incremental_snapshot(
            source_key,
            body.table,
            primary_key=body.primary_key,
            chunk_size=body.chunk_size,
            row_filter=body.row_filter,
            workspace_id=workspace_id,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return sig.to_dict()


@router.get("/cdc/snapshots")
async def list_snapshots(request: Request, source_key: str = "", status: str = "") -> dict[str, Any]:
    from services.cdc_incremental_snapshot import list_signals
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request)
    return {
        "signals": [
            s.to_dict() for s in list_signals(source_key, status=status) if scope.allows(s.workspace_id)
        ]
    }


@router.get("/cdc/snapshots/{signal_id}")
async def get_snapshot(signal_id: str, request: Request) -> dict[str, Any]:
    return _visible_signal(request, signal_id).to_dict()


@router.post("/cdc/snapshots/{signal_id}/cancel")
async def cancel_snapshot(signal_id: str, request: Request) -> dict[str, Any]:
    from services.cdc_incremental_snapshot import cancel_signal

    _visible_signal(request, signal_id, write=True)
    sig = cancel_signal(signal_id)
    if sig is None:
        raise HTTPException(404, "Snapshot signal not found")
    return sig.to_dict()


@router.post("/cdc/signals/execute-snapshot")
async def execute_snapshot_signal(body: SnapshotBody, request: Request) -> dict[str, Any]:
    """Debezium-compatible alias: enqueue incremental snapshot via signal API."""
    return await request_snapshot(body, request)


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
