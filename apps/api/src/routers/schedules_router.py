"""Scheduled pipeline syncs — recurring database-to-database transfers."""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from services.acknowledgment_contract import MIN_ACTOR_LEN
from services.schedule_approvals import STATUS_OPEN
from services.schedule_store import (
    INTERVALS,
    SYNC_MODES,
    PipelineSchedule,
    create_schedule,
    delete_schedule,
    get_schedule,
    list_schedules,
    update_schedule,
)
from services.workspace_access import (
    assert_resource_workspace,
    resolve_read_workspace,
    resolve_write_workspace,
)

router = APIRouter(prefix="/schedules", tags=["Scheduled Pipelines"])


def _schedule_or_404(schedule_id: str) -> PipelineSchedule:
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return sched


def _bound_schedule(request: Request, schedule_id: str) -> PipelineSchedule:
    """Load a schedule and refuse cross-workspace access."""
    sched = _schedule_or_404(schedule_id)
    assert_resource_workspace(request, getattr(sched, "workspace_id", "") or "")
    return sched

SyncMode = Literal[
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental",
    "incremental_append",
    "incremental_deduped",
    "cdc",
    "scd2",
    "mirror",
    "reverse_etl",
]
IntervalPreset = Literal["hourly", "daily", "weekly"]
SchemaPolicy = Literal[
    "manual_review",
    "propagate_columns",
    "propagate_all",
    "pause_on_change",
    "type_locked",
]


class ScheduleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: IntervalPreset = "daily"
    cron: str = ""
    timezone: str = "UTC"
    sync_mode: SyncMode = "full_refresh_overwrite"
    validation_mode: str = "strict"
    schema_policy: SchemaPolicy = "manual_review"
    backfill_new_fields: bool = False
    write_via_staging: bool = False
    priority_column: str = ""
    priority_direction: str = "desc"
    row_limit: int = Field(default=0, ge=0)
    delivery_guarantee: str = "at_least_once"
    snapshot_mode: str = ""
    allow_append_only: bool = False
    cdc_row_filter: str = ""
    multi_subnet_failover: bool = False
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    stream_contracts: list[dict[str, Any]] = Field(default_factory=list)
    cursor_column: str = ""
    primary_key: str = ""
    source_read_mode: str = ""
    procedure_call: str = ""
    source_query: str = ""
    procedure_params: dict[str, Any] = Field(default_factory=dict)
    workspace_id: str = ""
    contract_id: str = ""
    require_signed_contract: Optional[bool] = None
    date_locale: str = ""
    number_locale: str = ""
    shape_recipe: dict[str, Any] = Field(default_factory=dict)
    approved_shape_recipe_hash: str = ""
    approved_decision_artifact_hash: str = ""
    approved_ddl_identity_hash: str = ""
    max_retries: int = Field(default=0, ge=0, le=10)
    retry_backoff_seconds: int = Field(default=60, ge=0, le=3600)
    notify_on_failure: bool = True
    notify_on_success: bool = False
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    source_connector_id: Optional[str] = None
    source_table: Optional[str] = None
    dest_connector_id: Optional[str] = None
    dest_table: Optional[str] = None
    interval: Optional[IntervalPreset] = None
    cron: Optional[str] = None
    timezone: Optional[str] = None
    sync_mode: Optional[SyncMode] = None
    validation_mode: Optional[str] = None
    schema_policy: Optional[SchemaPolicy] = None
    backfill_new_fields: Optional[bool] = None
    write_via_staging: Optional[bool] = None
    priority_column: Optional[str] = None
    priority_direction: Optional[str] = None
    row_limit: Optional[int] = Field(default=None, ge=0)
    delivery_guarantee: Optional[str] = None
    snapshot_mode: Optional[str] = None
    allow_append_only: Optional[bool] = None
    cdc_row_filter: Optional[str] = None
    multi_subnet_failover: Optional[bool] = None
    mappings: Optional[list[dict[str, Any]]] = None
    stream_contracts: Optional[list[dict[str, Any]]] = None
    cursor_column: Optional[str] = None
    primary_key: Optional[str] = None
    source_read_mode: Optional[str] = None
    procedure_call: Optional[str] = None
    source_query: Optional[str] = None
    procedure_params: Optional[dict[str, Any]] = None
    workspace_id: Optional[str] = None
    contract_id: Optional[str] = None
    require_signed_contract: Optional[bool] = None
    date_locale: Optional[str] = None
    number_locale: Optional[str] = None
    shape_recipe: Optional[dict[str, Any]] = None
    approved_shape_recipe_hash: Optional[str] = None
    approved_decision_artifact_hash: Optional[str] = None
    approved_ddl_identity_hash: Optional[str] = None
    max_retries: Optional[int] = Field(default=None, ge=0, le=10)
    retry_backoff_seconds: Optional[int] = Field(default=None, ge=0, le=3600)
    notify_on_failure: Optional[bool] = None
    notify_on_success: Optional[bool] = None
    enabled: Optional[bool] = None


class ScheduleResponse(BaseModel):
    id: str
    name: str
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: str
    cron: str = ""
    timezone: str = "UTC"
    sync_mode: str = "full_refresh_overwrite"
    validation_mode: str = "strict"
    schema_policy: str = "manual_review"
    backfill_new_fields: bool = False
    write_via_staging: bool = False
    priority_column: str = ""
    priority_direction: str = "desc"
    row_limit: int = 0
    delivery_guarantee: str = "at_least_once"
    snapshot_mode: str = ""
    allow_append_only: bool = False
    cdc_row_filter: str = ""
    multi_subnet_failover: bool = False
    cursor_column: str = ""
    primary_key: str = ""
    cursor_value: str = ""
    source_read_mode: str = ""
    procedure_call: str = ""
    source_query: str = ""
    procedure_params: dict[str, Any] = Field(default_factory=dict)
    workspace_id: str = ""
    contract_id: str = ""
    require_signed_contract: bool = False
    date_locale: str = ""
    number_locale: str = ""
    shape_recipe: dict[str, Any] = Field(default_factory=dict)
    approved_shape_recipe_hash: str = ""
    approved_decision_artifact_hash: str = ""
    approved_ddl_identity_hash: str = ""
    max_retries: int = 0
    retry_backoff_seconds: int = 60
    notify_on_failure: bool = True
    notify_on_success: bool = False
    enabled: bool
    last_run_at: Optional[str] = None
    next_run_at: Optional[str] = None
    last_job_id: Optional[str] = None
    last_status: Optional[str] = None
    run_count: int = 0
    # A retry owed after a failed attempt, and cadence windows that elapsed with
    # no run: both are invisible from run_count alone.
    retry_at: Optional[str] = None
    retry_attempt: int = 0
    missed_window_count: int = 0
    last_missed_windows: int = 0
    running: bool = False
    created_at: str
    # Pipeline Detail needs schema map without a second Transfer Studio hop.
    mappings: list[dict[str, Any]] = Field(default_factory=list)
    stream_contracts: list[dict[str, Any]] = Field(default_factory=list)
    # The source shape this schedule last read. A run is refused when a column
    # it carries changes type, is dropped or is renamed, so the operator has to
    # be able to see the baseline that judged it — otherwise the refusal is a
    # dead end rather than a finding.
    source_schema: dict[str, str] = Field(default_factory=dict)
    source_schema_fingerprint: str = ""
    source_schema_observed_at: str = ""
    mapping_count: int = 0
    # Autopilot: the finding this schedule is parked on, and the standing
    # authority (if any) that lets later identical runs proceed unattended.
    approval_request: dict[str, Any] = Field(default_factory=dict)
    standing_authorization: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_schedule(cls, s: PipelineSchedule) -> ScheduleResponse:
        data = s.to_dict()
        mappings = list(data.get("mappings") or [])
        data["mappings"] = mappings
        data["mapping_count"] = len(mappings)
        # run_history / running_instance stay on the history endpoint.
        allowed = set(cls.model_fields)
        return cls(**{k: v for k, v in data.items() if k in allowed})


class ScheduleSummaryResponse(BaseModel):
    """List-row payload — omits bulky mappings for table views."""
    id: str
    name: str
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: str
    cron: str = ""
    timezone: str = "UTC"
    sync_mode: str = "full_refresh_overwrite"
    validation_mode: str = "strict"
    schema_policy: str = "manual_review"
    backfill_new_fields: bool = False
    write_via_staging: bool = False
    priority_column: str = ""
    priority_direction: str = "desc"
    row_limit: int = 0
    date_locale: str = ""
    number_locale: str = ""
    delivery_guarantee: str = "at_least_once"
    snapshot_mode: str = ""
    allow_append_only: bool = False
    cdc_row_filter: str = ""
    multi_subnet_failover: bool = False
    cursor_column: str = ""
    primary_key: str = ""
    cursor_value: str = ""
    source_read_mode: str = ""
    workspace_id: str = ""
    contract_id: str = ""
    require_signed_contract: bool = False
    max_retries: int = 0
    retry_backoff_seconds: int = 60
    notify_on_failure: bool = True
    notify_on_success: bool = False
    enabled: bool
    last_run_at: Optional[str] = None
    next_run_at: Optional[str] = None
    last_job_id: Optional[str] = None
    last_status: Optional[str] = None
    run_count: int = 0
    retry_at: Optional[str] = None
    retry_attempt: int = 0
    missed_window_count: int = 0
    last_missed_windows: int = 0
    running: bool = False
    created_at: str
    mapping_count: int = 0
    # A parked schedule reads as "off" from run_count and last_status alone. The
    # list row has to say a human owes it a decision, without the full finding.
    needs_approval: bool = False
    approval_id: str = ""
    approval_code: str = ""
    approval_finding: str = ""
    approvable: bool = False
    authorized: bool = False

    @classmethod
    def from_schedule(cls, s: PipelineSchedule) -> ScheduleSummaryResponse:
        full = ScheduleResponse.from_schedule(s)
        payload = full.model_dump()
        payload.pop("mappings", None)
        req = dict(s.approval_request or {})
        open_req = str(req.get("status") or "").strip().lower() == STATUS_OPEN
        grant = dict(s.standing_authorization or {})
        payload.update(
            needs_approval=open_req,
            approval_id=str(req.get("id") or "") if open_req else "",
            approval_code=str(req.get("code") or "") if open_req else "",
            approval_finding=str(req.get("finding") or "") if open_req else "",
            approvable=bool(req.get("approvable")) if open_req else False,
            authorized=bool(grant.get("id")) and not grant.get("revoked_at"),
        )
        return cls(**{k: v for k, v in payload.items() if k in cls.model_fields})


@router.get("/intervals")
async def schedule_intervals():
    from services.schedule_store import SCHEMA_POLICIES

    return {
        "intervals": [{"id": k, "label": k.capitalize()} for k in INTERVALS],
        "sync_modes": sorted(SYNC_MODES),
        "schema_policies": sorted(SCHEMA_POLICIES),
    }


@router.get("/export/dataflow")
def export_dataflow_manifest(
    request: Request,
    format: Literal["yaml", "json"] = "yaml",
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Export schedules (+ contracts) visible in this workspace as dataflow.yaml."""
    import yaml

    from services.gitops_manifest import build_dataflow_manifest
    from services.team_store import require_workspace_isolation

    ws = resolve_read_workspace(request, workspace_id)
    artifact = build_dataflow_manifest(
        workspace_id=ws,
        isolation=require_workspace_isolation(),
    )
    if format == "yaml":
        return Response(
            content=yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False),
            media_type="application/x-yaml",
            headers={"Content-Disposition": "attachment; filename=dataflow.yaml"},
        )
    return artifact


@router.post("/gitops/plan")
async def gitops_plan_manifest(
    payload: dict[str, Any],
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Dry-run a DatawrapManifest / PipelineSchedule / DataContract YAML body."""
    from services.gitops_manifest import plan_manifest

    resolve_read_workspace(request, workspace_id)
    try:
        return plan_manifest(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/gitops/apply")
async def gitops_apply_manifest(
    payload: dict[str, Any],
    request: Request,
    dry_run: bool = False,
    require_signed_contracts: bool = False,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Apply a GitOps manifest (create/update schedules + draft contracts).

    Contracts land as DRAFT — sign explicitly before ``require_signed_contract`` runs.
    Pass ``require_signed_contracts=true`` for CD/staging: every schedule must
    reference a SIGNED contract.
    """
    from services.gitops_manifest import apply_manifest

    ws = resolve_write_workspace(request, workspace_id)
    try:
        return apply_manifest(
            payload,
            dry_run=dry_run,
            require_signed_contracts=require_signed_contracts,
            workspace_id=ws,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/", response_model=list[ScheduleSummaryResponse])
async def list_pipeline_schedules(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    import asyncio

    ws = resolve_read_workspace(request, workspace_id)
    schedules = await asyncio.to_thread(list_schedules)
    if ws:
        from services.team_store import require_workspace_isolation

        if require_workspace_isolation():
            schedules = [s for s in schedules if s.workspace_id == ws]
        else:
            schedules = [s for s in schedules if not s.workspace_id or s.workspace_id == ws]
    return [ScheduleSummaryResponse.from_schedule(s) for s in schedules]


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_pipeline_schedule(
    schedule_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    import asyncio

    resolve_read_workspace(request, workspace_id)
    sched = await asyncio.to_thread(get_schedule, schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, getattr(sched, "workspace_id", "") or "")
    return ScheduleResponse.from_schedule(sched)


@router.post("/", response_model=ScheduleResponse, status_code=201)
async def create_pipeline_schedule(
    body: ScheduleCreate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    ws = resolve_write_workspace(request, workspace_id)
    payload = body.model_dump(exclude_none=True)
    if ws and not payload.get("workspace_id"):
        payload["workspace_id"] = ws
    elif payload.get("workspace_id"):
        assert_resource_workspace(request, str(payload.get("workspace_id") or ""))
    try:
        sched = create_schedule(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ScheduleResponse.from_schedule(sched)


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
async def patch_pipeline_schedule(
    schedule_id: str,
    body: ScheduleUpdate,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    resolve_write_workspace(request, workspace_id)
    existing = get_schedule(schedule_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, getattr(existing, "workspace_id", "") or "")
    data = {k: v for k, v in body.model_dump().items() if v is not None}
    # A PATCH must not re-home a schedule into another workspace.
    data.pop("workspace_id", None)
    if not data:
        return ScheduleResponse.from_schedule(existing)
    try:
        updated = update_schedule(schedule_id, data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not updated:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return ScheduleResponse.from_schedule(updated)


@router.get("/{schedule_id}/export")
def export_pipeline_schedule(
    schedule_id: str,
    request: Request,
    format: Literal["yaml", "json"] = "yaml",
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Export a schedule as a versionable YAML/JSON artifact for GitOps."""
    import yaml

    from services.gitops_manifest import schedule_artifact

    resolve_read_workspace(request, workspace_id)
    sched = _bound_schedule(request, schedule_id)
    artifact = schedule_artifact(sched)
    if format == "yaml":
        return Response(
            content=yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False),
            media_type="application/x-yaml",
            headers={"Content-Disposition": f"attachment; filename=schedule-{schedule_id}.yaml"},
        )
    return artifact


@router.post("/import", response_model=ScheduleResponse, status_code=201)
async def import_pipeline_schedule(
    payload: dict[str, Any],
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Import a PipelineSchedule GitOps artifact (create or replace by id)."""
    from services.gitops_manifest import apply_manifest

    ws = resolve_write_workspace(request, workspace_id)
    if ws:
        # Bind the imported spec to the caller's workspace so a pasted YAML
        # cannot land in another tenant.
        spec = payload.get("spec") if isinstance(payload.get("spec"), dict) else payload
        if isinstance(spec, dict):
            spec["workspace_id"] = ws
    result = apply_manifest(payload, dry_run=False, workspace_id=ws)
    rows = result.get("results") or []
    sched_row = next((r for r in rows if r.get("kind") == "PipelineSchedule" and r.get("ok")), None)
    if not sched_row:
        err = next((r.get("error") for r in rows if r.get("error")), None)
        raise HTTPException(status_code=400, detail=err or "Could not import PipelineSchedule")
    sched = get_schedule(str(sched_row.get("id") or ""))
    if not sched:
        raise HTTPException(status_code=500, detail="Schedule imported but not readable")
    assert_resource_workspace(request, getattr(sched, "workspace_id", "") or "")
    return ScheduleResponse.from_schedule(sched)

@router.delete("/{schedule_id}")
async def remove_pipeline_schedule(
    schedule_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    resolve_write_workspace(request, workspace_id)
    _bound_schedule(request, schedule_id)
    if not delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}


@router.get("/{schedule_id}/history")
async def get_pipeline_history(
    schedule_id: str,
    request: Request,
    limit: int = 25,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Return the persisted run history (most recent first)."""
    resolve_read_workspace(request, workspace_id)
    sched = _bound_schedule(request, schedule_id)
    history = list(reversed(sched.run_history))[: max(1, min(limit, 100))]
    return {"schedule_id": schedule_id, "runs": history}


@router.post("/{schedule_id}/accept-source-schema")
async def accept_source_schema(
    schedule_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Record the source's current shape as this schedule's baseline.

    A run refused for source drift is a finding, not a verdict: the operator has
    to be able to look at what changed and say whether the mapping still holds.
    Without this the refusal had no exit — the message asked for a review that no
    control performed, which is the dead end this product exists to remove.

    Deliberately explicit. Re-baselining is the operator asserting the change is
    understood, so it is never done for them by a retry.
    """
    from datetime import datetime, timezone

    from services.schedule_runner import (
        _apply_callable_schedule_source,
        _endpoint_from_connector,
        _resolve_connector,
        probe_schedule_source_schema,
    )
    from services.source_schema_memory import fingerprint_source

    resolve_write_workspace(request, workspace_id)
    sched = _bound_schedule(request, schedule_id)

    src = _resolve_connector(sched.source_connector_id)
    if not src:
        raise HTTPException(
            status_code=400,
            detail="Source connector is unavailable — cannot read the current schema.",
        )
    try:
        endpoint = _endpoint_from_connector(src, sched.source_table)
        _apply_callable_schedule_source(endpoint, sched)
        info = probe_schedule_source_schema(endpoint) or {}
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Could not read the source schema: {exc}"
        ) from exc

    schema = {str(k): str(v) for k, v in (info.get("schema") or {}).items()}
    if not schema:
        raise HTTPException(
            status_code=502,
            detail="Source returned no schema — nothing to record as a baseline.",
        )
    columns = [str(c) for c in (info.get("columns") or schema.keys())]
    fingerprint = fingerprint_source(columns, schema)
    updated = update_schedule(
        schedule_id,
        {
            "source_schema": schema,
            "source_schema_fingerprint": fingerprint,
            "source_schema_observed_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {
        "success": True,
        "schedule_id": schedule_id,
        "source_schema_fingerprint": fingerprint,
        "columns": len(schema),
        "message": (
            f"Baseline updated to the source's current shape ({len(schema)} columns). "
            "The next run compares against this."
        ),
    }


@router.post("/{schedule_id}/run")
async def run_pipeline_now(
    schedule_id: str,
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    """Trigger an immediate run (does not change the regular cadence)."""
    from ..services.schedule_runner import _run_schedule

    resolve_write_workspace(request, workspace_id)
    _bound_schedule(request, schedule_id)
    job_id = _run_schedule(schedule_id)
    if not job_id:
        raise HTTPException(status_code=400, detail="Could not start pipeline — check connectors")
    updated = get_schedule(schedule_id)
    return {"success": True, "job_id": job_id, "schedule": ScheduleResponse.from_schedule(updated)}


# --- Autopilot: the approval inbox -------------------------------------------
#
# A scheduled run that a gate refuses used to record one more failed run and fail
# identically on every later beat. It now parks on a single durable finding that a
# human can decide, and a decision can optionally mint a scoped, expiring,
# hash-bound standing authorization so later runs of the *same plan* proceed
# unattended. Nothing here weakens execution: preflight, the destination reread
# and Gate-8 reconciliation still decide whether a run is green.


class ApprovalDecision(BaseModel):
    reason: str = Field(..., min_length=8, max_length=1000)
    #: Attestations the operator is making. Only those the finding asked for are
    #: honoured; a scope with nothing signed behind it is refused.
    compliance: bool = False
    schema_drift: bool = False
    fk_risk: bool = False
    #: False approves this run only. True delegates the same signature to future
    #: runs of the identical plan, and needs ``schedule.authorize``.
    grant_standing: bool = False
    expires_in_days: int = Field(default=30, ge=1, le=90)
    scopes: Optional[list[str]] = None


class RejectDecision(BaseModel):
    reason: str = Field(..., min_length=8, max_length=1000)
    #: Leave the schedule paused (default) rather than let the next beat re-raise
    #: the same refusal.
    disable: bool = True


class AuthorizationGrant(BaseModel):
    reason: str = Field(..., min_length=8, max_length=1000)
    scopes: list[str] = Field(..., min_length=1)
    compliance: bool = False
    schema_drift: bool = False
    fk_risk: bool = False
    expires_in_days: int = Field(default=30, ge=1, le=90)


def _decider(request: Request, *, authorize: bool = False) -> str:
    """The named human behind this decision, or 403.

    The actor is taken from the verified session whenever there is one, never from
    the body: an audit trail the client names itself is not an audit trail. A
    signed-in operator therefore decides without naming themselves twice, and the
    role on that identity is still enforced. Only when no identity was verified —
    a single-operator deployment with enforcement off — may the caller name
    themselves through the ``X-Actor`` header.
    """
    from services.effective_role import (
        effective_permissions,
        workspace_id_from_request_headers,
    )
    from services.rbac import Permission
    from src.services import auth_service

    user = getattr(request.state, "user", None) or {}
    needed = Permission.SCHEDULE_AUTHORIZE if authorize else Permission.SCHEDULE_MANAGE
    if user:
        granted = effective_permissions(user, workspace_id_from_request_headers(request.headers))
        if needed not in granted:
            raise HTTPException(status_code=403, detail=f"Permission denied: {needed}")
        actor = str(user.get("email") or user.get("name") or "").strip()
        if len(actor) >= MIN_ACTOR_LEN:
            return actor
    if not auth_service.auth_required():
        actor = str(request.headers.get("X-Actor") or "").strip()
        if len(actor) >= MIN_ACTOR_LEN:
            return actor
        raise HTTPException(
            status_code=400,
            detail=(
                "X-Actor must name the person making this decision "
                f"(at least {MIN_ACTOR_LEN} characters)."
            ),
        )
    raise HTTPException(status_code=403, detail=f"Permission denied: {needed}")


@router.get("/approvals/open")
async def list_open_approvals(request: Request, workspace_id: str = ""):
    """Every schedule currently parked on a decision, newest first."""
    import asyncio

    from services.schedule_approvals import open_approvals

    scope = resolve_read_workspace(request, workspace_id)
    items = await asyncio.to_thread(open_approvals, scope or "")
    return {"count": len(items), "approvals": items}


@router.get("/{schedule_id}/approval")
async def get_schedule_approval(schedule_id: str, request: Request):
    """This schedule's open finding and its standing authorization, if any."""
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, sched.workspace_id or "")
    return {
        "schedule_id": schedule_id,
        "approval": dict(sched.approval_request or {}),
        "authorization": dict(sched.standing_authorization or {}),
    }


@router.post("/{schedule_id}/approvals/{approval_id}/approve")
async def approve_schedule_finding(
    schedule_id: str,
    approval_id: str,
    payload: ApprovalDecision,
    request: Request,
):
    """Approve one finding, and optionally delegate it to future identical runs."""
    from services.schedule_approvals import (
        AuthorizationRefused,
        approve_request,
    )

    actor = _decider(request, authorize=bool(payload.grant_standing))
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, sched.workspace_id or "")
    try:
        result = approve_request(
            schedule_id,
            approval_id,
            actor=actor,
            reason=payload.reason,
            acknowledgments={
                "compliance": payload.compliance,
                "schema_drift": payload.schema_drift,
                "fk_risk": payload.fk_risk,
            },
            scopes=payload.scopes,
            grant_standing=payload.grant_standing,
            expires_in_days=payload.expires_in_days,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except AuthorizationRefused as exc:
        # A refused delegation writes nothing, so the schedule is never left
        # half-approved.
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "success": True,
        "approval": result["approval"],
        "authorization": result["authorization"],
        "schedule": ScheduleResponse.from_schedule(result["schedule"]),
    }


@router.post("/{schedule_id}/approvals/{approval_id}/reject")
async def reject_schedule_finding(
    schedule_id: str,
    approval_id: str,
    payload: RejectDecision,
    request: Request,
):
    """Reject a finding; the schedule is paused rather than left to re-refuse."""
    from services.schedule_approvals import reject_request

    actor = _decider(request)
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, sched.workspace_id or "")
    try:
        result = reject_request(
            schedule_id,
            approval_id,
            actor=actor,
            reason=payload.reason,
            disable=payload.disable,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {
        "success": True,
        "approval": result["approval"],
        "schedule": ScheduleResponse.from_schedule(result["schedule"]),
    }


@router.post("/{schedule_id}/authorization")
async def grant_schedule_authorization(
    schedule_id: str,
    payload: AuthorizationGrant,
    request: Request,
):
    """Grant scoped, expiring standing authority for this schedule's current plan."""
    from services.schedule_approvals import (
        AuthorizationRefused,
        set_standing_authorization,
    )

    actor = _decider(request, authorize=True)
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, sched.workspace_id or "")
    try:
        result = set_standing_authorization(
            schedule_id,
            actor=actor,
            reason=payload.reason,
            scopes=list(payload.scopes),
            acknowledgments={
                "compliance": payload.compliance,
                "schema_drift": payload.schema_drift,
                "fk_risk": payload.fk_risk,
            },
            expires_in_days=payload.expires_in_days,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except AuthorizationRefused as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return {
        "success": True,
        "authorization": result["authorization"],
        "schedule": ScheduleResponse.from_schedule(result["schedule"]),
    }


@router.delete("/{schedule_id}/authorization")
async def revoke_schedule_authorization(
    schedule_id: str,
    request: Request,
    reason: str = "",
):
    """Revoke standing authority. The record is kept, permanently unusable."""
    from services.schedule_approvals import revoke_standing_authorization

    actor = _decider(request, authorize=True)
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    assert_resource_workspace(request, sched.workspace_id or "")
    try:
        result = revoke_standing_authorization(schedule_id, actor=actor, reason=reason)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {"success": True, "authorization": result["authorization"]}
