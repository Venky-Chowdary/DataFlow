"""Scheduled pipeline syncs — recurring database-to-database transfers."""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

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
    # The source shape this schedule last read. A run is refused when a column
    # it carries changes type, is dropped or is renamed, so the operator has to
    # be able to see the baseline that judged it — otherwise the refusal is a
    # dead end rather than a finding.
    source_schema: dict[str, str] = Field(default_factory=dict)
    source_schema_fingerprint: str = ""
    source_schema_observed_at: str = ""
    mapping_count: int = 0

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

    @classmethod
    def from_schedule(cls, s: PipelineSchedule) -> ScheduleSummaryResponse:
        full = ScheduleResponse.from_schedule(s)
        payload = full.model_dump()
        payload.pop("mappings", None)
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
def export_dataflow_manifest(format: Literal["yaml", "json"] = "yaml"):
    """Export all schedules (+ contracts) as a single ``dataflow.yaml`` GitOps manifest."""
    import yaml

    from services.gitops_manifest import build_dataflow_manifest

    artifact = build_dataflow_manifest()
    if format == "yaml":
        return Response(
            content=yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False),
            media_type="application/x-yaml",
            headers={"Content-Disposition": "attachment; filename=dataflow.yaml"},
        )
    return artifact


@router.post("/gitops/plan")
async def gitops_plan_manifest(payload: dict[str, Any]):
    """Dry-run a DatawrapManifest / PipelineSchedule / DataContract YAML body."""
    from services.gitops_manifest import plan_manifest

    try:
        return plan_manifest(payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/gitops/apply")
async def gitops_apply_manifest(
    payload: dict[str, Any],
    dry_run: bool = False,
    require_signed_contracts: bool = False,
):
    """Apply a GitOps manifest (create/update schedules + draft contracts).

    Contracts land as DRAFT — sign explicitly before ``require_signed_contract`` runs.
    Pass ``require_signed_contracts=true`` for CD/staging: every schedule must
    reference a SIGNED contract.
    """
    from services.gitops_manifest import apply_manifest

    try:
        return apply_manifest(
            payload,
            dry_run=dry_run,
            require_signed_contracts=require_signed_contracts,
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
def export_pipeline_schedule(schedule_id: str, format: Literal["yaml", "json"] = "yaml"):
    """Export a schedule as a versionable YAML/JSON artifact for GitOps."""
    import yaml

    from services.gitops_manifest import schedule_artifact

    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    artifact = schedule_artifact(sched)
    if format == "yaml":
        return Response(
            content=yaml.safe_dump(artifact, sort_keys=False, default_flow_style=False),
            media_type="application/x-yaml",
            headers={"Content-Disposition": f"attachment; filename=schedule-{schedule_id}.yaml"},
        )
    return artifact


@router.post("/import", response_model=ScheduleResponse, status_code=201)
async def import_pipeline_schedule(payload: dict[str, Any]):
    """Import a PipelineSchedule GitOps artifact (create or replace by id)."""
    from services.gitops_manifest import apply_manifest

    # Prefer single-resource apply so kind wrappers and bare specs both work.
    result = apply_manifest(payload, dry_run=False)
    rows = result.get("results") or []
    sched_row = next((r for r in rows if r.get("kind") == "PipelineSchedule" and r.get("ok")), None)
    if not sched_row:
        err = next((r.get("error") for r in rows if r.get("error")), None)
        raise HTTPException(status_code=400, detail=err or "Could not import PipelineSchedule")
    sched = get_schedule(str(sched_row.get("id") or ""))
    if not sched:
        raise HTTPException(status_code=500, detail="Schedule imported but not readable")
    return ScheduleResponse.from_schedule(sched)

@router.delete("/{schedule_id}")
async def remove_pipeline_schedule(schedule_id: str):
    if not delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}


@router.get("/{schedule_id}/history")
async def get_pipeline_history(schedule_id: str, limit: int = 25):
    """Return the persisted run history (most recent first)."""
    import asyncio

    sched = await asyncio.to_thread(get_schedule, schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    history = list(reversed(sched.run_history))[: max(1, min(limit, 100))]
    return {"schedule_id": schedule_id, "runs": history}


@router.post("/{schedule_id}/accept-source-schema")
async def accept_source_schema(schedule_id: str):
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

    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")

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
async def run_pipeline_now(schedule_id: str):
    """Trigger an immediate run (does not change the regular cadence)."""
    from ..services.schedule_runner import _run_schedule

    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    job_id = _run_schedule(schedule_id)
    if not job_id:
        raise HTTPException(status_code=400, detail="Could not start pipeline — check connectors")
    updated = get_schedule(schedule_id)
    return {"success": True, "job_id": job_id, "schedule": ScheduleResponse.from_schedule(updated)}
