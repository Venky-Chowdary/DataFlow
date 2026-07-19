"""Scheduled pipeline syncs — recurring database-to-database transfers."""

from __future__ import annotations

from typing import Any, Literal, Optional

from fastapi import APIRouter, HTTPException, Response
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

router = APIRouter(prefix="/schedules", tags=["Scheduled Pipelines"])

SyncMode = Literal[
    "full_refresh_overwrite",
    "full_refresh_append",
    "incremental",
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
    running: bool = False
    created_at: str
    # Pipeline Detail needs schema map without a second Transfer Studio hop.
    mappings: list[dict[str, Any]] = Field(default_factory=list)
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
    """Export all schedules as a single ``dataflow.yaml`` GitOps manifest."""
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


@router.get("/", response_model=list[ScheduleSummaryResponse])
async def list_pipeline_schedules():
    return [ScheduleSummaryResponse.from_schedule(s) for s in list_schedules()]


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_pipeline_schedule(schedule_id: str):
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return ScheduleResponse.from_schedule(sched)


@router.post("/", response_model=ScheduleResponse, status_code=201)
async def create_pipeline_schedule(body: ScheduleCreate):
    payload = body.model_dump(exclude_none=True)
    try:
        sched = create_schedule(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return ScheduleResponse.from_schedule(sched)


@router.patch("/{schedule_id}", response_model=ScheduleResponse)
async def patch_pipeline_schedule(schedule_id: str, body: ScheduleUpdate):
    data = {k: v for k, v in body.model_dump().items() if v is not None}
    if not data:
        sched = get_schedule(schedule_id)
        if not sched:
            raise HTTPException(status_code=404, detail="Schedule not found")
        return ScheduleResponse.from_schedule(sched)
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

    from services.gitops_manifest import schedule_spec

    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    artifact = {
        "apiVersion": "dataflow.space/v1",
        "kind": "PipelineSchedule",
        "spec": schedule_spec(sched),
    }
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
    spec = payload.get("spec") if payload.get("kind") == "PipelineSchedule" else payload
    if not isinstance(spec, dict):
        raise HTTPException(status_code=422, detail="Expected PipelineSchedule spec object")
    schedule_id = (spec.get("id") or "").strip()
    try:
        if schedule_id and get_schedule(schedule_id):
            updated = update_schedule(schedule_id, spec)
            if not updated:
                raise HTTPException(status_code=404, detail="Schedule not found")
            return ScheduleResponse.from_schedule(updated)
        created = create_schedule(spec)
        return ScheduleResponse.from_schedule(created)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{schedule_id}")
async def remove_pipeline_schedule(schedule_id: str):
    if not delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}


@router.get("/{schedule_id}/history")
async def get_pipeline_history(schedule_id: str, limit: int = 25):
    """Return the persisted run history (most recent first)."""
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    history = list(reversed(sched.run_history))[: max(1, min(limit, 100))]
    return {"schedule_id": schedule_id, "runs": history}


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
