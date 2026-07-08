"""Scheduled pipeline syncs — recurring database-to-database transfers."""

from __future__ import annotations

from typing import Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.schedule_store import (
    INTERVALS,
    PipelineSchedule,
    create_schedule,
    delete_schedule,
    get_schedule,
    list_schedules,
    mark_schedule_run,
    update_schedule,
)

router = APIRouter(prefix="/schedules", tags=["Scheduled Pipelines"])


class ScheduleCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: Literal["hourly", "daily", "weekly"] = "daily"
    enabled: bool = True


class ScheduleUpdate(BaseModel):
    name: Optional[str] = None
    source_connector_id: Optional[str] = None
    source_table: Optional[str] = None
    dest_connector_id: Optional[str] = None
    dest_table: Optional[str] = None
    interval: Optional[Literal["hourly", "daily", "weekly"]] = None
    enabled: Optional[bool] = None


class ScheduleResponse(BaseModel):
    id: str
    name: str
    source_connector_id: str
    source_table: str
    dest_connector_id: str
    dest_table: str
    interval: str
    enabled: bool
    last_run_at: Optional[str]
    next_run_at: Optional[str]
    last_job_id: Optional[str]
    run_count: int
    created_at: str

    @classmethod
    def from_schedule(cls, s: PipelineSchedule) -> ScheduleResponse:
        return cls(**s.to_dict())


@router.get("/intervals")
async def schedule_intervals():
    return {"intervals": [{"id": k, "label": k.capitalize()} for k in INTERVALS]}


@router.get("/", response_model=list[ScheduleResponse])
async def list_pipeline_schedules():
    return [ScheduleResponse.from_schedule(s) for s in list_schedules()]


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_pipeline_schedule(schedule_id: str):
    sched = get_schedule(schedule_id)
    if not sched:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return ScheduleResponse.from_schedule(sched)


@router.post("/", response_model=ScheduleResponse, status_code=201)
async def create_pipeline_schedule(body: ScheduleCreate):
    try:
        sched = create_schedule(body.model_dump())
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


@router.delete("/{schedule_id}")
async def remove_pipeline_schedule(schedule_id: str):
    if not delete_schedule(schedule_id):
        raise HTTPException(status_code=404, detail="Schedule not found")
    return {"success": True}


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
