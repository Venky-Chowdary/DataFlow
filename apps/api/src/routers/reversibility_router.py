"""Reversibility gate reads: can a plan's or a job's destination give the source back?

``GET /transfer/plans/{id}/reversibility`` is the plan's Reversibility tab (the
same report plan preflight attaches, and the gate ``g24_reversibility`` blocks
on when the plan requires reversibility). ``GET /jobs/{id}/reversibility``
answers from the job's stored request. Both read no rows beyond what the plan
already holds, and both are ``job.read``; another workspace's plan or job is a
404, exactly as its own GET.
"""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["Reversibility"])


@router.get("/transfer/plans/{plan_id}/reversibility")
async def plan_reversibility(plan_id: str, request: Request):
    from services.reversibility_sources import plan_report
    from services.transfer_plan_store import get_plan
    from services.workspace_access import assert_resource_workspace

    plan = get_plan(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    assert_resource_workspace(request, plan.workspace_id or None, not_found_detail="Plan not found")
    return await asyncio.to_thread(plan_report, plan)


@router.get("/jobs/{job_id}/reversibility")
async def job_reversibility(job_id: str, request: Request):
    from services.parity_state import job_store
    from services.reversibility_sources import job_report

    from .connectors_router import _can_access_job

    job = job_store().get_job(job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    return await asyncio.to_thread(job_report, job, job_id)
