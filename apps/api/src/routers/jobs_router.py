"""``/api/v1/jobs`` — the job history under its own name (QA enterprise #50).

The Jobs page and Pilot read jobs through ``/api/v1/connectors/jobs``. QA
called ``GET /api/v1/jobs`` (the natural REST name, also what API clients
guess) and got ``403 No RBAC rule is configured`` for every role: no route
existed, and the RBAC gate refused the unknown path before routing could
404. These are read-only aliases of the same handlers, so scoping, counts and
404-on-foreign-job behave identically; they are gated by ``job.read``.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from fastapi import APIRouter, Header, Query, Request

from .connectors_router import get_transfer_job, list_transfer_jobs

router = APIRouter(prefix="/jobs", tags=["Jobs"])


@router.get("")
async def list_jobs(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
    limit: int = Query(default=50, ge=1, le=100),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    status: Literal["all", "running", "completed", "failed"] = Query(default="all"),
    search: str = Query(default="", max_length=120),
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
):
    return await list_transfer_jobs(
        request,
        workspace_id,
        limit=limit,
        offset=offset,
        status=status,
        search=search,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/{job_id}")
async def get_job(job_id: str, request: Request):
    return await get_transfer_job(job_id, request)
