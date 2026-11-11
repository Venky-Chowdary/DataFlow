"""``/api/v1/jobs`` — the job history under its own name (QA enterprise #50).

The Jobs page and Pilot read jobs through ``/api/v1/connectors/jobs``. QA
called ``GET /api/v1/jobs`` (the natural REST name, also what API clients
guess) and got ``403 No RBAC rule is configured`` for every role: no route
existed, and the RBAC gate refused the unknown path before routing could
404. These are read-only aliases of the same handlers, so scoping, counts and
404-on-foreign-job behave identically; they are gated by ``job.read``.

``GET /jobs/{job_id}/export/download`` hands out the signed download of a file
export kept in the export bucket (``services.export_object_store``), under the
same read scope.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from .connectors_router import get_transfer_job, list_transfer_jobs

router = APIRouter(prefix="/jobs", tags=["Jobs"])
logger = logging.getLogger(__name__)
# A presigned URL is a bearer credential for its lifetime: never cached.
_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}


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


@router.get("/{job_id}/export/download")
async def download_job_export(job_id: str, request: Request):
    """A short-lived presigned URL for a job's file export in the export bucket.

    Same read scope as ``GET /jobs/{job_id}`` (``job.read`` and the caller's
    workspace): another workspace's job answers exactly like a missing one, and
    an anonymous request never reaches this handler (401). Each URL issued is
    audited (``job.export.download_url.issued``: actor, workspace, job, object
    key — never the URL). A self-hosted export in the exports folder answers
    with its ``/transfer/download`` path; an expired export answers 410.
    """
    from services.export_object_store import (
        AUDIT_ISSUED,
        export_expired,
        export_object_exists,
        mark_export_expired,
        presign_export,
        stored_export,
    )
    from services.audit_log import actor_from_request, append_audit_event
    from services.mongodb_service import get_mongodb_service

    from .connectors_router import _can_access_job

    job = await run_in_threadpool(get_mongodb_service().get_job, job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    ds = stored_export(job)
    if ds is None:
        local = job.get("destination_summary") or {}
        if isinstance(local, dict) and str(local.get("download_url") or "").startswith("/api/v1/transfer/download/"):
            return JSONResponse(
                {
                    "storage": "local",
                    "url": local["download_url"],
                    "filename": local.get("filename") or "",
                    "rows": local.get("rows"),
                },
                headers=_NO_STORE,
            )
        raise HTTPException(status_code=404, detail="This job has no file export")
    if export_expired(job, ds):
        raise HTTPException(status_code=410, detail="export expired")
    key = str(ds["object_key"])
    try:
        present = await run_in_threadpool(export_object_exists, key)
        if not present:
            mark_export_expired(job_id)
            raise HTTPException(status_code=410, detail="export expired")
        url, expires_at, ttl = await run_in_threadpool(presign_export, key, str(ds.get("filename") or ""))
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - storage outage: say so, never leak the error
        logger.warning("Export download for job %s unavailable: %s", job_id, type(exc).__name__)
        raise HTTPException(status_code=503, detail="Export storage is unavailable; try again shortly")
    workspace_id = str(job.get("workspace_id") or "")
    append_audit_event(
        action=AUDIT_ISSUED,
        resource=f"job:{job_id}",
        actor=actor_from_request(request),
        outcome="succeeded",
        workspace_id=workspace_id or None,
        details={
            "job_id": job_id,
            "workspace_id": workspace_id,
            "object_key": key,
            "sha256": ds.get("sha256"),
            "size_bytes": ds.get("size_bytes"),
            "expires_in_seconds": ttl,
        },
    )
    return JSONResponse(
        {
            "storage": ds.get("storage"),
            "url": url,
            "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
            "expires_in": ttl,
            "filename": ds.get("filename") or "",
            "object_key": key,
            "size_bytes": ds.get("size_bytes"),
            "rows": ds.get("rows"),
            "sha256": ds.get("sha256"),
            "retained_until": ds.get("expires_at"),
        },
        headers=_NO_STORE,
    )
