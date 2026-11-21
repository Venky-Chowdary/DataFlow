"""Parity at Scale routes: the verification phase and the difference explorer.

Behind ``DATAWRAP_PARITY_SCALE``; off, every route answers 404 as if absent.

* ``GET  /jobs/{id}/verification`` — the verification phase (state, progress).
* ``POST /jobs/{id}/verification/tables/{table}`` — re-verify one table
  (``job.run``: it reads both databases again).
* ``GET  /jobs/{id}/verification/tables/{table}/columns|keys|values`` — the
  Trust Report's Columns / Keys / Values tabs (values are a bounded live
  re-read; protected columns never show a value).
* ``GET  /jobs/{id}/verification/tables/{table}/repair-sql`` — repair
  statements as text for the reviewer to run themselves; never executed.

A job of another workspace is a 404, exactly as ``GET /jobs/{id}``.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from .connectors_router import _can_access_job, _can_mutate_job

router = APIRouter(tags=["Parity at Scale"])


def _enabled() -> None:
    from services.parity_scale import parity_scale_enabled

    if not parity_scale_enabled():
        raise HTTPException(status_code=404, detail="Not found")


def _job(job_id: str, request: Request, *, write: bool = False) -> dict[str, Any]:
    from services.parity_state import job_store

    _enabled()
    job = job_store().get_job(job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    if write and not _can_mutate_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace write access required")
    return job


async def _explore(fn, job: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
    from services.parity_scale.explorer import ExplorerUnavailable

    try:
        return await asyncio.to_thread(fn, job, *args, **kwargs)
    except ExplorerUnavailable as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/jobs/{job_id}/verification")
async def get_verification(job_id: str, request: Request):
    from services.parity_scale.explorer import verification_summary

    return verification_summary(_job(job_id, request))


@router.post("/jobs/{job_id}/verification/tables/{table}")
async def reverify_table(job_id: str, table: str, request: Request):
    from services.audit_log import actor_from_request
    from services.parity_scale.verification import is_running, start_for_job
    from services.parity_state import audit

    job = _job(job_id, request, write=True)
    if is_running(job):
        raise HTTPException(status_code=409, detail="Verification is already running for this job.")
    try:
        block = await asyncio.to_thread(start_for_job, job_id, only_table=table, force=True)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit("job.verification.restarted", job=job, job_id=job_id, actor=actor_from_request(request),
          outcome="succeeded", details={"table": table})
    return block


@router.get("/jobs/{job_id}/verification/tables/{table}/columns")
async def explorer_columns(job_id: str, table: str, request: Request):
    from services.parity_scale.explorer import columns

    return await _explore(columns, _job(job_id, request), table)


@router.get("/jobs/{job_id}/verification/tables/{table}/keys")
async def explorer_keys(job_id: str, table: str, request: Request):
    from services.parity_scale.explorer import keys

    return await _explore(keys, _job(job_id, request), table)


@router.get("/jobs/{job_id}/verification/tables/{table}/values")
async def explorer_values(
    job_id: str, table: str, request: Request,
    offset: int = Query(default=0, ge=0), limit: int = Query(default=50, ge=1, le=100),
):
    from services.parity_scale.explorer import values

    return await _explore(values, _job(job_id, request), table, offset=offset, limit=limit)


@router.get("/jobs/{job_id}/verification/tables/{table}/repair-sql")
async def explorer_repair_sql(job_id: str, table: str, request: Request):
    from services.parity_scale.explorer import repair_sql

    return await _explore(repair_sql, _job(job_id, request), table)
