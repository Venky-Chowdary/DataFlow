"""Parity Studio routes: the Trust Report of a job and the Migration health summary.

``GET /jobs/{id}/parity-report`` answers "does the destination match the source,
and is every difference explained?" from the run's own evidence. Fixes are
applied only when a person approves one (``job.manage``, the permission of the
repair approval workflow's decide route, plus write access to the job's
workspace); sign-off needs
``job.manage`` and the authority the configured mapping binds to the role
(:mod:`services.parity_authority`), and is refused while an unexplained
difference is left. A job of another
workspace is a 404, exactly as ``GET /jobs/{id}``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from services.audit_log import actor_from_request
from services.parity_fixes import ParityError

from .connectors_router import _can_access_job, _can_mutate_job

router = APIRouter(tags=["Parity Studio"])


class NoteBody(BaseModel):
    note: str = Field(default="", max_length=500)
    # The report version the person decided on; another version is a 409.
    expected_version: int | None = Field(default=None, ge=0)


class SignoffBody(BaseModel):
    # Required, but checked after the job is visible: another workspace's job is
    # a 404 whatever the body holds.
    role: str = Field(default="", max_length=64)
    note: str = Field(default="", max_length=500)
    expected_version: int | None = Field(default=None, ge=0)


#: How long an approve waits for the re-run route to answer.
_RERUN_START_SECONDS = 60.0


def _http(exc: ParityError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


def _detail_text(detail: Any) -> str:
    if isinstance(detail, dict):
        return str(detail.get("message") or detail.get("error") or detail.get("reason") or detail)
    return str(detail)


def _visible_job(job_id: str, request: Request, *, write: bool = False) -> dict[str, Any]:
    from services.parity_state import job_store

    job = job_store().get_job(job_id)
    if not job or not _can_access_job(request, job):
        raise HTTPException(status_code=404, detail="Job not found")
    if write and not _can_mutate_job(request, job):
        raise HTTPException(status_code=403, detail="Workspace write access required")
    return job


def _public(report: dict[str, Any]) -> dict[str, Any]:
    from services.parity_report import public_report

    return public_report(report)


def _report(job_id: str, job: dict[str, Any]) -> dict[str, Any]:
    from services.parity_report import build_parity_report
    from services.parity_state import state_of
    from services.parity_summary import remember_report

    if any(f.get("status") == "applying" for f in state_of(job)["fixes"].values()):
        # A re-sync job that finished (on any replica) is settled by the next read.
        from services.parity_apply import settle_pending
        from services.parity_state import job_store

        settle_pending(job_id)
        job = job_store().get_job(job_id) or job

    # The page polls: a re-read of this version within the cache window is reused (parity_cache).
    report = build_parity_report(job, reread=True, job_id=job_id, cache="use")
    remember_report(job_id, job, report)
    return _public(report)


@router.get("/jobs/{job_id}/parity-report")
async def get_parity_report(job_id: str, request: Request):
    job = _visible_job(job_id, request)
    return await asyncio.to_thread(_report, job_id, job)


@router.post("/jobs/{job_id}/parity-report/explain")
async def explain_parity_report(job_id: str, request: Request):
    from services.parity_explain import explain_job

    _visible_job(job_id, request)
    try:
        report = await asyncio.to_thread(explain_job, job_id, actor=actor_from_request(request))
    except ParityError as exc:
        raise _http(exc) from exc
    return _public(report)


@router.post("/jobs/{job_id}/parity-report/fixes/{fix_id}/approve")
async def approve_parity_fix(
    job_id: str, fix_id: str, request: Request, body: NoteBody | None = None,
):
    from services.parity_fixes import approve_fix

    _visible_job(job_id, request, write=True)
    loop = asyncio.get_running_loop()

    def start_rerun() -> str:
        # The existing re-run route, with its own refusals and audit trail. A
        # refusal wrote nothing, so the fix's claim is released (RerunRefused).
        from services.parity_apply import RerunRefused, scrub_text

        from .connectors_router import rerun_transfer_job

        # The route's own scheduling (BackgroundTasks) runs as soon as it answers, on the
        # loop and independent of this request: a later failure here (recording the
        # outcome) must not drop it and leave the new job pending forever.
        started_tasks = BackgroundTasks()
        future = asyncio.run_coroutine_threadsafe(
            rerun_transfer_job(job_id, started_tasks, request), loop
        )
        try:
            started = future.result(timeout=_RERUN_START_SECONDS)
        except concurrent.futures.TimeoutError as exc:
            if future.cancel():
                # Never started: no orphan re-run behind a failed fix.
                raise RerunRefused(
                    f"the re-run route did not answer within {_RERUN_START_SECONDS:g}s; nothing was started"
                ) from exc
            started = future.result()  # it finished as the wait ran out
        except HTTPException as exc:
            # The route's own refusal, scrubbed like the audit copy before it is a 409 body.
            raise RerunRefused(scrub_text(f"the re-run was refused: {_detail_text(exc.detail)}")) from exc
        asyncio.run_coroutine_threadsafe(started_tasks(), loop)
        return str((started or {}).get("job_id") or "")

    try:
        report = await asyncio.to_thread(
            approve_fix, job_id, fix_id, actor=actor_from_request(request),
            note=(body.note if body else ""), start_rerun=start_rerun,
            expected_version=(body.expected_version if body else None),
        )
    except ParityError as exc:
        raise _http(exc) from exc
    return _public(report)


@router.post("/jobs/{job_id}/parity-report/fixes/{fix_id}/dismiss")
async def dismiss_parity_fix(job_id: str, fix_id: str, request: Request, body: NoteBody | None = None):
    from services.parity_fixes import dismiss_fix

    _visible_job(job_id, request, write=True)
    try:
        report = await asyncio.to_thread(
            dismiss_fix, job_id, fix_id, actor=actor_from_request(request),
            note=(body.note if body else ""), expected_version=(body.expected_version if body else None),
        )
    except ParityError as exc:
        raise _http(exc) from exc
    return _public(report)


def _signer_role(request: Request, job: dict[str, Any]) -> str:
    """The signer's real authority: their gate role in the *job's* workspace.

    Resolved from the authenticated principal (membership, or a platform
    admin), never from anything the request body says. With authentication
    off the single local operator administers everything.
    """
    from services.effective_role import resolve_effective_role
    from src.services import auth_service

    if not auth_service.auth_required():
        return "admin"
    return resolve_effective_role(getattr(request.state, "user", None), str(job.get("workspace_id") or ""))


@router.post("/jobs/{job_id}/parity-report/signoff")
async def sign_off_parity_report(job_id: str, request: Request, body: SignoffBody | None = None):
    from services.parity_signoff import sign_off

    job = _visible_job(job_id, request)
    body = body or SignoffBody()
    if not body.role.strip():
        raise HTTPException(status_code=422, detail="role is required")
    try:
        report = await asyncio.to_thread(
            sign_off, job_id, role=body.role, actor=actor_from_request(request), note=body.note,
            signer_role=_signer_role(request, job), expected_version=body.expected_version,
        )
    except ParityError as exc:
        raise _http(exc) from exc
    return _public(report)


@router.get("/jobs/{job_id}/parity-report/export")
async def export_parity_report(
    job_id: str, request: Request, format: Literal["markdown", "json"] = Query(default="markdown"),
):
    """Markdown evidence pack, or (``format=json``) the signed pack the verify route checks."""
    from services.parity_signoff import export_markdown, signed_pack
    from services.parity_state import audit

    job = _visible_job(job_id, request)
    pack = signed_pack(job)
    if format == "json":
        if not pack:
            raise HTTPException(status_code=409, detail="The report is not signed yet.")
        out: Any = pack
    else:
        out = await asyncio.to_thread(export_markdown, job, job_id)
    audit("job.parity.exported", job=job, job_id=job_id, actor=actor_from_request(request),
          details={"format": format, "signed": bool(pack)})
    return out if format == "json" else PlainTextResponse(out, media_type="text/markdown")


@router.get("/parity/summary")
async def parity_summary(
    request: Request,
    workspace_id: str = Header(default="", alias="X-Workspace-Id"),
    limit: int = Query(default=30, ge=1, le=100),
):
    from services.parity_summary import build_summary
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request, workspace_id).filter
    return await asyncio.to_thread(build_summary, scope, limit=limit)
