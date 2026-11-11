"""Reversibility Proof routes: the exit inventory, rehearsals and the Exit Evidence Pack.

Reads are ``job.read``; declaring, changing or removing a system is
``job.manage`` (it configures recurring extracts of a saved connection);
starting a rehearsal is ``job.run``; signing is ``job.manage`` with the
authority Parity Studio's sign-off mapping binds to each role; verifying a pack
is a read. Every write also needs write access to the system's workspace, and
a system of another workspace is a 404, exactly like a job.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from fastapi import APIRouter, Body, Header, HTTPException, Query, Request, Response
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

from services.audit_log import actor_from_request
from services.exit_inventory import ExitError

router = APIRouter(tags=["Reversibility Proof"])


class SystemBody(BaseModel):
    """ExitSystemInput (contract 3). Unknown fields are refused, so a typo is not silently ignored."""

    model_config = {"extra": "forbid"}

    name: str | None = None
    description: str | None = None
    criticality: str | None = None
    source: dict[str, Any] | None = None
    tables: list[Any] | None = None
    exit_window_hours: int | None = None
    window_basis: str | None = None
    regulatory_dates: list[dict[str, Any]] | None = None
    rehearsal_every_days: int | None = None
    rehearsal_schedule: dict[str, Any] | None = None
    extract_format: str | None = None
    required_roles: list[str] | None = None
    expected_version: int | None = Field(default=None, ge=0)


def _http(exc: ExitError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=str(exc))


def _visible_system(system_id: str, request: Request, *, write: bool = False) -> dict[str, Any]:
    from services.exit_inventory import ExitNotFound, get_system
    from services.workspace_access import assert_resource_workspace

    try:
        system = get_system(system_id)
    except ExitNotFound as exc:
        raise HTTPException(status_code=404, detail="Exit system not found") from exc
    assert_resource_workspace(request, system.get("workspace_id") or None, write=write,
                              not_found_detail="Exit system not found")
    return system


def _body(body: SystemBody) -> dict[str, Any]:
    return body.model_dump(exclude_unset=True, exclude={"expected_version"})


@router.get("/exit/readiness")
async def exit_readiness(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    from services import exit_store
    from services.exit_inventory import board
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request, workspace_id)

    def build() -> dict[str, Any]:
        systems = exit_store.list_docs(exit_store.SYSTEMS, where=lambda d: scope.allows(d.get("workspace_id")))
        return board(sorted(systems, key=lambda s: str(s.get("name") or "").lower()))

    return await asyncio.to_thread(build)


@router.get("/exit/systems")
async def list_exit_systems(request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id")):
    from services import exit_store
    from services.exit_inventory import public_system
    from services.workspace_access import tenant_scope

    scope = tenant_scope(request, workspace_id)

    def build() -> dict[str, Any]:
        systems = exit_store.list_docs(exit_store.SYSTEMS, where=lambda d: scope.allows(d.get("workspace_id")))
        systems.sort(key=lambda s: str(s.get("name") or "").lower())
        return {"systems": [public_system(s) for s in systems]}

    return await asyncio.to_thread(build)


@router.post("/exit/systems", status_code=201)
async def declare_exit_system(
    body: SystemBody, request: Request, workspace_id: str = Header(default="", alias="X-Workspace-Id"),
):
    from services.exit_inventory import declare, public_system
    from services.workspace_access import assert_connector_accessible, resolve_tenant_scope

    target = resolve_tenant_scope(request, workspace_id).target_workspace_id(write=True)
    connector_id = str((body.source or {}).get("connector_id") or "")
    # The connection is read on every rehearsal: it must be one the caller can see.
    assert_connector_accessible(request, connector_id)
    try:
        system = await asyncio.to_thread(declare, _body(body), workspace_id=target,
                                         actor=actor_from_request(request))
    except ExitError as exc:
        raise _http(exc) from exc
    return public_system(system, [])


@router.get("/exit/systems/{system_id}")
async def get_exit_system(system_id: str, request: Request):
    from services.exit_inventory import public_system

    system = _visible_system(system_id, request)
    return await asyncio.to_thread(public_system, system)


@router.patch("/exit/systems/{system_id}")
async def update_exit_system(system_id: str, body: SystemBody, request: Request):
    from services.exit_inventory import public_system, update
    from services.workspace_access import assert_connector_accessible

    _visible_system(system_id, request, write=True)
    if body.source is not None:
        assert_connector_accessible(request, str(body.source.get("connector_id") or ""))
    try:
        system = await asyncio.to_thread(update, system_id, _body(body), actor=actor_from_request(request),
                                         expected_version=body.expected_version)
    except ExitError as exc:
        raise _http(exc) from exc
    return await asyncio.to_thread(public_system, system)


@router.delete("/exit/systems/{system_id}", status_code=204)
async def delete_exit_system(system_id: str, request: Request):
    from services.exit_inventory import remove

    _visible_system(system_id, request, write=True)
    try:
        await asyncio.to_thread(remove, system_id, actor=actor_from_request(request))
    except ExitError as exc:
        raise _http(exc) from exc
    return Response(status_code=204)


# ------------------------------------------------------------- PII review


class PiiAcknowledgementBody(BaseModel):
    """Why the system's extracts may carry the PII the gate found (the actor is the caller, never the body)."""

    model_config = {"extra": "forbid"}

    reason: str = Field(default="", max_length=500)
    expected_version: int | None = Field(default=None, ge=0)


@router.post("/exit/systems/{system_id}/pii-acknowledgement")
async def acknowledge_exit_pii(system_id: str, body: PiiAcknowledgementBody, request: Request):
    """Acknowledge PII for a system: its rehearsal extracts then pass the PII gate as an acknowledged transfer."""
    from services.exit_inventory import public_system
    from services.exit_pii import acknowledge

    _visible_system(system_id, request, write=True)
    try:
        system = await asyncio.to_thread(acknowledge, system_id, actor=actor_from_request(request),
                                         reason=body.reason, expected_version=body.expected_version)
    except ExitError as exc:
        raise _http(exc) from exc
    return await asyncio.to_thread(public_system, system)


@router.delete("/exit/systems/{system_id}/pii-acknowledgement")
async def withdraw_exit_pii(system_id: str, request: Request):
    """Withdraw the acknowledgement: the next rehearsal's extracts are held by the PII gate again."""
    from services.exit_inventory import public_system
    from services.exit_pii import withdraw

    _visible_system(system_id, request, write=True)
    try:
        system = await asyncio.to_thread(withdraw, system_id, actor=actor_from_request(request))
    except ExitError as exc:
        raise _http(exc) from exc
    return await asyncio.to_thread(public_system, system)


# ------------------------------------------------------------------ rehearsals


def _visible_rehearsal(rehearsal_id: str, request: Request, *, write: bool = False) -> dict[str, Any]:
    from services import exit_store
    from services.workspace_access import assert_resource_workspace

    doc = exit_store.get(exit_store.REHEARSALS, rehearsal_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Rehearsal not found")
    assert_resource_workspace(request, doc.get("workspace_id") or None, write=write,
                              not_found_detail="Rehearsal not found")
    return doc


@router.post("/exit/systems/{system_id}/rehearsals", status_code=202)
async def start_exit_rehearsal(system_id: str, request: Request):
    from services.exit_rehearsal import public, start

    system = _visible_system(system_id, request, write=True)
    try:
        doc = await asyncio.to_thread(start, system, actor=actor_from_request(request), trigger="manual")
    except ExitError as exc:
        raise _http(exc) from exc
    return await asyncio.to_thread(public, doc)


@router.get("/exit/systems/{system_id}/rehearsals")
async def list_exit_rehearsals(system_id: str, request: Request, limit: int = Query(default=20, ge=1, le=100)):
    from services.exit_inventory import rehearsal_history

    system = _visible_system(system_id, request)
    # The system is passed so each summary says whether it still proves the current definition.
    return await asyncio.to_thread(rehearsal_history, system, limit=limit)


@router.get("/exit/rehearsals/{rehearsal_id}")
async def get_exit_rehearsal(rehearsal_id: str, request: Request):
    from services.exit_rehearsal import public, settle

    doc = _visible_rehearsal(rehearsal_id, request)

    def build() -> dict[str, Any]:
        # Never verifies on the request thread: a finished rehearsal is verified in the background.
        return public(settle(rehearsal_id, inline=False) or doc)

    return await asyncio.to_thread(build)


# ----------------------------------------------------- sign-off and evidence


class SignoffBody(BaseModel):
    role: str = Field(default="", max_length=64)
    note: str = Field(default="", max_length=500)
    expected_version: int | None = Field(default=None, ge=0)


@router.post("/exit/rehearsals/{rehearsal_id}/signoff")
async def sign_off_exit_rehearsal(rehearsal_id: str, request: Request, body: SignoffBody | None = None):
    from services.exit_evidence import sign_off
    from services.exit_rehearsal import public

    from .parity_router import _signer_role

    doc = _visible_rehearsal(rehearsal_id, request)
    body = body or SignoffBody()
    if not body.role.strip():
        raise HTTPException(status_code=422, detail="role is required")
    try:
        after = await asyncio.to_thread(
            sign_off, rehearsal_id, role=body.role, actor=actor_from_request(request), note=body.note,
            # The signer's authority in the system's workspace, from the principal, never the body.
            signer_role=_signer_role(request, {"workspace_id": doc.get("workspace_id")}),
            expected_version=body.expected_version,
        )
    except ExitError as exc:
        raise _http(exc) from exc
    return await asyncio.to_thread(public, after)


@router.get("/exit/rehearsals/{rehearsal_id}/evidence")
async def export_exit_evidence(
    rehearsal_id: str, request: Request, format: Literal["json", "markdown"] = Query(default="json"),
):
    """The signed Exit Evidence Pack (json, 409 until signed) or a readable Markdown report."""
    from services.exit_evidence import exported_pack, render_markdown, signed_pack
    from services.exit_inventory import audit

    doc = _visible_rehearsal(rehearsal_id, request)
    pack = signed_pack(doc)
    if format == "json" and not pack:
        raise HTTPException(status_code=409, detail="The rehearsal is not signed yet.")
    audit("exit.evidence.exported", system={"id": doc.get("system_id"), "workspace_id": doc.get("workspace_id")},
          actor=actor_from_request(request),
          details={"rehearsal_id": rehearsal_id, "format": format, "signed": bool(pack)})
    if format == "json":
        return await asyncio.to_thread(exported_pack, doc, actor=actor_from_request(request))
    return PlainTextResponse(await asyncio.to_thread(render_markdown, doc), media_type="text/markdown")


@router.post("/exit/evidence/verify")
async def verify_exit_evidence(body: Any = Body(...)):
    """Verify an Exit Evidence Pack (``{"pack": ...}`` or the pack itself): signature, Merkle root, anchor.

    A body that is not a signed pack is a 422 naming what is missing, like the proof-pack verify route.
    """
    from services.exit_evidence import verify_pack
    from services.proof_pack_request import pack_from_body

    return await asyncio.to_thread(verify_pack, pack_from_body(body))
