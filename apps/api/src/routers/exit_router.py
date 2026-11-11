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
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request, Response
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
