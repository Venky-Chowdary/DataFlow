"""Ids named in a request body are checked for ownership when they are written (review round 3, Lows).

A route that stores or acts on an id taken from its body (a contract, a
connector, a job, an ACL's resource, a BYOK key) used to accept another
workspace's id as a dangling reference. Each of these helpers answers 404
for an id the caller's workspace does not own, byte-for-byte like an id that
does not exist (D222), using the same tenant-scope rule as the path-id routes
(``services.workspace_access.assert_resource_workspace``).
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException


def _system_scope():
    from services.workspace_filter import bound_workspace_filter

    return bound_workspace_filter(None)


def connector_owner(connector_id: str) -> tuple[bool, str]:
    from services.connector_store import connector_owner_workspace

    return connector_owner_workspace(str(connector_id or ""))


def job_owner(job_id: str) -> tuple[bool, str]:
    from services.mongodb_service import get_mongodb_service

    with _system_scope():
        job = get_mongodb_service().get_job(str(job_id or ""))
    return (job is not None), str((job or {}).get("workspace_id") or "")


def contract_owner(contract_id: str) -> tuple[bool, str]:
    from services.contract_read_model import load_contract

    with _system_scope():
        contract = load_contract(str(contract_id or ""))
    meta = getattr(contract, "metadata", None) or {}
    return (contract is not None), str(meta.get("workspace_id") or "")


def schedule_owner(schedule_id: str) -> tuple[bool, str]:
    from services.schedule_store import get_schedule

    with _system_scope():
        schedule = get_schedule(str(schedule_id or ""))
    return (schedule is not None), str(getattr(schedule, "workspace_id", "") or "")


def transform_owner(project_id: str) -> tuple[bool, str]:
    from services.transform_store import get_transform_store

    with _system_scope():
        project = get_transform_store().get(str(project_id or ""))
    return (project is not None), str(getattr(project, "workspace_id", "") or "")


def plan_owner(plan_id: str) -> tuple[bool, str]:
    from services.transfer_plan_store import get_plan

    with _system_scope():
        plan = get_plan(str(plan_id or ""))
    return (plan is not None), str(getattr(plan, "workspace_id", "") or "")


def recipe_owner(recipe_id: str) -> tuple[bool, str]:
    from services.transform_library import get_transform_library

    with _system_scope():
        recipe = get_transform_library().get(str(recipe_id or ""))
    return (recipe is not None), str(getattr(recipe, "workspace_id", "") or "")


#: ``resource_acl.RESOURCE_TYPES`` -> how to find the resource's owner.
RESOURCE_OWNERS = {
    "connector": connector_owner,
    "job": job_owner,
    "contract": contract_owner,
    "schedule": schedule_owner,
    "transform": transform_owner,
}


def assert_owned(request: Any, found_owner: tuple[bool, str], *, detail: str, write: bool = False) -> None:
    """404 unless the resource exists and the request's tenant scope covers it."""
    from services.workspace_access import assert_resource_workspace

    found, owner = found_owner
    if not found:
        raise HTTPException(status_code=404, detail=detail)
    assert_resource_workspace(request, owner, write=write, not_found_detail=detail)


def assert_body_ids(
    request: Any,
    *,
    connectors: tuple[str, ...] = (),
    jobs: tuple[str, ...] = (),
    contracts: tuple[str, ...] = (),
    plans: tuple[str, ...] = (),
    recipes: tuple[str, ...] = (),
    schedules: tuple[str, ...] = (),
    write: bool = False,
) -> None:
    """404 for any non-empty id in the body that the caller's workspace does not own."""
    for connector_id in (c for c in connectors if str(c or "").strip()):
        assert_owned(request, connector_owner(connector_id), detail="Connector not found", write=write)
    for job_id in (j for j in jobs if str(j or "").strip()):
        assert_owned(request, job_owner(job_id), detail="Job not found")
    for contract_id in (c for c in contracts if str(c or "").strip()):
        assert_owned(request, contract_owner(contract_id), detail="Contract not found")
    for plan_id in (p for p in plans if str(p or "").strip()):
        assert_owned(request, plan_owner(plan_id), detail="Plan not found")
    for recipe_id in (r for r in recipes if str(r or "").strip()):
        assert_owned(request, recipe_owner(recipe_id), detail="Transform recipe not found")
    for schedule_id in (s for s in schedules if str(s or "").strip()):
        assert_owned(request, schedule_owner(schedule_id), detail="Schedule not found")


def assert_schedule_body_ids(request: Any, payload: dict) -> None:
    """The connectors, contract, recipe and replayed schedule a schedule stores
    must be the caller's (review round 3, body routes): B's schedule on A's
    source connector was saved, and its key probe read A's catalog."""
    assert_body_ids(request, connectors=(str(payload.get("source_connector_id") or ""),))
    assert_body_ids(
        request,
        connectors=(str(payload.get("dest_connector_id") or ""),),
        contracts=(str(payload.get("contract_id") or ""),),
        recipes=(str(payload.get("transform_recipe_id") or ""),),
        schedules=(str(payload.get("replay_schedule_id") or ""),),
        write=True,
    )


def assert_resource_in_workspace(resource_type: str, resource_id: str, workspace_id: str) -> None:
    """An ACL grant's resource must be the granting workspace's own (404 otherwise)."""
    lookup = RESOURCE_OWNERS.get(str(resource_type or ""))
    if lookup is None:
        return  # an unsupported type is refused by resource_acl itself (400)
    found, owner = lookup(resource_id)
    if not found or owner != str(workspace_id or ""):
        raise HTTPException(status_code=404, detail="Resource not found")


def assert_byok_key_in_workspace(key_id: str, workspace_id: str) -> None:
    """A tenant's ``byok_key_id`` must name a key of one of this workspace's tenants (404 otherwise)."""
    if not str(key_id or "").strip():
        return
    from services.byok_key_manager import get_key
    from services.tenant_store import get_tenant

    key = get_key(str(key_id))
    tenant = get_tenant(str(getattr(key, "tenant_id", "") or "")) if key is not None else None
    if tenant is None or str(tenant.workspace_id or "") != str(workspace_id or ""):
        raise HTTPException(status_code=404, detail="BYOK key not found")


__all__ = [
    "RESOURCE_OWNERS",
    "assert_schedule_body_ids",
    "assert_body_ids",
    "assert_byok_key_in_workspace",
    "assert_owned",
    "assert_resource_in_workspace",
]
