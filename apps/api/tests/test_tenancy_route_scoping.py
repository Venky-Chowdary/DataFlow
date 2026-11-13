"""Tenancy regression suite: every route that takes a resource id is scoped.

``fixtures/route_tenancy_scoping.json`` is the one registry of how an
id-addressed route is scoped (security review M4: the older
``test_cross_workspace_resource_matrix`` id loop, its skip lists and its
sentinel ids are gone; that file keeps only the body-id checks). Driven from
the RBAC route inventory (``fixtures/rbac_route_permissions.json``, kept in
step with the live app by ``test_rbac_route_inventory``): every ``METHOD path``
there with a path parameter is declared as one of

* ``workspace`` -- the id names a resource one workspace owns. A resource of
  that kind is seeded in workspace A (a fresh one for every route that is not
  a GET) and the route is called by three callers who must not reach it:

  - an *administrator of workspace B* (so the refusal is tenancy, not a
    missing role);
  - an *API key of workspace B* with the admin role;
  - a *viewer of A who is an editor of B*, sending no ``X-Workspace-Id``
    (routes that are not a GET only: a viewer may read).

  Each answer must be 404/403 (or byte-for-byte what an id that does not
  exist gets), carry nothing of A's, and leave A's resource as it was.

  **Owner control.** The same call is then made as A's own administrator and
  its answer must differ from workspace B's: otherwise B's refusal proves
  nothing (a resource that was never really seeded answers everyone alike).
  An entry where that cannot hold says why in ``owner_control_reason``.
* ``platform`` -- deployment-wide configuration, account or workspace
  lifecycle administration: the same callers (none a platform administrator)
  are refused and nothing changes.
* ``exempt`` -- the parameter is not a tenant-owned resource; ``reason`` says why.

A new id-addressed route fails this suite until it says how it is scoped, and
a ``workspace`` declaration fails until the route really refuses another
workspace and really serves its own (audit findings H-1, H-2 and H-3 were
routes the older matrix skipped for want of a seeded resource).
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import EDITOR_A, tenancy_world  # noqa: E402,F401

FIXTURES = _TESTS / "fixtures"
SENTINEL = "TENANCY_A_SENTINEL"
ADMIN_A = "admin-a@tenancy.example.com"
ADMIN_B = "admin-b@tenancy.example.com"


def _inventory() -> list[str]:
    routes = json.loads((FIXTURES / "rbac_route_permissions.json").read_text(encoding="utf-8"))
    return sorted(route for route in routes if "{" in route)


def _declarations() -> dict[str, dict[str, str]]:
    return json.loads((FIXTURES / "route_tenancy_scoping.json").read_text(encoding="utf-8"))["routes"]


# --------------------------------------------------------------------------- declarations


def test_every_id_route_declares_its_tenant_scope():
    inventory, declared = set(_inventory()), _declarations()
    missing = sorted(inventory - set(declared))
    stale = sorted(set(declared) - inventory)
    assert not missing, (
        "These routes take a resource id but do not say how they are scoped to a tenant. Add each to "
        "tests/fixtures/route_tenancy_scoping.json (workspace / platform / exempt):\n  " + "\n  ".join(missing)
    )
    assert not stale, "Declared routes that no longer exist:\n  " + "\n  ".join(stale)
    for route, entry in declared.items():
        scope = entry.get("scope")
        assert scope in {"workspace", "platform", "exempt"}, (route, entry)
        assert set(entry) <= _ENTRY_KEYS, f"{route}: unknown keys {sorted(set(entry) - _ENTRY_KEYS)}"
        if scope == "exempt":
            assert len(entry.get("reason", "")) >= 30, f"{route}: an exemption needs a real reason"
        else:
            assert entry.get("resource") in SEEDERS, f"{route}: no seeder for resource {entry.get('resource')!r}"
            assert len(entry.get("reason", "x" * 30)) >= 30, f"{route}: write a real reason or none"
        for written in ("owner_control_reason", "viewer_may_call"):
            if written in entry:
                assert scope == "workspace", f"{route}: {written} only applies to a workspace route"
                assert len(entry[written]) >= 30, f"{route}: {written} needs a real reason"


_ENTRY_KEYS = {"scope", "resource", "reason", "owner_control_reason", "viewer_may_call"}


# --------------------------------------------------------------------------- seeding (workspace A)


def _next(world) -> int:
    """A fresh number per seed, for ids a store refuses to reuse."""
    world.seq = getattr(world, "seq", 0) + 1
    return world.seq


class Seeded:
    """One resource of workspace A: its path parameters and how to check it is intact."""

    def __init__(self, params: dict[str, str], intact: Callable[[], bool] | None = None):
        self.params = params
        self.intact = intact or (lambda: True)

    @property
    def missing(self) -> dict[str, str]:
        """The same shape of ids, naming nothing that exists."""
        return {name: _MISSING_IDS.get(name, f"no-such-{name}") for name in self.params}


_MISSING_IDS = {
    "plan_id": "00000000-0000-4000-8000-000000000000",
    "job_id": "0123456789abcdef01234567",
    "schedule_id": "00000000-0000-4000-8000-000000000001",
    "email": "nobody@tenancy.example.com",
    "filename": "no-such-export.csv",
}


def _seed_plan(world) -> Seeded:
    """A plan with a mapping revision (so ``run-payload`` builds) and a finished
    Validate run (so ``GET .../preflight`` has something to serve its owner)."""
    import time

    from services import transfer_plan_store

    made = world.admin_a.post("/api/v1/transfer/plans", json={
        "name": SENTINEL, "sample_rows": [{"id": SENTINEL}],
        "source": {"kind": "database", "format": "postgresql", "host": f"{SENTINEL}.internal"},
        "source_columns": ["id"], "target_columns": ["id"],
    })
    assert made.status_code == 200, made.text
    plan_id = made.json()["plan"]["id"]
    mapped = world.admin_a.post(
        f"/api/v1/transfer/plans/{plan_id}/mappings", json={"mappings": [{"source": "id", "target": "id"}]}
    )
    assert mapped.status_code == 200, mapped.text
    started = world.admin_a.post(f"/api/v1/transfer/plans/{plan_id}/preflight", json={"async_run": True})
    assert started.status_code == 202, started.text
    deadline = time.monotonic() + 20
    while world.admin_a.get(f"/api/v1/transfer/plans/{plan_id}/preflight").json().get("status") == "running":
        assert time.monotonic() < deadline, "the seeded Validate run never finished"
        time.sleep(0.05)
    # The Validate run fails (no such source); re-syncing the mappings makes
    # the plan runnable again and keeps the run on record.
    remapped = world.admin_a.post(
        f"/api/v1/transfer/plans/{plan_id}/mappings", json={"mappings": [{"source": "id", "target": "id"}]}
    )
    assert remapped.status_code == 200, remapped.text

    def state() -> tuple:
        plan = next(p for p in transfer_plan_store._load_all() if p.id == plan_id)
        return plan.name, plan.status, plan.active_version, len(plan.revisions), list(plan.job_ids)

    before = state()
    assert before[:2] == (SENTINEL, "mapped"), before
    return Seeded({"plan_id": plan_id}, lambda: state() == before)


def _seed_job(world) -> Seeded:
    from services.mongodb_service import get_mongodb_service

    job_id = world.job(
        world.workspace_a,
        name=SENTINEL,
        status="failed",
        sync_mode="cdc",
        rejected_details=[{"row": 1, "column": "c", "value": SENTINEL, "reason": "bad"}],
        destination_summary={"filename": "x.csv", "download_url": "/api/v1/transfer/download/x.csv"},
        transfer_request={
            "source": {"kind": "database", "format": "postgresql", "host": f"{SENTINEL}.src", "table": "t"},
            "destination": {"kind": "database", "format": "postgresql", "host": f"{SENTINEL}.dst", "table": "t"},
            "sync_mode": "cdc",
        },
    )

    # A real snapshot signal of this job, so the job-scoped signal routes are
    # judged on something the owner can reach.
    signal = world.admin_a.post(f"/api/v1/transfer/{job_id}/cdc/snapshots", json={"table": "t", "primary_key": "id"})
    assert signal.status_code == 200, signal.text

    def intact() -> bool:
        job = get_mongodb_service().get_job(job_id) or {}
        return job.get("status") == "failed" and job.get("name") == SENTINEL

    return Seeded({"job_id": job_id, "fix_id": "fix-1", "signal_id": signal.json()["id"]}, intact)


def _seed_proposal(world) -> Seeded:
    from services import agentic_repair

    job_id = world.job(world.workspace_a, name=SENTINEL)
    made = world.admin_a.post("/api/v1/repair/propose/quarantine", json={
        "job_id": job_id, "rejected_details": [{"column": SENTINEL, "reason": "value is not a number"}],
    })
    assert made.status_code == 200, made.text
    proposal_id = made.json()["id"]

    def intact() -> bool:
        with world.as_workspace(world.workspace_a):
            return agentic_repair.get_proposal(proposal_id).status == "proposed"

    return Seeded({"proposal_id": proposal_id}, intact)


def _seed_export(world) -> Seeded:
    from services.export_paths import generated_export_path

    name = "tenancy-a-ledger.csv"
    path = generated_export_path(name, workspace_id=world.workspace_a)
    path.write_text(f"id\n{SENTINEL}\n", encoding="utf-8")
    return Seeded({"filename": name}, path.is_file)


def _seed_saved_connector(world) -> Seeded:
    from services import connector_store

    connector = connector_store.create_connector({
        "name": SENTINEL, "type": "postgresql", "host": f"{SENTINEL}.db", "port": 5432,
        "database": SENTINEL, "workspace_id": world.workspace_a,
    })

    def intact() -> bool:
        with world.as_workspace(world.workspace_a):
            found = connector_store.get_connector(connector.id, workspace_id=world.workspace_a)
        return found is not None and found.name == SENTINEL

    return Seeded({"connector_id": connector.id}, intact)


def _seed_legacy_connector(world) -> Seeded:
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    connector_id = store.save_connector({
        "name": SENTINEL, "host": f"{SENTINEL}.legacy", "type": "mongodb", "workspace_id": world.workspace_a,
    })
    return Seeded({"connector_id": str(connector_id)}, lambda: store.get_connector(str(connector_id)) is not None)


def _seed_schedule(world) -> Seeded:
    from services import connector_store, schedule_store

    name = f"{SENTINEL}-{_next(world)}"  # pipeline names are unique per workspace
    connector = connector_store.create_connector({
        "name": f"{SENTINEL}-sched", "type": "postgresql", "host": f"{SENTINEL}.sched", "port": 5432,
        "database": "d", "workspace_id": world.workspace_a,
    })
    schedule = schedule_store.create_schedule({
        "name": name, "source_connector_id": connector.id, "source_table": SENTINEL,
        "dest_connector_id": connector.id, "dest_table": name, "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}], "workspace_id": world.workspace_a,
    })

    def intact() -> bool:
        found = schedule_store.get_schedule(schedule.id)
        return found is not None and found.name == name and bool(found.enabled) == bool(schedule.enabled)

    return Seeded({"schedule_id": schedule.id, "approval_id": "approval-1"}, intact)


def _seed_contract(world) -> Seeded:
    from services import contract_store
    from services.data_contract import DataContract

    contract = DataContract(name=SENTINEL, metadata={"workspace_id": world.workspace_a})
    store = contract_store.get_contract_store()
    store.save_contract(contract)
    return Seeded({"contract_id": contract.id}, lambda: store.get_contract(contract.id) is not None)


def _seed_upload(world) -> Seeded:
    from services import file_parser

    path = world.data_dir / "a_upload.csv"
    path.write_text(f"id,name\n1,{SENTINEL}\n", encoding="utf-8")
    file_id = "aupload1"
    world.monkeypatch.setitem(file_parser._file_registry, file_id, {
        "file_id": file_id, "filename": "a_upload.csv", "path": str(path), "format": "csv",
        "workspace_id": world.workspace_a,
    })
    return Seeded({"file_id": file_id})


def _seed_journal(world) -> Seeded:
    from services import transfer_journal

    world.monkeypatch.setenv("DATAFLOW_JOURNAL_DIR", str(world.data_dir / "journals"))
    journal_id = f"ajournal.{_next(world)}"
    folder = transfer_journal.journal_root() / transfer_journal._workspace_dir(world.workspace_a) / journal_id
    folder.mkdir(parents=True)
    (folder / transfer_journal.MANIFEST_NAME).write_text(json.dumps({
        "journal_id": journal_id, "workspace_id": world.workspace_a, "job_id": SENTINEL,
        "streams": [{"name": SENTINEL}],
    }), encoding="utf-8")
    return Seeded({"journal_id": journal_id}, folder.is_dir)


def _seed_preflight_run(world) -> Seeded:
    from services import preflight_run_store

    world.monkeypatch.setattr(preflight_run_store, "STORE_PATH", world.data_dir / "preflight-runs.jsonl")
    with world.as_workspace(world.workspace_a):
        run = preflight_run_store.save_preflight_run(
            {"passed": True, "blockers": []}, source_label=SENTINEL, dest_label="destination",
        )
    return Seeded({"run_id": run["run_id"]})


def _seed_benchmark_run(world) -> Seeded:
    from services import benchmark_runs

    run_id = "a" * 32
    world.monkeypatch.setitem(benchmark_runs._RUNS, run_id, {
        "id": run_id, "rows": 1000, "status": "completed", "report": {"target": SENTINEL},
        "workspace_id": world.workspace_a,
    })
    return Seeded({"run_id": run_id})


def _seed_acl_grant(world) -> Seeded:
    from services import resource_acl

    grant = resource_acl.upsert_grant(
        tenant_id=world.workspace_a, resource_type="schedule", resource_id="sched-a",
        principal="nobody@example.com", role="viewer", created_by="tenancy",
    )

    def intact() -> bool:
        return any(g.id == grant.id for g in resource_acl.list_grants(tenant_id=world.workspace_a))

    return Seeded({"grant_id": grant.id}, intact)


def _seed_cdc_signal(world) -> Seeded:
    from services import cdc_incremental_snapshot as snapshots

    world.monkeypatch.setattr(snapshots, "_DATA_DIR", str(world.data_dir))
    world.monkeypatch.setattr(snapshots, "_PATH", str(world.data_dir / "cdc_incremental_snapshots.json"))
    signal = snapshots.request_incremental_snapshot(
        f"postgresql://{SENTINEL}.cdc/app", SENTINEL, primary_key="id", workspace_id=world.workspace_a
    )

    def intact() -> bool:
        found = snapshots.get_signal(signal.id)
        return found is not None and found.status == signal.status

    return Seeded({"signal_id": signal.id}, intact)


def _seed_mapping_review(world) -> Seeded:
    from services import cdc_mapping_review

    world.monkeypatch.setattr(cdc_mapping_review, "_store_path", lambda: world.data_dir / "cdc_mapping_reviews.json")
    review = cdc_mapping_review.flag_mapping_review(
        source_key=f"postgresql://{SENTINEL}.cdc/app", table=SENTINEL, reason="schema_drift",
        workspace_id=world.workspace_a,
    )

    def intact() -> bool:
        rows = [r for r in cdc_mapping_review.list_reviews() if r.get("id") == review["id"]]
        return bool(rows) and not rows[0].get("acknowledged")

    return Seeded({"review_id": review["id"]}, intact)


def _seed_recipe(world) -> Seeded:
    from services import transform_library

    transform_library.reset_transform_library()
    made = world.admin_a.post("/api/v1/transform-library", json={
        "name": SENTINEL, "description": SENTINEL, "steps": [{"column": "name", "op": "trim"}],
    })
    assert made.status_code in {200, 201}, made.text
    body = made.json()
    recipe_id = (body.get("recipe") or body).get("id")

    def intact() -> bool:
        return world.admin_a.get(f"/api/v1/transform-library/{recipe_id}").status_code == 200

    return Seeded({"recipe_id": recipe_id}, intact)


def _seed_transform_project(world) -> Seeded:
    from services import transform_store
    from services.transform_store import TransformProject

    from services import connector_store

    transform_store.reset_transform_store()
    warehouse = connector_store.create_connector({
        "name": f"{SENTINEL}-warehouse", "type": "postgresql", "host": f"{SENTINEL}.wh", "port": 5432,
        "database": "d", "workspace_id": world.workspace_a,
    })
    project = transform_store.get_transform_store().save(TransformProject(
        id="aproject1", name=SENTINEL, schema="public", workspace_id=world.workspace_a,
        description=SENTINEL, destination_connector_id=warehouse.id,
    ))
    return Seeded(
        {"project_id": project.id},
        lambda: transform_store.get_transform_store().get(project.id) is not None,
    )


def _seed_notification_channel(world) -> Seeded:
    from services import notification_store

    world.monkeypatch.setattr(notification_store, "STORE_PATH", world.data_dir / "notifications.json")
    channel = notification_store.create_channel(
        workspace_id=world.workspace_a, kind="webhook", label=SENTINEL,
        config={"url": f"https://hooks.example.com/{SENTINEL}"}, created_by=ADMIN_A,
    )

    def intact() -> bool:
        found = notification_store.get_channel(channel.id)
        return found is not None and found.label == SENTINEL

    return Seeded({"channel_id": channel.id}, intact)


def _seed_api_key(world) -> Seeded:
    from services import integrations_store

    world.monkeypatch.setattr(integrations_store, "STORE_PATH", world.data_dir / "integrations.json")
    world.monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    key = integrations_store.create_api_key(
        SENTINEL, ADMIN_A, role="viewer", kind="service_account", workspace_id=world.workspace_a
    )
    key_id = str(key.get("id") or key.get("key_id"))

    def intact() -> bool:
        rows = integrations_store.list_api_keys() if hasattr(integrations_store, "list_api_keys") else []
        mine = [r for r in rows if str(r.get("id")) == key_id]
        return bool(mine) and not mine[0].get("revoked") and not mine[0].get("revoked_at")

    return Seeded({"key_id": key_id}, intact)


def _seed_tenant(world) -> Seeded:
    from services import tenant_store

    stale = tenant_store.get_tenant_for_workspace(world.workspace_a)
    if stale is not None:  # one tenant per workspace: a fresh seed replaces the last
        tenant_store.delete_tenant(stale.id)
    tenant = tenant_store.create_tenant(workspace_id=world.workspace_a, name=SENTINEL)

    def intact() -> bool:
        found = tenant_store.get_tenant(tenant.id)
        return found is not None and found.name == SENTINEL

    world.tenant_a = tenant.id
    return Seeded({"tenant_id": tenant.id}, intact)


def _seed_byok_key(world) -> Seeded:
    from services import byok_key_manager, tenant_store

    tenant = tenant_store.get_tenant_for_workspace(world.workspace_a) or tenant_store.create_tenant(
        workspace_id=world.workspace_a, name=SENTINEL
    )
    key = byok_key_manager.create_key(tenant.id, SENTINEL)
    return Seeded({"key_id": key.id})


def _seed_workspace(world) -> Seeded:
    """Workspace A itself, with a real pending invitation and a real member."""
    from services import team_store, workspace_invites

    if team_store.get_workspace_role(workspace_id=world.workspace_a, email=EDITOR_A) != "editor":
        world.add_membership(EDITOR_A, world.workspace_a, "editor")  # a route before removed or changed it
    invited = world.admin_a.post(
        f"/api/v1/team/workspaces/{world.workspace_a}/invites",
        json={"email": f"invitee-{_next(world)}@tenancy.example.com", "role": "viewer"},
    )
    assert invited.status_code == 200, invited.text
    invite_id = invited.json()["invite"]["id"]

    def intact() -> bool:
        ws = team_store.get_workspace(world.workspace_a)
        invite = workspace_invites.store.get(invite_id) or {}
        return ws is not None and ws.name == "Tenancy workspace A" and invite.get("status") == "pending"

    return Seeded({"workspace_id": world.workspace_a, "invite_id": invite_id, "email": EDITOR_A}, intact)


def _seed_account(world) -> Seeded:
    from services import user_store

    def intact() -> bool:
        user = user_store.get_user(EDITOR_A)
        return user is not None and user.get("status", "active") == "active" and user.get("role") == "member"

    return Seeded({"email": EDITOR_A}, intact)


def _seed_ai_provider(world) -> Seeded:
    return Seeded({"provider": "openai"})


def _seed_sso_config(world) -> Seeded:
    return Seeded({"sso_type": "oidc"})


def _seed_exit_system(world) -> Seeded:
    """A Reversibility Proof exit system of A, declared through the real route on A's own connection."""
    from services import exit_inventory, exit_store

    connector_id = _seed_saved_connector(world).params["connector_id"]
    made = world.admin_a.post("/api/v1/exit/systems", json={
        "name": SENTINEL, "source": {"connector_id": connector_id},
        "tables": [{"name": "tenancy_a_table", "key_columns": ["id"]}], "exit_window_hours": 24,
    })
    assert made.status_code == 201, made.text
    system_id = made.json()["id"]
    version = exit_store.get(exit_store.SYSTEMS, system_id)["version"]

    def intact() -> bool:
        kept = exit_store.get(exit_store.SYSTEMS, system_id)
        return (kept is not None and kept["name"] == SENTINEL and kept["version"] == version
                and kept.get("pii_acknowledgement") is None and not exit_inventory.rehearsals_of(system_id))

    return Seeded({"system_id": system_id}, intact)


def _seed_exit_rehearsal(world) -> Seeded:
    """A finished, unsigned rehearsal of one of A's exit systems."""
    from services import exit_inventory, exit_store

    system = exit_store.get(exit_store.SYSTEMS, _seed_exit_system(world).params["system_id"])
    rehearsal_id = f"exr_tenancy{_next(world):05d}"
    exit_store.insert(exit_store.REHEARSALS, {
        "id": rehearsal_id, "system_id": system["id"], "system_name": SENTINEL,
        "workspace_id": world.workspace_a, "n": 1, "trigger": "manual", "actor": ADMIN_A,
        "started_at": "2026-01-01T00:00:00Z", "finished_at": "2026-01-01T00:05:00Z",
        "verdict": "proven", "verdict_reason": SENTINEL, "source": dict(system["source"]),
        "extract_format": "jsonl", "window_seconds": 86400,
        "definition_sha256": exit_inventory.definition_sha256(system), "pii_acknowledgement": None,
        "required_roles": list(system["required_roles"]),
        "tables": [{"name": "tenancy_a_table", "schema": None, "key_columns": ["id"], "job_id": None,
                    "status": "proven", "reason": SENTINEL}],
        "approvals": [], "signed": None, "_verify": None,
    })

    def intact() -> bool:
        kept = exit_store.get(exit_store.REHEARSALS, rehearsal_id)
        return kept is not None and kept["approvals"] == [] and kept["verdict_reason"] == SENTINEL

    return Seeded({"rehearsal_id": rehearsal_id}, intact)


SEEDERS: dict[str, Callable[[Any], Seeded]] = {
    "exit_system": _seed_exit_system,
    "exit_rehearsal": _seed_exit_rehearsal,
    "plan": _seed_plan,
    "job": _seed_job,
    "proposal": _seed_proposal,
    "export": _seed_export,
    "saved_connector": _seed_saved_connector,
    "legacy_connector": _seed_legacy_connector,
    "schedule": _seed_schedule,
    "contract": _seed_contract,
    "upload": _seed_upload,
    "journal": _seed_journal,
    "preflight_run": _seed_preflight_run,
    "benchmark_run": _seed_benchmark_run,
    "acl_grant": _seed_acl_grant,
    "cdc_signal": _seed_cdc_signal,
    "mapping_review": _seed_mapping_review,
    "recipe": _seed_recipe,
    "transform_project": _seed_transform_project,
    "notification_channel": _seed_notification_channel,
    "api_key": _seed_api_key,
    "tenant": _seed_tenant,
    "byok_key": _seed_byok_key,
    "workspace": _seed_workspace,
    "account": _seed_account,
    "ai_provider": _seed_ai_provider,
    "sso_config": _seed_sso_config,
}

# --------------------------------------------------------------------------- request shapes

_GENERIC_BODY = {
    "name": "renamed-by-b", "label": "renamed-by-b", "role": "viewer", "reason": "tenancy probe",
    "password": "Tenancy-Probe-Password-2026!", "email": "probe@tenancy.example.com",
    "enabled": False, "note": "tenancy probe", "approve": False,
}
#: Bodies for routes whose model the generic one does not satisfy (422 would
#: answer before the handler ever looked the resource up).
_BODIES: dict[str, Any] = {
    "POST /api/v1/transfer/plans/{plan_id}/mappings": {"mappings": [{"source": "id", "target": "id"}]},
    "POST /api/v1/transfer/plans/{plan_id}/map": {"use_llm": False},
    "POST /api/v1/transfer/plans/{plan_id}/preflight": {},
    "PATCH /api/v1/connectors/saved/{connector_id}": {"name": "renamed-by-b"},
    "PUT /api/v1/connectors/saved/{connector_id}": {
        "name": "renamed-by-b", "type": "postgresql", "host": "b.example.com", "port": 5432,
    },
    "POST /api/v1/connectors/jobs/{job_id}/rerun": {},
    "POST /api/v1/connectors/jobs/{job_id}/retry": {},
    "POST /api/v1/connectors/jobs/{job_id}/sync": {"mode": "incremental"},
    "POST /api/v1/connectors/jobs/{job_id}/quarantine/replay": {},
    "POST /api/v1/jobs/{job_id}/parity-report/signoff": {"role": "data_owner", "note": "probe"},
    "POST /api/v1/transfer/{job_id}/cdc/snapshots": {"table": "t", "primary_key": "id"},
    "POST /api/v1/transfer/{job_id}/rollback/execute": {"confirm": True, "reason": "tenancy probe"},
    "PUT /api/v1/schedules/{schedule_id}": {
        "name": "renamed-by-b", "source_connector_id": "x", "source_table": "t",
        "dest_connector_id": "x", "dest_table": "t", "interval": "daily",
    },
    "POST /api/v1/schedules/{schedule_id}/authorization": {"reason": "tenancy probe", "scopes": ["run"]},
    "POST /api/v1/schedules/{schedule_id}/transform-recipe/upgrade": {"target_version": 2},
    "POST /api/v1/contracts/{contract_id}/sign": {"strict": True},
    "POST /api/v1/team/workspaces/{workspace_id}/invites": {"email": "probe@tenancy.example.com", "role": "viewer"},
    "POST /api/v1/team/workspaces/{workspace_id}/members": {"email": "probe@tenancy.example.com", "role": "viewer"},
    "POST /api/v1/team/workspaces/{workspace_id}/break-glass/restore-admin": {
        "email": "probe@tenancy.example.com", "reason": "tenancy probe",
    },
    "POST /api/v1/team/users/{email}/platform-role": {"role": "admin", "reason": "tenancy probe"},
    "PATCH /api/v1/workspace/sso/{sso_type}": {"enabled": True, "issuer": "https://idp.example.com"},
    "PATCH /api/v1/workspace/ai-providers/{provider}": {"api_key": "sk-tenancy-probe", "enabled": True},
    "PATCH /api/v1/workspace/tenant/{tenant_id}": {"name": "renamed-by-b"},
    "POST /api/v1/transform-library/{recipe_id}/preview": {"rows": [{"a": 1}]},
    # The exit bodies refuse unknown fields (422 before the workspace check would run).
    "PATCH /api/v1/exit/systems/{system_id}": {"name": "renamed-by-b"},
    "POST /api/v1/exit/systems/{system_id}/pii-acknowledgement": {"reason": "tenancy probe"},
    "POST /api/v1/exit/rehearsals/{rehearsal_id}/signoff": {"role": "engineering", "note": "tenancy probe"},
}
_QUERY: dict[str, dict[str, str]] = {
    "POST /api/v1/connectors/jobs/{job_id}/resume": {"request": "{}"},
}
#: Statuses that mean "refused for who is asking".
_REFUSED = {403, 404}
VIEWER_ELSEWHERE = "viewer-a-editor-b@tenancy.example.com"


def _url(route: str, params: dict[str, str]) -> tuple[str, str]:
    method, template = route.split(" ", 1)
    path = template
    for name in re.findall(r"{([^}:]+)", template):
        assert name in params, f"{route}: the seeder provides no {name!r}"
        path = path.replace("{" + name + "}", params[name])
    return method, path


def _call(client, route: str, params: dict[str, str]):
    method, path = _url(route, params)
    body = None
    if method in {"POST", "PUT", "PATCH"}:
        body = _BODIES.get(route, _GENERIC_BODY)
    return client.request(method, path, json=body, params=_QUERY.get(route))


def _without_reference(text: str) -> str:
    """Drop the per-response support reference ``safe_http_exception`` appends."""
    return re.sub(r" Reference: [0-9a-f-]{36}", "", text)


def _answer(response) -> tuple[int, str]:
    return response.status_code, _without_reference(response.text)


def _answers_like_a_missing_id(client, route: str, resource: Seeded, response) -> bool:
    """A 4xx that is byte-for-byte what an id that does not exist gets.

    A few routes answer an unknown id with their own 400 rather than a 404
    (plan Validate and run-payload). For a foreign caller, A's resource is
    exactly that: an unknown id. Never accepted for a 2xx -- and on its own it
    proves nothing, which is why every workspace route also has an owner control.
    """
    if not 400 <= response.status_code < 500:
        return False
    return _answer(_call(client, route, resource.missing)) == _answer(response)


def _leaks(response) -> bool:
    return SENTINEL.lower() in response.text.lower()


# --------------------------------------------------------------------------- the suite


@pytest.fixture
def world(tenancy_world, monkeypatch):  # noqa: F811
    from fastapi.testclient import TestClient

    from services import contract_store, exit_store, integrations_store, resource_acl

    monkeypatch.setattr(resource_acl, "STORE_PATH", tenancy_world.data_dir / "resource-acls.jsonl")
    monkeypatch.setattr(exit_store, "STORE_PATH", tenancy_world.data_dir / "exit_inventory.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tenancy_world.data_dir / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    contract_store.reset_contract_store()
    tenancy_world.monkeypatch = monkeypatch
    tenancy_world.admin_a = tenancy_world.member(ADMIN_A, tenancy_world.workspace_a, "admin")
    tenancy_world.admin_b = tenancy_world.member(ADMIN_B, tenancy_world.workspace_b, "admin")
    # An API key workspace B's administrator minted for B, with the admin role.
    minted = tenancy_world.admin_b.post(
        "/api/v1/workspace/api-keys", json={"name": "tenancy-b-key", "role": "admin", "expires_in": "7d"}
    )
    assert minted.status_code == 200 and minted.json()["workspace_id"] == tenancy_world.workspace_b, minted.text
    tenancy_world.key_b = TestClient(tenancy_world.app, headers={"Authorization": f"Bearer {minted.json()['key']}"})
    # A viewer of A who is an editor of B and names no workspace: the RBAC
    # permission comes from the editor role, the per-object check must not.
    viewer = tenancy_world.member(VIEWER_ELSEWHERE, tenancy_world.workspace_b, "editor")
    tenancy_world.add_membership(VIEWER_ELSEWHERE, tenancy_world.workspace_a, "viewer")
    viewer.headers.pop("X-Workspace-Id")
    tenancy_world.viewer_elsewhere = viewer
    return tenancy_world


def _foreign_callers(world, route: str, entry: dict[str, str]) -> list[tuple[str, Any]]:
    callers = [("workspace B's administrator", world.admin_b), ("workspace B's API key", world.key_b)]
    reads = route.startswith("GET ") and entry["scope"] == "workspace"
    if not reads and "viewer_may_call" not in entry:
        callers.append(("a viewer of A who edits B, no workspace header", world.viewer_elsewhere))
    return callers


def _route_problems(world, route: str, entry: dict[str, str], resource: Seeded) -> list[str]:
    problems: list[str] = []
    label = f"{entry['scope'].upper()} {route}"
    foreign = None
    for who, client in _foreign_callers(world, route, entry):
        response = _call(client, route, resource.params)
        if foreign is None:
            foreign = response
        if response.status_code not in _REFUSED and not _answers_like_a_missing_id(client, route, resource, response):
            problems.append(f"{label}: {who} got {response.status_code}: {response.text[:200]}")
        if _leaks(response):
            problems.append(f"LEAK {route}: {who} was shown workspace A's data: {response.text[:200]}")
        try:
            intact = resource.intact()
        except Exception as exc:  # noqa: BLE001 - a vanished resource is the finding
            intact, who = False, f"{who} ({type(exc).__name__}: {exc})"
        if not intact:
            problems.append(f"CHANGED {route}: {who} altered or removed workspace A's resource")
            return problems
    if entry["scope"] == "workspace" and "owner_control_reason" not in entry:
        own = _call(world.admin_a, route, resource.params)
        if _answer(own) == _answer(foreign):
            problems.append(
                f"VACUOUS {route}: A's own administrator gets the same answer as workspace B "
                f"({own.status_code}: {own.text[:160]}), so B's refusal proves nothing. Seed a real "
                "resource, or write an owner_control_reason in the fixture."
            )
    return problems


def test_every_id_route_refuses_other_tenants_and_serves_its_owner(world):
    declared = {route: entry for route, entry in _declarations().items() if entry["scope"] != "exempt"}
    problems: list[str] = []
    # Reads share one seed of each kind (checked untouched afterwards); every
    # other route gets a fresh seed, so a route that changes or deletes its
    # resource (as the owner's control call may) cannot blind the next.
    shared = {kind: seed(world) for kind, seed in SEEDERS.items()}
    reads = sorted((r for r in declared if r.startswith("GET ")), key=lambda r: (r.split(" ", 1)[1], r))
    for route in reads:
        problems += _route_problems(world, route, declared[route], shared[declared[route]["resource"]])
    problems += [
        f"CHANGED {kind}: a read route altered workspace A's resource"
        for kind, resource in shared.items() if not resource.intact()
    ]
    for route in sorted(set(declared) - set(reads), key=lambda r: (r.split(" ", 1)[1], r)):
        entry = declared[route]
        problems += _route_problems(world, route, entry, SEEDERS[entry["resource"]](world))
    assert not problems, f"{len(problems)} tenancy problem(s):\n  " + "\n  ".join(problems)


def test_deciding_a_proposal_needs_write_membership_in_its_workspace(world):
    """``POST /repair/proposals/{id}/decide`` asks for write (review M4): a viewer
    of the proposal's workspace is refused whatever role they hold elsewhere,
    and an editor of it may decide."""
    from services import agentic_repair

    proposal_id = _seed_proposal(world).params["proposal_id"]
    decide = f"/api/v1/repair/proposals/{proposal_id}/decide"
    viewer_with_header = world.member("viewer-a@tenancy.example.com", world.workspace_a, "viewer")
    for client in (world.viewer_elsewhere, viewer_with_header):
        refused = client.post(decide, json={"approve": False, "note": "probe"})
        assert refused.status_code == 403, refused.text
    with world.as_workspace(world.workspace_a):
        assert agentic_repair.get_proposal(proposal_id).status == "proposed"
    decided = world.a.post(decide, json={"approve": False, "note": "editor of A"})
    assert decided.status_code == 200, decided.text
    assert decided.json()["status"] != "proposed", decided.text


def test_ids_named_in_a_request_body_are_scoped_too(world):
    """Moved from the old resource matrix: B naming A's schedule, contract or
    saved connector inside a body (GitOps plan, fidelity check) is refused."""
    schedule = _seed_schedule(world)
    connector_id = _seed_saved_connector(world).params["connector_id"]
    contract_id = _seed_contract(world).params["contract_id"]

    spec = {
        "id": schedule.params["schedule_id"], "name": "caller-supplied-name",
        "source_connector_id": connector_id, "source_table": "source",
        "dest_connector_id": connector_id, "dest_table": "dest", "interval": "daily",
    }
    endpoint = {"kind": "database", "format": "postgresql", "connector_id": connector_id}
    answers = [
        world.admin_b.post("/api/v1/schedules/gitops/plan", json={"kind": "PipelineSchedule", "spec": spec}),
        world.admin_b.post("/api/v1/schedules/gitops/plan", json={"kind": "DataContract", "spec": {"id": contract_id}}),
        world.admin_b.post("/api/v1/fidelity/check", json={"source": endpoint, "destination": endpoint}),
    ]
    assert [r.status_code for r in answers] == [403, 403, 403], [r.text[:200] for r in answers]
    assert not any(_leaks(r) for r in answers)
    assert schedule.intact()
    # Owner control: A's administrator naming the same ids is not refused.
    own = world.admin_a.post("/api/v1/schedules/gitops/plan", json={"kind": "PipelineSchedule", "spec": spec})
    assert own.status_code != 403, own.text


# --------------------------------------------------------------------------- ids in a request body


def _body_declarations() -> dict[str, dict[str, Any]]:
    return json.loads((FIXTURES / "route_tenancy_scoping.json").read_text(encoding="utf-8")).get("body_routes", {})


#: Stands for the calling workspace's own connector on the side of a body that is
#: not under test (B's when B calls, A's for the owner control).
OWN = "__CALLERS_OWN_CONNECTOR__"


def _b_connector(world) -> str:
    return OWN


def _own_connector(world, workspace_id: str) -> str:
    from services import connector_store

    return connector_store.create_connector({
        "name": f"own-{_next(world)}", "type": "sqlite", "database": ":memory:", "workspace_id": workspace_id,
    }).id


def _as_caller(body: dict[str, Any], own: str) -> dict[str, Any]:
    return json.loads(json.dumps(body).replace(OWN, own))


def _a_connector(world) -> str:
    return SEEDERS["saved_connector"](world).params["connector_id"]


def _a_endpoint(connector_id: str) -> dict[str, Any]:
    return {"kind": "database", "format": "postgresql", "connector_id": connector_id, "table": "t"}


_FILE_DEST = {"kind": "file_export", "format": "csv"}
_MAP = [{"source": "id", "target": "id", "confidence": 1.0}]


def _cdc_snapshot_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    return [{"table": "t", "primary_key": "id", "job_id": seeded.params["job_id"]}]


def _connector_bodies(*shapes: str) -> Callable[[Any, Seeded], list[dict[str, Any]]]:
    """Bodies naming A's connector under each key in ``shapes`` (dotted = nested)."""
    def bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
        cid = seeded.params["connector_id"]
        out = []
        for shape in shapes:
            out.append(_BODY_SHAPES[shape](world, cid))
        return out

    return bodies


#: Body templates: A's connector id where the route reads it.
_BODY_SHAPES: dict[str, Callable[[Any, str], dict[str, Any]]] = {
    "query": lambda w, c: {"connector_id": c, "query": "select 1", "language": "sql"},
    "schema": lambda w, c: {"connector_id": c},
    "export.source": lambda w, c: {"connector_id": c, "query": "select 1 as id", "language": "sql", "format": "csv"},
    "export.destination": lambda w, c: {"connector_id": _b_connector(w), "query": "select 1 as id", "language": "sql",
                                        "destination_connector_id": c, "destination": "t"},
    "cdc.cleanup": lambda w, c: {"connector_id": c},
    "cdc.execute": lambda w, c: {"connector_id": c, "table": "t", "primary_key": "id"},
    "connectors.transfer": lambda w, c: {"connector_id": c, "table": "t", "destination_type": "postgresql",
                                         "destination_host": "b.example.com", "destination_database": "b",
                                         "destination_table": "t"},
    "execute.source": lambda w, c: {"source": _a_endpoint(c), "destination": _FILE_DEST, "mappings": _MAP,
                                    "async_mode": True},
    "execute.destination": lambda w, c: {"source": _a_endpoint(_b_connector(w)), "destination": _a_endpoint(c),
                                         "mappings": _MAP, "async_mode": True},
    "mapping.memory": lambda w, c: {"destination": _a_endpoint(c), "pairs": [{"source": "a", "target": "b"}]},
    "plans.source": lambda w, c: {"name": "p", "source": _a_endpoint(c), "source_columns": ["id"],
                                  "target_columns": ["id"]},
    "plans.destination": lambda w, c: {"name": "p", "destination": _a_endpoint(c), "source_columns": ["id"],
                                       "target_columns": ["id"]},
    "preflight.run": lambda w, c: {"connector_id": c, "table": "t", "columns": ["id"], "mappings": _MAP},
    "schedules.source": lambda w, c: {"name": f"s{_next(w)}", "interval": "daily", "source_connector_id": c,
                                      "source_table": "t", "dest_connector_id": _b_connector(w), "dest_table": "t",
                                      "mappings": _MAP},
    "schedules.destination": lambda w, c: {"name": f"s{_next(w)}", "interval": "daily",
                                           "source_connector_id": _b_connector(w), "source_table": "t",
                                           "dest_connector_id": c, "dest_table": "t", "mappings": _MAP},
    "transforms": lambda w, c: {"name": f"z{_next(w)}", "destination_connector_id": c,
                                "models": [{"name": "m", "sql": "select 1"}]},
}


def _job_bodies(*kinds: str) -> Callable[[Any, Seeded], list[dict[str, Any]]]:
    def bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
        job_id = seeded.params["job_id"]
        made = {
            "propose.quarantine": {"job_id": job_id, "rejected_details": [{"column": "c", "reason": "bad"}]},
            "propose.preflight": {"job_id": job_id, "preflight": {"passed": False, "blockers": []}},
            "map": {"job_id": job_id, "source_columns": ["id"], "target_columns": ["id"]},
            "acl": {"resource_type": "job", "resource_id": job_id, "principal": "nobody@example.com", "role": "viewer"},
        }
        return [made[k] for k in kinds]

    return bodies


def _contract_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    cid = seeded.params["contract_id"]
    source = _a_endpoint(_b_connector(world))
    return [
        {"contract_id": cid, "rows": [{"id": 1}]},
        {"source": source, "destination": _FILE_DEST, "mappings": _MAP, "async_mode": True, "contract_id": cid},
    ]


def _schedule_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    sid = seeded.params["schedule_id"]
    return [
        {"schedule_id": sid},
        {"resource_type": "schedule", "resource_id": sid, "principal": "nobody@example.com", "role": "viewer"},
    ]


def _plan_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    return [{"plan_id": seeded.params["plan_id"], "async_mode": True,
             "source": _a_endpoint(_b_connector(world)), "destination": _FILE_DEST, "mappings": _MAP}]


def _recipe_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    return [{"source": _a_endpoint(_b_connector(world)), "destination": _FILE_DEST, "mappings": _MAP,
             "async_mode": True, "transform_recipe_id": seeded.params["recipe_id"]}]


def _preflight_explain_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    return [{"run_id": seeded.params["run_id"]}]


def _byok_bodies(world, seeded: Seeded) -> list[dict[str, Any]]:
    return [{"workspace_id": world.workspace_b, "name": "B", "byok_key_id": seeded.params["key_id"]}]


#: For each declared body route: the bodies that name workspace A's seeded ids.
_BODY_ROUTE_BODIES: dict[str, Callable[[Any, Seeded], list[dict[str, Any]]]] = {
    "POST /api/v1/cdc/snapshots": _cdc_snapshot_bodies,
    "POST /api/v1/cdc/signals/execute-snapshot": _connector_bodies("cdc.execute"),
    "POST /api/v1/contracts/test": lambda w, s: _contract_bodies(w, s)[:1],
    "POST /api/v1/fidelity/check": lambda w, s: _schedule_bodies(w, s)[:1],
    "POST /api/v1/ops/cdc-captures/cleanup": _connector_bodies("cdc.cleanup"),
    "POST /api/v1/preflight/explain": _preflight_explain_bodies,
    "POST /api/v1/preflight/run": _connector_bodies("preflight.run"),
    "POST /api/v1/query/execute": _connector_bodies("query"),
    "POST /api/v1/query/export": _connector_bodies("export.source", "export.destination"),
    "POST /api/v1/query/schema": _connector_bodies("schema"),
    "POST /api/v1/query/schemas": _connector_bodies("schema"),
    "POST /api/v1/repair/propose/preflight": _job_bodies("propose.preflight"),
    "POST /api/v1/repair/propose/quarantine": _job_bodies("propose.quarantine"),
    "POST /api/v1/resource-acls": _job_bodies("acl"),
    "POST /api/v1/schedules/": _connector_bodies("schedules.source", "schedules.destination"),
    "POST /api/v1/schedules/admin/orphans/reconcile": lambda w, s: [{"schedule_ids": [s.params["schedule_id"]]}],
    "POST /api/v1/transfer/execute": _connector_bodies("execute.source", "execute.destination"),
    "POST /api/v1/transfer/execute#contract_id": lambda w, s: _contract_bodies(w, s)[1:],
    "POST /api/v1/transfer/execute#plan_id": _plan_bodies,
    "POST /api/v1/transfer/execute#transform_recipe_id": _recipe_bodies,
    "POST /api/v1/transfer/map": _job_bodies("map"),
    "POST /api/v1/transfer/mapping/memory": _connector_bodies("mapping.memory"),
    "POST /api/v1/transfer/plans": _connector_bodies("plans.source", "plans.destination"),
    "POST /api/v1/transforms/": _connector_bodies("transforms"),
    "POST /api/v1/workspace/tenant": _byok_bodies,
}


def test_every_body_id_route_refuses_other_tenants_and_serves_its_owner(world):
    """Security review round 2 (M2) and round 3 (body routes): every route whose
    resource id travels in the body is declared in ``body_routes`` and judged
    like a path-id route: B's administrator and API key are refused (403/404)
    for A's ids and leave nothing of A's, and A's own administrator is not
    refused (owner control: 200, or the entry's declared ``owner_status`` when
    the route cannot complete in this world, never 403/404). Routes whose body
    ids name no workspace resource (auth challenges, devices, Pilot acks) are
    declared with ``scope: none`` and the reason."""
    declared = _body_declarations()
    judged = {route for route, entry in declared.items() if entry["scope"] == "workspace"}
    assert judged == set(_BODY_ROUTE_BODIES), "every declared body route needs request bodies here"
    for route, entry in declared.items():
        assert entry["scope"] in ("workspace", "none") and entry.get("reason"), route
    problems: list[str] = []
    for route in sorted(judged):
        entry = declared[route]
        assert entry["resource"] in SEEDERS, (route, entry)
        method, path = route.split("#", 1)[0].split(" ", 1)
        seeded = SEEDERS[entry["resource"]](world)
        owner_ok = set(entry.get("owner_status") or [200])
        b_own, a_own = _own_connector(world, world.workspace_b), _own_connector(world, world.workspace_a)
        b_header = {"X-Workspace-Id": world.workspace_b}
        for body in _BODY_ROUTE_BODIES[route](world, seeded):
            for who, client in (("B's administrator", world.admin_b), ("B's API key", world.key_b)):
                r = client.request(method, path, json=_as_caller(body, b_own), headers=b_header)
                if r.status_code not in (403, 404) or _leaks(r):
                    problems.append(f"{route} {who} {body}: {r.status_code} {r.text[:160]}")
            if not seeded.intact():  # judged before A's own call, which may use it
                problems.append(f"{route}: A's resource changed")
            if entry.get("owner_control", True):
                own = world.admin_a.request(method, path, json=_as_caller(body, a_own))
                if own.status_code not in owner_ok or own.status_code in (403, 404):
                    problems.append(f"{route} owner control {body}: {own.status_code} {own.text[:160]}")
    assert not problems, "\n".join(problems)


def test_a_cdc_snapshot_signal_names_only_a_source_the_caller_can_write(world):
    """B naming A's connector as ``connector:<id>`` or ``connector_id``, or an
    arbitrary fingerprint, never queues a signal on A's source."""
    connector_id = _seed_saved_connector(world).params["connector_id"]
    base = {"table": "t", "primary_key": "id"}
    answers = [
        world.admin_b.post("/api/v1/cdc/snapshots", json={**base, "source_key": f"connector:{connector_id}"}),
        world.admin_b.post("/api/v1/cdc/snapshots", json={**base, "connector_id": connector_id}),
        world.admin_b.post("/api/v1/cdc/snapshots", json={**base, "source_key": f"postgresql:{SENTINEL}.db:5432/x"}),
    ]
    assert [r.status_code for r in answers] == [404, 404, 400], [r.text[:160] for r in answers]
    own = world.admin_a.post("/api/v1/cdc/snapshots", json={**base, "connector_id": connector_id})
    assert own.status_code == 200 and own.json()["workspace_id"] == world.workspace_a, own.text


def test_verifying_a_connector_region_needs_a_platform_admin_even_for_its_own_workspace(world):
    """Security review round 2 (mutant ``verify_route_no_admin_check``): another
    workspace's id is a 404 anyway, so the platform-admin rule is only proven on
    an id the caller's own workspace owns."""
    connector_id = _seed_saved_connector(world).params["connector_id"]
    path = f"/api/v1/connectors/saved/{connector_id}/region-verification"
    own_admin = world.admin_a.post(path, json={"region": "eu-west-1"})
    assert own_admin.status_code == 403, own_admin.text
    from services import connector_store

    with world.as_workspace(world.workspace_a):
        assert not connector_store.get_connector(connector_id, workspace_id=world.workspace_a).region_verification
    platform = world.admin.post(path, json={"region": "eu-west-1"}, headers={"X-Workspace-Id": world.workspace_a})
    assert platform.status_code == 200, platform.text
