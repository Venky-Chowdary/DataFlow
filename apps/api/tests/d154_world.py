"""Shared two-tenant world for the D154 isolation tests.

``build_world`` points every tenant-owned store at ``tmp_path``, creates the
bootstrap admin and workspaces/accounts for A and B, and seeds A's workspace —
plus legacy unscoped rows — with data whose names all carry one unique
``tag``. A test then asks "does the response for B contain the tag?", which is
the same question for every route and needs no per-route knowledge.

``signup=True`` is the multi-tenant deployment (isolation enforced; accounts
provisioned the way self-serve signup does). ``signup=False`` is the legacy
single-tenant deployment (isolation off; accounts created by the admin, with
``M`` belonging to no workspace at all).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient

from services.workspace_filter import bound_workspace_filter

ADMIN = "root-d154@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
PASSWORD = "Tenant-Password-2026!"


@dataclass
class World:
    tag: str
    admin: TestClient
    a: TestClient
    b: TestClient
    m: TestClient | None  # member of no workspace (legacy mode only)
    ws_a: str
    ws_b: str
    ids: dict[str, str]
    a_email: str = ""
    b_email: str = ""


def _login(email: str, password: str) -> TestClient:
    from src.main import app

    client = TestClient(app)
    login = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _signup_account(email: str) -> str:
    from services import signup_service
    from services.password_hash import hash_password_argon2

    _user, _org, workspace = signup_service._provision_verified_user({
        "email": email,
        "name": email.split("@")[0],
        "password_hash": hash_password_argon2(PASSWORD),
        "canonical_email": email,
    })
    return workspace.id


def _admin_account(admin: TestClient, email: str, workspace_id: str = "") -> TestClient:
    created = admin.post("/api/v1/team/users", json={
        "email": email,
        "platform_role": "member",
        "workspace_id": workspace_id,
        "workspace_role": "editor",
    })
    assert created.status_code == 200, created.text
    return _login(email, created.json()["temporary_password"])


def build_world(tmp_path, monkeypatch, *, signup: bool) -> World:
    from services import (
        audit_log,
        connector_store,
        mongodb_service,
        ops_metrics,
        quarantine_dlq,
        schedule_store,
        team_store,
        transform_library,
        transform_store,
        usage_metering,
        user_store,
    )
    from services import preflight_run_store, shape_draft_store, transfer_plan_store
    from services.auth_rate_limit import reset_auth_rate_limits
    from src.services.contract_store import reset_contract_store

    env = {
        "DATAFLOW_DATA_DIR": str(tmp_path),
        "DATAFLOW_TEAM_STORE": str(tmp_path / "teams.json"),
        "DATAFLOW_CONTRACTS_PATH": str(tmp_path / "contracts.json"),
        "DATAFLOW_TRANSFORMS_PATH": str(tmp_path / "transforms.json"),
        "DATAFLOW_TRANSFORM_LIBRARY_PATH": str(tmp_path / "library.json"),
        "DATAFLOW_REQUIRE_AUTH": "1",
        "DATAFLOW_ADMIN_EMAIL": ADMIN,
        "DATAFLOW_ADMIN_PASSWORD": ADMIN_PASSWORD,
        "DATAFLOW_AUTH_SECRET": "d154-world-test-secret",
        "DATAFLOW_JOB_STORE": "memory",
        "DATAFLOW_SIGNUP_ENABLED": "1" if signup else "0",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for name in ("REQUIRE_WORKSPACE",):
        for prefix in ("DATAFLOW_", "DATAWRAP_"):
            monkeypatch.delenv(prefix + name, raising=False)
    memory = mongodb_service.MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongodb_service, "_mongodb_service", memory)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(usage_metering, "STORE_PATH", tmp_path / "usage.json")
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(quarantine_dlq, "_dlq_coll", lambda: None)
    monkeypatch.setattr(preflight_run_store, "STORE_PATH", tmp_path / "preflight_runs.jsonl")
    monkeypatch.setattr(transfer_plan_store, "STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr(shape_draft_store, "STORE_PATH", tmp_path / "shape_drafts.json")
    for series in ("dataflow_pipeline_lag_seconds", "dataflow_pipeline_lag_bytes"):
        monkeypatch.setitem(ops_metrics._labeled_gauges, series, {})
    monkeypatch.setitem(ops_metrics._labeled_counters, "dataflow_pipeline_cdc_polls_total", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_heartbeat", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_workspace", {})
    reset_auth_rate_limits()
    reset_contract_store()
    transform_store.reset_transform_store()
    transform_library.reset_transform_library()

    tag = uuid4().hex[:8]
    a_email, b_email = f"alice-{tag}@example.com", f"bob-{tag}@example.com"
    admin = _login(ADMIN, ADMIN_PASSWORD)
    m = None
    if signup:
        ws_a, ws_b = _signup_account(a_email), _signup_account(b_email)
        a, b = _login(a_email, PASSWORD), _login(b_email, PASSWORD)
    else:
        ws_a = admin.post("/api/v1/team/workspaces", json={"name": f"A-{tag}"}).json()["workspace"]["id"]
        ws_b = admin.post("/api/v1/team/workspaces", json={"name": f"B-{tag}"}).json()["workspace"]["id"]
        a = _admin_account(admin, a_email, ws_a)
        b = _admin_account(admin, b_email, ws_b)
        m = _admin_account(admin, f"nomember-{tag}@example.com")

    ids = seed(tag=tag, ws_a=ws_a, a_email=a_email, memory=memory, data_dir=tmp_path)
    return World(
        tag=tag, admin=admin, a=a, b=b, m=m, ws_a=ws_a, ws_b=ws_b, ids=ids,
        a_email=a_email, b_email=b_email,
    )


def owner_marker(label: str, tag: str) -> str:
    """The string every row seeded for ``label`` ("a" or "legacy") carries."""
    return ("zowna" if label == "a" else "zlegacy") + tag


def seed(*, tag: str, ws_a: str, a_email: str, memory: Any, data_dir: Any = None) -> dict[str, str]:
    """A's workspace rows and legacy unscoped rows; every name carries ``tag``."""
    from services import (
        audit_log,
        connector_store,
        ops_metrics,
        quarantine_dlq,
        schedule_store,
        usage_metering,
    )
    from services.preflight_run_store import save_preflight_run
    from services.shape_draft_store import save_shape_draft
    from services.transfer_plan_store import create_plan
    from services.transform_library import get_transform_library
    from services.transform_recipe import RecipeStep, TransformRecipe
    from services.transform_store import TransformProject, get_transform_store
    from src.services.contract_store import get_contract_store
    from src.services.data_contract import ColumnRule, DataContract

    ids: dict[str, str] = {}
    for label, ws in (("a", ws_a), ("legacy", "")):
        marker = owner_marker(label, tag)
        connector = connector_store.create_connector({
            "name": f"{marker}-conn", "type": "postgresql", "host": f"{marker}.example.com",
            "port": 5432, "workspace_id": ws,
        })
        ids[f"{label}_connector"] = connector.id
        if data_dir is not None:
            # A connector that really connects: a SQLite file with a table and rows.
            import sqlite3

            # The service data directory is off-limits to connectors; its
            # ``sqlite`` volume sub-directory is where connector files belong.
            (data_dir / "sqlite").mkdir(parents=True, exist_ok=True)
            db_path = data_dir / "sqlite" / f"{marker}.sqlite"
            with sqlite3.connect(db_path) as db:
                db.execute(f"CREATE TABLE {marker}_tbl (id INTEGER, note TEXT)")
                db.executemany(
                    f"INSERT INTO {marker}_tbl VALUES (?, ?)",
                    [(i, f"{marker}-secret-row-{i}") for i in range(1, 4)],
                )
            live = connector_store.create_connector({
                "name": f"{marker}-db", "type": "sqlite", "host": str(db_path), "database": str(db_path),
                "port": 0, "workspace_id": ws, "role": "both",
            })
            ids[f"{label}_db"] = live.id
        sched = schedule_store.create_schedule({
            "name": f"{marker}-sched", "source_connector_id": connector.id,
            "source_table": f"{marker}_orders", "dest_connector_id": connector.id,
            "dest_table": f"{marker}_copy", "interval": "daily", "workspace_id": ws,
            "mappings": [{"source": "id", "target": "id"}],
        })
        ids[f"{label}_schedule"] = sched.id
        job_id = f"job-{marker}"
        memory._jobs[job_id] = {
            "_id": job_id, "status": "completed", "workspace_id": ws, "triggered_by": a_email,
            "source_name": f"{marker}-src", "created_at": "2026-10-01T00:00:00+00:00",
            "transfer_request": {"source": {"connector_id": connector.id}, "destination": {}},
        }
        ids[f"{label}_job"] = job_id
        usage_metering.record_transfer_usage(job_id=job_id, workspace_id=ws, rows_written=7)
        audit_log.append_audit_event(
            action=f"{marker}.action", resource=f"res-{marker}", actor=a_email, workspace_id=ws or None
        )
        quarantine_dlq.append_dlq_event(job_id=job_id, action="quarantine.fail", rows=3, workspace_id=ws)
        contract = DataContract(
            name=f"{marker}-contract",
            columns=[ColumnRule(source_name="id", target_name="id", source_type="INTEGER", target_type="INTEGER")],
            metadata={"workspace_id": ws},
        )
        get_contract_store().save_contract(contract)
        ids[f"{label}_contract"] = contract.id
        project = get_transform_store().save(TransformProject(
            name=f"{marker}-project", destination_connector_id=connector.id, workspace_id=ws,
        ))
        ids[f"{label}_project"] = project.id
        recipe = get_transform_library().save(TransformRecipe(
            name=f"{marker}-recipe", workspace_id=ws,
            steps=[RecipeStep(op="trim", column="name", options={})],
        ))
        ids[f"{label}_recipe"] = recipe.id
        # Stores that stamp the workspace of the request that wrote them.
        with bound_workspace_filter(None, ws):
            run = save_preflight_run(
                {"passed": True, "gates": []}, source_label=f"{marker}-src", dest_label=f"{marker}-dst"
            )
            plan = create_plan({"name": f"{marker}-plan"})
            save_shape_draft(route_key=f"{marker}-src→dst", recipe={"steps": []})
        ids[f"{label}_preflight_run"] = run["run_id"]
        ids[f"{label}_plan"] = plan.id
    ops_metrics.record_cdc_poll(
        lag_seconds=500.0, job_id=ids["a_job"], schedule_id=ids["a_schedule"],
        stream=f"stream-{owner_marker('a', tag)}", workspace_id=ws_a,
    )
    return ids
