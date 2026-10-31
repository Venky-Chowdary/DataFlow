from __future__ import annotations

import json
import importlib
import re
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.auth_rate_limit import reset_auth_rate_limits
from services import (
    audit_log,
    connector_store,
    contract_store,
    resource_acl,
    schedule_store,
    team_store,
    user_store,
)
from services.data_contract import DataContract
from src.main import app

SKIPPED_WITH_REASON = {
    "email": "Account lifecycle routes require dedicated user-account fixtures.",
    "group_id": "SCIM group routes require SCIM bearer-token fixtures.",
    "key_id": "API-key/service-account routes require secret-bearing key records.",
    "plan_id": "Transfer plans require a completed plan/preflight lifecycle.",
    "project_id": "Transform projects require their project-store fixture.",
    "recipe_id": "Transform-library recipes are covered by the D154 cross-tenant sweep.",
    "key": "Template keys address the global recipe catalog, not an owned resource.",
    "proposal_id": "Repair proposals require generated repair evidence.",
    "provider": "AI provider routes require provider-secret configuration.",
    "review_id": "CDC mapping reviews require a live capture review fixture.",
    "signal_id": "Standalone CDC signals require a live capture signal fixture.",
    "sso_type": "SSO routes are protocol callbacks, not resource-ID reads.",
    "tenant_id": "Tenant settings require the platform tenant configuration fixture.",
    "user_id": "SCIM user routes require SCIM bearer-token fixtures.",
    "channel_id": "Notification channel tests require channel-specific delivery fixtures.",
    "filename": "Download filenames address generated artifacts, not owned resources.",
}
ALLOWLISTED_WITH_REASON = {
    ("DELETE", "/api/v1/auth/sessions/{jti}"): (
        "Account-scoped: revokes only the caller's own session; another user's jti is a 404."
    ),
    ("DELETE", "/auth/sessions/{jti}"): (
        "Same router mounted at the legacy /auth prefix; account-scoped."
    ),
    ("GET", "/api/v1/catalog/connectors/{connector_id}"): (
        "Global connector catalog metadata; this is not a saved workspace connector."
    ),
    ("PATCH", "/api/v1/team/workspaces/{workspace_id}"): (
        "Platform-admin-only workspace lifecycle route; the A-only editor cannot mutate it."
    ),
    ("DELETE", "/api/v1/team/workspaces/{workspace_id}"): (
        "Platform-admin-only workspace lifecycle route; the A-only editor cannot mutate it."
    ),
}
ROUTE_SKIPPED_WITH_REASON = {
    ("POST", "/api/v1/connectors/jobs/{job_id}/resume"): (
        "Resume requires a serialized transfer request query parameter and can start work."
    ),
}
PREFLIGHT_RUN_XFAIL = (
    "GET",
    "/api/v1/preflight/runs/{run_id}",
)


def _resource_route_inventory():
    rows = []
    ignored = {"filename", "provider", "sso_type"}
    for included in app.routes:
        router = getattr(included, "original_router", None)
        context = getattr(included, "include_context", None)
        if router is None:
            continue
        prefix = str(getattr(context, "prefix", "") or "")
        for route in router.routes:
            path = str(getattr(route, "path", "") or "")
            params = re.findall(r"{([^}]+)}", path)
            if not params or all(param in ignored for param in params):
                continue
            full_path = f"{prefix}{path}"
            for method in sorted(route.methods or []):
                rows.append((method, full_path, tuple(params)))
    return sorted(set(rows))


@pytest.fixture
def resource_matrix(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root-matrix@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "matrix-test-secret-not-production")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource-acls.jsonl")
    contract_store.reset_contract_store()
    reset_auth_rate_limits()
    yield tmp_path
    contract_store.reset_contract_store()


def _admin() -> TestClient:
    client = TestClient(app)
    response = client.post(
        "/api/v1/auth/login",
        json={
            "email": "root-matrix@example.com",
            "password": "Bootstrap-Admin-2026",
        },
    )
    assert response.status_code == 200, response.text
    client.headers["Authorization"] = f"Bearer {response.json()['token']}"
    return client


def _workspace(admin: TestClient, name: str) -> str:
    response = admin.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _member(admin: TestClient, workspace_id: str) -> TestClient:
    created = admin.post(
        "/api/v1/team/users",
        json={
            "email": "matrix-editor@example.com",
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": "editor",
        },
    )
    assert created.status_code == 200, created.text
    client = TestClient(app)
    login = client.post(
        "/api/v1/auth/login",
        json={
            "email": "matrix-editor@example.com",
            "password": created.json()["temporary_password"],
        },
    )
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    client.headers["X-Workspace-Id"] = workspace_id
    return client


def _request(member, method: str, path: str, *, body=None):
    correlation_id = f"matrix-{uuid4().hex}"
    headers = {
        "X-Workspace-Id": member.headers["X-Workspace-Id"],
        "X-Correlation-ID": correlation_id,
    }
    response = member.request(
        method,
        path,
        headers=headers,
        json=body if body is not None else None,
    )
    print(
        f"{method} {path}: status={response.status_code} "
        f"body={json.dumps(response.json(), sort_keys=True) if response.content else ''}"
    )
    return response


class _MongoFixture:
    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id
        self.deleted_connector = False

    def get_connector(self, connector_id: str):
        if connector_id != "B_MONGO_CONNECTOR_ID":
            return None
        return {
            "_id": connector_id,
            "id": connector_id,
            "name": "B_MONGO_CONNECTOR_SENTINEL",
            "host": "B_MONGO_HOST_SENTINEL",
            "workspace_id": self.workspace_id,
        }

    def delete_connector(self, connector_id: str):
        self.deleted_connector = connector_id == "B_MONGO_CONNECTOR_ID"
        return self.deleted_connector

    def get_job(self, job_id: str):
        if job_id != "B_JOB_ID":
            return None
        return {
            "_id": job_id,
            "id": job_id,
            "name": "B_JOB_SENTINEL",
            "workspace_id": self.workspace_id,
            "status": "failed",
            "transfer_request": {},
        }


def test_dynamic_cross_workspace_resource_id_routes(resource_matrix, monkeypatch):
    admin = _admin()
    workspace_a = _workspace(admin, "Matrix workspace A")
    workspace_b = _workspace(admin, "Matrix workspace B")
    editor = _member(admin, workspace_a)
    connector = connector_store.create_connector(
        {
            "name": "B_SAVED_CONNECTOR_SENTINEL",
            "type": "postgresql",
            "host": "B_SAVED_HOST_SENTINEL",
            "port": 5432,
            "database": "B_SAVED_DATABASE_SENTINEL",
            "workspace_id": workspace_b,
        }
    )
    schedule = schedule_store.create_schedule(
        {
            "name": "B_SCHEDULE_SENTINEL",
            "source_connector_id": connector.id,
            "source_table": "B_SCHEDULE_SENTINEL",
            "dest_connector_id": connector.id,
            "dest_table": "B_SCHEDULE_SENTINEL",
            "interval": "daily",
            "mappings": [{"source": "id", "target": "id"}],
            "workspace_id": workspace_b,
        }
    )
    contract = DataContract(
        name="B_CONTRACT_SENTINEL",
        metadata={"workspace_id": workspace_b},
    )
    contract_store.get_contract_store().save_contract(contract)
    grant = resource_acl.upsert_grant(
        tenant_id=workspace_b,
        resource_type="schedule",
        resource_id=schedule.id,
        principal="nobody@example.com",
        role="viewer",
        created_by="matrix",
    )
    from services import benchmark_runs

    bench_run_id = "b" * 32
    monkeypatch.setitem(
        benchmark_runs._RUNS,
        bench_run_id,
        {
            "id": bench_run_id,
            "rows": 1000,
            "status": "completed",
            "report": {"target": "B_BENCH_SENTINEL"},
            "workspace_id": workspace_b,
        },
    )
    mongo = _MongoFixture(workspace_b)
    connectors_module = importlib.import_module("src.routers.connectors_router")
    monkeypatch.setattr(connectors_module, "get_mongodb_service", lambda: mongo)
    from services import mongodb_service

    monkeypatch.setattr(mongodb_service, "get_mongodb_service", lambda: mongo)
    rows = _resource_route_inventory()
    assert rows, "app.routes yielded no resource-ID routes"
    covered = 0
    skipped = 0
    allowlisted = 0
    for method, template, params in rows:
        key = (method, template)
        if key in ALLOWLISTED_WITH_REASON:
            allowlisted += 1
            continue
        if key in ROUTE_SKIPPED_WITH_REASON:
            skipped += 1
            continue
        if key == PREFLIGHT_RUN_XFAIL:
            skipped += 1
            continue
        if "{schedule_id}" in template and template.startswith("/api/v1/schedules/"):
            values = {"schedule_id": schedule.id, "approval_id": "B_APPROVAL_SENTINEL"}
        elif template.startswith("/api/v1/connectors/saved/{connector_id}"):
            values = {"connector_id": connector.id}
        elif template in {
            "/api/v1/connectors/{connector_id}",
            "/api/v1/connectors/{connector_id}",
        } and "connector_id" in params:
            values = {"connector_id": "B_MONGO_CONNECTOR_ID"}
        elif template.startswith("/api/v1/connectors/jobs/{job_id}"):
            values = {"job_id": "B_JOB_ID"}
        elif template.startswith("/api/v1/transfer/{job_id}/"):
            values = {"job_id": "B_JOB_ID", "signal_id": "B_SIGNAL_SENTINEL"}
        elif "{contract_id}" in template and template.startswith("/api/v1/contracts/"):
            values = {"contract_id": contract.id}
        elif "{grant_id}" in template:
            values = {"grant_id": grant.id}
        elif template == "/api/v1/workspace/benchmark/runs/{run_id}":
            values = {"run_id": bench_run_id}
        elif template == "/api/v1/workspace/workspaces/{workspace_id}":
            values = {"workspace_id": workspace_b}
        elif template == "/api/v1/team/workspaces/{workspace_id}/break-glass/restore-admin":
            values = {"workspace_id": workspace_b}
        elif template.startswith("/api/v1/team/workspaces/{workspace_id}/members"):
            values = {
                "workspace_id": workspace_b,
                "email": "B_MEMBER_SENTINEL@example.com",
            }
        else:
            unseeded = next(
                (param for param in params if param in SKIPPED_WITH_REASON),
                None,
            )
            assert unseeded is not None, f"Unclassified resource route: {key}"
            skipped += 1
            continue

        path = template
        for name in params:
            value = values.get(name, f"B_{name.upper()}_SENTINEL")
            path = path.replace(f"{{{name}}}", str(value))
        body = None
        if method == "PATCH":
            body = {"name": "matrix update", "role": "viewer"}
        elif method == "PUT":
            body = {
                "name": "matrix update",
                "type": "postgresql",
                "host": "B_HOST_SENTINEL",
                "port": 5432,
            }
        elif method == "POST":
            body = {"password": "matrix-rotated-secret", "reason": "matrix probe"}
            if "/contracts/" in template:
                body = {"strict": True}
            if template.endswith("/transform-recipe/upgrade"):
                body = {"target_version": 2}
            if template.endswith("/authorization"):
                body = {"reason": "matrix probe", "scopes": ["run"]}
            if template.endswith("/break-glass/restore-admin"):
                body = {"email": "B_MEMBER_SENTINEL@example.com", "reason": "matrix probe"}
            if template.endswith("/members"):
                body = {"email": "B_MEMBER_SENTINEL@example.com", "role": "viewer"}
        response = _request(editor, method, path, body=body)
        assert response.status_code in {403, 404}, (
            f"unexpected response for {method} {template}: {response.status_code} {response.text}"
        )
        for sentinel in (
            "B_SAVED_CONNECTOR_SENTINEL",
            "B_SAVED_HOST_SENTINEL",
            "B_SAVED_DATABASE_SENTINEL",
            "B_SCHEDULE_SENTINEL",
            "B_CONTRACT_SENTINEL",
            "B_MONGO_CONNECTOR_SENTINEL",
            "B_MONGO_HOST_SENTINEL",
            "B_JOB_SENTINEL",
            "B_MEMBER_SENTINEL",
            "B_BENCH_SENTINEL",
        ):
            assert sentinel not in response.text, f"{method} {template} leaked {sentinel}"
        covered += 1

    assert covered > 0
    print(
        f"resource routes: {len(rows)} discovered; {covered} exercised; "
        f"{allowlisted} allowlisted; {skipped} skipped/xfail"
    )
    assert not mongo.deleted_connector

    body_schedule = _request(
        editor,
        "POST",
        "/api/v1/schedules/gitops/plan",
        body={
            "kind": "PipelineSchedule",
            "spec": {
                "id": schedule.id,
                "name": "caller-supplied-name",
                "source_connector_id": connector.id,
                "source_table": "source",
                "dest_connector_id": connector.id,
                "dest_table": "dest",
                "interval": "daily",
            },
        },
    )
    body_contract = _request(
        editor,
        "POST",
        "/api/v1/schedules/gitops/plan",
        body={"kind": "DataContract", "spec": {"id": contract.id}},
    )
    body_fidelity = _request(
        editor,
        "POST",
        "/api/v1/fidelity/check",
        body={
            "source": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": connector.id,
            },
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": connector.id,
            },
        },
    )
    assert [body_schedule.status_code, body_contract.status_code, body_fidelity.status_code] == [
        403,
        403,
        403,
    ]


def test_preflight_run_id_is_not_visible_cross_workspace(resource_matrix, monkeypatch):
    admin = _admin()
    workspace_a = _workspace(admin, "Preflight workspace A")
    workspace_b = _workspace(admin, "Preflight workspace B")
    editor = _member(admin, workspace_a)
    from services import preflight_run_store

    preflight_run_store.STORE_PATH = resource_matrix / "preflight-runs.jsonl"
    run = preflight_run_store.save_preflight_run(
        {"passed": True, "blockers": []},
        source_label="B_PREFLIGHT_SENTINEL",
        dest_label="destination",
        route={"workspace_id": workspace_b},
    )
    response = _request(
        editor,
        "GET",
        f"/api/v1/preflight/runs/{run['run_id']}",
    )
    assert response.status_code in {403, 404}
    assert "B_PREFLIGHT_SENTINEL" not in response.text
