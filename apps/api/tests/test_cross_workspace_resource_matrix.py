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
    from services import transfer_journal

    monkeypatch.setenv("DATAFLOW_JOURNAL_DIR", str(resource_matrix / "journals"))
    journal_id = "bjournal.1"
    journal_dir = (
        transfer_journal.journal_root()
        / transfer_journal._workspace_dir(workspace_b)
        / journal_id
    )
    journal_dir.mkdir(parents=True)
    (journal_dir / transfer_journal.MANIFEST_NAME).write_text(
        json.dumps(
            {
                "journal_id": journal_id,
                "workspace_id": workspace_b,
                "job_id": "B_JOURNAL_SENTINEL",
                "streams": [{"name": "B_JOURNAL_SENTINEL"}],
            }
        ),
        encoding="utf-8",
    )
    from services import file_parser

    upload_id = "bupload1"
    upload_path = resource_matrix / "b_upload.csv"
    upload_path.write_text("id,name\n1,B_UPLOAD_SENTINEL\n", encoding="utf-8")
    monkeypatch.setitem(
        file_parser._file_registry,
        upload_id,
        {
            "file_id": upload_id,
            "filename": "b_upload.csv",
            "path": str(upload_path),
            "format": "csv",
            "workspace_id": workspace_b,
        },
    )
    # Reversibility Proof: a declared exit system of B and one finished rehearsal of it.
    from services import exit_inventory, exit_store

    monkeypatch.setattr(exit_store, "STORE_PATH", resource_matrix / "exit_inventory.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    exit_system = exit_inventory.declare(
        {
            "name": "B_EXIT_SYSTEM_SENTINEL",
            "source": {"connector_id": connector.id},
            "tables": [{"name": "B_EXIT_TABLE_SENTINEL", "key_columns": ["id"]}],
            "exit_window_hours": 24,
        },
        workspace_id=workspace_b,
        actor="matrix",
    )
    exit_rehearsal = exit_store.insert(
        exit_store.REHEARSALS,
        {
            "id": "exr_bmatrix00001",
            "system_id": exit_system["id"],
            "system_name": "B_EXIT_SYSTEM_SENTINEL",
            "workspace_id": workspace_b,
            "n": 1,
            "trigger": "manual",
            "actor": "matrix",
            "started_at": "2026-01-01T00:00:00Z",
            "finished_at": "2026-01-01T00:05:00Z",
            "verdict": "proven",
            "verdict_reason": "B_EXIT_REHEARSAL_SENTINEL",
            "source": dict(exit_system["source"]),
            "extract_format": "jsonl",
            "window_seconds": 86400,
            "definition_sha256": exit_inventory.definition_sha256(exit_system),
            "pii_acknowledgement": None,
            "required_roles": list(exit_system["required_roles"]),
            "tables": [
                {
                    "name": "B_EXIT_TABLE_SENTINEL",
                    "schema": None,
                    "key_columns": ["id"],
                    "job_id": "B_JOB_ID",
                    "status": "proven",
                    "reason": "B_EXIT_REHEARSAL_SENTINEL",
                }
            ],
            "approvals": [],
            "signed": None,
            "_verify": None,
        },
    )
    exit_owner_headers = {"X-Workspace-Id": workspace_b}
    # Not a vacuous 404: the same ids answer B's own caller, sentinels and all.
    for owner_path, sentinel in (
        (f"/api/v1/exit/systems/{exit_system['id']}", "B_EXIT_SYSTEM_SENTINEL"),
        (f"/api/v1/exit/systems/{exit_system['id']}/rehearsals", exit_rehearsal["id"]),
        (f"/api/v1/exit/rehearsals/{exit_rehearsal['id']}", "B_EXIT_REHEARSAL_SENTINEL"),
        (f"/api/v1/exit/rehearsals/{exit_rehearsal['id']}/evidence?format=markdown", "B_EXIT_REHEARSAL_SENTINEL"),
    ):
        owned = admin.get(owner_path, headers=exit_owner_headers)
        assert owned.status_code == 200, f"{owner_path}: {owned.status_code} {owned.text}"
        assert sentinel in owned.text, f"{owner_path} does not return B's resource to B's own caller"
    # Modules that bind ``get_mongodb_service`` by name at import must be imported before it is patched:
    # one first imported inside the patch (the exit routes reach the transfer scheduler) would keep the
    # fixture after the test and every later job in the process would fail on it.
    importlib.import_module("src.transfer.background")
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
    exit_probed: set[tuple[str, str]] = set()
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
        elif template.startswith(("/api/v1/connectors/jobs/{job_id}", "/api/v1/jobs/{job_id}")):
            values = {"job_id": "B_JOB_ID"}
        elif template.startswith("/api/v1/transfer/{job_id}/"):
            values = {"job_id": "B_JOB_ID", "signal_id": "B_SIGNAL_SENTINEL"}
        elif "{contract_id}" in template and template.startswith("/api/v1/contracts/"):
            values = {"contract_id": contract.id}
        elif "{grant_id}" in template:
            values = {"grant_id": grant.id}
        elif template.startswith("/api/v1/connectors/uploads/{file_id}"):
            values = {"file_id": upload_id}
        elif template.startswith("/api/v1/journals/{journal_id}"):
            values = {"journal_id": journal_id}
        elif template == "/api/v1/workspace/benchmark/runs/{run_id}":
            values = {"run_id": bench_run_id}
        elif template == "/api/v1/workspace/workspaces/{workspace_id}":
            values = {"workspace_id": workspace_b}
        elif template == "/api/v1/team/workspaces/{workspace_id}/break-glass/restore-admin":
            values = {"workspace_id": workspace_b}
        elif template == "/api/v1/team/sandbox-workspaces/{workspace_id}":
            # Platform-admin-only sandbox teardown (C18), probed against B like
            # break-glass: the A-only editor must neither delete B nor learn it.
            values = {"workspace_id": workspace_b}
        elif template.startswith("/api/v1/exit/systems/{system_id}"):
            values = {"system_id": exit_system["id"]}
        elif template.startswith("/api/v1/exit/rehearsals/{rehearsal_id}"):
            values = {"rehearsal_id": exit_rehearsal["id"]}
        elif template.startswith("/api/v1/team/workspaces/{workspace_id}/invites"):
            values = {"workspace_id": workspace_b, "invite_id": "B_INVITE_SENTINEL"}
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
            if template.endswith("/invites"):
                body = {"email": "B_MEMBER_SENTINEL@example.com", "role": "viewer"}
            if template.endswith("/members"):
                body = {"email": "B_MEMBER_SENTINEL@example.com", "role": "viewer"}
            if template.endswith("/pii-acknowledgement"):
                body = {"reason": "matrix probe of another workspace's system"}
            if template.endswith("/signoff"):
                body = {"role": "engineering", "note": "matrix probe"}
        if template.startswith("/api/v1/exit/systems/{system_id}") and method == "PATCH":
            # The exit bodies refuse unknown fields (422) before the workspace check would run.
            body = {"name": "matrix update"}
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
            "B_JOURNAL_SENTINEL",
            "B_EXIT_SYSTEM_SENTINEL",
            "B_EXIT_TABLE_SENTINEL",
            "B_EXIT_REHEARSAL_SENTINEL",
        ):
            assert sentinel not in response.text, f"{method} {template} leaked {sentinel}"
        if template.startswith("/api/v1/exit/"):
            exit_probed.add((method, template))
        covered += 1

    assert journal_dir.is_dir(), "a cross-workspace journal DELETE removed workspace B's journal"
    # Every exit route with an id was really called from workspace A, and none of them changed B.
    assert exit_probed == {
        ("GET", "/api/v1/exit/systems/{system_id}"),
        ("PATCH", "/api/v1/exit/systems/{system_id}"),
        ("DELETE", "/api/v1/exit/systems/{system_id}"),
        ("POST", "/api/v1/exit/systems/{system_id}/pii-acknowledgement"),
        ("DELETE", "/api/v1/exit/systems/{system_id}/pii-acknowledgement"),
        ("POST", "/api/v1/exit/systems/{system_id}/rehearsals"),
        ("GET", "/api/v1/exit/systems/{system_id}/rehearsals"),
        ("GET", "/api/v1/exit/rehearsals/{rehearsal_id}"),
        ("POST", "/api/v1/exit/rehearsals/{rehearsal_id}/signoff"),
        ("GET", "/api/v1/exit/rehearsals/{rehearsal_id}/evidence"),
    }, sorted(exit_probed)
    kept = exit_store.get(exit_store.SYSTEMS, exit_system["id"])
    assert kept is not None, "an A-only editor deleted workspace B's exit system"
    assert kept["name"] == "B_EXIT_SYSTEM_SENTINEL" and kept.get("pii_acknowledgement") is None
    assert kept["version"] == exit_system["version"], "a cross-workspace write changed B's exit system"
    after_rehearsal = exit_store.get(exit_store.REHEARSALS, exit_rehearsal["id"])
    assert after_rehearsal is not None and after_rehearsal["approvals"] == []
    assert exit_inventory.rehearsals_of(exit_system["id"]) == [after_rehearsal], "a rehearsal of B was started from A"
    # The id-less exit reads are scoped too: A's board and list never show B's system.
    for listing in ("/api/v1/exit/readiness", "/api/v1/exit/systems"):
        listed = _request(editor, "GET", listing)
        assert listed.status_code == 200, listed.text
        assert "B_EXIT_SYSTEM_SENTINEL" not in listed.text and exit_system["id"] not in listed.text
    # Verify takes a pack, not an id: naming B's rehearsal in a forged pack discloses nothing of B.
    forged = _request(
        editor,
        "POST",
        "/api/v1/exit/evidence/verify",
        body={
            "kind": "exit_evidence",
            "system_id": exit_system["id"],
            "rehearsal_id": exit_rehearsal["id"],
            "content_sha256": "0" * 64,
            "signature": {"alg": "HMAC-SHA256", "key_id": "k", "value": "0" * 64},
        },
    )
    assert forged.status_code == 200, forged.text
    assert forged.json()["ok"] is False and forged.json()["signature_ok"] is False
    for sentinel in ("B_EXIT_SYSTEM_SENTINEL", "B_EXIT_TABLE_SENTINEL", "B_EXIT_REHEARSAL_SENTINEL"):
        assert sentinel not in forged.text
    assert team_store.get_workspace(workspace_b) is not None, "an A-only editor deleted workspace B"
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
