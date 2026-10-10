"""Real-app coverage for sensitive-route and authorization-denial audits."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def audit_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-audit-coverage-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))

    from services import audit_log, integrations_store, team_store
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")

    from src.main import app

    actor = "audit-actor@example.test"
    key = integrations_store.create_api_key("Audit test", actor, role="admin")
    workspace = team_store.create_workspace(name="Audit coverage", created_by=actor)
    transfer_plan_service = __import__(
        "services.transfer_plan_service", fromlist=["approve_plan"]
    )
    monkeypatch.setattr(
        transfer_plan_service,
        "approve_plan",
        lambda *_args, **_kwargs: SimpleNamespace(to_dict=lambda: {"id": "plan-coverage"}),
    )

    with TestClient(app) as client:
        yield {
            "client": client,
            "key": key["key"],
            "key_id": key["id"],
            "actor": actor,
            "workspace_id": workspace.id,
            "audit_log": audit_log,
            "integrations_store": integrations_store,
        }


def _headers(context, *, correlation_id="audit-correlation-1"):
    return {
        "Authorization": f"Bearer {context['key']}",
        "X-Workspace-Id": context["workspace_id"],
        "X-Correlation-ID": correlation_id,
    }


def _events(context):
    path = context["audit_log"].STORE_PATH
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_sensitive_routes_record_actor_status_and_correlation_without_secrets(
    audit_app,
):
    client = audit_app["client"]
    headers = _headers(audit_app)
    created = client.post(
        "/api/v1/connectors/",
        headers=headers,
        json={
            "name": "audit connector",
            "type": "postgresql",
            "host": "db.example.test",
            "port": 5432,
            "database": "warehouse",
            "username": "audit-user",
            "password": "connector-password-do-not-audit",
        },
    )
    assert created.status_code == 200, created.text

    settings = client.get("/api/v1/workspace/sso", headers=headers)
    assert settings.status_code == 200, settings.text

    approval = client.post(
        "/api/v1/transfer/plans/plan-coverage/approve",
        headers=headers,
    )
    assert approval.status_code == 200, approval.text

    rows = _events(audit_app)
    expected = {
        "connector.create": created.status_code,
        "secret.sso.read": settings.status_code,
        "transfer_plan.approve.request": approval.status_code,
    }
    for action, status in expected.items():
        matching = [event for event in rows if event.get("action") == action]
        assert len(matching) == 1, (action, matching)
        event = matching[0]
        assert event["actor"] == audit_app["actor"]
        assert event["workspace_id"] == audit_app["workspace_id"]
        assert event["correlation_id"] == "audit-correlation-1"
        assert event["details"]["status"] == status
        assert event["details"]["api_key_id"] == audit_app["key_id"]

    assert "connector-password-do-not-audit" not in audit_app["audit_log"].STORE_PATH.read_text(
        encoding="utf-8"
    )


def test_failed_sensitive_handler_is_recorded_with_its_status(audit_app):
    response = audit_app["client"].patch(
        "/api/v1/workspace/sso/not-a-provider",
        headers=_headers(audit_app),
        json={"enabled": True},
    )
    assert response.status_code >= 400
    event = next(
        row for row in _events(audit_app) if row.get("action") == "secret.sso.update"
    )
    assert event["details"]["status"] == response.status_code


def test_audit_store_failure_does_not_break_sensitive_request(audit_app, monkeypatch):
    from services import audit_coverage

    def fail_append(**_kwargs):
        raise OSError("audit store unavailable")

    monkeypatch.setattr(audit_app["audit_log"], "append_audit_event", fail_append)
    response = audit_app["client"].get(
        "/api/v1/workspace/sso", headers=_headers(audit_app)
    )
    assert response.status_code == 200
    assert audit_coverage.audit_access_failure_count() == 1
    tip = audit_app["client"].get(
        "/api/v1/audit/tip", headers=_headers(audit_app)
    )
    assert tip.status_code == 200
    assert tip.json()["audit_access_failures"] == 1


def test_every_audit_rule_matches_a_real_included_app_route(audit_app):
    from services.audit_coverage import SENSITIVE_ROUTE_RULES
    from src.main import app

    def routes(items, prefix=""):
        for route in items:
            included = getattr(route, "original_router", None)
            if included is not None:
                yield from routes(
                    included.routes,
                    prefix + route.include_context.prefix.rstrip("/"),
                )
                continue
            path = prefix + getattr(route, "path", "")
            yield path, set(getattr(route, "methods", None) or ())

    registered = list(routes(app.routes))
    for method, path_prefix, _action in SENSITIVE_ROUTE_RULES:
        assert any(
            (method == "*" or method in methods) and path.startswith(path_prefix)
            for path, methods in registered
        ), f"stale audit rule: {method} {path_prefix}"


def test_rbac_denial_audit_deduplicates_and_reports_suppressed_count(
    audit_app, monkeypatch
):
    from services import audit_coverage, integrations_store

    viewer = integrations_store.create_api_key(
        "Viewer", "viewer@example.test", role="viewer"
    )
    now = [100.0]
    monkeypatch.setattr(audit_coverage.time, "monotonic", lambda: now[0])
    headers = {
        "Authorization": f"Bearer {viewer['key']}",
        "X-Workspace-Id": audit_app["workspace_id"],
    }

    for second in range(5):
        now[0] = 100.0 + second
        response = audit_app["client"].post("/api/v1/transfer/run", headers=headers)
        assert response.status_code == 403

    denied = [row for row in _events(audit_app) if row.get("action") == "authz.denied"]
    assert len(denied) == 1

    now[0] = 161.0
    response = audit_app["client"].post("/api/v1/transfer/run", headers=headers)
    assert response.status_code == 403
    denied = [row for row in _events(audit_app) if row.get("action") == "authz.denied"]
    assert len(denied) == 2
    assert denied[-1]["details"]["suppressed_since_last"] == 4
