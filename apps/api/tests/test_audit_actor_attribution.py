"""Audit events identify key principals without changing authorization identity."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def attribution_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-audit-actor-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv(
        "DATAFLOW_AUTH_USERS",
        json.dumps(
            [
                {"email": "admin@x", "password": "test-password", "role": "admin"},
            ]
        ),
    )
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))

    from services import (
        audit_log,
        auth_sessions,
        connector_store,
        integrations_store,
        scim_store,
        team_store,
        user_store,
    )
    from src.services import auth_service
    from services.rbac import role_permissions

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_store_path", lambda: tmp_path / "connectors.json")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    monkeypatch.setattr(scim_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "STORE_PATH", tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)

    from src.main import app

    admin_token, _expires = auth_service.create_token("admin@x")
    admin_headers = {"Authorization": f"Bearer {admin_token}"}
    workspace = team_store.create_workspace(name="Audit actor attribution", created_by="admin@x")

    with TestClient(app) as client:
        service_account_response = client.post(
            "/api/v1/iam/service-accounts",
            headers=admin_headers,
            json={
                "name": "Audit actor service account",
                "role": "admin",
                "scopes": sorted(role_permissions("admin")),
            },
        )
        assert service_account_response.status_code == 200, service_account_response.text
        service_account = service_account_response.json()

        scim_response = client.post(
            "/api/v1/iam/scim-token",
            headers=admin_headers,
            json={"name": "Audit actor SCIM token"},
        )
        assert scim_response.status_code == 200, scim_response.text
        scim_token = scim_response.json()

        api_key_response = client.post(
            "/api/v1/workspace/api-keys",
            headers=admin_headers,
            json={"name": "Audit actor workspace key", "role": "admin"},
        )
        assert api_key_response.status_code == 200, api_key_response.text
        api_key = api_key_response.json()

        yield {
            "client": client,
            "admin_token": admin_token,
            "workspace_id": workspace.id,
            "service_account": service_account,
            "scim_token": scim_token,
            "api_key": api_key,
            "audit_log": audit_log,
            "app": app,
        }


def _events(context) -> list[dict]:
    path = context["audit_log"].STORE_PATH
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _connector_payload(name: str) -> dict[str, object]:
    return {
        "name": name,
        "type": "postgresql",
        "host": "db.example.test",
        "port": 5432,
        "database": "warehouse",
    }


def _request_events(context, send):
    before = len(_events(context))
    response = send()
    return response, _events(context)[before:]


def _key_headers(context, token: str, correlation_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Correlation-ID": correlation_id,
        "X-Workspace-Id": context["workspace_id"],
    }


def _assert_key_events(events, *, kind: str, key_id: str, status: int) -> None:
    assert events
    assert len(events) == 1
    for event in events:
        assert event["actor"] == f"{kind}:{key_id}"
        assert event["actor_type"] == kind
        assert event["actor_id"] == key_id
        assert event["auth_method"] == "api_key"
        assert event["created_by"] == "admin@x"
    assert not any(event["actor"] == "admin@x" for event in events)
    assert all(event["details"].get("status", status) == status for event in events)


def test_service_account_write_audits_the_key_principal(attribution_app):
    service_account = attribution_app["service_account"]
    response, events = _request_events(
        attribution_app,
        lambda: attribution_app["client"].post(
            "/api/v1/connectors/",
            headers=_key_headers(
                attribution_app, service_account["key"], "sa-write-actor-1"
            ),
            json=_connector_payload("service account audit connector"),
        ),
    )

    assert response.status_code == 200, response.text
    assert {event["action"] for event in events} == {"connector.create"}
    _assert_key_events(
        events,
        kind="service_account",
        key_id=service_account["id"],
        status=response.status_code,
    )


def test_scim_write_audits_the_scim_token_principal(attribution_app):
    scim_token = attribution_app["scim_token"]
    response, events = _request_events(
        attribution_app,
        lambda: attribution_app["client"].post(
            "/api/v1/scim/v2/Users",
            headers={
                **_key_headers(attribution_app, scim_token["key"], "scim-write-actor-1"),
                "Content-Type": "application/scim+json",
            },
            json={"userName": "scim-audit-user@example.test"},
        ),
    )

    assert response.status_code == 201, response.text
    assert {event["action"] for event in events} == {"scim.user.create"}
    _assert_key_events(
        events,
        kind="scim",
        key_id=scim_token["id"],
        status=response.status_code,
    )


def test_workspace_api_key_write_audits_the_key_principal(attribution_app):
    api_key = attribution_app["api_key"]
    response, events = _request_events(
        attribution_app,
        lambda: attribution_app["client"].post(
            "/api/v1/connectors/",
            headers=_key_headers(attribution_app, api_key["key"], "api-key-write-actor-1"),
            json=_connector_payload("workspace key audit connector"),
        ),
    )

    assert response.status_code == 200, response.text
    assert {event["action"] for event in events} == {"connector.create"}
    _assert_key_events(
        events,
        kind="api_key",
        key_id=api_key["id"],
        status=response.status_code,
    )


def test_user_session_write_keeps_email_actor_without_key_creator(attribution_app):
    response, events = _request_events(
        attribution_app,
        lambda: attribution_app["client"].post(
            "/api/v1/connectors/",
            headers=_key_headers(
                attribution_app, attribution_app["admin_token"], "session-write-actor-1"
            ),
            json=_connector_payload("session audit connector"),
        ),
    )

    assert response.status_code == 200, response.text
    assert {event["action"] for event in events} == {"connector.create"}
    assert events
    for event in events:
        assert event["actor"] == "admin@x"
        assert event["actor_type"] == "user"
        assert event["actor_id"] == "admin@x"
        assert event["auth_method"] == "session"
        assert "created_by" not in event


def test_explicit_system_actor_is_preserved_inside_key_request(attribution_app, monkeypatch):
    from services import connector_store

    audit_log = attribution_app["audit_log"]
    original_create = connector_store.create_connector
    system_events = []

    def create_with_system_event(*args, **kwargs):
        created = original_create(*args, **kwargs)
        system_events.append(
            audit_log.append_audit_event(
                action="test.explicit_system",
                resource="test-system-resource",
                actor="system",
            )
        )
        return created

    monkeypatch.setattr(connector_store, "create_connector", create_with_system_event)
    key = attribution_app["service_account"]
    response = attribution_app["client"].post(
        "/api/v1/connectors/",
        headers=_key_headers(attribution_app, key["key"], "system-actor-write-1"),
        json=_connector_payload("system actor audit connector"),
    )

    assert response.status_code == 200, response.text
    event = system_events[0]
    assert event["actor"] == "system"
    assert event["actor_type"] == "service_account"
    assert event["actor_id"] == key["id"]
    assert event["auth_method"] == "api_key"
    assert event["created_by"] == "admin@x"


def test_old_and_new_event_shapes_verify_together(attribution_app):
    audit_log = attribution_app["audit_log"]
    old_event = audit_log.append_audit_event(
        action="legacy.audit.event",
        resource="legacy-resource",
        actor="admin@x",
    )
    assert "actor_type" not in old_event

    key = attribution_app["service_account"]
    response = attribution_app["client"].post(
        "/api/v1/connectors/",
        headers=_key_headers(attribution_app, key["key"], "mixed-hash-chain-write-1"),
        json=_connector_payload("mixed chain connector"),
    )
    assert response.status_code == 200, response.text

    from services.evidence_chain import verify_chain

    report = verify_chain(limit=100)
    assert report["verified"] is True, report["findings"]
    assert report["checked"] >= 3


def test_audit_exports_include_principal_fields(attribution_app):
    key = attribution_app["service_account"]
    response = attribution_app["client"].post(
        "/api/v1/connectors/",
        headers=_key_headers(attribution_app, key["key"], "export-principal-write-1"),
        json=_connector_payload("export principal connector"),
    )
    assert response.status_code == 200, response.text

    headers = {
        "Authorization": f"Bearer {attribution_app['admin_token']}",
        "X-Workspace-Id": attribution_app["workspace_id"],
    }
    csv_response = attribution_app["client"].get(
        "/api/v1/audit/export?format=csv", headers=headers
    )
    assert csv_response.status_code == 200, csv_response.text
    csv_header = next(line for line in csv_response.text.splitlines() if not line.startswith("#"))
    assert {"actor_type", "actor_id"} <= set(csv_header.split(","))

    cef_response = attribution_app["client"].get(
        "/api/v1/audit/export?format=cef", headers=headers
    )
    assert cef_response.status_code == 200, cef_response.text
    assert "cs4Label=actor_type" in cef_response.text
    assert "cs5Label=actor_id" in cef_response.text

    ndjson_response = attribution_app["client"].get(
        "/api/v1/audit/export?format=ndjson", headers=headers
    )
    assert ndjson_response.status_code == 200, ndjson_response.text
    exported_events = [
        json.loads(line) for line in ndjson_response.text.splitlines() if line.strip()
    ]
    assert any(
        event.get("actor_type") == "service_account"
        and event.get("actor_id") == key["id"]
        for event in exported_events
    )


def test_auth_context_is_reset_before_a_direct_audit_append(attribution_app):
    from starlette.requests import Request
    from starlette.responses import Response
    from src.middleware.auth_middleware import AuthMiddleware

    from services import audit_coverage

    current_principal = getattr(audit_coverage, "current_request_principal", None)
    assert callable(current_principal)
    key = attribution_app["service_account"]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/api/v1/connectors/",
        "raw_path": b"/api/v1/connectors/",
        "query_string": b"",
        "headers": [
            (b"authorization", f"Bearer {key['key']}".encode()),
            (b"x-workspace-id", attribution_app["workspace_id"].encode()),
        ],
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "root_path": "",
    }
    request = Request(scope)
    in_request = []

    async def run_dispatch():
        async def call_next(inner_request):
            in_request.append(current_principal())
            event = attribution_app["audit_log"].append_audit_event(
                action="test.context.inside",
                resource="inside-request",
                actor="admin@x",
            )
            in_request.append(event)
            return Response(status_code=200)

        middleware = AuthMiddleware(attribution_app["app"])
        response = await middleware.dispatch(request, call_next)
        assert response.status_code == 200
        return attribution_app["audit_log"].append_audit_event(
            action="test.context.after",
            resource="after-request",
            actor="admin@x",
        )

    after_event = asyncio.run(run_dispatch())
    assert in_request[0]["actor_type"] == "service_account"
    assert in_request[1]["actor"] == f"service_account:{key['id']}"
    assert after_event["actor"] == "admin@x"
    assert "actor_type" not in after_event
    assert current_principal() is None
