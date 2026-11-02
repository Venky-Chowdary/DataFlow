"""Request-level audit de-duplication contracts for the real API app."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def one_event_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "one-event-audit-test-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_AUTH_USERS", json.dumps([
        {"email": "audit-admin@example.test", "password": "test-password", "role": "admin"}
    ]))
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))

    from services import (
        audit_log,
        auth_sessions,
        connector_store,
        integrations_store,
        resource_acl,
        schedule_store,
        scim_store,
        team_store,
        user_store,
    )
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_store_path", lambda: tmp_path / "connectors.json")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource_acls.jsonl")
    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(scim_store, "STORE_PATH", tmp_path / "scim.json")
    monkeypatch.setattr(scim_store, "mongo_database", lambda: None)
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)

    from src import main
    from src.main import app

    from services import audit_coverage

    with audit_coverage._DENIAL_LOCK:
        audit_coverage._DENIALS.clear()
    monkeypatch.setattr(main, "_SECURITY_SETTINGS_SNAPSHOT_RECORDED", False, raising=False)

    admin_email = "audit-admin@example.test"
    admin_token, _expires = auth_service.create_token(admin_email)
    verification_token, _verification_expires = auth_service.create_token(admin_email)
    workspace = team_store.create_workspace(
        name="One-event audit workspace",
        created_by=admin_email,
    )
    scim_key = integrations_store.create_api_key(
        "One-event SCIM token",
        admin_email,
        role="admin",
        scopes=["scim.provision"],
        kind="scim",
    )
    with TestClient(app) as client:
        yield {
            "client": client,
            "admin_email": admin_email,
            "admin_token": admin_token,
            "verification_token": verification_token,
            "workspace_id": workspace.id,
            "scim_token": scim_key["key"],
            "audit_log": audit_log,
            "team_store": team_store,
            "user_store": user_store,
            "auth_service": auth_service,
        }


def _correlation_id() -> str:
    return str(uuid4())


def _headers(context, correlation_id: str, *, token: str | None = None) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token or context['admin_token']}",
        "X-Correlation-ID": correlation_id,
        "X-Workspace-Id": context["workspace_id"],
    }


def _events(context) -> list[dict]:
    path = context["audit_log"].STORE_PATH
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _events_for(context, correlation_id: str) -> list[dict]:
    return [event for event in _events(context) if event.get("correlation_id") == correlation_id]


def _verify_chain(context) -> None:
    response = context["client"].get(
        "/api/v1/audit/verify",
        headers=_headers(
            context,
            _correlation_id(),
            token=context["verification_token"],
        ),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    # A1 (round 11): a workspace with no records of its own is
    # ``nothing_checked`` (verified null), never "verified" over nothing.
    assert body["verified"] is not False, body["findings"]
    assert body["status"] in {"verified", "nothing_checked"}, body


def test_each_named_write_produces_one_correlated_event(one_event_app):
    client = one_event_app["client"]
    observed: list[tuple[str, str, str, int]] = []

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/connectors/saved",
        headers=_headers(one_event_app, correlation_id),
        json={
            "name": "One-event saved connector",
            "type": "postgresql",
            "role": "both",
            "host": "db.example.test",
            "port": 5432,
            "database": "warehouse",
            "username": "audit-user",
            "password": "do-not-audit-this",
            "schema": "public",
        },
    )
    assert response.status_code == 200, response.text
    observed.append(("saved connector create", correlation_id, "connector.create", response.status_code))

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/workspace/api-keys",
        headers=_headers(one_event_app, correlation_id),
        json={"name": "One-event workspace key", "role": "editor"},
    )
    assert response.status_code == 200, response.text
    observed.append(("workspace API-key create", correlation_id, "workspace.api_key.create", response.status_code))

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/resource-acls",
        headers=_headers(one_event_app, correlation_id),
        json={
            "resource_type": "schedule",
            "resource_id": "one-event-schedule",
            "principal": "group:reporting",
            "role": "viewer",
        },
    )
    assert response.status_code == 200, response.text
    observed.append(("resource ACL grant", correlation_id, "acl.grant.create", response.status_code))

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/schedules/",
        headers=_headers(one_event_app, correlation_id),
        json={
            "name": "One-event schedule",
            "source_connector_id": "source-1",
            "source_table": "public.source_table",
            "dest_connector_id": "destination-1",
            "dest_table": "public.destination_table",
        },
    )
    assert response.status_code == 201, response.text
    observed.append(("schedule create", correlation_id, "schedule.definition.create", response.status_code))

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/scim/v2/Users",
        headers=_headers(
            one_event_app,
            correlation_id,
            token=one_event_app["scim_token"],
        )
        | {"Content-Type": "application/scim+json"},
        json={"userName": "one-event-scim@example.test"},
    )
    assert response.status_code == 201, response.text
    observed.append(
        ("SCIM user create", correlation_id, "scim.user.create", response.status_code)
    )

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/team/users",
        headers=_headers(one_event_app, correlation_id),
        json={
            "email": "one-event-invite@example.test",
            "platform_role": "member",
            "workspace_id": one_event_app["workspace_id"],
            "workspace_role": "editor",
        },
    )
    assert response.status_code == 200, response.text
    observed.append(("team invite", correlation_id, "team.user.create", response.status_code))

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/ai/analyze/column",
        headers=_headers(one_event_app, correlation_id),
        json={"column_name": "account_id", "sample_values": ["1", "2", "3"]},
    )
    assert response.status_code == 200, response.text
    generic = _events_for(one_event_app, correlation_id)
    assert len(generic) == 1
    assert generic[0]["action"] == "http.post"
    assert generic[0]["details"]["status"] == response.status_code

    correlation_id = _correlation_id()
    response = client.post(
        "/api/v1/auth/logout",
        headers=_headers(one_event_app, correlation_id),
    )
    assert response.status_code == 200, response.text
    observed.append(("logout", correlation_id, "auth.logout", response.status_code))

    violations = []
    for label, correlation_id, expected_action, status in observed:
        matching = _events_for(one_event_app, correlation_id)
        actions = [event["action"] for event in matching]
        if len(matching) != 1 or actions != [expected_action]:
            violations.append(
                {
                    "write": label,
                    "expected": expected_action,
                    "observed": actions,
                    "count": len(matching),
                }
            )
        if matching and "status" in matching[0].get("details", {}):
            if matching[0]["details"]["status"] != status:
                violations.append(
                    {
                        "write": label,
                        "status": matching[0]["details"]["status"],
                    }
                )
        assert not violations, json.dumps(violations, sort_keys=True)

    _verify_chain(one_event_app)


def test_acl_named_event_retains_invalid_correlation_rejection(one_event_app):
    raw_correlation_id = "invalid correlation sentinel"
    response = one_event_app["client"].post(
        "/api/v1/resource-acls",
        headers={
            **_headers(one_event_app, raw_correlation_id),
            "X-Correlation-ID": raw_correlation_id,
        },
        json={
            "resource_type": "schedule",
            "resource_id": "invalid-correlation-grant",
            "principal": "group:reporting",
            "role": "viewer",
        },
    )

    assert response.status_code == 200, response.text
    matching = _events_for(one_event_app, response.headers["X-Correlation-ID"])
    assert len(matching) == 1
    assert matching[0]["action"] == "acl.grant.create"
    assert matching[0]["details"]["correlation_id_rejected"] is True
    _verify_chain(one_event_app)


def test_named_logout_retains_spoofed_workspace_rejection(one_event_app):
    from services.team_store import add_workspace_member, create_workspace

    member_email = "workspace-member@example.test"
    one_event_app["user_store"].create_user(
        email=member_email,
        name="Workspace member",
        role="member",
        password="member-password-123",
        created_by=one_event_app["admin_email"],
    )
    add_workspace_member(
        workspace_id=one_event_app["workspace_id"],
        email=member_email,
        role="editor",
        added_by=one_event_app["admin_email"],
        actor_is_platform_admin=True,
    )
    unrelated_workspace = create_workspace(
        name="Unrelated workspace",
        created_by="unrelated-owner@example.test",
    )
    member_token, _expires = one_event_app["auth_service"].create_token(member_email)
    correlation_id = _correlation_id()
    response = one_event_app["client"].post(
        "/api/v1/auth/logout",
        headers={
            "Authorization": f"Bearer {member_token}",
            "X-Correlation-ID": correlation_id,
            "X-Workspace-Id": unrelated_workspace.id,
        },
    )

    assert response.status_code == 200, response.text
    matching = _events_for(one_event_app, correlation_id)
    assert len(matching) == 1
    assert matching[0]["action"] == "auth.logout"
    assert matching[0]["workspace_id"] == ""
    assert matching[0]["details"]["workspace_header_rejected"] is True
    _verify_chain(one_event_app)


def test_four_xx_write_without_a_named_event_keeps_one_generic_event(one_event_app):
    correlation_id = _correlation_id()
    response = one_event_app["client"].post(
        "/api/v1/ai/analyze/column",
        headers=_headers(one_event_app, correlation_id),
        json={},
    )

    assert response.status_code == 422
    matching = _events_for(one_event_app, correlation_id)
    assert len(matching) == 1
    assert matching[0]["action"] == "http.post"
    assert matching[0]["details"]["status"] == 422
    _verify_chain(one_event_app)
