from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import audit_log, audit_coverage, team_store, tracing, user_store, workspace_access
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app


@pytest.fixture
def request_context(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "request-context-test-secret-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.delenv("DATAFLOW_MONGO_URI", raising=False)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    reset_auth_rate_limits()

    with TestClient(app) as client:
        admin_login = client.post(
            "/api/v1/auth/login",
            json={
                "email": "root@example.com",
                "password": "Bootstrap-Admin-2026",
            },
        )
        assert admin_login.status_code == 200, admin_login.text
        client.headers["Authorization"] = f"Bearer {admin_login.json()['token']}"

        ws1_response = client.post(
            "/api/v1/team/workspaces", json={"name": "Request context one"}
        )
        ws2_response = client.post(
            "/api/v1/team/workspaces", json={"name": "Request context two"}
        )
        assert ws1_response.status_code == 200, ws1_response.text
        assert ws2_response.status_code == 200, ws2_response.text
        ws1 = ws1_response.json()["workspace"]["id"]
        ws2 = ws2_response.json()["workspace"]["id"]

        created = client.post(
            "/api/v1/team/users",
            json={
                "email": "context-member@example.test",
                "platform_role": "member",
                "workspace_id": ws1,
                "workspace_role": "editor",
            },
        )
        assert created.status_code == 200, created.text
        yield {
            "client": client,
            "workspace_id": ws1,
            "other_workspace_id": ws2,
            "member_password": created.json()["temporary_password"],
            "audit_path": audit_log.STORE_PATH,
        }


def _audit_events(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def _auth_logout_event(path: Path, *, correlation_id: str) -> dict:
    return next(
        event
        for event in _audit_events(path)
        if event.get("action") == "auth.logout"
        and event.get("correlation_id") == correlation_id
    )


def _member_authorization(context) -> str:
    login = context["client"].post(
        "/api/v1/auth/login",
        json={
            "email": "context-member@example.test",
            "password": context["member_password"],
        },
    )
    assert login.status_code == 200, login.text
    return f"Bearer {login.json()['token']}"


@pytest.mark.parametrize(
    "raw",
    [
        "a" * 129,
        "abc\ndef",
        "x y",
        "<script>",
        "id;drop",
        "é",
    ],
)
def test_sanitize_correlation_id_replaces_invalid_values(raw):
    sanitized, rejected = tracing.sanitize_correlation_id(raw)

    assert rejected is True
    assert sanitized != raw
    UUID(sanitized)


@pytest.mark.parametrize("raw", ["550e8400-e29b-41d4-a716-446655440000", "a.b:c-d_1", "a" * 128])
def test_sanitize_correlation_id_keeps_valid_values(raw):
    assert tracing.sanitize_correlation_id(raw) == (raw, False)


@pytest.mark.parametrize("raw", [None, ""])
def test_sanitize_correlation_id_generates_id_for_missing_values(raw):
    sanitized, rejected = tracing.sanitize_correlation_id(raw)

    assert rejected is False
    UUID(sanitized)


def test_rejected_correlation_id_is_sanitized_audited_and_emitted_once(
    request_context, caplog
):
    raw = "RAW-CORRELATION-SENTINEL with spaces"
    caplog.set_level(logging.INFO)
    response = request_context["client"].post(
        "/api/v1/auth/logout",
        headers={"X-Correlation-ID": raw},
    )

    assert response.status_code == 200
    correlation_id = response.headers["X-Correlation-ID"]
    assert correlation_id != raw
    UUID(correlation_id)
    event = _auth_logout_event(
        request_context["audit_path"],
        correlation_id=correlation_id,
    )
    assert event["details"]["correlation_id_rejected"] is True
    assert raw not in request_context["audit_path"].read_bytes().decode("utf-8")
    assert raw not in caplog.text

    operational_events = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "http.correlation_id.rejected"
    ]
    assert len(operational_events) == 1
    assert operational_events[0].correlation_id_rejected is True
    assert operational_events[0].length == len(raw)
    assert operational_events[0].route_path == "/api/v1/auth/logout"
    assert operational_events[0].correlation_id == correlation_id


def test_valid_correlation_id_is_echoed_and_audited_without_rejection(request_context):
    correlation_id = "request.ctx:valid-1"
    response = request_context["client"].post(
        "/api/v1/auth/logout",
        headers={"X-Correlation-ID": correlation_id},
    )

    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == correlation_id
    event = _auth_logout_event(
        request_context["audit_path"],
        correlation_id=correlation_id,
    )
    assert "correlation_id_rejected" not in event["details"]


def _request_with_workspace(workspace_id: str, *, email: str = "member@example.test") -> Request:
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-workspace-id", workspace_id.encode("utf-8"))],
        }
    )
    request.state.user_email = email
    return request


def test_authorized_workspace_id_caches_member_and_rejects_other_cases(monkeypatch):
    calls: list[tuple[str, str]] = []

    def can_read(workspace_id: str, email: str) -> bool:
        calls.append((workspace_id, email))
        return workspace_id == "ws1" and email == "member@example.test"

    monkeypatch.setattr(workspace_access, "can_read_workspace", can_read)

    member = _request_with_workspace(" ws1 ")
    assert workspace_access.authorized_workspace_id(member) == "ws1"
    assert workspace_access.authorized_workspace_id(member) == "ws1"
    assert member.state.workspace_header_rejected is False
    assert calls == [("ws1", "member@example.test")]

    non_member = _request_with_workspace("ws2")
    assert workspace_access.authorized_workspace_id(non_member) == ""
    assert non_member.state.workspace_header_rejected is True

    empty = _request_with_workspace("")
    assert workspace_access.authorized_workspace_id(empty) == ""

    def fail_closed(*_args):
        raise RuntimeError("membership store unavailable")

    monkeypatch.setattr(workspace_access, "can_read_workspace", fail_closed)
    failed = _request_with_workspace("ws1")
    assert workspace_access.authorized_workspace_id(failed) == ""
    assert failed.state.workspace_header_rejected is True


def test_named_logout_never_uses_an_unauthorized_workspace_header(request_context):
    client = request_context["client"]
    route = "/api/v1/auth/logout"
    bad_correlation = "workspace-scope-bad"
    bad_response = client.post(
        route,
        headers={
            "Authorization": _member_authorization(request_context),
            "X-Correlation-ID": bad_correlation,
            "X-Workspace-Id": request_context["other_workspace_id"],
        },
    )
    assert bad_response.status_code == 200
    rejected = _auth_logout_event(
        request_context["audit_path"],
        correlation_id=bad_correlation,
    )
    assert rejected["workspace_id"] == ""
    assert rejected["details"]["workspace_header_rejected"] is True

    good_correlation = "workspace-scope-good"
    good_response = client.post(
        route,
        headers={
            "Authorization": _member_authorization(request_context),
            "X-Correlation-ID": good_correlation,
            "X-Workspace-Id": request_context["workspace_id"],
        },
    )
    assert good_response.status_code == 200
    accepted = _auth_logout_event(
        request_context["audit_path"],
        correlation_id=good_correlation,
    )
    assert accepted["workspace_id"] == request_context["workspace_id"]
    assert "workspace_header_rejected" not in accepted["details"]


def test_authorized_workspace_id_does_not_reject_missing_header(request_context, monkeypatch):
    calls: list[tuple[str, str]] = []

    def can_read(workspace_id: str, email: str) -> bool:
        calls.append((workspace_id, email))
        return False

    monkeypatch.setattr(workspace_access, "can_read_workspace", can_read)
    request = _request_with_workspace("")

    assert workspace_access.authorized_workspace_id(request) == ""
    assert request.state.workspace_header_rejected is False
    assert calls == []


def test_named_logout_does_not_flag_missing_workspace_header(request_context):
    correlation_id = "workspace-header-absent"
    route = "/api/v1/auth/logout"
    response = request_context["client"].post(
        route,
        headers={"X-Correlation-ID": correlation_id},
    )

    assert response.status_code == 200
    event = _auth_logout_event(
        request_context["audit_path"],
        correlation_id=correlation_id,
    )
    assert event["workspace_id"] == ""
    assert "workspace_header_rejected" not in event["details"]


def test_legacy_team_member_audit_captures_rejected_workspace_header(request_context):
    client = request_context["client"]
    authorization = _member_authorization(request_context)
    correlation_id = "legacy-team-member-spoof"
    response = client.post(
        f"/api/v1/team/workspaces/{request_context['workspace_id']}/members",
        headers={
            "Authorization": authorization,
            "X-Correlation-ID": correlation_id,
            "X-Workspace-Id": request_context["other_workspace_id"],
        },
        json={"email": "legacy-target@example.test", "role": "viewer"},
    )
    assert response.status_code == 200, response.text
    matching = [
        event
        for event in _audit_events(request_context["audit_path"])
        if event.get("correlation_id") == correlation_id
    ]
    assert len(matching) == 1
    assert matching[0]["action"] == "team.member.add"
    assert matching[0]["workspace_id"] != request_context["other_workspace_id"]
    assert matching[0]["details"]["workspace_header_rejected"] is True

    no_header_correlation = "legacy-team-member-no-header"
    no_header_response = client.post(
        f"/api/v1/team/workspaces/{request_context['workspace_id']}/members",
        headers={
            "Authorization": authorization,
            "X-Correlation-ID": no_header_correlation,
        },
        json={"email": "legacy-target-no-header@example.test", "role": "viewer"},
    )
    assert no_header_response.status_code == 200, no_header_response.text
    no_header_event = next(
        event
        for event in _audit_events(request_context["audit_path"])
        if event.get("correlation_id") == no_header_correlation
    )
    assert no_header_event["action"] == "team.member.add"
    assert "workspace_header_rejected" not in no_header_event["details"]


def test_audit_coverage_prefers_sanitized_request_state_correlation_id():
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-correlation-id", b"untrusted-header")],
        }
    )
    request.state.correlation_id = "validated.state-id"

    assert audit_coverage._correlation_id(request) == "validated.state-id"
    unset_state = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [(b"x-correlation-id", b"unit-helper-fallback")],
        }
    )
    assert audit_coverage._correlation_id(unset_state) == "unit-helper-fallback"
