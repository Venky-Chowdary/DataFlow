"""Audit persistence failures must preserve request evidence when possible."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def audit_failure_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "audit-failure-test-" + "x" * 40)
    monkeypatch.setenv(
        "DATAFLOW_AUTH_USERS",
        json.dumps(
            [
                {
                    "email": "audit-admin@example.test",
                    "password": "test-password",
                    "role": "admin",
                }
            ]
        ),
    )
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))

    from services import (
        audit_log,
        auth_sessions,
        integrations_store,
        resource_acl,
        team_store,
        user_store,
    )
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource_acls.jsonl")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)

    from services import audit_coverage
    from src import main
    from src.main import app

    monkeypatch.setattr(audit_coverage, "_AUDIT_ACCESS_FAILURES", 0)
    with audit_coverage._DENIAL_LOCK:
        audit_coverage._DENIALS.clear()
    monkeypatch.setattr(main, "_SECURITY_SETTINGS_SNAPSHOT_RECORDED", False, raising=False)

    email = "audit-admin@example.test"
    token, _expires = auth_service.create_token(email)
    verification_token, _verification_expires = auth_service.create_token(email)
    workspace = team_store.create_workspace(
        name="Audit failure workspace",
        created_by=email,
    )
    with TestClient(app) as client:
        yield {
            "client": client,
            "email": email,
            "token": token,
            "verification_token": verification_token,
            "workspace_id": workspace.id,
            "audit_log": audit_log,
            "auth_service": auth_service,
        }


def _headers(context, correlation_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {context['token']}",
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
    return [
        event
        for event in _events(context)
        if event.get("correlation_id") == correlation_id
    ]


def _assert_chain_verifies(context) -> None:
    response = context["client"].get(
        "/api/v1/audit/verify",
        headers={
            "Authorization": f"Bearer {context['verification_token']}",
            "X-Correlation-ID": str(uuid4()),
            "X-Workspace-Id": context["workspace_id"],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["verified"] is True


def _create_acl_grant(context, correlation_id: str, resource_id: str):
    return context["client"].post(
        "/api/v1/resource-acls",
        headers=_headers(context, correlation_id),
        json={
            "resource_type": "schedule",
            "resource_id": resource_id,
            "principal": "group:reporting",
            "role": "viewer",
        },
    )


def test_failed_named_write_keeps_generic_request_event(
    audit_failure_app,
    monkeypatch,
    caplog,
):
    audit_log = audit_failure_app["audit_log"]
    persist = getattr(audit_log, "_persist_event", None)

    def fail_acl_persistence(event):
        if event.get("action") == "acl.grant.create":
            raise OSError("file store unavailable")
        assert persist is not None
        return persist(event)

    monkeypatch.setattr(audit_log, "_persist_event", fail_acl_persistence, raising=False)
    correlation_id = str(uuid4())
    response = _create_acl_grant(
        audit_failure_app,
        correlation_id,
        "AUDIT_DETAILS_SENTINEL",
    )

    assert response.status_code == 200, response.text
    matching = _events_for(audit_failure_app, correlation_id)
    assert [event["action"] for event in matching] == ["http.post"]
    failures = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "audit.write.failed"
    ]
    assert len(failures) == 1
    assert failures[0].action == "acl.grant.create"
    assert failures[0].store == "file"
    assert failures[0].correlation_id == correlation_id
    assert "AUDIT_DETAILS_SENTINEL" not in caplog.text
    _assert_chain_verifies(audit_failure_app)


def test_mongo_failure_falls_back_and_counts_named_write(audit_failure_app, monkeypatch, caplog):
    class BrokenMongoCollection:
        def insert_one(self, _event):
            raise OSError("mongo is unavailable")

    monkeypatch.setattr(
        audit_failure_app["audit_log"],
        "_mongo_collection",
        lambda: BrokenMongoCollection(),
    )
    correlation_id = str(uuid4())
    response = _create_acl_grant(
        audit_failure_app,
        correlation_id,
        "mongo-fallback-schedule",
    )

    assert response.status_code == 200, response.text
    matching = _events_for(audit_failure_app, correlation_id)
    assert [event["action"] for event in matching] == ["acl.grant.create"]
    failures = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "audit.write.failed"
    ]
    assert len(failures) == 1
    assert failures[0].action == "acl.grant.create"
    assert failures[0].stage == "persist"
    assert failures[0].levelno == logging.ERROR
    assert failures[0].store == "mongo"
    assert failures[0].fallback == "file"
    assert failures[0].correlation_id == correlation_id
    # A1 (round 11): with Mongo the store and unreadable, verify reports it
    # instead of verifying the local fallback file under Mongo's name ...
    response = audit_failure_app["client"].get(
        "/api/v1/audit/verify",
        headers={
            "Authorization": f"Bearer {audit_failure_app['verification_token']}",
            "X-Correlation-ID": str(uuid4()),
            "X-Workspace-Id": audit_failure_app["workspace_id"],
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "store_unavailable"
    assert response.json()["verified"] is None
    # ... and the records the fallback wrote still form a valid chain there.
    monkeypatch.setattr(audit_failure_app["audit_log"], "_mongo_collection", lambda: None)
    _assert_chain_verifies(audit_failure_app)


@pytest.mark.parametrize(
    ("stage", "patched_name"),
    [
        ("chain_read", "_chain_tip"),
        ("redact", "_redact"),
        ("hash", "_hmac_event_hash"),
        ("persist", "_persist_event"),
    ],
)
def test_append_stage_failures_emit_once_without_partial_store_write(
    audit_failure_app,
    monkeypatch,
    caplog,
    stage,
    patched_name,
):
    audit_log = audit_failure_app["audit_log"]
    store_path = audit_log.STORE_PATH
    before = store_path.read_bytes() if store_path.exists() else b""

    def fail_stage(*_args, **_kwargs):
        raise RuntimeError("SENTINEL_STAGE")

    monkeypatch.setattr(audit_log, patched_name, fail_stage)
    correlation_id = str(uuid4())
    with pytest.raises(RuntimeError, match="SENTINEL_STAGE"):
        audit_log.append_audit_event(
            action="audit.stage.test",
            resource="RESOURCE_SENTINEL",
            correlation_id=correlation_id,
            details={"value": "DETAIL_SENTINEL"},
        )

    after = store_path.read_bytes() if store_path.exists() else b""
    assert after == before
    failures = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "audit.write.failed"
    ]
    assert len(failures) == 1
    assert failures[0].action == "audit.stage.test"
    assert failures[0].stage == stage
    assert failures[0].levelno == logging.ERROR
    assert failures[0].store == "file"
    assert failures[0].fallback == "none"
    assert failures[0].correlation_id == correlation_id
    assert "SENTINEL_STAGE" not in caplog.text
    assert "DETAIL_SENTINEL" not in caplog.text
    assert "RESOURCE_SENTINEL" not in caplog.text


def test_context_derivation_failures_do_not_skip_audit_write(
    audit_failure_app,
    monkeypatch,
):
    from services import audit_coverage, tracing, workspace_access

    def fail_context(*_args, **_kwargs):
        raise RuntimeError("context unavailable")

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/resource-acls",
        "headers": [],
        "query_string": b"",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 1234),
        "scheme": "http",
    }
    request = Request(scope)
    request.state.correlation_id = str(uuid4())
    request.scope["route"] = SimpleNamespace(path="/api/v1/resource-acls")
    monkeypatch.setattr(audit_coverage, "request_principal", fail_context)
    monkeypatch.setattr(workspace_access, "authorized_workspace_id", fail_context)
    monkeypatch.setattr(tracing, "current_trace_id", fail_context)

    audit_coverage.record_security_event(
        "acl.grant.create",
        outcome="succeeded",
        request=request,
        actor="test-actor",
    )

    matching = _events_for(audit_failure_app, request.state.correlation_id)
    assert len(matching) == 1
    event = matching[0]
    assert event["action"] == "acl.grant.create"
    assert event["workspace_id"] == ""
    assert event["trace_id"] == ""
    assert event.get("actor_id", "") == ""
