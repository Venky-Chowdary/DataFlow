"""Named security-event contracts and request-level audit coverage."""

from __future__ import annotations

import ast
import json
import logging
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from starlette.requests import Request

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def security_app(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-security-events-" + "x" * 40)
    monkeypatch.setenv("DATAFLOW_AUTH_ADMIN_EMAIL", "admin@x")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@x")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Strong-admin-password-123")
    monkeypatch.setenv("DATAFLOW_AUTH_LOCKOUT_FAILURES", "50")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "phase2c-secret-sentinel")

    from services import (
        audit_coverage,
        audit_log,
        auth_rate_limit,
        auth_sessions,
        integrations_store,
        team_store,
        user_store,
    )
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "_REQUIRE_AUTH", True)
    monkeypatch.setattr(auth_service, "_ADMIN_USER_CACHE", None)
    monkeypatch.setattr(auth_service, "_ADMIN_CACHE_KEY", None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(auth_sessions, "_path", lambda: tmp_path / "sessions.json")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setattr(integrations_store, "_keys_collection", lambda: None)
    monkeypatch.setattr(team_store, "STORE_PATH", tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "_store_path", lambda: tmp_path / "teams.json")
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "_store_path", lambda: tmp_path / "users.json")
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    auth_rate_limit.reset_auth_rate_limits()
    with audit_coverage._DENIAL_LOCK:
        audit_coverage._DENIALS.clear()

    from src import main

    monkeypatch.setattr(main, "_SECURITY_SETTINGS_SNAPSHOT_RECORDED", False, raising=False)
    with TestClient(main.app) as client:
        yield {
            "client": client,
            "audit_log": audit_log,
            "auth_service": auth_service,
            "user_store": user_store,
            "main": main,
        }


@pytest.fixture
def domain_security_app(security_app, monkeypatch, tmp_path):
    from services import integrations_store, resource_acl, schedule_store, team_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(resource_acl, "STORE_PATH", tmp_path / "resource_acls.jsonl")
    workspace = team_store.create_workspace(name="Security events", created_by="admin@x")
    key = integrations_store.create_api_key(
        "security events", "admin@x", role="admin"
    )
    return {
        **security_app,
        "headers": {
            "Authorization": f"Bearer {key['key']}",
            "X-Workspace-Id": workspace.id,
            "X-Correlation-ID": "security-events-correlation",
        },
        "workspace_id": workspace.id,
        "key_id": key["id"],
    }


def _events(context) -> list[dict]:
    path = context["audit_log"].STORE_PATH
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _rejection(context, reason_code: str) -> dict:
    return next(
        event
        for event in reversed(_events(context))
        if event.get("action") == "auth.request.rejected"
        and event.get("reason_code") == reason_code
    )


def test_security_event_registry_is_valid_and_call_sites_are_literal():
    from services import audit_coverage, ops_events

    registry = getattr(audit_coverage, "SECURITY_EVENTS", None)
    assert isinstance(registry, dict)
    assert registry
    for name in registry:
        ops_events.validate_event(name, "succeeded")

    for root in (_API_ROOT / "src", _API_ROOT / "services"):
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                if not isinstance(node.func, ast.Name) or node.func.id != "record_security_event":
                    continue
                if (
                    path in {
                        _API_ROOT / "services" / "scim_service.py",
                        _API_ROOT / "src" / "routers" / "auth_router.py",
                        # Names come from its ADMIN_ROUTES table, all registered.
                        _API_ROOT / "services" / "admin_audit.py",
                    }
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id == "action"
                ):
                    continue
                assert node.args and isinstance(node.args[0], ast.Constant)
                assert isinstance(node.args[0].value, str)
                assert node.args[0].value in registry
            if path == _API_ROOT / "services" / "scim_service.py":
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    if not isinstance(node.func, ast.Name) or node.func.id != "_update_user":
                        continue
                    audit_action = next(
                        (keyword.value for keyword in node.keywords if keyword.arg == "audit_action"),
                        None,
                    )
                    assert isinstance(audit_action, ast.Constant)
                    assert audit_action.value in registry
            if path == _API_ROOT / "src" / "routers" / "auth_router.py":
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    if (
                        not isinstance(node.func, ast.Name)
                        or node.func.id != "_record_login_audit"
                    ):
                        continue
                    action = next(
                        (keyword.value for keyword in node.keywords if keyword.arg == "action"),
                        None,
                    )
                    assert isinstance(action, ast.Constant)
                    assert action.value in {"auth.login.failure", "auth.login.locked"}


def test_unknown_security_event_warns_but_is_still_audited(security_app, caplog):
    from services.audit_coverage import record_security_event

    record_security_event(
        "custom.security.test",
        outcome="succeeded",
        actor="system",
    )
    event = next(
        row for row in reversed(_events(security_app))
        if row["action"] == "custom.security.test"
    )
    assert event["outcome"] == "succeeded"
    assert "Unknown security audit event" in caplog.text


def test_context_failure_is_logged_without_leaking_exception_and_event_is_written(
    security_app,
    monkeypatch,
    caplog,
):
    from services import audit_coverage, workspace_access

    def fail_workspace_lookup(_request):
        raise RuntimeError("SENTINEL_CTX")

    monkeypatch.setattr(
        workspace_access,
        "authorized_workspace_id",
        fail_workspace_lookup,
    )
    caplog.set_level(logging.DEBUG, logger="services.audit_coverage")
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/context-failure",
            "headers": [],
        }
    )
    request.state.correlation_id = "context-failure-correlation"

    audit_coverage.record_security_event(
        "acl.grant.create",
        outcome="succeeded",
        request=request,
        resource="acl:context-failure",
    )

    assert any(
        record.message == "audit context workspace_id unavailable: RuntimeError"
        for record in caplog.records
    )
    assert "SENTINEL_CTX" not in caplog.text
    event = next(
        event
        for event in _events(security_app)
        if event.get("correlation_id") == "context-failure-correlation"
    )
    assert event["action"] == "acl.grant.create"


@pytest.mark.parametrize(
    ("token", "reason_code"),
    [
        ("", "missing_credentials"),
        ("not-a-valid-bearer-sentinel", "invalid_token"),
        ("dfk_unknown-api-key-sentinel", "unknown_api_key"),
    ],
)
def test_auth_rejections_record_closed_reason_codes_without_credentials(
    security_app, token, reason_code, caplog
):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    response = security_app["client"].get("/api/v1/auth/me", headers=headers)
    assert response.status_code == 401

    event = _rejection(security_app, reason_code)
    assert event["outcome"] == "failed"
    assert event["details"]["status"] == 401
    assert event["details"]["client_ip"] == "testclient"
    assert "route_template" in event and event["route_template"] == ""
    stored = security_app["audit_log"].STORE_PATH.read_text(encoding="utf-8")
    if token:
        assert token not in stored
        assert token not in caplog.text


def test_disabled_user_and_revoked_session_are_invalidated_for_auth_audit(
    security_app, caplog
):
    from services import auth_sessions

    email = "disabled-security@example.test"
    account, _ = security_app["user_store"].create_user(
        email=email,
        password="Disabled-user-password-123",
        created_by="admin@x",
    )
    security_app["user_store"].update_user(email=email, status="disabled")
    disabled_token, _ = security_app["auth_service"].create_token(account["email"])
    disabled = security_app["client"].get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {disabled_token}"}
    )
    assert disabled.status_code == 401
    assert _rejection(security_app, "disabled_user")["details"]["status"] == 401
    assert disabled_token not in security_app["audit_log"].STORE_PATH.read_text(
        encoding="utf-8"
    )
    assert disabled_token not in caplog.text

    login = security_app["client"].post(
        "/api/v1/auth/login",
        json={"email": "admin@x", "password": "Strong-admin-password-123"},
    )
    assert login.status_code == 200, login.text
    token = login.json()["token"]
    jti = security_app["auth_service"].token_jti(token)
    logout = security_app["client"].post(
        "/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"}
    )
    assert logout.status_code == 200
    assert auth_sessions.session_active(jti, "admin@x") is False
    replay = security_app["client"].get(
        "/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"}
    )
    assert replay.status_code == 401
    assert _rejection(security_app, "invalid_token")["details"]["status"] == 401
    stored = security_app["audit_log"].STORE_PATH.read_text(encoding="utf-8")
    assert token not in stored
    assert jti not in stored
    assert token not in caplog.text


def test_invalid_mcp_oauth_token_uses_oauth_reason_code(security_app, caplog):
    token = "invalid-mcp-oauth-sentinel"
    response = security_app["client"].post(
        "/api/v1/mcp/tools/call",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "health", "arguments": {}},
    )
    assert response.status_code == 401
    event = _rejection(security_app, "oauth_token_invalid")
    assert event["details"]["status"] == 401
    assert token not in security_app["audit_log"].STORE_PATH.read_text(encoding="utf-8")
    assert token not in caplog.text


def test_identical_auth_rejections_are_deduplicated(security_app):
    from services import audit_coverage

    with audit_coverage._DENIAL_LOCK:
        audit_coverage._DENIALS.clear()
    for _ in range(5):
        response = security_app["client"].get("/api/v1/auth/me")
        assert response.status_code == 401
    matching = [
        event for event in _events(security_app)
        if event.get("action") == "auth.request.rejected"
        and event.get("reason_code") == "missing_credentials"
    ]
    assert len(matching) == 1
    assert matching[0]["details"]["suppressed_since_last"] == 0


def test_authz_denial_uses_named_event_and_keeps_denial_details(security_app):
    from services.audit_coverage import record_authz_denial

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/v1/secured",
        "raw_path": b"/api/v1/secured",
        "query_string": b"",
        "headers": [],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
        "scheme": "http",
        "http_version": "1.1",
        "root_path": "",
    }
    request = Request(scope)
    request.state.user = {"email": "admin@x", "role": "viewer"}
    request.state.user_email = "admin@x"
    request.state.api_key_id = "key-123"
    record_authz_denial(
        request,
        required_permission="schedule.write",
        effective_role="viewer",
        reason="missing_permission",
    )
    event = next(row for row in reversed(_events(security_app)) if row["action"] == "authz.denied")
    assert event["outcome"] == "failed"
    assert event["reason_code"] == "missing_permission"
    assert event["details"]["status"] == 403
    assert event["details"]["required_permission"] == "schedule.write"
    assert event["details"]["effective_role"] == "viewer"
    assert event["details"]["path"] == "/api/v1/secured"
    assert event["details"]["method"] == "POST"
    assert event["details"]["suppressed_since_last"] == 0
    assert event["details"]["api_key_id"] == "key-123"


def test_login_and_logout_share_hashed_session_label_and_failure_has_reason_code(security_app):
    login = security_app["client"].post(
        "/api/v1/auth/login",
        json={"email": "admin@x", "password": "Strong-admin-password-123"},
    )
    assert login.status_code == 200, login.text
    token = login.json()["token"]
    jti = security_app["auth_service"].token_jti(token)
    logout = security_app["client"].post(
        "/api/v1/auth/logout", headers={"Authorization": f"Bearer {token}"}
    )
    assert logout.status_code == 200

    rows = _events(security_app)
    login_event = next(row for row in rows if row["action"] == "auth.login")
    logout_event = next(row for row in rows if row["action"] == "auth.logout")
    assert login_event["outcome"] == "succeeded"
    assert login_event["actor"] == "admin@x"
    assert login_event["details"]["status"] == 200
    assert login_event["session_id"].startswith("ses_")
    assert login_event["session_id"] == logout_event["session_id"]
    assert logout_event["details"] == {"revoked": True, "status": 200}

    failed = security_app["client"].post(
        "/api/v1/auth/login",
        json={"email": "admin@x", "password": "wrong-password-sentinel"},
    )
    assert failed.status_code == 401
    failed_event = next(
        row for row in reversed(_events(security_app))
        if row["action"] == "auth.login" and row.get("outcome") == "failed"
    )
    assert failed_event["reason_code"] == "bad_credentials"
    assert failed_event["level"] == "warn"
    stored = security_app["audit_log"].STORE_PATH.read_text(encoding="utf-8")
    assert jti not in stored
    assert token not in stored
    assert "wrong-password-sentinel" not in stored


def test_startup_snapshot_is_safe_and_written_once(security_app):
    snapshots = [
        event for event in _events(security_app)
        if event.get("action") == "system.security_settings.snapshot"
    ]
    assert len(snapshots) == 1
    event = snapshots[0]
    assert event["outcome"] == "succeeded"
    assert event["actor"] == "system"
    details = event["details"]
    assert details["production"] is False
    assert details["auth_required"] is True
    assert isinstance(details["workspace_isolation"], bool)
    assert details["rbac_mode"] in {"deny", "allow_and_log"}
    assert details["audit_store"] in {"mongo", "file"}
    assert details["audit_retention_days"] is None or isinstance(
        details["audit_retention_days"], (int, str)
    )
    assert isinstance(details["docs_enabled"], bool)
    assert isinstance(details["config_error_count"], int)
    assert "phase2c-secret-sentinel" not in security_app["audit_log"].STORE_PATH.read_text(
        encoding="utf-8"
    )


def test_domain_security_events_include_safe_request_metadata(domain_security_app, caplog):
    from services.log_redaction import is_sensitive_key

    client = domain_security_app["client"]
    headers = domain_security_app["headers"]
    workspace_id = domain_security_app["workspace_id"]
    assert not is_sensitive_key("principal")

    created_acl = client.post(
        "/api/v1/resource-acls",
        headers=headers,
        json={
            "resource_type": "schedule",
            "resource_id": "sched-test",
            "principal": "group:analytics",
            "role": "viewer",
        },
    )
    assert created_acl.status_code == 200, created_acl.text
    grant_id = created_acl.json()["id"]
    revoked_acl = client.delete(
        f"/api/v1/resource-acls/{grant_id}", headers=headers
    )
    assert revoked_acl.status_code == 200, revoked_acl.text

    schedule_payload = {
        "name": "Security event schedule",
        "source_connector_id": "source-1",
        "source_table": "public.source_table",
        "dest_connector_id": "destination-1",
        "dest_table": "public.destination_table",
    }
    created_schedule = client.post(
        "/api/v1/schedules/", headers=headers, json=schedule_payload
    )
    assert created_schedule.status_code == 201, created_schedule.text
    response_correlation_id = created_schedule.headers["X-Correlation-ID"]
    assert response_correlation_id == "security-events-correlation"
    schedule = created_schedule.json()
    schedule_id = schedule["id"]
    updated_schedule = client.patch(
        f"/api/v1/schedules/{schedule_id}",
        headers=headers,
        json={"enabled": False},
    )
    assert updated_schedule.status_code == 200, updated_schedule.text

    manifest_response = client.get(
        f"/api/v1/schedules/{schedule_id}/export?format=json", headers=headers
    )
    assert manifest_response.status_code == 200, manifest_response.text
    imported_schedule = client.post(
        "/api/v1/schedules/import",
        headers=headers,
        json=manifest_response.json(),
    )
    assert imported_schedule.status_code == 201, imported_schedule.text
    granted = client.post(
        f"/api/v1/schedules/{schedule_id}/authorization",
        headers=headers,
        json={
            "reason": "Authorize additive schedule drift",
            "scopes": ["net_additive_drift"],
        },
    )
    assert granted.status_code == 200, granted.text
    revoked = client.delete(
        f"/api/v1/schedules/{schedule_id}/authorization?reason=No%20longer%20needed",
        headers=headers,
    )
    assert revoked.status_code == 200, revoked.text
    from services.schedule_approvals import build_approval_request, open_approval_request
    from services.schedule_store import get_schedule
    from services.standing_authorization import binding_from_schedule

    approval = build_approval_request(
        kind="authorization",
        code="TEST_APPROVAL",
        finding="Review the test schedule plan",
        corrective_action="Approve the safe test plan",
        binding=binding_from_schedule(get_schedule(schedule_id)),
    )
    open_approval_request(schedule_id, approval)
    approved = client.post(
        f"/api/v1/schedules/{schedule_id}/approvals/{approval['id']}/approve",
        headers=headers,
        json={"reason": "Approve the test schedule plan"},
    )
    assert approved.status_code == 200, approved.text

    gitops_response = client.post(
        "/api/v1/schedules/gitops/apply",
        headers=headers,
        json={
            "kind": "PipelineSchedule",
            "spec": {
                **schedule_payload,
                "name": "GitOps security event schedule",
            },
        },
    )
    assert gitops_response.status_code == 200, gitops_response.text
    dry_run_response = client.post(
        "/api/v1/schedules/gitops/apply?dry_run=true",
        headers=headers,
        json={
            "kind": "PipelineSchedule",
            "spec": {
                **schedule_payload,
                "name": "GitOps security event schedule",
            },
        },
    )
    assert dry_run_response.status_code == 200, dry_run_response.text
    deleted_schedule = client.delete(
        f"/api/v1/schedules/{schedule_id}", headers=headers
    )
    assert deleted_schedule.status_code == 200, deleted_schedule.text

    exported = client.get("/api/v1/audit/export?format=json", headers=headers)
    assert exported.status_code == 200, exported.text
    verified = client.get("/api/v1/audit/verify", headers=headers)
    assert verified.status_code == 200, verified.text
    # PP3 (round 11): a body that is not a pack is a 422; a real (here
    # unsigned-for-this-key) pack shape is verified and audited.
    proof = client.post(
        "/api/v1/audit/verify-pack",
        headers=headers,
        json={"pack": {"job_id": "j", "content_sha256": "0" * 64, "signature": {"value": "x"}}},
    )
    assert proof.status_code == 200, proof.text
    scanned = client.get("/api/v1/audit/secret-scan", headers=headers)
    assert scanned.status_code == 200, scanned.text

    expected = {
        "acl.grant.create": "/api/v1/resource-acls",
        "acl.grant.revoke": "/api/v1/resource-acls/{grant_id}",
        "schedule.definition.create": "/api/v1/schedules",
        "schedule.definition.update": "/api/v1/schedules/{schedule_id}",
        "schedule.definition.import": "/api/v1/schedules/import",
        "schedule.definition.delete": "/api/v1/schedules/{schedule_id}",
        "schedule.gitops.apply": "/api/v1/schedules/gitops/apply",
        "schedule.approval.approved": "",
        "schedule.authorization.granted": "",
        "schedule.authorization.revoked": "",
        "audit.export.read": "/api/v1/audit/export",
        "audit.verify.read": "/api/v1/audit/verify",
        "audit.verify_pack.read": "/api/v1/audit/verify-pack",
        "audit.secret_scan.read": "/api/v1/audit/secret-scan",
    }
    events = _events(domain_security_app)
    for name, route in expected.items():
        matching_events = [row for row in events if row["action"] == name]
        assert matching_events
        for event in matching_events:
            assert event["outcome"] == "succeeded"
            assert event["actor_type"] == "api_key"
            assert event["actor_id"] == domain_security_app["key_id"]
            assert event["auth_method"] == "api_key"
            assert event["workspace_id"] == workspace_id
            assert event["route_template"] == route
            assert "trace_id" in event
            assert event["details"]["status"] in {200, 201}
            assert event["correlation_id"] == response_correlation_id
            assert any(name in record.getMessage() for record in caplog.records)

    assert next(row for row in events if row["action"] == "acl.grant.create")[
        "details"
    ]["principal"] == "group:analytics"
    acl_event = next(row for row in events if row["action"] == "acl.grant.create")
    assert set(acl_event["details"]) == {
        "resource_type", "resource_id", "principal", "role", "status"
    }
    schedule_event = next(
        row for row in events if row["action"] == "schedule.definition.create"
    )
    assert set(schedule_event["details"]) == {
        "id", "source_connector_id", "source_table", "dest_connector_id",
        "dest_table", "sync_mode", "enabled", "status",
    }
    gitops_event = next(row for row in events if row["action"] == "schedule.gitops.apply")
    assert set(gitops_event["details"]) == {
        "created", "updated", "deleted", "unchanged", "dry_run", "status",
    }
    dry_run_event = next(
        row for row in events
        if row["action"] == "schedule.gitops.apply" and row["details"]["dry_run"]
    )
    assert dry_run_event["details"]["updated"] == 1
    export_event = next(row for row in events if row["action"] == "audit.export.read")
    assert export_event["details"]["format"] == "json"
    assert isinstance(export_event["details"]["row_count"], int)
    from services.evidence_chain import verify_chain

    assert verify_chain()["verified"] is True


def test_real_main_app_route_template_includes_mounted_api_prefix(domain_security_app):
    client = domain_security_app["client"]
    headers = {
        **domain_security_app["headers"],
        "X-Correlation-ID": "route-template-prefix-check",
    }
    response = client.get("/api/v1/audit/export?format=json", headers=headers)

    assert response.status_code == 200, response.text
    event = next(
        row
        for row in _events(domain_security_app)
        if row["action"] == "audit.export.read"
        and row["correlation_id"] == "route-template-prefix-check"
    )
    assert event["route_template"] == "/api/v1/audit/export"


def test_rekey_flag_failed_event_stays_registered():
    """Back-fill for 918326f0: email_change.rekey_flag_failed must stay in
    the registry — it is emitted when flagging a half-moved account for
    rekey retry itself fails."""
    from services import audit_coverage, ops_events
    assert "email_change.rekey_flag_failed" in audit_coverage.SECURITY_EVENTS
    ops_events.validate_event("email_change.rekey_flag_failed", "succeeded")
