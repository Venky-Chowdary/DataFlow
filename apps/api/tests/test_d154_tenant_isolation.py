"""D154: a fresh self-serve account must not see another account's data.

Auth ON and signup ON (which now enforces workspace isolation). Two accounts
provisioned the way signup does it (user + org + personal workspace), plus the
bootstrap platform admin. Account A owns a saved connector, a job, a schedule,
usage, audit, DLQ and CDC freshness rows in A's workspace; the deployment also
holds legacy unscoped rows created by A before workspaces existed.

B — with no header, with B's own header, and with A's workspace forced into
the header — sees none of it and gets 403/404 on A's ids and the legacy ids.
A sees their own workspace; the admin sees everything. After the backfill the
legacy rows belong to A's personal workspace and A sees them.
"""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import (  # noqa: E402
    audit_log,
    connector_store,
    mongodb_service,
    ops_metrics,
    quarantine_dlq,
    schedule_store,
    signup_service,
    team_store,
    usage_metering,
    user_store,
)
from services.auth_rate_limit import reset_auth_rate_limits  # noqa: E402
from services.password_hash import hash_password_argon2  # noqa: E402
from src.main import app  # noqa: E402

ADMIN = "root-d154@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
PASSWORD = "Tenant-Password-2026!"


def _provision(email: str) -> str:
    """Create the account exactly as a verified signup does; return its workspace."""
    _user, _org, workspace = signup_service._provision_verified_user({
        "email": email,
        "name": email.split("@")[0],
        "password_hash": hash_password_argon2(PASSWORD),
        "canonical_email": email,
    })
    return workspace.id


def _client(email: str, password: str) -> TestClient:
    client = TestClient(app)
    login = client.post("/api/v1/auth/login", json={"email": email, "password": password})
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


@pytest.fixture
def tenants(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_SIGNUP_ENABLED", "1")
    monkeypatch.delenv("DATAFLOW_REQUIRE_WORKSPACE", raising=False)
    monkeypatch.delenv("DATAWRAP_REQUIRE_WORKSPACE", raising=False)
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "d154-tenant-isolation-test-secret")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
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
    for series in ("dataflow_pipeline_lag_seconds", "dataflow_pipeline_lag_bytes"):
        monkeypatch.setitem(ops_metrics._labeled_gauges, series, {})
    monkeypatch.setitem(ops_metrics._labeled_counters, "dataflow_pipeline_cdc_polls_total", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_heartbeat", {})
    monkeypatch.setattr(ops_metrics, "_pipeline_workspace", {})
    reset_auth_rate_limits()

    tag = uuid4().hex[:8]
    a_email, b_email = f"alice-{tag}@example.com", f"bob-{tag}@example.com"
    ws_a, ws_b = _provision(a_email), _provision(b_email)
    a, b, admin = _client(a_email, PASSWORD), _client(b_email, PASSWORD), _client(ADMIN, ADMIN_PASSWORD)

    # A's own resources, created through the API without a header: they land
    # in A's personal workspace (isolation on, no more 400).
    made = a.post("/api/v1/connectors/saved", json={
        "name": f"a-pg-{tag}", "type": "postgresql", "host": "localhost", "port": 5432,
        "database": "a", "username": "a", "password": "secret-a",
    })
    assert made.status_code == 200, made.text
    a_connector = made.json()["id"]
    assert made.json()["workspace_id"] == ws_a
    sched = a.post("/api/v1/schedules/", json={
        "name": f"a-sched-{tag}", "source_connector_id": a_connector, "source_table": "orders",
        "dest_connector_id": a_connector, "dest_table": "orders_copy", "interval": "daily",
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert sched.status_code == 201, sched.text
    a_schedule = sched.json()["id"]
    assert sched.json()["workspace_id"] == ws_a

    a_job, legacy_job = f"job-a-{tag}", f"job-legacy-{tag}"
    memory._jobs[a_job] = {"_id": a_job, "status": "completed", "workspace_id": ws_a,
                           "triggered_by": a_email, "created_at": "2026-10-01T00:00:00+00:00"}
    legacy_connector = connector_store.create_connector({
        "name": f"legacy-{tag}", "type": "postgresql", "host": "localhost", "port": 5432,
        "workspace_id": "",
    }).id
    # A legacy job: created before workspaces, by A, through the legacy connector.
    memory._jobs[legacy_job] = {
        "_id": legacy_job, "status": "failed", "workspace_id": "", "triggered_by": a_email,
        "created_at": "2026-09-01T00:00:00+00:00",
        "transfer_request": {"source": {"connector_id": legacy_connector}, "destination": {}},
    }
    usage_metering.record_transfer_usage(job_id=a_job, workspace_id=ws_a, rows_written=7)
    usage_metering.record_transfer_usage(job_id=legacy_job, workspace_id="", rows_written=11)
    audit_log.append_audit_event(action=f"a.action.{tag}", resource="x", actor=a_email, workspace_id=ws_a)
    audit_log.append_audit_event(action=f"legacy.action.{tag}", resource="x", actor=a_email)
    quarantine_dlq.append_dlq_event(job_id=a_job, action="quarantine.fail", rows=3, workspace_id=ws_a)
    quarantine_dlq.append_dlq_event(job_id=legacy_job, action="quarantine.fail", rows=5)
    ops_metrics.record_cdc_poll(lag_seconds=500.0, job_id=a_job, schedule_id=a_schedule,
                                stream="orders", workspace_id=ws_a)
    return {
        "a": a, "b": b, "admin": admin, "ws_a": ws_a, "ws_b": ws_b, "tag": tag,
        "a_connector": a_connector, "a_schedule": a_schedule, "a_job": a_job,
        "legacy_connector": legacy_connector, "legacy_job": legacy_job,
    }


def _views(client: TestClient, headers: dict[str, str]) -> dict[str, object]:
    """Every surface the dashboard reads, as ids/numbers; 403 is recorded as None."""
    def get(path: str):
        r = client.get(path, headers=headers)
        if r.status_code == 403:
            return None
        assert r.status_code == 200, (path, r.status_code, r.text)
        return r.json()

    out: dict[str, object] = {}
    body = get("/api/v1/connectors/")
    out["connectors"] = None if body is None else {c["id"] for c in body["connectors"]}
    body = get("/api/v1/connectors/saved")
    out["saved"] = None if body is None else {c["id"] for c in body["connectors"]}
    body = get("/api/v1/connectors/jobs")
    out["jobs"] = None if body is None else {j["_id"] for j in body["jobs"]}
    out["job_total"] = None if body is None else body["total"]
    body = get("/api/v1/schedules/")
    out["schedules"] = None if body is None else {s["id"] for s in body}
    body = get("/api/v1/usage/summary")
    out["usage_rows"] = None if body is None else body["rows_written"]
    body = get("/api/v1/audit/events?limit=500")
    out["audit"] = None if body is None else {e["action"] for e in body["events"]}
    body = get("/api/v1/ops/dlq")
    out["dlq"] = None if body is None else body["total"]
    body = get("/api/v1/ops/freshness")
    out["freshness"] = None if body is None else len(body.get("pipelines") or [])
    body = get("/api/v1/ops/metrics/json")
    out["metric_series"] = None if body is None else len(body.get("pipeline_lag_seconds") or {})
    return out


def _sees_nothing_of_a(view: dict[str, object], t: dict[str, str]) -> None:
    a_ids = {t["a_connector"], t["legacy_connector"], t["a_job"], t["legacy_job"], t["a_schedule"]}
    for key in ("connectors", "saved", "jobs", "schedules"):
        assert not (set(view[key] or ()) & a_ids), (key, view[key])
    assert not view["job_total"], view
    assert not view["usage_rows"], view
    assert not {a for a in (view["audit"] or ()) if t["tag"] in a}, view["audit"]
    assert not view["dlq"], view
    assert not view["freshness"], view
    assert not view["metric_series"], view


def test_fresh_account_sees_none_of_another_accounts_data(tenants):
    t = tenants
    b = t["b"]
    for headers in ({}, {"X-Workspace-Id": t["ws_b"]}):
        _sees_nothing_of_a(_views(b, headers), t)
    # Forcing A's workspace into the header is refused outright.
    forced = {"X-Workspace-Id": t["ws_a"]}
    for path in ("/api/v1/connectors/", "/api/v1/connectors/saved", "/api/v1/connectors/jobs",
                 "/api/v1/schedules/", "/api/v1/usage/summary", "/api/v1/audit/events",
                 "/api/v1/ops/dlq", "/api/v1/ops/freshness"):
        assert b.get(path, headers=forced).status_code == 403, path
    assert b.get(f"/api/v1/usage/summary?workspace_id={t['ws_a']}").status_code == 403

    # Id-addressed reads of A's and of the legacy resources.
    for headers in ({}, {"X-Workspace-Id": t["ws_b"]}):
        for path in (
            f"/api/v1/connectors/saved/{t['a_connector']}",
            f"/api/v1/connectors/saved/{t['legacy_connector']}",
            f"/api/v1/connectors/jobs/{t['a_job']}",
            f"/api/v1/connectors/jobs/{t['legacy_job']}",
            f"/api/v1/schedules/{t['a_schedule']}",
        ):
            assert b.get(path, headers=headers).status_code in {403, 404}, (path, headers)
        assert b.delete(
            f"/api/v1/connectors/saved/{t['a_connector']}", headers=headers
        ).status_code in {403, 404}


def test_owner_sees_own_workspace_and_admin_sees_everything(tenants):
    t = tenants
    mine = _views(t["a"], {})
    assert t["a_connector"] in mine["saved"]
    assert t["a_job"] in mine["jobs"] and mine["job_total"] == 1
    assert t["a_schedule"] in mine["schedules"]
    assert mine["usage_rows"] == 7
    assert f"a.action.{t['tag']}" in mine["audit"]
    assert mine["dlq"] == 1
    assert mine["freshness"] == 1
    # Legacy unscoped rows are platform-admin only until the backfill runs.
    assert t["legacy_connector"] not in mine["saved"] | mine["connectors"]
    assert t["legacy_job"] not in mine["jobs"]
    assert f"legacy.action.{t['tag']}" not in mine["audit"]
    assert t["a"].get(f"/api/v1/connectors/jobs/{t['legacy_job']}").status_code == 404

    everything = _views(t["admin"], {})
    assert {t["a_connector"], t["legacy_connector"]} <= everything["saved"]
    assert {t["a_job"], t["legacy_job"]} <= everything["jobs"]
    assert t["a_schedule"] in everything["schedules"]
    assert everything["usage_rows"] == 18
    assert {f"a.action.{t['tag']}", f"legacy.action.{t['tag']}"} <= everything["audit"]
    assert everything["dlq"] == 2
    assert t["admin"].get(f"/api/v1/connectors/jobs/{t['legacy_job']}").status_code == 200
    # An admin's header narrows to that workspace instead of 403ing.
    narrowed = _views(t["admin"], {"X-Workspace-Id": t["ws_a"]})
    assert narrowed["jobs"] == {t["a_job"]}


def test_backfill_assigns_legacy_rows_to_their_owner(tenants):
    from services.legacy_workspace_backfill import backfill_legacy_workspaces

    t = tenants
    dry = backfill_legacy_workspaces()
    assert dry.counts["jobs"]["assigned"] == 1 and dry.counts["connectors"]["assigned"] == 1
    assert t["legacy_job"] not in _views(t["a"], {})["jobs"]  # dry run wrote nothing

    applied = backfill_legacy_workspaces(apply=True)
    assert applied.counts["jobs"] == {"assigned": 1, "skipped": 0}
    assert applied.counts["usage_events"]["assigned"] == 1
    assert applied.counts["quarantine_dlq"]["assigned"] == 1
    mine = _views(t["a"], {})
    assert t["legacy_connector"] in mine["saved"]
    assert t["legacy_job"] in mine["jobs"] and mine["job_total"] == 2
    assert mine["usage_rows"] == 18 and mine["dlq"] == 2
    assert t["a"].get(f"/api/v1/connectors/jobs/{t['legacy_job']}").status_code == 200
    _sees_nothing_of_a(_views(t["b"], {}), t)

    again = backfill_legacy_workspaces(apply=True)
    assert all(c["assigned"] == 0 for c in again.counts.values()), again.counts
