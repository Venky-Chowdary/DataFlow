"""Round 10 live QA regressions.

Each test names the finding it locks: drift ack through execute, re-run,
SSRF before dial, rename, sync-mode ids, list limits, request bounds,
briefing/catalog honesty, and operator-facing error text.
"""

from __future__ import annotations

import time

import pytest
from pydantic import ValidationError


def test_schema_drift_ack_round_trips_and_restores_from_the_job_root():
    from services.confirmed_transfer import apply_stored_schema_drift_ack, transfer_request_from_ack
    from src.ai.copilot.job_reads import job_governance
    from src.transfer.models import transfer_request_from_dict, transfer_request_to_dict

    request = transfer_request_from_ack({
        "source": {"kind": "database", "connector_id": "src", "format": "postgresql", "table": "orders"},
        "destination": {"kind": "database", "connector_id": "dst", "format": "postgresql", "table": "orders"},
        "mappings": [{"source": "id", "target": "id"}],
        "schema_drift_acknowledged": True,
    })
    assert request.schema_drift_acknowledged is True
    assert request.acknowledgment_actor == "operator"
    assert "drift" in request.acknowledgment_reason.lower()

    stored = transfer_request_to_dict(request)
    assert stored["schema_drift_acknowledged"] is True
    # Production dropped the nested flag and left only the job root.
    nested = dict(stored)
    nested["schema_drift_acknowledged"] = False
    job = {
        "schema_drift_acknowledged": True,
        "acknowledgment_actor": "operator",
        "acknowledgment_reason": "Accept drift for this run",
        "transfer_request": nested,
    }
    reloaded = transfer_request_from_dict(nested)
    assert reloaded.schema_drift_acknowledged is False
    assert apply_stored_schema_drift_ack(reloaded, job) is True
    assert reloaded.schema_drift_acknowledged is True
    gov = job_governance({"transfer_request": transfer_request_to_dict(reloaded), **job})
    assert gov["acknowledgments"]["schema_drift"] is True


def test_additive_drift_names_the_column_and_ack_clears_it():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema as fp

    blocked = run_file_preflight(
        columns=["id", "email", "new_col"],
        column_types={"id": "INTEGER", "email": "VARCHAR", "new_col": "VARCHAR"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
            {"source": "email", "target": "email", "confidence": 0.99, "transform": "none"},
            {"source": "new_col", "target": "new_col", "confidence": 0.99, "transform": "none"},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=[{"id": "1", "email": "a@b.com", "new_col": "x"}],
        destination_column_types={"id": "INTEGER", "email": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        sync_mode="full_refresh_append",
        estimated_bytes=1000,
        stored_source_fp=fp(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"}),
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
    )
    drift = next(g for g in blocked["gates"] if g["id"] == "schema_drift")
    assert "new_col" in drift["message"]
    assert blocked["passed"] is False

    cleared = run_file_preflight(
        columns=["id", "email", "new_col"],
        column_types={"id": "INTEGER", "email": "VARCHAR", "new_col": "VARCHAR"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
            {"source": "email", "target": "email", "confidence": 0.99, "transform": "none"},
            {"source": "new_col", "target": "new_col", "confidence": 0.99, "transform": "none"},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=[{"id": "1", "email": "a@b.com", "new_col": "x"}],
        destination_column_types={"id": "INTEGER", "email": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="manual_review",
        sync_mode="full_refresh_append",
        estimated_bytes=1000,
        stored_source_fp=fp(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"}),
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
        schema_drift_acknowledged=True,
        acknowledgment_actor="operator",
        acknowledgment_reason="Accept drift for this run",
    )
    drift_after = [g for g in cleared["gates"] if g["id"] == "schema_drift" and g.get("status") == "block"]
    assert drift_after == []


def test_hard_narrow_still_blocks_when_acknowledged_and_names_the_column():
    from services.preflight_service import run_file_preflight
    from services.schema_fingerprint import fingerprint_schema as fp

    result = run_file_preflight(
        columns=["id", "email"],
        column_types={"id": "INTEGER", "email": "INTEGER"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99, "transform": "none"},
            {"source": "email", "target": "email", "confidence": 0.99, "transform": "none"},
        ],
        destination_connected=True,
        source_connected=True,
        source_kind="file",
        sample_rows=[{"id": "1", "email": "2"}],
        destination_column_types={"id": "INTEGER", "email": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="postgresql",
        schema_policy="propagate_columns",
        sync_mode="full_refresh_append",
        estimated_bytes=1000,
        stored_source_fp=fp(["id", "email"], {"id": "INTEGER", "email": "VARCHAR"}),
        previous_source_columns=["id", "email"],
        previous_source_schema={"id": "INTEGER", "email": "VARCHAR"},
        schema_drift_acknowledged=True,
        acknowledgment_actor="operator",
        acknowledgment_reason="Accept drift for this run",
    )
    drift = next(g for g in result["gates"] if g["id"] == "schema_drift")
    assert drift["status"] == "block"
    assert "email" in drift["message"]
    assert result["passed"] is False


def test_dataset_stage_keeps_schema_drift_ack_on_the_preflight_and_the_payload(tmp_path, monkeypatch):
    from pathlib import Path
    from types import SimpleNamespace

    import services.dataset_file as dataset_file
    from src.ai.copilot import transfer_tools
    from src.ai.copilot.dataset_transfer import stage_dataset_transfer

    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr(dataset_file, "dataset_roots", lambda: [tmp_path])
    csv_path = Path(tmp_path) / "payments.csv"
    csv_path.write_text("id,amount\n1,10\n", encoding="utf-8")
    schema = SimpleNamespace(name="payments", source="upload", path=str(csv_path), file_type="csv")
    import src.ai.copilot.data_analyst as data_analyst

    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: SimpleNamespace(
        resolve_dataset=lambda hint: schema,
    ))
    monkeypatch.setattr(
        transfer_tools, "_safe_connector",
        lambda *_a, **_k: ({"id": "dest-1", "name": "Warehouse", "type": "postgresql", "schema": "public"}, None),
    )
    monkeypatch.setattr(
        transfer_tools, "_introspect",
        lambda *_a, **_k: {
            "ok": True,
            "columns": [
                {"name": "id", "inferred_type": "INTEGER", "nullable": False},
                {"name": "amount", "inferred_type": "INTEGER", "nullable": True},
            ],
            "db_type": "postgresql",
            "table_exists": True,
        },
    )
    seen: dict = {}

    def _preflight(**kwargs):
        seen.update(kwargs)
        return {"passed": True, "run_id": "pf_r10", "proof_bundle": {"transfer_decision": {"decision": "approve"}}}

    monkeypatch.setattr(transfer_tools, "_run_preflight", _preflight)
    monkeypatch.setattr(transfer_tools, "_stage_bound_contract", lambda *_a, **_k: {})

    result = stage_dataset_transfer(
        dataset_name="payments",
        dest_connector_name="Warehouse",
        schema_drift_acknowledged=True,
    )
    assert result.success, result.error
    assert seen.get("schema_drift_acknowledged") is True
    from src.ai.copilot.ack_ledger import get_ack_ledger

    payload, err = get_ack_ledger().get_pending_payload(result.output["ack_id"])
    assert not err
    assert payload["schema_drift_acknowledged"] is True
    assert payload["skip_preflight"] is False


def test_private_10_and_metadata_are_refused_before_a_dial():
    from services.egress_guard import metadata_endpoint_refusal
    from src.transfer.connector_registry import run_probe

    assert metadata_endpoint_refusal("10.0.0.5")
    assert metadata_endpoint_refusal("169.254.169.254")
    assert "192.168/16" in metadata_endpoint_refusal("192.168.0.8")
    assert "172.16/12" in metadata_endpoint_refusal("172.16.1.4")

    for host in ("10.0.0.5", "169.254.169.254", "192.168.0.8", "172.16.1.4"):
        started = time.monotonic()
        ok, message = run_probe(
            "postgresql",
            {"host": host, "port": 5432, "database": "app", "username": "u", "password": "p"},
        )
        assert time.monotonic() - started < 2, host
        assert ok is False
        assert "Refused" in message


def test_rename_to_a_free_prefix_is_staged_and_an_exact_name_is_not(tmp_path, monkeypatch):
    import services.connector_store as cs
    import src.ai.copilot.ack_ledger as ledger_mod
    from src.ai.copilot.lifecycle_tools import update_connector

    monkeypatch.setattr(cs, "_backend_choice", "file")
    monkeypatch.setattr(cs, "_store_path", lambda: tmp_path / "connectors.json")

    class _Ledger:
        def put(self, *, kind, payload, preview):
            return "ack-rename"

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: _Ledger())

    sales = cs.create_connector({"name": "sales", "type": "sqlite", "database": "a.db"})
    cs.create_connector({"name": "sales_archive", "type": "sqlite", "database": "b.db"})

    free = update_connector(connector_id=sales.id, new_name="sale")
    assert free.success, free.error

    taken = update_connector(connector_id=sales.id, new_name="sales_archive")
    assert taken.success is False
    assert "already exists" in taken.error


def test_recommend_sync_mode_returns_engine_ids_and_rejects_bad_inputs():
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    res = tools.execute(
        "recommend_sync_mode",
        {"workload": "orders, updates and deletes, near-real-time", "has_primary_key": True, "has_cursor": True},
    )
    assert res.success
    assert res.output["recommended_mode"] == "cdc_incremental"
    assert " " not in res.output["recommended_mode"]

    bad = tools.execute("recommend_sync_mode", {"has_cursor": "yes", "row_count": -3})
    assert bad.success is False
    assert "boolean" in bad.error

    negative = tools.execute("recommend_sync_mode", {"row_count": -1})
    assert negative.success is False
    assert "row_count" in negative.error


def test_list_jobs_rejects_negative_and_unbounded_limits():
    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools()
    for limit in (-5, 0, 99999999999):
        res = tools._list_jobs(limit=limit)
        assert res.success is False, limit
        assert "1 to 100" in res.error
    from services.connector_request_limits import bounded_job_list_limit

    assert bounded_job_list_limit(-5) == 0
    assert bounded_job_list_limit(10**12) == 1000
    assert bounded_job_list_limit(25) == 25


def test_request_models_reject_bad_ports_and_limits():
    from src.routers.connectors_router import ConnectorConfig
    from src.routers.saved_connectors_router import ConnectorSaveDTO
    from src.routers.transfer_router import EndpointDTO, ExecuteTransferRequest

    with pytest.raises(ValidationError):
        ConnectorSaveDTO(name="pg", type="postgresql", host="db", port=99999)
    with pytest.raises(ValidationError):
        ConnectorConfig(name="pg", type="postgresql", host="db", port=-1)
    ConnectorSaveDTO(name="local", type="sqlite", port=0, database="a.db")
    with pytest.raises(ValidationError):
        EndpointDTO(kind="database", format="postgresql", port=99999)
    with pytest.raises(ValidationError):
        ExecuteTransferRequest(
            source=EndpointDTO(kind="database", format="postgresql"),
            destination=EndpointDTO(kind="database", format="postgresql"),
            limit=-1,
        )


def test_create_connector_refuses_a_bad_port_and_a_10_host_before_probe(monkeypatch):
    from src.ai.copilot.tools import DataPilotTools

    called: list[str] = []

    def _probe(*_a, **_k):
        called.append("dial")
        return True, "should not dial"

    monkeypatch.setattr("src.transfer.connector_registry.run_probe", _probe)
    monkeypatch.setattr("services.connector_store.connector_name_taken", lambda *a, **k: False)
    tools = DataPilotTools()
    bad_port = tools._create_connector(
        name="pg-r10",
        type="postgresql",
        host="db.example",
        port=99999,
        database="app",
        username="app",
        password="secret",
    )
    assert bad_port.success is False
    assert "65535" in (bad_port.error or "")
    private = tools._create_connector(
        name="pg-private",
        type="postgresql",
        host="10.0.0.5",
        port=5432,
        database="app",
        username="app",
        password="secret",
    )
    assert private.success is False
    assert "Refused" in (private.error or "")
    assert called == []


def test_next_schedule_is_the_soonest_future_run(monkeypatch):
    import src.ai.copilot.workspace_briefing as wb

    monkeypatch.setattr(wb, "_load_connectors", lambda workspace_id: [])
    monkeypatch.setattr(wb, "_load_jobs", lambda workspace_id: ([], {"total": 0, "by_status": {}}))
    monkeypatch.setattr(wb, "_contract_facts", lambda *a, **k: {})
    monkeypatch.setattr(
        wb, "_load_schedules",
        lambda workspace_id: [
            {"name": "stale", "enabled": True, "next_run_at": "2026-10-10T00:00:00+00:00"},
            {"name": "later", "enabled": True, "next_run_at": "2026-12-02T00:00:00+00:00"},
            {"name": "soon", "enabled": True, "next_run_at": "2026-12-01T00:00:00+00:00"},
        ],
    )
    brief = wb.collect_workspace_briefing()
    assert "soon" in brief["next_schedule"]
    assert "stale" not in brief["next_schedule"]
    assert "later" not in brief["next_schedule"]

    monkeypatch.setattr(
        wb, "_load_schedules",
        lambda workspace_id: [
            {"name": "stale", "enabled": True, "next_run_at": "2026-10-10T00:00:00+00:00"},
        ],
    )
    past = wb.collect_workspace_briefing()
    assert past["next_schedule"] == ""
    assert any("past" in line for line in past["attention"])


def test_search_connectors_publishes_capability_driver_counts():
    from services.catalog_service import search_catalog
    from src.transfer.connector_capabilities import (
        dest_live_driver_types,
        source_live_driver_types,
        transfer_live_driver_types,
    )

    result = search_catalog("", "all")
    assert result["capabilities_source_live"] == len(source_live_driver_types())
    assert result["capabilities_dest_live"] == len(dest_live_driver_types())
    assert result["capabilities_transfer_live"] == len(transfer_live_driver_types())
    assert "not transfer-live" in result["count_note"]
    # Tile counts stay the list the operator is looking at.
    assert "source_live" in result and "dest_live" in result


def test_unknown_transform_is_named_and_not_an_empty_value():
    from services.decision_kernel.findings import FailureClass, classify_transform_failure
    from services.root_cause_engine import build_root_causes

    kind = classify_transform_failure(
        "Unknown transform: 'uppercase' empty value cannot coerce"
    )
    assert kind is FailureClass.SEMANTIC_TRANSFORM_FAILURE
    roots = build_root_causes({
        "gates": [{
            "id": "g5_dry_run",
            "status": "block",
            "message": "Unknown transform: 'uppercase'",
            "details": {"kind": "transform_errors"},
        }],
        "blockers": [],
    })
    assert roots
    assert roots[0].title == "Unknown transform: uppercase"
    assert "uppercase" in roots[0].summary


def test_sql_syntax_is_not_called_read_only_and_writes_still_are():
    from src.routers.query_router import sql_refusal_message

    syntax = sql_refusal_message("SELEC * FROM orders")
    assert "SELEC" in syntax
    assert "syntax" in syntax.lower()
    assert "read-only" not in syntax.lower()
    write = sql_refusal_message("DELETE FROM orders")
    assert "read-only" in write.lower()
    playground = sql_refusal_message("SELEC * FROM orders", playground=True)
    assert "SELEC" in playground
    assert "playground" not in playground or "syntax" in playground.lower()


def test_every_zero_minutes_does_not_claim_the_step_is_longer():
    from src.ai.copilot.schedule_cadence import parse_cadence

    zero = parse_cadence("every 0 minutes")
    assert zero.question
    assert "not a cadence" in zero.question
    assert "anything longer" not in zero.question
    longer = parse_cadence("every 90 minutes")
    assert longer.question
    assert "anything longer" in longer.question


def test_rerun_stages_finished_jobs_and_retry_points_at_it(monkeypatch):
    import src.ai.copilot.ack_ledger as ledger_mod
    import src.ai.copilot.lifecycle_tools as lt

    class Ledger:
        def put(self, *, kind, payload, preview):
            return "ack-rerun"

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    job = {
        "id": "job-abc123def456",
        "status": "completed",
        "transfer_request": {
            "source": {"connector_name": "Src", "table": "orders"},
            "destination": {"connector_name": "Dst"},
        },
    }
    monkeypatch.setattr(lt, "resolve_job", lambda job_id="", selector="": (job, ""))
    rerun = lt._job_tool("rerun_job", "job-abc123def456", "")
    assert rerun.success, rerun.error
    retry = lt._job_tool("retry_job", "job-abc123def456", "")
    assert retry.success is False
    assert "rerun_job" in retry.error

    job["status"] = "completed_with_quarantine"
    assert lt._job_tool("rerun_job", "job-abc123def456", "").success
    job["status"] = "cancelled"
    assert lt._job_tool("rerun_job", "job-abc123def456", "").success
    job["status"] = "failed"
    failed = lt._job_tool("rerun_job", "job-abc123def456", "")
    assert failed.success is False
    assert "retry_job" in failed.error


def _job_shell(status: str, sync_mode: str, rows: int) -> dict:
    return {
        "job_id": "job-r10",
        "status": status,
        "records_processed": rows,
        "transfer_request": {
            "source": {"kind": "database", "type": "sqlite", "table": "src"},
            "destination": {"kind": "database", "type": "sqlite", "table": "dst"},
            "sync_mode": sync_mode,
            "skip_preflight": True,
        },
    }


def _post_job(monkeypatch, path: str, job: dict):
    from fastapi.testclient import TestClient

    from services.mongodb_service import get_mongodb_service
    from src.main import app

    mongo = get_mongodb_service()
    created: dict = {}
    captured: dict = {}
    monkeypatch.setattr(mongo, "get_job", lambda jid: job if jid == job["job_id"] else None)
    monkeypatch.setattr(
        mongo, "update_job_status",
        lambda jid, status, **kw: created.update({"job_id": jid, "status": status, **kw}) or True,
    )
    import src.transfer.engine as engine_mod

    class _Engine:
        def _create_pending_job(self, req):
            captured["req"] = req
            return "new-job"

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr("src.transfer.background.run_transfer_async", lambda *a, **k: None)
    with TestClient(app) as client:
        response = client.post(path)
    return response, created, captured


def test_rest_retry_of_a_finished_job_points_at_rerun(monkeypatch):
    res, _, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-r10/retry",
        _job_shell("completed", "full_refresh_overwrite", 10),
    )
    assert res.status_code == 409
    detail = res.json()["detail"]
    assert detail["primary_action"] == "rerun"
    assert detail["rerun_url"].endswith("/jobs/job-r10/rerun")


def test_rest_rerun_keeps_preflight_and_links_rerun_of(monkeypatch):
    res, created, captured = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-r10/rerun",
        _job_shell("completed", "full_refresh_overwrite", 10),
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["rerun_of"] == "job-r10"
    assert body["skip_preflight"] is False
    assert created["rerun_of"] == "job-r10"
    assert captured["req"].skip_preflight is False


def test_rest_rerun_refuses_append_duplicates_unless_forced(monkeypatch):
    res, _, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-r10/rerun",
        _job_shell("completed", "incremental_append", 500),
    )
    assert res.status_code == 409
    assert "duplicate" in res.json()["detail"]["reason"].lower() or "committed" in res.json()["detail"]["reason"].lower()

    forced, created, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-r10/rerun?force=true",
        _job_shell("completed_with_quarantine", "incremental_append", 500),
    )
    assert forced.status_code == 200, forced.text
    assert created.get("duplicate_risk_acknowledged") is True


def test_operator_retry_of_a_cancelled_job_is_allowed(monkeypatch):
    res, created, _ = _post_job(
        monkeypatch,
        "/api/v1/connectors/jobs/job-r10/retry",
        _job_shell("cancelled", "full_refresh_overwrite", 0),
    )
    assert res.status_code == 200, res.text
    assert created["retry_of"] == "job-r10"


def test_reconcile_paint_stays_below_99_until_the_scan_finishes():
    from services.batch_progress import (
        compute_transfer_progress_pct,
        reconcile_display_pct,
        reconcile_scan_pct,
    )

    assert compute_transfer_progress_pct(phase="reconcile") == 99
    assert reconcile_display_pct(0, 1000) == 98.0
    mid = reconcile_display_pct(500, 1000)
    assert 98.0 < mid < 99
    assert reconcile_display_pct(1000, 1000) == 98.99
    assert reconcile_scan_pct(0, 100) == 0
    assert reconcile_scan_pct(40, 100) == 40
    assert reconcile_scan_pct(100, 100) == 99
