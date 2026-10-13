"""Operator-facing honesty for sync advice, grants, Kafka, SQL, and acks.

These are the shared paths behind DEF-C-011, DEF-C-015, DEF-C-039, DEF-C-040,
DEF-C-003, DEF-C-014, DEF-C-021, DEF-B-012, and DEF-C-012. A unit pass is not
a live QA retest.
"""

from __future__ import annotations

import importlib
import time

import pytest

from services.copilot_sql_guard import assert_identifiers_allowed
from services.preflight_sample import EngineSample
from services.preflight_service import run_file_preflight
from src.ai.copilot.ack_ledger import PilotAckLedger
from src.ai.copilot.tools import DataPilotTools
from connectors.kafka_reader import accepted_kafka_configs


def _gate(result: dict, gate_id: str) -> dict:
    return next(g for g in result["gates"] if g["id"] == gate_id)


def test_near_realtime_updates_and_deletes_recommend_cdc():
    res = DataPilotTools().execute(
        "recommend_sync_mode",
        {
            "workload": "orders table, 10M rows, updates and deletes, need near-real-time",
            "has_cursor": True,
            "has_primary_key": True,
        },
    )
    assert res.success
    assert res.output["recommended_mode"] == "Incremental CDC"
    assert res.output["requires"]["cdc_log_access"] is True
    assert res.output["requires"]["primary_key"] is True
    assert "append" not in res.output["recommended_mode"].lower()


def test_query_with_deletes_recommends_overwrite_without_a_cursor():
    res = DataPilotTools().execute(
        "recommend_sync_mode",
        {
            "workload": "report query output, deletes happen",
            "has_cursor": False,
            "has_primary_key": False,
            "source_read_mode": "query",
        },
    )
    assert res.success
    assert res.output["recommended_mode"] == "Full Refresh Overwrite"
    assert res.output["requires"]["cursor"] is False
    assert res.output["requires"]["cdc_log_access"] is False


def test_full_refresh_append_does_not_require_a_cursor():
    res = DataPilotTools().execute(
        "recommend_sync_mode",
        {"workload": "nightly file drop", "has_cursor": False, "has_primary_key": False},
    )
    assert res.success
    assert res.output["recommended_mode"] == "Full Refresh Append"
    assert res.output["requires"]["cursor"] is False


def test_pilot_preflight_forwards_a_denied_insert_probe(monkeypatch):
    captured: dict = {}

    def fake_inspect(**kwargs):
        return {
            "connected": True,
            "can_write": False,
            "can_create_table": False,
            "privilege_probe": {
                "status": "denied",
                "detail": "User can connect but lacks INSERT on public.qa6c7_perm_dst",
                "method": "has_table_privilege",
            },
            "column_types": {"id": "INTEGER"},
        }

    def fake_preflight(**kwargs):
        captured.update(kwargs)
        return {"passed": False, "gates": [], "blockers": [], "warnings": [], "proof_bundle": {}}

    monkeypatch.setattr(
        "services.preflight_service.inspect_destination_for_preflight", fake_inspect
    )
    monkeypatch.setattr("services.preflight_service.run_file_preflight", fake_preflight)
    monkeypatch.setattr(
        "services.preflight_service.run_transfer_policy_gates", lambda **k: []
    )
    monkeypatch.setattr(
        "services.preflight_service.apply_policy_gates", lambda result, *a, **k: result
    )
    monkeypatch.setattr(
        "services.preflight_run_store.save_preflight_run", lambda result, **k: result
    )
    from src.ai.copilot.transfer_tools import _run_preflight

    _run_preflight(
        src_conn={"id": "s", "name": "src", "type": "postgresql"},
        dst_conn={"id": "d", "name": "dst", "type": "postgresql", "schema": "public"},
        src_table="sales",
        dst_table="qa6c7_perm_dst",
        src_rows=[{"name": "id", "inferred_type": "INTEGER", "nullable": True}],
        sample_rows=[],
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        mode="full_refresh_overwrite",
        schema_policy="manual_review",
        validation_mode="balanced",
        src_db_type="postgresql",
        source_config={"type": "postgresql"},
        dest_db_type="postgresql",
        dest_exists=True,
        source_read_mode="query",
    )
    assert captured["destination_can_write"] is False
    assert captured["privilege_probe"]["status"] == "denied"
    assert "lacks INSERT" in captured["privilege_probe"]["detail"]


def test_select_denied_blocks_gate_1_instead_of_a_cast(monkeypatch):
    def denied(**kwargs):
        return EngineSample(
            unavailable_reason=(
                "source sample read failed: permission denied for table qa6c7_cust_upd"
            ),
            attempted=True,
        )

    monkeypatch.setattr("services.preflight_sample.engine_sample_rows", denied)
    cols = ["id", "region", "qty", "price"]
    result = run_file_preflight(
        columns=cols,
        column_types={c: "VARCHAR" for c in cols},
        row_count=0,
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="postgresql",
        source_config={"type": "postgresql"},
        source_table="qa6c7_cust_upd",
        destination_table_exists=True,
        destination_can_create=False,
        destination_can_write=True,
        destination_db_type="mysql",
        destination_table="qa6c7_ro_noaccess_my",
        sync_mode="full_refresh_overwrite",
    )
    gate = _gate(result, "g1_source")
    assert gate["status"] == "block"
    assert "SELECT was denied" in gate["message"]
    assert "Source readable" not in gate["message"]


@pytest.mark.parametrize(
    "error",
    [
        "server closed the connection unexpectedly",
        'relation "public.orders_v2" does not exist',
        "canceling statement due to statement timeout",
    ],
)
def test_mx3_22_failed_source_read_blocks_gate_1_not_source_readable(monkeypatch, error):
    """QA MX3-22: a reader that raised left G1 green ("Source readable") —
    catalog columns alone were accepted as a readable source."""

    def failed(**kwargs):
        return EngineSample(
            unavailable_reason=f"source sample read failed: {error}",
            attempted=True,
            read_failed=True,
        )

    monkeypatch.setattr("services.preflight_sample.engine_sample_rows", failed)
    cols = ["id", "amount"]
    result = run_file_preflight(
        columns=cols,
        column_types={"id": "INTEGER", "amount": "DECIMAL(10,2)"},
        row_count=0,
        mappings=[{"source": c, "target": c, "confidence": 1.0} for c in cols],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="postgresql",
        source_config={"type": "postgresql"},
        source_table="orders_v2",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="mysql",
        destination_table="orders_v2",
        sync_mode="full_refresh_overwrite",
    )
    gate = _gate(result, "g1_source")
    assert gate["status"] == "block"
    assert "Source readable" not in gate["message"]
    assert error in gate["message"]
    assert result["passed"] is False


def test_row_filter_matching_nothing_is_not_a_source_read_failure(monkeypatch):
    def filtered(**kwargs):
        return EngineSample(
            unavailable_reason="none of the first 50 source row(s) match the row filter",
            attempted=True,
        )

    monkeypatch.setattr("services.preflight_sample.engine_sample_rows", filtered)
    cols = ["id"]
    result = run_file_preflight(
        columns=cols,
        column_types={"id": "INTEGER"},
        row_count=0,
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
        sample_rows=[],
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="postgresql",
        source_config={"type": "postgresql"},
        source_table="orders",
        destination_table_exists=False,
        destination_can_create=True,
        destination_can_write=True,
        destination_db_type="mysql",
        destination_table="orders",
        sync_mode="full_refresh_overwrite",
    )
    assert _gate(result, "g1_source")["status"] == "pass"


def test_kafka_listing_drops_an_unrecognized_timeout():
    filtered = accepted_kafka_configs(
        {
            "bootstrap_servers": "localhost:9092",
            "request_timeout_ms": 8000,
            "api_version_auto_timeout_ms": 8000,
        },
        {"bootstrap_servers", "request_timeout_ms", "consumer_timeout_ms"},
    )
    assert "api_version_auto_timeout_ms" not in filtered
    assert filtered["request_timeout_ms"] == 8000
    kept = accepted_kafka_configs(
        {"api_version_auto_timeout_ms": 8000, "bootstrap_servers": "localhost:9092"},
        None,
    )
    assert kept["api_version_auto_timeout_ms"] == 8000


def test_ordinary_sql_functions_are_not_schema_identifiers():
    allowed = {"created_at", "amount", "id", "qa6c_kill"}
    sql = (
        "SELECT date_trunc('day', created_at) AS d, count(*), sum(amount), "
        "coalesce(max(id),0), round(avg(amount)::numeric,2) FROM qa6c_kill GROUP BY 1"
    )
    assert_identifiers_allowed(sql, allowed=allowed)
    try:
        assert_identifiers_allowed("SELECT secret_col FROM qa6c_kill", allowed=allowed)
    except ValueError as exc:
        assert "secret_col" in str(exc)
    else:
        raise AssertionError("an unknown column must still be refused")


def test_bad_tool_argument_names_the_schema_not_the_python_signature():
    res = DataPilotTools().execute("test_connector", {"bogus": "QA6-C PG Kill"})
    assert res.success is False
    assert "Accepted parameters" in (res.error or "")
    assert "connector_id" in (res.error or "")
    assert "_test_connector" not in (res.error or "")
    assert "unexpected keyword" not in (res.error or "").lower()


def test_connector_name_is_accepted_as_the_connector_argument():
    res = DataPilotTools().execute("test_connector", {"connector_name": "QA6-C PG Kill"})
    assert "_test_connector" not in (res.error or "")
    assert "unexpected keyword" not in (res.error or "").lower()
    assert "No connector matched" in (res.error or "")


def test_expired_transfer_ack_does_not_say_create_the_connector(tmp_path):
    ledger = PilotAckLedger(path=tmp_path / "acks.json")
    ack = ledger.put(kind="start_transfer", payload={"source_table": "orders"}, ttl_sec=60)
    ledger._entries[ack]["expires_at"] = time.time() - 5
    message = ledger.unusable_reason(ack)
    assert "plan the transfer again" in message
    assert "create the connector" not in message
    missing = ledger.unusable_reason("ack_does_not_exist")
    assert "stage the action again" in missing
    assert "create the connector" not in missing


def test_create_connector_refuses_a_duplicate_name(monkeypatch, tmp_path):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SQLITE_ROOT", str(tmp_path))
    monkeypatch.setenv("SQLITE_ROOT", str(tmp_path))
    import services.connector_store as cs

    importlib.reload(cs)
    monkeypatch.setattr(cs, "_backend_choice", "file", raising=False)
    cs.create_connector(
        {"name": "QA6-C PG Kill", "type": "sqlite", "database": str(tmp_path / "a.db")}
    )
    res = DataPilotTools()._create_connector(
        name="QA6-C PG Kill",
        type="sqlite",
        database=str(tmp_path / "b.db"),
    )
    assert res.success is False
    assert "already exists" in (res.error or "")
    assert "plaintext" not in (res.error or "").lower()
    assert "Fernet" not in (res.error or "")
    assert res.output is None or "ack_id" not in (res.output or {})


def test_unreadable_workbook_is_operator_text_not_the_library_sentence():
    import zipfile

    from services.excel_parser import _load_workbook, explain_unreadable_file

    zip_msg = explain_unreadable_file(zipfile.BadZipFile("File is not a zip file"))
    assert zip_msg != "File is not a zip file"
    assert "readable" in zip_msg.lower()
    assert ".xlsx" in zip_msg
    magic = explain_unreadable_file(Exception("Bad magic number for file header"))
    assert "Bad magic number for file header" not in magic
    assert "format" in magic.lower()
    assert explain_unreadable_file(Exception(zip_msg)) == zip_msg
    try:
        _load_workbook(b"this is not a workbook")
    except ValueError as exc:
        opened = str(exc)
    else:
        raise AssertionError("garbage bytes opened as a workbook")
    assert "File is not a zip file" not in opened
    assert "Bad magic number for file header" not in opened


def test_remediate_rejects_an_unknown_run_and_does_not_stage_a_write(monkeypatch):
    from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn

    monkeypatch.setattr(
        "services.preflight_run_store.get_preflight_run",
        lambda _run_id: None,
    )
    missing = DataPilotTools().execute(
        "remediate_validation",
        {"kind": "normalize_control_chars", "run_id": "pf_doesnotexist"},
    )
    assert missing.success is False
    assert "not found" in (missing.error or "").lower()
    assert missing.output is None

    monkeypatch.setattr(
        "services.preflight_run_store.get_preflight_run",
        lambda run_id: {"run_id": run_id},
    )
    known = DataPilotTools().execute(
        "remediate_validation",
        {"kind": "normalize_control_chars", "run_id": "pf_df8f4f31dca5"},
    )
    assert known.success is True
    assert known.output["ui_only"] is True
    assert known.output["requires_confirm"] is False
    assert known.output["risk"] == "safe"
    assert "ack_id" not in known.output
    assert known.output["run_id"] == "pf_df8f4f31dca5"

    turn = PilotTurn()
    DataPilotAgent._append_tool_actions(turn, known)
    assert turn.pending_actions == []
    assert any(a.get("type") == "navigate" and a.get("screen") == "transfer" for a in turn.actions)


def test_describe_pilot_names_confirm_gated_delete():
    out = DataPilotTools().execute("describe_pilot", {}).output or {}
    cannot = " ".join(out.get("cannot_yet") or [])
    can = " ".join(out.get("can") or [])
    assert "Create a brand-new schedule" not in cannot
    assert "Delete connectors, jobs, or data" not in cannot
    assert "Delete jobs or warehouse rows from chat" in cannot
    assert "delete a pipeline schedule after you Confirm" in can
    assert "Delete a saved connector after you Confirm" in can


def test_cdc_does_not_require_an_incremental_cursor():
    from src.ai.rag.product_facts import _sync_modes_section

    text = _sync_modes_section().text
    cdc = next(line for line in text.splitlines() if line.startswith("Sync mode cdc "))
    incremental = next(
        line for line in text.splitlines() if line.startswith("Sync mode incremental_append ")
    )
    assert "cursor field" not in cdc.lower()
    assert "gtid is optional" in cdc.lower()
    assert "cursor column" in cdc.lower()
    assert "cursor field" in incremental.lower()


def test_gtid_answer_says_file_and_position_is_enough():
    from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
    from src.ai.rag.product_facts import _gtid_section

    fact = _gtid_section().text.lower()
    assert "optional" in fact
    assert "file and position are enough" in fact
    assert "not required" in fact
    assert "incremental cursor column" in fact
    body = " ".join(
        (compose_product_answer(retrieve_product_answer("Does MySQL CDC require GTID")) or "").split()
    ).lower()
    assert "optional" in body or "not required" in body
    assert "file and position" in body
    assert "cursor field" not in body


def test_resume_fact_restarts_a_full_refresh():
    from src.ai.rag.product_facts import _resume_section

    text = _resume_section().text.lower()
    assert "committed cursor" in text
    assert "full refresh" in text
    assert "restarts from the beginning" in text


def test_minio_list_uses_the_s3_probe(monkeypatch):
    from types import SimpleNamespace

    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    seen: dict = {}

    class Probe:
        ok = True
        tables = ["exports/a.csv"]
        message = "listed 1 object"
        error = ""

    def fake_test_s3(**kwargs):
        seen["list"] = kwargs
        return Probe()

    def fake_read(**kwargs):
        seen["read"] = kwargs
        return SimpleNamespace(
            headers=["id"],
            rows=[{"id": 1}],
            total_rows=1,
            meta={},
        )

    monkeypatch.setattr("connectors.s3.test_s3", fake_test_s3)
    monkeypatch.setattr("connectors.s3.s3_object_exists", lambda *a, **k: True)
    monkeypatch.setattr("connectors.s3_reader.read_object", fake_read)
    for fmt in ("minio", "wasabi", "backblaze_b2", "digitalocean_spaces", "cloudflare_r2"):
        seen.clear()
        info = introspect_endpoint(
            EndpointConfig(
                kind="database",
                format=fmt,
                host="minio",
                port=9000,
                database="qa19-bucket",
                table="exports/a.csv",
                username="minio",
                password="minio123",
            )
        )
        assert "not yet implemented" not in (info.get("message") or ""), fmt
        assert info["connected"] is True, fmt
        assert seen["list"]["host"] == "minio"
        assert seen["read"]["bucket"] == "qa19-bucket"
        assert seen["read"]["key"] == "exports/a.csv"
        assert info["columns"] == ["id"]


def test_bigquery_probe_failure_names_the_service_account(monkeypatch):
    monkeypatch.setattr(
        "services.connector_store.connector_name_taken",
        lambda *a, **k: False,
    )
    monkeypatch.setattr(
        "src.transfer.connector_registry.run_probe",
        lambda *_a, **_k: (False, "invalid_grant"),
    )
    res = DataPilotTools()._create_connector(
        name="bq-qa",
        type="bigquery",
        database="my-project",
        service_account='{"type":"service_account"}',
        test_first=True,
    )
    assert res.success is False
    assert "service_account" in (res.error or "")
    assert "project id" in (res.error or "")
    assert "Railway" not in (res.error or "")
    assert "host/port" not in (res.error or "")


def test_sql_probe_failure_still_names_host_and_port(monkeypatch):
    monkeypatch.setattr(
        "services.connector_store.connector_name_taken",
        lambda *a, **k: False,
    )
    monkeypatch.setattr(
        "src.transfer.connector_registry.run_probe",
        lambda *_a, **_k: (False, "connection refused"),
    )
    res = DataPilotTools()._create_connector(
        name="pg-qa",
        type="postgresql",
        host="db.internal",
        port=5432,
        database="app",
        username="app",
        password="secret",
        test_first=True,
    )
    assert res.success is False
    assert "Railway" not in (res.error or "")
    assert "db.internal:5432" in (res.error or "")
    assert "host/port/user/password" in (res.error or "")


def test_railway_sql_probe_failure_keeps_railway_advice(monkeypatch):
    monkeypatch.setattr(
        "services.connector_store.connector_name_taken",
        lambda *a, **k: False,
    )
    monkeypatch.setattr(
        "src.transfer.connector_registry.run_probe",
        lambda *_a, **_k: (False, "connection refused"),
    )
    res = DataPilotTools()._create_connector(
        name="pg-railway-qa",
        type="postgresql",
        host="tokaido.proxy.rlwy.net",
        port=5432,
        database="app",
        username="app",
        password="secret",
        test_first=True,
    )
    assert res.success is False
    assert "Railway" in (res.error or "")


def test_service_account_camel_case_binds():
    from src.ai.copilot.connector_create import build_connector_draft
    from src.ai.copilot.tools import DataPilotTools as Tools
    from src.ai.copilot.tools import _bind_tool_arguments

    draft = build_connector_draft(
        "",
        {
            "name": "bq",
            "type": "bigquery",
            "database": "proj",
            "serviceAccount": '{"type":"service_account"}',
        },
    )
    assert draft["service_account"].startswith("{")
    bound, err = _bind_tool_arguments(
        "create_connector",
        Tools._create_connector,
        {"serviceAccount": '{"type":"service_account"}', "type": "bigquery"},
    )
    assert err == ""
    assert bound["service_account"].startswith("{")
    assert "serviceAccount" not in bound
