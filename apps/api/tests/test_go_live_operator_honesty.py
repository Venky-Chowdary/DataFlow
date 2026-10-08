"""Operator-facing honesty for sync advice, grants, Kafka, SQL, and acks.

These are the shared paths behind DEF-C-011, DEF-C-015, DEF-C-039, DEF-C-040,
DEF-C-003, DEF-C-014, DEF-C-021, DEF-B-012, and DEF-C-012. A unit pass is not
a live QA retest.
"""

from __future__ import annotations

import importlib
import time

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
