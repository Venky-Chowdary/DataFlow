"""D406, D408, D389, D384, D386.

Unit proofs. A passing test is not a live retest of the QA job.
"""

from __future__ import annotations

import sys
import types

import pytest


def _stub_requests() -> None:
    if getattr(sys.modules.get("requests"), "Session", None):
        return
    module = types.ModuleType("requests")
    module.Session = type("Session", (), {})
    sys.modules["requests"] = module


def test_pending_rollback_is_a_dropped_connection_and_rolls_back():
    from connectors.write_resilience import (
        is_connection_lost,
        pending_rollback,
        rollback_pending_transaction,
    )
    from services.error_taxonomy import classify

    text = "Can't reconnect until invalid transaction is rolled back"
    assert pending_rollback(text) is True
    assert is_connection_lost(text) is True
    # A data error is still not a dropped socket.
    assert is_connection_lost("unknown collation: utf8mb4_0900_ai_ci") is False
    classified = classify(text)
    assert classified is not None
    assert classified.code == "network.connection_lost"
    assert "couldn't identify" not in classified.message
    assert "invalid transaction" not in classified.message.lower()

    class _Conn:
        def __init__(self) -> None:
            self.rolled = False

        def rollback(self) -> None:
            self.rolled = True

    conn = _Conn()
    assert rollback_pending_transaction(conn) is True
    assert conn.rolled is True


def test_verified_transfer_stays_complete_when_the_session_then_dies():
    from src.transfer.job_failure import transfer_already_complete

    exc = RuntimeError("Can't reconnect until invalid transaction is rolled back")
    done = transfer_already_complete(
        exc,
        {
            "records_processed": 30,
            "destination_summary": {
                "source_row_count": 30,
                "reconcile": {"passed": True},
            },
        },
    )
    assert "30" in done
    assert "completed transfer" in done
    assert "invalid transaction" not in done.lower()
    assert transfer_already_complete(exc, {"records_processed": 30}) == ""


def _mongo_client(captured: dict):
    class _Result:
        inserted_ids = [1, 2]

    class _Coll:
        def insert_many(self, docs, ordered=False):
            captured["docs"] = list(docs)
            return _Result()

        def create_index(self, *_a, **_k):
            return "idx"

    class _DB:
        def list_collection_names(self, **_k):
            return []

        def __getitem__(self, _name):
            return _Coll()

    class _Client:
        def __getitem__(self, _name):
            return _DB()

    return _Client()


def _write(monkeypatch, *, mappings, rows, column_types, conflict=None):
    _stub_requests()
    from connectors.mongodb_writer import write_mapped_rows
    import connectors.mongodb_common as mc

    captured: dict = {}
    monkeypatch.setattr(mc, "_mongo_client", lambda *_a, **_k: _mongo_client(captured))
    result = write_mapped_rows(
        host="localhost",
        port=27017,
        database="db",
        username="",
        password="",
        connection_string="",
        ssl=False,
        schema="db",
        table_name="events",
        headers=["id", "name", "ts"],
        data_rows=rows,
        mappings=mappings,
        column_types=column_types,
        error_policy="quarantine",
        write_mode="insert",
        conflict_columns=conflict or ["id"],
    )
    return result, captured


def test_assume_timezone_and_ms_contract_lands_bson_dates(monkeypatch):
    from datetime import datetime, timezone

    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="ts",
        source_type="TIMESTAMP",
        destination_type="DATE",
        approved_by="qa@example.com",
        reason="Accept millisecond precision",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
        expected_truncation=True,
    ).to_dict()
    result, captured = _write(
        monkeypatch,
        mappings=[
            {"source": "id", "target": "id", "source_type": "INTEGER"},
            {"source": "name", "target": "name", "source_type": "VARCHAR", "transform": "trim"},
            {
                "source": "ts",
                "target": "ts",
                "source_type": "TIMESTAMP",
                "transform": "assume_timezone:UTC",
                "risk_contract": contract,
            },
        ],
        rows=[["1", "Ada", "2024-11-03 05:37:00.123456"], ["30", "Zoe", None]],
        column_types={"id": "INTEGER", "name": "VARCHAR", "ts": "TIMESTAMP"},
    )
    assert result.ok is True, result.error
    assert result.rows_written == 2
    docs = captured["docs"]
    # The writer hands the driver the UTC instant; the BSON Date it stores is
    # int64 milliseconds, so the encoded value is floored to .123 (the same
    # contract test_document_instant_precision pins on the writer).
    import bson

    stored = bson.decode(bson.encode({"ts": docs[0]["ts"]}), codec_options=bson.CodecOptions(tz_aware=True))
    assert stored["ts"] == datetime(2024, 11, 3, 5, 37, 0, 123000, tzinfo=timezone.utc)
    assert docs[0]["ts"].utcoffset() == timezone.utc.utcoffset(None)
    assert docs[1]["ts"] is None


def test_unacknowledged_sub_ms_names_every_refused_row(monkeypatch):
    result, captured = _write(
        monkeypatch,
        mappings=[
            {"source": "id", "target": "id", "source_type": "INTEGER"},
            {"source": "name", "target": "name", "source_type": "VARCHAR"},
            {
                "source": "ts",
                "target": "ts",
                "source_type": "TIMESTAMP",
                "transform": "assume_timezone:UTC",
            },
        ],
        rows=[["1", "Ada", "2024-11-03 05:37:00.123456"], ["2", "Bea", "2024-11-03 06:00:00.654321"]],
        column_types={"id": "INTEGER", "name": "VARCHAR", "ts": "TIMESTAMP"},
    )
    assert result.ok is False
    assert result.rows_written == 0
    assert "docs" not in captured
    assert result.rejected_rows == 2
    assert len(result.rejected_details) == 2
    assert sorted(d["source_pk"] for d in result.rejected_details) == [[1], [2]]
    assert "millisecond" in (result.rejected_details[0]["reason"] or "").lower()


def test_proof_cites_the_write_refusal_not_the_name_step():
    from src.transfer.reconcile_step import rejection_explains_shortfall

    reason = (
        "MongoDB date keeps milliseconds and 29 values in this batch carry finer digits"
    )
    text = rejection_explains_shortfall(
        {"expected_rows": 30, "actual_rows": 1},
        {
            "rejected_rows": 29,
            "rejected_details": [{"row": 1, "column": "ts", "reason": reason, "source_pk": [1]}],
        },
    )
    assert "29 row(s) were refused" in text
    assert "trim" not in text
    assert "did not apply" not in text
    # A real recipe miss that the refusals do not cover stays unexplained here.
    assert rejection_explains_shortfall(
        {"expected_rows": 30, "actual_rows": 10},
        {"rejected_rows": 1, "rejected_details": [{"reason": reason}]},
    ) == ""


def test_timescaledb_history_is_planned_and_cdc_stays_blocked():
    from services.destination_sync_modes import destination_sync_mode_refusal
    from services.preflight_cursor_gate import SQL_HISTORY_SYNC_DESTS, cdc_source_supported

    assert "timescaledb" in SQL_HISTORY_SYNC_DESTS
    assert "timescale" in SQL_HISTORY_SYNC_DESTS
    assert destination_sync_mode_refusal("timescaledb", "scd2") is None
    assert destination_sync_mode_refusal("timescaledb", "mirror") is None
    assert cdc_source_supported("timescaledb") is False
    assert cdc_source_supported("timescale") is False


def test_cloud_extract_states_the_refusal(monkeypatch):
    from services.exit_inventory import ExitInvalid
    from services.exit_job_reasons import engine_finding, failed_reason
    from services.exit_rehearsal import extract_request
    from services.local_file_policy import local_destination_message

    monkeypatch.setattr(
        "services.local_file_policy.local_file_destinations_allowed", lambda: False
    )
    monkeypatch.setattr(
        "services.export_object_store.exports_use_object_store", lambda: False
    )
    with pytest.raises(ExitInvalid) as raised:
        extract_request(
            {"id": "exr_1", "extract_format": "csv", "workspace_id": "w", "actor": "qa"},
            0,
            {"name": "orders", "key_columns": ["id"]},
            object(),
        )
    assert "File exports aren't available as a destination" in str(raised.value)
    assert "see the server log" not in str(raised.value)

    job = {"status": "failed", "error": local_destination_message("file_export")}
    assert "see the server log" not in engine_finding(job)
    assert "see the server log" not in failed_reason(job, "6afbb1055eafee38493e1923")
    assert "object store" in failed_reason(job, "6afbb1055eafee38493e1923").lower()


def test_overwrite_with_committed_rows_must_restart():
    from services.checkpoint_service import evaluate_resume_safety

    empty = {"chunk_index": 0, "rows_processed": 0, "offset": 0}
    job = {
        "status": "cancelled",
        "sync_mode": "full_refresh_overwrite",
        "records_processed": 4096,
        "message": "Cancelled after 4,096 row(s) were already committed",
        "transfer_request": {"sync_mode": "full_refresh_overwrite"},
    }
    out = evaluate_resume_safety(empty, job=job)
    assert out["ok"] is False
    assert any("overwrite must restart" in reason for reason in out["reasons"])
    assert not any("no committed progress" in reason for reason in out["reasons"])

    fresh = evaluate_resume_safety(
        empty,
        job={"transfer_request": {"sync_mode": "full_refresh_overwrite"}, "records_processed": 0},
    )
    assert any("no committed progress" in reason for reason in fresh["reasons"])


def test_dropped_partial_collection_says_so(monkeypatch):
    from src.transfer.models import EndpointConfig
    from services.batch_undo import _clear

    monkeypatch.setattr("services.batch_undo._drop_run_created", lambda *_a, **_k: True)
    note = _clear(
        EndpointConfig(kind="database", format="mongodb", database="db", table="events"),
        {"table": "events", "dest_table_existed_before": False, "rejected_rows": 29},
    )
    assert "was dropped" in note
    assert "29" in note
    assert "refused" in note
