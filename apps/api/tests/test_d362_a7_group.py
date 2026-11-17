"""A7 group: Oracle LOB probe, skip counts, workspace stamp, file dedupe,
empty run, SFTP pin, cancel drop, paused preview, hosted SQLite, DSN refusal,
REST error reference.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.oracle import CLOB, NCLOB


def test_oracle_clob_probe_uses_dbms_lob_substr_not_in():
    from services.null_marker_probe import _exact_in

    dialect = sa.dialects.oracle.dialect()
    values = sa.bindparam("m", value=["__df_sql_null__"], expanding=True, literal_execute=True)
    for type_ in (CLOB(), NCLOB()):
        clause = _exact_in(sa.column("NOTE", type_), "oracle", values)
        sql = str(clause.compile(dialect=dialect)).upper()
        assert "DBMS_LOB.SUBSTR" in sql
        assert " IN " in sql
    plain = _exact_in(sa.column("NAME", sa.String(40)), "oracle", values)
    plain_sql = str(plain.compile(dialect=dialect)).upper()
    assert "DBMS_LOB" not in plain_sql


def test_lob_comparison_error_names_the_real_cause():
    from services.null_marker_guard import _unmeasured_note

    text = _unmeasured_note({
        "table": "R42_PG_ORA",
        "reason": "ORA-22848: cannot use CLOB type as comparison key",
    })
    assert "DBMS_LOB.SUBSTR" in text
    assert "ORA-22848" in text
    assert "balanced" not in text.lower()


def test_rows_skipped_is_promoted_onto_the_job_and_the_trust_line():
    from services.job_status import promote_rows_skipped
    from services.reconcile_coverage import append_row_count_report

    updates: dict = {"destination_summary": {"rows_skipped": 30, "rows_written": 0}}
    promote_rows_skipped(updates, None)
    assert updates["rows_skipped"] == 30
    report = append_row_count_report(
        source_rows=30,
        target_rows=30,
        expected_rows=0,
        source_checksum="a",
        target_checksum="b",
        sample_note="",
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=30,
        sample_compare=None,
        target_rows_before=30,
        keyed_expected_delta=0,
    )
    assert report.passed
    assert "kept as already stored" in report.message
    assert "30" in report.message


def test_start_transfer_payload_keeps_the_caller_workspace():
    from services.confirmed_transfer import transfer_request_from_ack
    from src.ai.copilot.ack_ledger import stamp_caller_workspace

    stamped = stamp_caller_workspace({"workspace_id": "", "source": {}})
    # No bound request in this process: an empty stamp stays empty.
    assert "workspace_id" not in stamped or stamped.get("workspace_id") == ""
    explicit = stamp_caller_workspace({"workspace_id": "ws-1"})
    assert explicit["workspace_id"] == "ws-1"
    request = transfer_request_from_ack({
        "workspace_id": "ws-9",
        "source": {"kind": "database", "connector_id": "src", "format": "postgresql"},
        "destination": {"kind": "database", "connector_id": "dst", "format": "postgresql"},
    })
    assert request.workspace_id == "ws-9"


def test_file_overwrite_with_a_dedupe_key_keeps_the_last_row():
    from services.data_integrity import DUPLICATE_KEY_ROUTES
    from services.duplicate_policy import keep_last_reads_whole, keep_last_records
    from services.preflight_source_kind import file_overwrite_keep_last, source_is_record_batch

    assert source_is_record_batch("database", "minio")
    assert source_is_record_batch("database", "postgresql") is False
    assert file_overwrite_keep_last(
        source_kind="database", source_format="minio",
        sync_mode="full_refresh_overwrite", dedupe_key="id",
    )
    assert not file_overwrite_keep_last(
        source_kind="database", source_format="postgresql",
        sync_mode="full_refresh_overwrite", dedupe_key="id",
    )
    assert "dedupe_key" in DUPLICATE_KEY_ROUTES
    assert "in_file_dedupe=keep_last" in DUPLICATE_KEY_ROUTES
    assert "duplicate_policy=append" in DUPLICATE_KEY_ROUTES
    req = SimpleNamespace(
        source=SimpleNamespace(kind="database", format="minio", extra={"in_file_dedupe": "keep_last"}),
        destination=SimpleNamespace(extra={}),
        stream_contracts=[{"name": "r43", "primary_key": ["id"], "selected": True, "sync_mode": "full_refresh_overwrite"}],
    )
    assert keep_last_reads_whole(req)
    rows = [{"id": "3", "name": "first"}, {"id": "3", "name": "dup_three"}]
    assert keep_last_records(req, rows) == [{"id": "3", "name": "dup_three"}]


def test_sftp_pin_in_extra_reaches_a_nested_path_and_a_missing_folder_is_named(monkeypatch):
    from connectors.sftp_common import host_key_settings, parse_sftp_config, test_sftp

    cfg = {"host": "files.example", "username": "u", "extra": {"host_key": "SHA256:pinned"}}
    assert host_key_settings(cfg)["host_key"] == "SHA256:pinned"
    parsed = parse_sftp_config(
        host="files.example", username="u", table="qav3/q3_csv.csv", extra={"host_key": "SHA256:pinned"}
    )
    assert parsed.host_key == "SHA256:pinned"
    assert parsed.path.endswith("qav3/q3_csv.csv")

    class _SFTP:
        def stat(self, path):
            raise OSError("no such file")

        def close(self):
            return None

    monkeypatch.setattr(
        "connectors.sftp_common.connect_sftp",
        lambda _cfg: (SimpleNamespace(close=lambda: None), _SFTP()),
    )
    ok, message = test_sftp(
        host="files.example", username="u", table="qav3/q3_csv.csv",
        host_key="SHA256:pinned", require_object=False,
    )
    assert ok, message
    assert "qav3" in message and "does not exist yet" in message
    assert "not trusted" not in message


def test_cancel_names_and_drops_a_table_this_run_created(monkeypatch):
    from services.batch_undo import note_dropped_empty_create
    from services.error_handling import TransferCancelled
    from services.job_status import cancel_outcome_for

    dropped: list[tuple] = []

    def _drop(cfg, table, schema=None):
        dropped.append((table, schema))
        return True

    monkeypatch.setattr("connectors.generic_sql.drop_table", _drop)
    exc = TransferCancelled("cancelled", rows_written=0)
    note_dropped_empty_create(exc, {"type": "sqlserver"}, "r44_big_ms", "dbo")
    assert dropped == [("r44_big_ms", "dbo")]
    outcome = cancel_outcome_for(
        0, destination_rows_by_run=0, destination_table_exists=False,
        dropped_created_table=exc.dropped_created_table,
    )
    assert "dbo.r44_big_ms" in outcome["message"]
    assert "dropped" in outcome["message"]
    assert outcome["rows_committed"] == 0
    # A pre-existing table is not passed to the drop. The message without the
    # stamp does not claim a drop.
    plain = cancel_outcome_for(0, destination_rows_by_run=0, destination_rows_now=0)
    assert "dropped" not in plain["message"]


def test_paused_schedule_preview_stays_paused():
    from services.schedule_store import coerce_enabled, schedule_is_enabled

    assert coerce_enabled("false") is False
    assert coerce_enabled("true") is True
    assert coerce_enabled(False) is False
    paused = SimpleNamespace(enabled="false")
    assert schedule_is_enabled(paused) is False
    assert schedule_is_enabled({"enabled": False}) is False


def test_hosted_sqlite_source_is_refused_before_any_open(monkeypatch, tmp_path):
    from services.local_file_policy import sqlite_hosted_refusal

    monkeypatch.setenv("ENV", "test")
    monkeypatch.setenv("RAILWAY_DEPLOYMENT_ID", "d276-src")
    monkeypatch.delenv("DATAWRAP_SQLITE_DESTINATION", raising=False)
    monkeypatch.delenv("DATAFLOW_SQLITE_DESTINATION", raising=False)
    monkeypatch.delenv("DATAWRAP_LOCAL_FILE_DESTINATIONS", raising=False)
    message = sqlite_hosted_refusal("sqlite", f"sqlite:///{tmp_path}/qav.db")
    assert message and "isn't available as a destination" in message
    assert "source" in message
    opened: list[str] = []
    monkeypatch.setattr(sqlite3, "connect", lambda *a, **k: opened.append("opened"))
    from services.connector_store import create_connector

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    import services.connector_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(store, "_backend_choice", "file")
    with pytest.raises(ValueError, match="isn't available as a destination"):
        create_connector({
            "name": "qa-sqlite",
            "type": "sqlite",
            "role": "source",
            "database": str(tmp_path / "qav_test.db"),
        })
    assert opened == []


def test_server_side_dsn_keys_are_refused_and_a_driver_name_is_not():
    from services.connector_store import server_side_dsn_refusal

    for raw in (
        "postgresql://db.example/app?service=prod",
        "postgresql:///?servicefile=/etc/pg_service.conf",
        "host=db passfile=/home/app/.pgpass",
        "host=db sslkey=/etc/ssl/client.key",
        "DSN=Prod;UID=sa;",
        "FILEDSN=/etc/odbc/app.dsn",
        "Driver=/opt/odbc/libmsodbcsql.so;Server=db;",
    ):
        reason = server_side_dsn_refusal(raw)
        assert reason, raw
        assert "this server" in reason
    allowed = server_side_dsn_refusal(
        "Driver={ODBC Driver 18 for SQL Server};Server=db;Database=app;"
    )
    assert allowed == ""


def test_rest_errors_keep_detail_and_add_a_reference():
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient

    from src.main import http_exception_reference

    app = FastAPI()
    app.add_exception_handler(HTTPException, http_exception_reference)

    @app.get("/missing")
    def _missing():
        raise HTTPException(status_code=404, detail="Job not found")

    body = TestClient(app).get("/missing").json()
    assert body["detail"] == "Job not found"
    assert body["error"]["reference"]
    assert body["error"]["code"] == "platform.not_found"


def test_empty_transfer_run_names_both_sides(monkeypatch):
    """The guard is the first lines of the route. Exercise it through the app."""
    from fastapi.testclient import TestClient

    from src.main import app

    client = TestClient(app)
    response = client.post("/api/v1/transfer/run", data={})
    assert response.status_code == 422, response.text
    assert "source" in response.json()["detail"] and "destination" in response.json()["detail"]
    assert response.json()["error"]["reference"]
