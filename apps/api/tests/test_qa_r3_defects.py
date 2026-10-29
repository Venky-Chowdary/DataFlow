"""Round-3 QA regressions (Neon Postgres + S3).

Each test pins one defect from the round-3 report: empty object-store
exports, prefix, CDC wal_level, schedule claim, sqlite source files, query
caps, schema-drift acknowledgement, and the low-severity fidelity warnings.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_preflight_src = Path(__file__).resolve().parents[3] / "packages" / "preflight" / "src"
if _preflight_src.is_dir() and str(_preflight_src) not in sys.path:
    sys.path.insert(0, str(_preflight_src))

from connectors.object_store_common import normalize_object_base_key
from connectors.object_store_materialize import materialize_object_store_export
from connectors.sqlite_common import require_sqlite_source_file
from services.blank_cell_contract import typed_blank_stores_sql_null
from services.cdc_capability import (
    CAUSE_SERVER_NOT_CONFIGURED,
    LogCaptureRefusal,
    LogCaptureUnavailable,
)
from services.cdc_log_capture_probe import LogCaptureProbe
from services.mapping_quality import apply_confidence_class
from services.preflight_cursor_gate import build_sync_contract_gate
from services.preflight_population import _cdc_log_capture_gates
from services.root_cause_engine import _is_duplicate_signal
from services.standing_authorization import delegable_scopes_for
from services.transform_engine import ambiguous_number_columns
from src.ai.copilot.ack_ledger import redact_payload
from src.ai.copilot.query_tools import _MAX_QUERY_ROWS
from src.transfer.cdc_transfer import _query_cdc_downgrade
from src.transfer.models import EndpointConfig


def test_object_key_keeps_schema_prefix() -> None:
    assert (
        normalize_object_base_key("out/customers.jsonl", "QA_R3")
        == "QA_R3/out/customers.jsonl"
    )
    assert (
        normalize_object_base_key("QA_R3/out/customers.jsonl", "QA_R3")
        == "QA_R3/out/customers.jsonl"
    )


def test_empty_object_export_refuses_before_upload() -> None:
    result = materialize_object_store_export(
        key="out/customers.jsonl",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[{"source": "id", "target": "id"}],
        target_cols=[],
        column_types={"id": "INTEGER"},
        dest_types={},
        error_policy="strict",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=1024,
    )
    assert result.export is None
    assert result.rows_written == 0
    assert "no destination columns" in (result.abort_error or "")


def test_s3_create_new_when_existence_is_unproven() -> None:
    from services.semantic_mapper import map_columns

    rows = map_columns(
        ["id", "name"],
        [],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER"},
            {"name": "name", "inferred_type": "VARCHAR"},
        ],
        destination_db_type="s3",
        destination_table_exists=None,
    )
    assert rows
    assert all(row.get("assignment_strategy") != "pending_dest_schema" for row in rows)
    assert all(str(row.get("target_type") or "").strip() for row in rows)


def test_sql_unproven_catalog_stays_pending() -> None:
    from services.semantic_mapper import map_columns

    rows = map_columns(
        ["id"],
        [],
        source_schemas=[{"name": "id", "inferred_type": "INTEGER"}],
        destination_db_type="mysql",
        destination_table_exists=None,
    )
    assert rows
    assert rows[0].get("assignment_strategy") == "pending_dest_schema"


def test_failed_object_batch_is_deleted(monkeypatch) -> None:
    deleted: list[str] = []

    def _delete(kind, cfg, bucket, key):
        deleted.append(key)

    def _list(kind, cfg, bucket, key):
        return [] if deleted else ["QA_R3/out/customers.jsonl"]

    monkeypatch.setattr(
        "connectors.object_store_leftover._delete_object_key", _delete
    )
    monkeypatch.setattr("services.dest_precount.observed_object_export_keys", _list)
    from services.batch_undo import undo_failed_batch_if_dest_was_empty
    from services.dest_precount import PRECOUNT_KEY

    note = undo_failed_batch_if_dest_was_empty(
        destination=EndpointConfig(
            kind="database",
            format="s3",
            database="bucket",
            schema="QA_R3",
            table="out/customers.jsonl",
        ),
        dest_summary={PRECOUNT_KEY: 0, "table": "out/customers.jsonl"},
        sync_mode="full_refresh_overwrite",
    )
    assert deleted
    assert "still empty" not in note
    assert "removed" in note


def test_absent_export_is_not_reported_as_left_in_the_bucket(monkeypatch) -> None:
    """Gate-8's read set invents ``[base]`` when the prefix is empty.

    Undo used to treat that invented key as a file that survived the delete
    (R7-001). An empty listing is an absent object.
    """
    monkeypatch.setattr(
        "connectors.s3_reader.list_objects", lambda cfg, bucket, prefix="": []
    )
    monkeypatch.setattr(
        "connectors.object_store_leftover._delete_object_key", lambda *a, **k: None
    )
    from services.batch_undo import undo_failed_batch_if_dest_was_empty
    from services.dest_precount import PRECOUNT_KEY

    note = undo_failed_batch_if_dest_was_empty(
        destination=EndpointConfig(
            kind="database",
            format="s3",
            database="bucket",
            table="exports/payments.json",
        ),
        dest_summary={PRECOUNT_KEY: 0, "table": "exports/payments.json"},
        sync_mode="full_refresh_overwrite",
    )
    assert "still in the bucket" not in note
    assert "may still" not in note
    assert "absent" in note


def test_json_null_export_matches_postgres_text_and_sql_null_stays_absent() -> None:
    """jsonb ``null`` is the text ``null``. SQL NULL is an omitted key (R7-001).

    Parsing the export token back to Python None made Gate-8 fingerprint the
    destination as SQL NULL (``\\x00NULL\\x00``) against a source cell ``null``,
    and the undo note then claimed a partial object that the delete had removed.
    """
    import json

    from connectors.writer_common import mapped_row_to_json_record
    from services.dest_precount import _json_null_cells_as_engine_text
    from services.reconciliation import fingerprint_for_reconcile
    from services.value_serializer import json_dumps_exact_numbers

    types = {"payload": "JSON"}
    present = mapped_row_to_json_record(
        ("null",), ["payload"], types, omit_sql_null=True
    )
    absent = mapped_row_to_json_record(
        (None,), ["payload"], types, omit_sql_null=True
    )
    assert present.get("payload") is not None
    assert "payload" not in absent
    loaded = json.loads(json_dumps_exact_numbers([present, absent]))
    reread = list(_json_null_cells_as_engine_text(loaded))
    assert reread[0]["payload"] == "null"
    assert "payload" not in reread[1]
    source_null = fingerprint_for_reconcile(
        "null", ddl_type="JSON", engine="postgresql"
    )
    dest_null = fingerprint_for_reconcile(
        reread[0]["payload"], ddl_type="JSON", engine="s3"
    )
    source_sql = fingerprint_for_reconcile(None, ddl_type="JSON", engine="postgresql")
    dest_sql = fingerprint_for_reconcile(None, ddl_type="JSON", engine="s3")
    assert source_null == dest_null
    assert source_sql == dest_sql
    assert source_null != source_sql


def test_postgres_json_null_exports_to_s3_without_a_false_leftover() -> None:
    """Postgres → S3 must checksum jsonb null, and a failed undo must not invent a key.

    The wide row is the Round-7 shape: JSON null, SQL NULL, numeric, bytea,
    uuid, date, time, and a text array. Gate-8 used to stop at 99% on
    ``payload`` and then claim ``export.json`` was still in the bucket.
    """
    import uuid

    try:
        import moto
        import psycopg2
    except ImportError as exc:
        pytest.skip(f"postgres/moto export proof needs psycopg2 and moto: {exc}")

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    src_table = "pg_s3_json_null_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE {src_table} (
            id int PRIMARY KEY,
            amount numeric(38,10),
            payload jsonb,
            raw bytea,
            uid uuid,
            day date,
            clock time,
            tags text[],
            blank text
        )
        """
    )
    cur.execute(
        f"""
        INSERT INTO {src_table} VALUES
        (
            1,
            1000.50,
            'null'::jsonb,
            decode('00ff', 'hex'),
            '11111111-1111-1111-1111-111111111111',
            DATE '2024-01-02',
            TIME '03:04:05',
            ARRAY['a','b'],
            ''
        ),
        (
            2,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
        )
        """
    )
    conn.close()
    key = f"exports/{src_table}/export.json"
    try:
        with moto.mock_aws():
            request = TransferRequest(
                source=EndpointConfig(
                    kind="database",
                    format="postgresql",
                    host="127.0.0.1",
                    port=5432,
                    database="dataflow",
                    username="dataflow",
                    password="dataflow",
                    schema="public",
                    table=src_table,
                ),
                destination=EndpointConfig(
                    kind="database",
                    format="s3",
                    host="s3.amazonaws.com",
                    port=443,
                    database="dataflow-r7",
                    username="mock",
                    password="mock",
                    table=key,
                    region="us-east-1",
                ),
                sync_mode="full_refresh_overwrite",
                stream_contracts=[
                    {
                        "name": "pg_s3",
                        "sync_mode": "full_refresh_overwrite",
                        "primary_key": "id",
                        "selected": True,
                    }
                ],
                skip_preflight=True,
                validation_mode="strict",
            )
            result = UniversalTransferEngine().execute_tracked(
                request, uuid.uuid4().hex[:24]
            )
            error = str(result.error or "")
            assert "still in the bucket" not in error
            assert result.success is True, error
            recon = result.reconciliation or {}
            assert recon.get("passed") is True, recon
            assert recon.get("source_checksum") == recon.get("target_checksum")
    finally:
        cleanup = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
        )
        cleanup.autocommit = True
        cleanup.cursor().execute(f"DROP TABLE IF EXISTS {src_table}")
        cleanup.close()


def test_postgres_wal_probe_runs_without_a_primary_key(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def _probe(source_type, cfg, **kwargs):
        seen["pk"] = str(kwargs.get("primary_key") or "")
        return LogCaptureProbe(
            "postgresql",
            False,
            "pgoutput",
            CAUSE_SERVER_NOT_CONFIGURED,
            "wal_level=replica — logical decoding is off on this server",
        )

    monkeypatch.setattr(
        "services.cdc_log_capture_probe.probe_log_capture", _probe
    )
    gates = _cdc_log_capture_gates(
        [{"name": "customers"}],
        source_type="postgresql",
        source_config={"host": "neon.example", "database": "db"},
        source_table="customers",
        catalog_primary_key_columns=None,
        mappings=None,
    )
    assert "pk" in seen
    assert gates
    assert gates[0]["status"] == "block"
    assert "wal_level" in gates[0]["message"] or "change log" in gates[0]["message"]


def test_wal_level_is_not_a_duplicate_key() -> None:
    assert _is_duplicate_signal(
        "PostgreSQL wal_level is not logical (wal_level=replica). "
        "the poll cursor 'id' is the primary key",
        {},
        "execute",
    ) is False


def test_s3_cdc_names_the_reason() -> None:
    gate = build_sync_contract_gate(
        [],
        sync="cdc",
        validation="strict",
        dest="s3",
        src="postgresql",
        kind="database",
        source_columns=["id"],
        pass_status="pass",
        block_status="block",
    )
    assert gate["status"] == "block"
    assert gate["message"] != "Sync mode contract incomplete"
    assert "write-ahead log" in gate["message"] or "Object stores" in gate["message"]


def test_distinct_schedule_tables_are_not_serialized(tmp_path, monkeypatch) -> None:
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    a = store.create_schedule({
        "name": "a",
        "source_connector_id": "src",
        "source_table": "customers",
        "dest_connector_id": "dst",
        "dest_table": "customers",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    b = store.create_schedule({
        "name": "b",
        "source_connector_id": "src",
        "source_table": "payments",
        "dest_connector_id": "dst",
        "dest_table": "payments",
        "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert store.mark_schedule_running(a.id, "inst-1") is not None
    assert store.mark_schedule_running(b.id, "inst-2") is not None


def test_missed_slots_survive_the_claim(tmp_path, monkeypatch) -> None:
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    sched = store.create_schedule({
        "name": "minutely",
        "source_connector_id": "src",
        "source_table": "t",
        "dest_connector_id": "dst",
        "dest_table": "u",
        "interval": "hourly",
        "cron": "*/1 * * * *",
        "mappings": [{"source": "id", "target": "id"}],
    })
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    current = store.get_schedule(sched.id)
    assert current is not None
    store._save_all([
        store.PipelineSchedule.from_dict({**current.to_dict(), "next_run_at": past})
    ])
    claimed = store.mark_schedule_running(sched.id, "inst")
    assert claimed is not None
    assert claimed.claimed_missed_windows > 0
    finished = store.mark_schedule_run(
        sched.id, "job-1", status="completed", run_entry={"job_id": "job-1"}
    )
    assert finished is not None
    assert finished.missed_window_count >= claimed.claimed_missed_windows
    assert finished.last_missed_windows >= claimed.claimed_missed_windows


def test_missing_sqlite_source_is_not_created(tmp_path) -> None:
    missing = tmp_path / "gone.db"
    with pytest.raises(ValueError, match="not on this server"):
        require_sqlite_source_file(str(missing))
    assert not missing.exists()


def test_query_cap_matches_the_rest_limit() -> None:
    assert _MAX_QUERY_ROWS == 10_000


def test_s3_preview_hides_the_access_key() -> None:
    redacted = redact_payload({
        "type": "s3",
        "username": "AKIAEXAMPLE",
        "password": "secret",
        "database": "bucket",
    })
    assert redacted["username"] == "***"
    assert redacted["has_username"] is True
    assert redacted["database"] == "bucket"
    plain = redact_payload({"type": "postgresql", "username": "alice"})
    assert plain["username"] == "alice"


def test_text_empty_string_stays_stored() -> None:
    assert typed_blank_stores_sql_null(
        "",
        {"target_type": "TEXT"},
        "note",
        empty_cells_as_null=True,
        dest_nullability={},
    ) is False
    assert typed_blank_stores_sql_null(
        "",
        {"target_type": "INTEGER"},
        "qty",
        empty_cells_as_null=True,
        dest_nullability={},
    ) is True
    assert typed_blank_stores_sql_null(
        "",
        {},
        "phone",
        empty_cells_as_null=True,
        dest_nullability={},
    ) is True


def test_number_locale_skips_a_text_column() -> None:
    rows = [{"label": "1,234", "amount": "1,234"}]
    assert ambiguous_number_columns(
        rows, ["label"], column_types={"label": "TEXT"}
    ) == []
    found = ambiguous_number_columns(
        rows, ["amount"], column_types={"amount": "NUMERIC"}
    )
    assert found and found[0]["column"] == "amount"


def test_sql_expression_create_new_clears_the_confidence_floor() -> None:
    lifted = apply_confidence_class(
        0.84,
        {
            "confidence_class": "create_new_projected",
            "type_path_class": "cast",
            "source_expr": "round(amount*1.18, 2)",
            "semantic_role": "",
        },
    )
    assert lifted >= 0.91
    blocked = apply_confidence_class(
        0.84,
        {
            "confidence_class": "create_new_projected",
            "type_path_class": "cast",
            "source_expr": "notes",
            "type_same_declared": False,
            "semantic_role": "",
        },
    )
    assert blocked < 0.85


def test_schema_change_detected_is_delegable() -> None:
    scopes = delegable_scopes_for(
        "Schema change detected — policy 'manual_review' requires review "
        "before Execute. Use Acknowledge drift for this run"
    )
    assert scopes
    assert delegable_scopes_for(
        "Schema change detected — type change narrowing on amount"
    ) == ()


def test_cdc_policy_reads_wal_level_from_the_plan_endpoint() -> None:
    """plan_transfer passes the connector as source_endpoint. The policy gate
    must still read wal_level and refuse replica (R3-H1).
    """
    import socket

    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")

    from services.preflight_service import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        sync_mode="cdc",
        source_type="postgresql",
        source_kind="database",
        source_table="qa_r3_src",
        stream_contracts=[{"name": "qa_r3_src", "primary_key": ["id"]}],
        source_endpoint={
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
        },
    )
    import psycopg2

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            level = str(cur.fetchone()[0]).lower()
    finally:
        conn.close()
    wal_blocks = [
        g
        for g in gates
        if g.get("status") == "block" and "wal_level" in str(g.get("message") or "")
    ]
    # The probe reads this server. replica/minimal must block and name
    # wal_level. logical is allowed through that gate (R3-H1).
    if level == "logical":
        assert not wal_blocks, wal_blocks
    else:
        assert wal_blocks, gates


def test_postgres_wal_downgrade_polls_only_a_declared_cursor() -> None:
    """wal_level=replica is a server with no change log (D28).

    The run may poll a declared modification-timestamp cursor; a poll on the
    primary key is still refused by ``query_cdc_change_refusal``, and the run
    appends the downgrade detail so that refusal names wal_level (R3-H1).
    """
    from services.cdc_capability import query_cdc_change_refusal

    refusal = LogCaptureRefusal(
        CAUSE_SERVER_NOT_CONFIGURED,
        "wal_level=replica — logical decoding is off on this server",
        "set wal_level=logical",
    )
    fields = _query_cdc_downgrade(LogCaptureUnavailable(refusal, "postgresql"), "postgresql")
    assert fields["cdc_capture_used"] == "query_cursor"
    assert "wal_level" in str(fields["cdc_capture_downgrade_detail"])
    assert query_cdc_change_refusal(
        dialect="postgresql", cursor_field="id", cursor_semantics="",
        primary_key_columns=["id"], downgrade_cause=CAUSE_SERVER_NOT_CONFIGURED,
    )
    assert not query_cdc_change_refusal(
        dialect="postgresql", cursor_field="updated_at",
        cursor_semantics="modification_timestamp",
        primary_key_columns=["id"], downgrade_cause=CAUSE_SERVER_NOT_CONFIGURED,
    )
