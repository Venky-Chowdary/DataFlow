"""D388, D385, D390, D394, D398. Unit proofs, not a live retest."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from services.parity_evidence import _check_kind
from services.parity_report import build_parity_report
from services.quarantine_dlq import quarantine_row_ledger
from services.reversibility import LOSSY, classify_mapping
from services.reversibility_sources import (
    _catalog_or_create_new_types,
    execute_gate,
    plan_report,
)
from src.transfer.reconcile_step import canonical_bson_instant


def _db(fmt, table):
    return SimpleNamespace(
        kind="database", format=fmt, db_type=fmt, table=table, schema="",
        extra={}, connector_id="",
    )


def _req(mappings, *, column_types):
    return SimpleNamespace(
        source=_db("postgresql", "qav4_seed"),
        destination=_db("sqlserver", "qv4_rv1"),
        mappings=mappings,
        require_reversibility=True,
        column_types=column_types,
        stream_contracts=[],
        shape_recipe=None,
        read_options=None,
        limit=0,
        source_filter=None,
        job_id="j",
    )


def test_jsonb_on_a_new_sql_server_table_is_reversible_and_a_hash_is_named():
    types = _catalog_or_create_new_types(
        [{"source": "payload", "source_type": "jsonb"}],
        {},
        dest_db="sqlserver",
        source_db="postgresql",
    )
    assert "NVARCHAR(MAX)" in types["payload"].upper()
    live = _catalog_or_create_new_types(
        [{"source": "payload", "source_type": "jsonb"}],
        {"payload": "nvarchar(20)"},
        dest_db="sqlserver",
        source_db="postgresql",
    )
    assert live["payload"] == "nvarchar(20)"

    payload = {"source": "payload", "target": "payload", "source_type": "jsonb"}
    ident = {"source": "id", "target": "id", "source_type": "integer"}
    gate = execute_gate(
        _req([ident, payload], column_types={"id": "integer", "payload": "jsonb"}),
        ([ident, payload], {"id": "integer", "payload": "jsonb"}, {}, [], None),
    )
    assert gate["status"] == "pass", gate
    assert gate["details"]["lossy_columns"] == []
    assert gate["details"]["verdict"] == "reversible"

    hashed = {**payload, "transform": "hash_pii"}
    blocked = execute_gate(
        _req([ident, hashed], column_types={"id": "integer", "payload": "jsonb"}),
        ([ident, hashed], {"id": "integer", "payload": "jsonb"}, {}, [], None),
    )
    assert blocked["status"] == "block"
    assert any(col["column"] == "payload" for col in blocked["details"]["lossy_columns"])
    assert "qv4_rv1.payload" in blocked["message"]

    rev = SimpleNamespace(mappings=[ident, hashed])
    plan = SimpleNamespace(
        id="p", source={"kind": "database", "format": "postgresql", "table": "qav4_seed"},
        destination={"kind": "database", "format": "sqlserver", "table": "qv4_rv1"},
        source_columns=["id", "payload"],
        source_schema={"id": "integer", "payload": "jsonb"},
        target_schema={},
        sample_rows=[],
        policies={"require_reversibility": True},
        active_revision=lambda: rev,
    )
    report = plan_report(plan)
    assert report["gate"]["status"] == "block"
    assert any(col["column"] == "payload" for col in report["lossy_columns"])


def test_a_case_or_trim_that_changes_values_is_lossy_and_a_no_op_stays_conditional():
    dirty = classify_mapping(
        {"source": "name", "target": "name", "source_type": "text", "target_type": "nvarchar(max)",
         "transform": "upper"},
        observed=["ADA", "bob"],
        dest_db="sqlserver",
        source_db="postgresql",
    )
    assert dirty["invertibility"] == LOSSY
    assert dirty["reason_code"] == "folding"
    assert "1 of 2" in dirty["reason"]

    trimmed = classify_mapping(
        {"source": "name", "target": "name", "source_type": "text", "target_type": "text",
         "transform": "trim"},
        observed=["a", " b"],
    )
    assert trimmed["invertibility"] == LOSSY
    assert "1 of 2" in trimmed["reason"]

    clean = classify_mapping(
        {"source": "name", "target": "name", "source_type": "text", "target_type": "text",
         "transform": "trim"},
        observed=["a", "b"],
    )
    assert clean["invertibility"] == "conditional"
    assert clean["check"]["status"] == "fits"


def test_two_findings_on_one_row_are_one_rejected_row():
    details = [
        {"row": 7, "column": "name", "reason": "too long"},
        {"row": 7, "column": "note", "reason": "too long"},
    ]
    ledger = quarantine_row_ledger(
        details, job_rejected_rows=1, details_total=2,
    )
    assert ledger["rejected_rows"] == 1
    assert ledger["issue_count"] == 2
    assert ledger["finding_rows"] == 1
    assert ledger["rows_unaccounted"] == 0
    closed = quarantine_row_ledger(details, job_rejected_rows=1, details_total=2)
    assert closed["rows_unaccounted"] == 0


def test_a_mirror_read_back_that_matched_is_not_an_uncompared_count():
    recon = {
        "passed": True,
        "source_rows": 30,
        "target_rows": 30,
        "source_checksum": "abc",
        "target_checksum": "abc",
        "coverage": "write_pass_dest_readback",
    }
    assert _check_kind(recon, sync_mode="mirror") == "readback"
    assert _check_kind({"passed": True, "source_rows": 30, "target_rows": 30}, sync_mode="mirror") == "count_only"
    job = {
        "_id": "job-1",
        "status": "completed",
        "reconciliation": recon,
        "destination_summary": {
            "table": "orders",
            "primary_key_columns": ["id"],
            "sync_mode": "mirror",
        },
    }
    report = build_parity_report(job, reread=False)
    assert report["verdict"] == "no_difference_found"
    assert "Values were not compared" not in report["verdict_reason"]
    assert "tombstoned rows are outside the live population" in report["verdict_reason"]
    # P0-3: the confidence block agrees with the verdict (a value comparison ran).
    assert report["confidence"]["method"] == "readback"
    assert "No value comparison ran" not in report["confidence"]["statement"]


def test_assume_timezone_and_a_bson_date_match_at_milliseconds():
    bson = datetime(2024, 3, 10, 1, 30, 0, 123456, tzinfo=timezone.utc)
    wall = "2024-03-10 01:30:00.123999+00:00"
    zulu = "2024-03-10T01:30:00.123Z"
    assert canonical_bson_instant(bson) == canonical_bson_instant(wall) == canonical_bson_instant(zulu)
    assert canonical_bson_instant(bson) == "2024-03-10T01:30:00.123"
    later = datetime(2024, 3, 10, 1, 30, 0, 124000, tzinfo=timezone.utc)
    assert canonical_bson_instant(later) != canonical_bson_instant(bson)
    assert canonical_bson_instant("Ada") == "Ada"
