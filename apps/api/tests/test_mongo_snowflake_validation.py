"""MongoDB → Snowflake validation robustness — value-aware coercion + AI assist.

These tests prove the real fix for the reported failure: schemaless MongoDB
sources widened to TEXT no longer produce false "lossy coercion" hard-blocks,
while genuinely non-castable values (e.g. a bare scalar into a VARIANT/JSON
column) are still caught with per-row/per-value evidence and an actionable fix.

Most tests are pure (no external services). One end-to-end test runs against a
real local MongoDB + the fakesnow Snowflake emulator and skips cleanly when they
are unavailable.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.coercion_probe import analyze_coercion
from services.preflight_service import (
    apply_policy_gates,
    confidence_threshold_for_mode,
    run_file_preflight,
    run_transfer_policy_gates,
)
from services.validation_assistant import explain_validation


# ─────────────────────────────────────────────────────────────────────────────
# Unit: value-aware coercion probe mirrors the write path
# ─────────────────────────────────────────────────────────────────────────────
def test_probe_clean_numeric_text_to_number_does_not_block():
    """All sampled values are valid numbers → no failure, no false block."""
    report = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "3.14"}, {"score": "42"}],
        mappings=[{"source": "score", "target": "score"}],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,10)"},
        dest_db_type="snowflake",
    )
    col = report["by_source"]["score"]
    assert col["failed"] == 0
    assert col["severity"] == "ok"
    assert report["has_blocking_failures"] is False


def test_probe_placeholder_values_become_null_and_warn():
    """Non-empty placeholders (N/A) coerce to NULL — surfaced as a warning."""
    report = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "N/A"}, {"score": "20"}],
        mappings=[{"source": "score", "target": "score"}],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,0)"},
        dest_db_type="snowflake",
    )
    col = report["by_source"]["score"]
    assert col["failed"] == 0
    assert col["sentinel_nulls"] == 1
    assert col["severity"] == "warn"
    assert report["has_blocking_failures"] is False


def test_probe_hard_failure_reports_row_value_reason():
    """A genuinely non-numeric value is a hard failure with evidence."""
    report = analyze_coercion(
        sample_rows=[{"amount": "100"}, {"amount": "not-a-number"}],
        mappings=[{"source": "amount", "target": "amount"}],
        source_types={"amount": "TEXT"},
        dest_types={"amount": "NUMBER(38,10)"},
        dest_db_type="snowflake",
    )
    col = report["by_source"]["amount"]
    assert col["failed"] == 1
    assert col["severity"] == "block"
    assert col["sample_failures"][0]["row"] == 1
    assert "not-a-number" in col["sample_failures"][0]["value"]
    assert col["suggested_target_type"]  # a safe widen target is suggested
    assert report["has_blocking_failures"] is True


def test_probe_bare_scalar_into_variant_wraps_losslessly():
    """MongoDB field that is an array in one doc and a scalar in another:
    the scalar is losslessly wrapped as JSON so it loads into VARIANT — no
    false hard-block (item 1 auto-wrap)."""
    report = analyze_coercion(
        sample_rows=[{"tags": '["a","b"]'}, {"tags": "single"}, {"tags": ""}],
        mappings=[{"source": "tags", "target": "tags"}],
        source_types={"tags": "TEXT"},
        dest_types={"tags": "VARIANT"},
        dest_db_type="snowflake",
    )
    col = report["by_source"].get("tags")
    # Column is analyzed (TEXT→VARIANT coercion) but every value now coerces.
    assert col is not None
    assert col["failed"] == 0
    assert col["severity"] == "ok"
    assert report["has_blocking_failures"] is False


def test_probe_valid_json_into_variant_passes():
    report = analyze_coercion(
        sample_rows=[{"profile": '{"age":30}'}, {"profile": '{"age":"x"}'}],
        mappings=[{"source": "profile", "target": "profile"}],
        source_types={"profile": "TEXT"},
        dest_types={"profile": "VARIANT"},
        dest_db_type="snowflake",
    )
    # profile is always valid JSON → no entry / no block
    assert report["has_blocking_failures"] is False


def test_probe_text_target_is_never_a_risk():
    report = analyze_coercion(
        sample_rows=[{"name": "alice"}, {"name": "bob"}],
        mappings=[{"source": "name", "target": "name"}],
        source_types={"name": "TEXT"},
        dest_types={"name": "VARCHAR"},
        dest_db_type="snowflake",
    )
    assert report["checked"] == 0
    assert report["has_blocking_failures"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Value-aware G3: no false blocks, real failures still caught
# ─────────────────────────────────────────────────────────────────────────────
def _run_preflight(sample_rows, dest_types, validation_mode="strict"):
    headers = list(dest_types.keys())
    mappings = [{"source": h, "target": h, "confidence": 0.99} for h in headers]
    result = run_file_preflight(
        columns=headers,
        column_types={h: "TEXT" for h in headers},
        row_count=len(sample_rows),
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mongodb",
        sync_mode="full_refresh_overwrite",
        sample_rows=sample_rows,
        confidence_threshold=confidence_threshold_for_mode(validation_mode),
        destination_column_types=dest_types,
        destination_table_exists=True,
        destination_can_create=False,
        destination_db_type="snowflake",
    )
    return apply_policy_gates(
        result,
        run_transfer_policy_gates(
            sync_mode="full_refresh_overwrite",
            schema_policy="manual_review",
            validation_mode=validation_mode,
            stream_contracts=[{"name": "s", "primary_key": "id", "selected": True,
                               "sync_mode": "full_refresh_overwrite"}],
            backfill_new_fields=False,
        ),
        validation_mode=validation_mode,
    )


def _gate(pf, gate_id):
    return next((g for g in pf["gates"] if g["id"] == gate_id), None)


def test_g3_no_false_block_when_values_coerce_cleanly():
    """Mongo TEXT→NUMBER where every value is numeric must NOT hard-block G3."""
    pf = _run_preflight(
        sample_rows=[{"id": "1", "score": "10"}, {"id": "2", "score": "3.14"}],
        dest_types={"id": "NUMBER(38,0)", "score": "NUMBER(38,10)"},
    )
    g3 = _gate(pf, "g3_schema_contract")
    assert g3 is not None
    assert g3["status"] == "pass"
    # The coercion report is surfaced for the UI / AI assistant.
    assert "coercion_report" in pf


def test_preflight_reports_structured_coercion_failures():
    """A genuinely non-castable value (text → NUMBER) produces a structured,
    actionable coercion report entry with per-row evidence."""
    pf = _run_preflight(
        sample_rows=[{"id": "1", "age": "30"}, {"id": "2", "age": "abc"}],
        dest_types={"id": "NUMBER(38,0)", "age": "NUMBER(38,0)"},
    )
    report = pf["coercion_report"]
    age = report["by_source"].get("age")
    assert age is not None
    assert age["severity"] == "block"
    assert age["failed"] == 1
    assert any("abc" in f["value"] for f in age["sample_failures"])
    assert age["suggested_fix"]
    assert not pf["passed"]


def test_preflight_variant_target_no_longer_false_blocks():
    """Mixed Mongo field into VARIANT must not block — scalars are wrapped."""
    pf = _run_preflight(
        sample_rows=[{"id": "1", "tags": '["a"]'}, {"id": "2", "tags": "single"}],
        dest_types={"id": "NUMBER(38,0)", "tags": "VARIANT"},
    )
    g3 = _gate(pf, "g3_schema_contract")
    assert g3["status"] == "pass"
    g5 = _gate(pf, "g5_dry_run")
    assert g5 is None or g5["status"] == "pass"
    assert pf["coercion_report"]["has_blocking_failures"] is False


# ─────────────────────────────────────────────────────────────────────────────
# AI-assist explain & suggest fix
# ─────────────────────────────────────────────────────────────────────────────
def test_explain_validation_structured_and_actionable():
    pf = _run_preflight(
        sample_rows=[{"id": "1", "age": "30"}, {"id": "2", "age": "abc"}],
        dest_types={"id": "NUMBER(38,0)", "age": "NUMBER(38,0)"},
    )
    explained = explain_validation(pf, dest_kind="snowflake", use_llm=False)
    assert explained["passed"] is False
    assert explained["assistant_provider"] == "deterministic"
    assert "age" in " ".join(str(a) for a in explained["suggested_actions"])
    # A concrete widen action is offered.
    assert any(a["kind"] == "change_target_type" and a["column"] == "age"
               for a in explained["suggested_actions"])
    assert explained["narrative"]
    assert explained["column_fixes"]


def test_explain_validation_passed_is_clean():
    pf = _run_preflight(
        sample_rows=[{"id": "1", "score": "10"}, {"id": "2", "score": "20"}],
        dest_types={"id": "NUMBER(38,0)", "score": "NUMBER(38,10)"},
    )
    explained = explain_validation(pf, dest_kind="snowflake", use_llm=False)
    if pf["passed"]:
        assert "safe to run" in explained["narrative"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end: real MongoDB + fakesnow Snowflake
# ─────────────────────────────────────────────────────────────────────────────
def test_real_mongo_messy_docs_to_typed_snowflake_validation():
    pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"MongoDB not reachable: {exc}")

    from datetime import datetime, timezone

    from bson.decimal128 import Decimal128
    from pymongo import MongoClient

    from src.transfer.adapters import read_source_database
    from src.transfer.models import EndpointConfig

    coll = "vld_messy_" + uuid.uuid4().hex[:8]
    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    client["dataflow"][coll].insert_many([
        {"id": 1, "score": 10, "tags": ["vip", "beta"], "profile": {"age": 30},
         "created": datetime(2024, 1, 1, tzinfo=timezone.utc), "balance": Decimal128("100.5")},
        {"id": 2, "score": "N/A", "tags": "single", "profile": {"age": "unknown"},
         "created": "2024-06-01"},
        {"id": 3, "score": 3.14, "tags": [], "profile": None},
    ])
    try:
        source = EndpointConfig(kind="database", format="mongodb", host="localhost",
                                port=27017, database="dataflow", table=coll)
        records, headers, schema = read_source_database(source, limit=500)
        assert "tags" in headers and "score" in headers

        # Existing typed Snowflake table: tags→VARIANT, score→NUMBER.
        dest_types = {
            "id": "NUMBER(38,0)", "score": "NUMBER(38,10)",
            "tags": "VARIANT", "profile": "VARIANT",
        }
        rows = [{h: r.get(h) for h in dest_types} for r in records]
        pf = _run_preflight(sample_rows=rows, dest_types=dest_types)

        report = pf["coercion_report"]
        # tags has a bare scalar "single" but is now wrapped losslessly into
        # VARIANT → NOT a hard block (item 1 auto-wrap).
        assert report["by_source"]["tags"]["severity"] != "block"
        # score has "N/A" placeholder → warn (nulled), NOT a hard block.
        assert report["by_source"]["score"]["severity"] == "warn"
        assert report["has_blocking_failures"] is False
    finally:
        client["dataflow"][coll].drop()
        client.close()


def test_real_mongo_messy_docs_roundtrip_variant_queryable():
    """End-to-end: messy Mongo docs → Snowflake VARIANT, then query the nested
    values back to prove queryability (no data loss, no JSON-in-VARCHAR)."""
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"MongoDB not reachable: {exc}")

    from pymongo import MongoClient

    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    coll = "vld_rt_" + uuid.uuid4().hex[:8]
    dst = "vld_rt_sf_" + uuid.uuid4().hex[:8]
    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    client["dataflow"][coll].insert_many([
        {"id": 1, "tags": ["vip", "beta"], "profile": {"age": 30}},
        {"id": 2, "tags": "single", "profile": {"age": "x"}},   # mixed scalar
        {"id": 3, "tags": [], "profile": None},                  # empty / null
    ])
    try:
        with fakesnow.patch():
            import snowflake.connector as sc

            engine = UniversalTransferEngine()
            req = TransferRequest(
                source=EndpointConfig(kind="database", format="mongodb", host="localhost",
                                      port=27017, database="dataflow", table=coll),
                destination=EndpointConfig(kind="database", format="snowflake", host="localhost",
                                           port=443, database="dataflow", username="t", password="t",
                                           schema="public", table=dst),
                sync_mode="full_refresh_overwrite",
                stream_contracts=[{"name": coll, "primary_key": "id", "selected": True,
                                   "sync_mode": "full_refresh_overwrite"}],
                skip_preflight=True,
            )
            res = engine.execute_tracked(req, uuid.uuid4().hex[:24])
            assert res.success is True, res.error
            assert res.records_transferred == 3
            assert res.reconciliation.get("rejected_rows", 0) == 0  # no data dropped

            conn = sc.connect(account="localhost", user="t", password="t",
                              database="dataflow", schema="public")
            cur = conn.cursor()
            # Nested array element is queryable as real VARIANT, not flat text.
            cur.execute(f'SELECT "tags"[0] FROM "{dst}" WHERE "id" = 1')
            assert cur.fetchall()[0][0].strip('"') == "vip"
            # Bare scalar loaded as a VARIANT string (not dropped).
            cur.execute(f'SELECT "tags" FROM "{dst}" WHERE "id" = 2')
            assert cur.fetchall()[0][0].strip('"') == "single"
            # Nested object field is queryable.
            cur.execute(f'SELECT "profile":age FROM "{dst}" WHERE "id" = 1')
            assert str(cur.fetchall()[0][0]).strip('"') == "30"
    finally:
        client["dataflow"][coll].drop()
        client.close()
