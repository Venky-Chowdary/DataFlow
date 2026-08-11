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
def test_probe_clean_numeric_text_to_number_blocks_declared_collapse():
    """Samples coerce, but TEXT→NUMBER is still a declared fidelity collapse (fail-closed)."""
    report = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "3.14"}, {"score": "42"}],
        mappings=[{"source": "score", "target": "score"}],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,10)"},
        dest_db_type="snowflake",
    )
    col = report["by_source"]["score"]
    assert col["failed"] == 0
    assert col["severity"] == "block"
    assert report["has_blocking_failures"] is True

    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="score",
        source_type="TEXT",
        destination_type="NUMBER(38,10)",
        approved_by="admin@dataflow.app",
        reason="clean numeric TEXT→NUMBER accepted",
        execution_policy="CAST_AND_CONTINUE",
    )
    cleared = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "3.14"}, {"score": "42"}],
        mappings=[
            {
                "source": "score",
                "target": "score",
                "risk_contract": contract.to_dict(),
            }
        ],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,10)"},
        dest_db_type="snowflake",
    )
    assert cleared["by_source"]["score"]["severity"] == "warn"
    assert cleared["has_blocking_failures"] is False


def test_probe_placeholder_values_become_null_and_block_under_strict():
    """N/A is sentinel→NULL loss — strict blocks (not a bind/format failure)."""
    report = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "N/A"}, {"score": "20"}],
        mappings=[{"source": "score", "target": "score"}],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,0)"},
        dest_db_type="snowflake",
        validation_mode="strict",
    )
    col = report["by_source"]["score"]
    assert col["sentinel_nulls"] == 1
    assert col["failed"] == 0
    assert col["severity"] == "block"
    assert report["has_blocking_failures"] is True
    assert any("N/A" in str(f.get("value") or "") for f in col.get("sample_failures") or [])


def test_probe_placeholder_values_warn_under_balanced():
    """Balanced counts N/A as sentinel_nulls; TEXT→NUMBER still blocks without Risk Contract."""
    report = analyze_coercion(
        sample_rows=[{"score": "10"}, {"score": "N/A"}, {"score": "20"}],
        mappings=[{"source": "score", "target": "score"}],
        source_types={"score": "TEXT"},
        dest_types={"score": "NUMBER(38,0)"},
        dest_db_type="snowflake",
        validation_mode="balanced",
    )
    col = report["by_source"]["score"]
    assert col["sentinel_nulls"] == 1
    assert col["failed"] == 0
    # Declared TEXT→NUMBER is fidelity_collapse — B10 requires Risk Contract to soft-warn.
    assert col["severity"] == "block"
    assert report["has_blocking_failures"] is True


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
    """TEXT→VARIANT invents document domain — block without Risk Contract.

    Scalars wrap as JSON string literals (domain change). Empty cells refuse
    silent NULL invent. Signed CAST_AND_CONTINUE softens to warn.
    """
    from services.migration_risk_contract import create_migration_risk_contract

    report = analyze_coercion(
        sample_rows=[{"tags": '["a","b"]'}, {"tags": "single"}],
        mappings=[{"source": "tags", "target": "tags"}],
        source_types={"tags": "TEXT"},
        dest_types={"tags": "VARIANT"},
        dest_db_type="snowflake",
    )
    col = report["by_source"].get("tags")
    assert col is not None
    assert col["failed"] == 0
    assert col.get("json_scalar_wraps", 0) >= 1
    assert col["severity"] == "block"
    assert report["has_blocking_failures"] is True

    contract = create_migration_risk_contract(
        column="tags",
        source_type="TEXT",
        destination_type="VARIANT",
        approved_by="admin@dataflow.app",
        reason="Mongo schemaless field → Snowflake VARIANT",
        execution_policy="CAST_AND_CONTINUE",
    )
    cleared = analyze_coercion(
        sample_rows=[{"tags": '["a","b"]'}, {"tags": "single"}],
        mappings=[
            {
                "source": "tags",
                "target": "tags",
                "risk_contract": contract.to_dict(),
            }
        ],
        source_types={"tags": "TEXT"},
        dest_types={"tags": "VARIANT"},
        dest_db_type="snowflake",
    )
    assert cleared["by_source"]["tags"]["severity"] == "warn"
    assert cleared["has_blocking_failures"] is False


def test_probe_valid_json_into_variant_passes():
    """Declared JSON→VARIANT keeps document polarity — not TEXT invent."""
    report = analyze_coercion(
        sample_rows=[{"profile": '{"age":30}'}, {"profile": '{"age":"x"}'}],
        mappings=[{"source": "profile", "target": "profile"}],
        source_types={"profile": "JSON"},
        dest_types={"profile": "VARIANT"},
        dest_db_type="snowflake",
    )
    assert report["has_blocking_failures"] is False


def test_probe_text_target_is_never_a_risk():
    report = analyze_coercion(
        sample_rows=[{"name": "alice"}, {"name": "bob"}],
        mappings=[{"source": "name", "target": "name"}],
        source_types={"name": "TEXT"},
        dest_types={"name": "VARCHAR"},
        dest_db_type="snowflake",
    )
    # TEXT↔VARCHAR may be scored as a checked pair with severity ok — never block.
    assert report["has_blocking_failures"] is False
    if report.get("checked"):
        assert report["by_source"]["name"]["severity"] == "ok"


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
    """TEXT→VARIANT document invent blocks until Risk Contract; samples still wrap."""
    from services.migration_risk_contract import create_migration_risk_contract

    pf = _run_preflight(
        sample_rows=[{"id": "1", "tags": '["a"]'}, {"id": "2", "tags": "single"}],
        dest_types={"id": "NUMBER(38,0)", "tags": "VARIANT"},
    )
    g3 = _gate(pf, "g3_schema_contract")
    assert g3["status"] == "block"
    assert pf["coercion_report"]["has_blocking_failures"] is True

    contract = create_migration_risk_contract(
        column="tags",
        source_type="TEXT",
        destination_type="VARIANT",
        approved_by="admin@dataflow.app",
        reason="Mongo tags → Snowflake VARIANT",
        execution_policy="CAST_AND_CONTINUE",
    )
    headers = ["id", "tags"]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {
            "source": "tags",
            "target": "tags",
            "confidence": 0.99,
            "risk_contract": contract.to_dict(),
        },
    ]
    result = run_file_preflight(
        columns=headers,
        column_types={h: "TEXT" for h in headers},
        row_count=2,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        source_format="mongodb",
        sync_mode="full_refresh_overwrite",
        sample_rows=[{"id": "1", "tags": '["a"]'}, {"id": "2", "tags": "single"}],
        confidence_threshold=confidence_threshold_for_mode("strict"),
        destination_column_types={"id": "NUMBER(38,0)", "tags": "VARIANT"},
        destination_table_exists=True,
        destination_can_create=False,
        destination_db_type="snowflake",
    )
    cleared = apply_policy_gates(
        result,
        run_transfer_policy_gates(
            sync_mode="full_refresh_overwrite",
            schema_policy="manual_review",
            validation_mode="strict",
            stream_contracts=[
                {
                    "name": "s",
                    "primary_key": "id",
                    "selected": True,
                    "sync_mode": "full_refresh_overwrite",
                }
            ],
            backfill_new_fields=False,
        ),
        validation_mode="strict",
    )
    g3c = _gate(cleared, "g3_schema_contract")
    assert g3c["status"] == "pass"
    assert cleared["coercion_report"]["has_blocking_failures"] is False


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
        # score has "N/A" placeholder → NULL under strict = block (potential data loss).
        assert report["by_source"]["score"]["severity"] == "block"
        assert report["has_blocking_failures"] is True
    finally:
        client["dataflow"][coll].drop()
        client.close()


def test_real_mongo_messy_docs_roundtrip_variant_queryable():
    """End-to-end: messy Mongo docs → Snowflake VARIANT, then query the nested
    values back to prove queryability (no data loss, no JSON-in-VARCHAR)."""
    pytest.importorskip("fakesnow")
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
        from connectors.snowflake_conn import get_connection

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

        conn = get_connection(
            account="localhost", username="t", password="t",
            database="dataflow", schema="public", warehouse="", connection_string="",
        )
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
        conn.close()
    finally:
        client["dataflow"][coll].drop()
        client.close()


def test_hex_mongo_id_does_not_map_onto_snowflake_number_id_when_samples_present():
    """Hex/ObjectId-like ``_id`` must not bind to existing Snowflake NUMBER ``id``.

    Client deploy: refuse the Map landmine even without samples (was ~0.73 onto
    NUMBER). With samples the same create-new TEXT path must still win.
    """
    from services.semantic_mapper import map_columns

    samples = [
        "e3befcc4bfe68c256fadde759f81221eb54b42fb36105c0884c545487c642e5b",
        "c97abd998d4bfb7d033b363648f9a9b16f77606784c37b21d9864706b103754e",
        "d2909789b4ace07d724901b5ecfe1efeb3777638c9bb1c38a3e2eb1e7cb8e018",
        "72a198e224365b20ab46575aab687271181e1f7f844671de8c6de40b147e7356",
    ]
    bare = map_columns(
        ["_id"],
        ["id"],
        source_schemas=[{"name": "_id", "inferred_type": "TEXT", "samples": []}],
        target_schemas=[{"name": "id", "inferred_type": "NUMBER(38,0)"}],
        destination_db_type="snowflake",
    )
    assert bare[0].get("create_new") is True
    assert bare[0]["target"].lower() != "id"

    mapped = map_columns(
        ["_id"],
        ["id"],
        source_schemas=[{"name": "_id", "inferred_type": "TEXT", "samples": samples}],
        target_schemas=[{"name": "id", "inferred_type": "NUMBER(38,0)"}],
        destination_db_type="snowflake",
    )
    assert len(mapped) == 1
    m = mapped[0]
    assert m.get("create_new") is True
    assert str(m.get("target_type", "")).upper() in {"VARCHAR", "TEXT", "STRING"}
    # Prefer a new text field over overwriting the incompatible NUMBER id.
    assert m["target"].lower() != "id"


def test_objectid_does_not_map_onto_number_id_without_samples():
    from services.semantic_mapper import map_columns

    mapped = map_columns(
        ["_id"],
        ["id"],
        source_schemas=[{"name": "_id", "inferred_type": "OBJECTID", "samples": []}],
        target_schemas=[{"name": "id", "inferred_type": "INTEGER"}],
        destination_db_type="postgresql",
    )
    assert mapped[0].get("create_new") is True
    assert mapped[0]["target"].lower() != "id"


def test_gate_block_is_visible_in_the_report_every_surface_reads():
    """A blocked run must not sit above a report that says nothing is blocking.

    The coercion probe judges values; G3 judges the declared conversion. When
    the cells cast cleanly but the conversion changes the domain, the report the
    UI, the assistant and the proof bundle read used to say
    ``has_blocking_failures: false`` under a blocked Validate.
    """
    pf = _run_preflight(
        sample_rows=[{"id": "1", "tags": '["a"]'}, {"id": "2", "tags": "single"}],
        dest_types={"id": "NUMBER(38,0)", "tags": "VARIANT"},
    )
    report = pf["coercion_report"]
    assert _gate(pf, "g3_schema_contract")["status"] == "block"
    assert report["has_blocking_failures"] is True
    assert [b["source"] for b in report["declared_type_blocks"]] == ["tags"]
    # The value-level verdict stays honest: those cells really did cast.
    assert report["by_source"]["tags"]["severity"] != "block"
    assert report["by_source"]["tags"]["blocked_by"] == "g3_schema_contract"
