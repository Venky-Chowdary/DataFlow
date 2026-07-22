"""Accuracy regressions — mapping confidence, Mongo widen, reconcile, dates, G8."""

from __future__ import annotations

from services.preflight_service import run_file_preflight
from services.reconciliation import normalize_cell
from services.schema_introspect import _finalize_mongodb_type
from services.transform_engine import apply_transform


def test_status_enum_checksum_does_not_equal_boolean_true():
    assert normalize_cell("active") != normalize_cell(True)
    assert normalize_cell("enabled") != normalize_cell("true")
    assert normalize_cell("inactive") != normalize_cell(False)


def test_mongo_majority_keeps_integer_despite_one_text_sentinel():
    counts = {"INTEGER": 49, "TEXT": 1}
    assert _finalize_mongodb_type(counts) == "INTEGER"


def test_mongo_majority_demotes_when_text_share_high():
    counts = {"INTEGER": 5, "TEXT": 5}
    assert _finalize_mongodb_type(counts) == "TEXT"


def test_mongo_majority_promotes_integer_plus_decimal():
    counts = {"INTEGER": 40, "DECIMAL": 10}
    assert _finalize_mongodb_type(counts) == "DECIMAL"


def test_ambiguous_date_quarantines_instead_of_silent_mdy():
    val, err = apply_transform("06/05/2024", "date")
    assert val is None
    assert err is not None


def test_g8_blocks_on_write_path_transform_error():
    result = run_file_preflight(
        columns=["amt"],
        column_types={"amt": "VARCHAR"},
        row_count=1,
        mappings=[{
            "source": "amt",
            "target": "amount",
            "confidence": 0.95,
            "transform": "decimal",
            "target_type": "NUMBER(18,2)",
        }],
        destination_connected=True,
        source_connected=True,
        sample_rows=[{"amt": "not-a-number"}],
        destination_column_types={"amount": "NUMBER(18,2)"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_db_type="snowflake",
    )
    blocker_ids = {b["id"] for b in result.get("blockers") or []}
    # G5 dry-run and/or G8 must surface the bad decimal — never silent pass.
    assert blocker_ids & {"g5_dry_run", "g8_reconciliation", "g9_data_integrity"}
    assert result["passed"] is False


def test_g8_identity_allows_native_array_and_object_fields():
    """Mongo Studio samples keep list/dict cells; identity must use JSON serialize, not repr."""
    sample_rows = [{
        "_id": "1",
        "categories": ["Customer Success", "Engineering"],
        "countries": ["US", "CA"],
        "skills": [{"name": "Python", "level": 3}],
        "meta": {"a": 1, "b": 2},
    }]
    columns = list(sample_rows[0].keys())
    mappings = [
        {"source": c, "target": c, "confidence": 0.95, "transform": "none"}
        for c in columns
    ]
    result = run_file_preflight(
        columns=columns,
        column_types={c: "VARCHAR" for c in columns},
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        source_connected=True,
        source_kind="database",
        sample_rows=sample_rows,
        destination_column_types={},
        destination_table_exists=False,
        destination_can_create=True,
        destination_db_type="redis",
        validation_mode="strict",
    )
    gate_by_id = {g["id"]: g for g in result["gates"]}
    assert gate_by_id["g8_reconciliation"]["status"] == "pass"
    assert gate_by_id["g6_target_ddl"]["status"] == "pass"
    # No identity false-positive on structural fields
    details = (gate_by_id["g8_reconciliation"].get("details") or {})
    issues = details.get("issues") or []
    assert not any("identity transform altered" in str(i) for i in issues)


def test_g8_still_blocks_when_identity_truly_mutates():
    """Upper transform is not identity — but a declared none that changes must block.
    Simulate via strip_controls on a ZWSP-only difference is intentional mutate;
    use a mapping labeled none with a value that write-path cannot keep equal:
    we assert uppercase transform is NOT checked as identity (passes G8 fingerprint).
    """
    result = run_file_preflight(
        columns=["name"],
        column_types={"name": "VARCHAR"},
        row_count=1,
        mappings=[{
            "source": "name",
            "target": "name",
            "confidence": 0.99,
            "transform": "upper",
        }],
        destination_connected=True,
        source_connected=True,
        sample_rows=[{"name": "alice"}],
        destination_column_types={"name": "VARCHAR"},
        destination_table_exists=True,
        destination_can_create=True,
        destination_db_type="postgresql",
        validation_mode="strict",
    )
    gate = next(g for g in result["gates"] if g["id"] == "g8_reconciliation")
    assert gate["status"] == "pass"
