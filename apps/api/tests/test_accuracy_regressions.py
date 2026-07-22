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
