"""Dest NUMBER/DECIMAL widen from the cell the writer refused — one owner.

flights-1m.csv → Snowflake NUMBER(9,6): ``7.9166665`` (float32 of 7+55/60)
and ``0.016666668`` (1/60) overflow dest scale. Write stays fail-closed.
Suggested fix names the dest-spelled carrier that would hold the cell.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.writer_common import (  # noqa: E402
    fits_decimal,
    quarantine_unfit_decimals,
)
from services.decimal_observe import (  # noqa: E402
    decimal_scale_overflow_fix,
    decimal_widen_carrier,
    decimal_widen_precision_scale,
)
from services.population_fit_scan import bounded_targets, scan_population_fit  # noqa: E402


def test_flights_clock_residue_does_not_fit_existing_number():
    assert fits_decimal("7.9166665", 9, 6, dest_db="snowflake") is False
    assert fits_decimal("0.016666668", 9, 6, dest_db="snowflake") is False
    assert fits_decimal("0.76666665", 10, 7, dest_db="snowflake") is False
    assert fits_decimal("7.916667", 9, 6, dest_db="snowflake") is True


def test_widen_keeps_dest_int_width_and_spells_dialect():
    assert decimal_widen_precision_scale(
        "7.9166665", dest_db="snowflake", current_type="NUMBER(9,6)"
    ) == (10, 7)
    assert (
        decimal_widen_carrier(
            "7.9166665", dest_db="snowflake", current_type="NUMBER(9,6)"
        )
        == "NUMBER(10,7)"
    )
    assert (
        decimal_widen_carrier(
            "0.76666665", dest_db="snowflake", current_type="NUMBER(10,7)"
        )
        == "NUMBER(11,8)"
    )
    assert (
        decimal_widen_carrier("1.2345", dest_db="mysql", current_type="DECIMAL(10,2)")
        == "DECIMAL(12,4)"
    )
    assert (
        decimal_widen_carrier(
            "1.2345", dest_db="postgresql", current_type="NUMERIC(10,2)"
        )
        == "NUMERIC(12,4)"
    )
    assert (
        decimal_widen_carrier(
            "1.2345678901", dest_db="bigquery", current_type="NUMERIC(5,2)"
        )
        == "BIGNUMERIC(13,10)"
    )


def test_suggested_fix_names_widen_not_truncate():
    fix = decimal_scale_overflow_fix(
        "7.9166665",
        dest_db="snowflake",
        current_type="NUMBER(9,6)",
        column="DEP_TIME",
    )
    assert "NUMBER(10,7)" in fix
    assert "DEP_TIME" in fix
    assert "truncate" in fix.lower()


def test_file_inferred_typmod_is_not_a_declared_domain():
    targets, _, safe = bounded_targets(
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        source_kind="file",
        source_format="csv",
    )
    assert safe == ()
    assert [t.target for t in targets] == ["DEP_TIME"]

    warehouse, _, warehouse_safe = bounded_targets(
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        source_kind="database",
        source_format="snowflake",
    )
    assert warehouse == ()
    assert warehouse_safe == ("DEP_TIME",)

    omitted, _, omitted_safe = bounded_targets(
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
    )
    assert omitted_safe == ()
    assert [t.target for t in omitted] == ["DEP_TIME"]


def test_population_scan_blocks_file_clock_residue_and_stamps_widen():
    rows = [{"DEP_TIME": "7.5"}] * 292 + [{"DEP_TIME": "7.9166665"}]
    report = scan_population_fit(
        rows,
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(9,6)"}],
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_are_population=True,
        source_kind="file",
        source_format="csv",
    )
    assert report.findings
    assert report.findings[0].example_rows == (293,)
    assert report.findings[0].suggested_target_type == "NUMBER(10,7)"


def test_quarantine_stamps_suggested_fix_on_snowflake_number():
    details: list[dict] = []
    out = quarantine_unfit_decimals(
        [("7.9166665",)],
        ["DEP_TIME"],
        ["NUMBER(9,6)"],
        details,
        policy="fail",
        dialect_label="Snowflake NUMBER",
        dest_db="snowflake",
    )
    assert out == []
    assert details
    assert "NUMBER(9,6)" in details[0]["reason"]
    assert details[0]["suggested_target_type"] == "NUMBER(10,7)"
    assert "NUMBER(10,7)" in details[0]["suggested_fix"]
    assert fits_decimal(
        "7.9166665", 10, 7, dest_db="snowflake"
    ), "widen must be the carrier that actually fits"
