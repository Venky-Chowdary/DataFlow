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
    decimal_widen_from_envelope,
    decimal_widen_precision_scale,
)
from services.population_fit_scan import (  # noqa: E402
    bounded_targets,
    build_population_fit_gate,
    scan_population_fit,
)


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


def test_population_scan_widens_from_all_unfit_cells_not_the_first():
    """flights-1m: first overflow suggested (11,8); a later cell still needed (12,9).

    One Approve must stamp the envelope of every unfit value so Validate
    does not play stepwise NUMBER(9,6) → (11,8) → (12,9).
    """
    assert (
        decimal_widen_from_envelope(
            max_int_digits=1,
            max_scale=9,
            dest_db="snowflake",
            current_type="NUMBER(9,6)",
        )
        == "NUMBER(12,9)"
    )
    rows = (
        [{"DEP_TIME": "7.5"}] * 292
        + [{"DEP_TIME": "0.23333333"}]
        + [{"DEP_TIME": "7.5"}] * 44
        + [{"DEP_TIME": "0.016666668"}]
    )
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
        dest_table_exists=False,
    )
    assert report.findings
    assert report.findings[0].suggested_target_type == "NUMBER(12,9)"
    assert "CREATE" in report.findings[0].suggested_fix
    assert "CSV is not modified" in report.findings[0].suggested_fix
    gate = build_population_fit_gate(report)
    assert gate["details"]["create_new_table"] is True
    actions = gate["details"]["suggested_actions"]
    assert actions[0]["to_type"] == "NUMBER(12,9)"
    assert actions[0]["requires_ddl"] is False

    widened = scan_population_fit(
        rows,
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(12,9)"}],
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_are_population=True,
        source_kind="file",
        source_format="csv",
        dest_table_exists=False,
    )
    assert widened.findings == ()
    assert widened.evidence == "exact"


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


def test_remap_on_existing_dest_still_scans_live_ddl():
    """Map NUMBER(10,7) does not ALTER live Snowflake NUMBER(9,6).

    That remap used to green Validate and fail Execute on the same 7.9166665
    cells — the errors-every-Run loop. Validate must keep judging live DDL.
    """
    rows = [{"DEP_TIME": "7.5"}] * 292 + [{"DEP_TIME": "7.9166665"}]
    report = scan_population_fit(
        rows,
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(10,7)"}],
        dest_types={"DEP_TIME": "NUMBER(9,6)"},
        source_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_are_population=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        dest_table_exists=True,
    )
    assert report.findings
    assert report.findings[0].target.target_type == "NUMBER(9,6)"
    assert report.findings[0].target.binds_live_ddl is True
    assert report.findings[0].suggested_target_type == "NUMBER(10,7)"
    assert "does not ALTER" in report.findings[0].suggested_fix
    assert "Resume" in report.findings[0].suggested_fix


def test_overwrite_judges_mapping_type_not_dropped_live_ddl():
    """Overwrite recreates the object — mapping NUMBER(10,7) is the write bind."""
    rows = [{"DEP_TIME": "7.9166665"}]
    report = scan_population_fit(
        rows,
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(10,7)"}],
        dest_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_are_population=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_overwrite",
        dest_table_exists=True,
    )
    assert report.findings == ()
    assert report.targets[0].target_type == "NUMBER(10,7)"
    assert report.targets[0].binds_live_ddl is False


def test_create_new_uses_mapping_even_when_projected_types_are_narrow():
    targets, _, _ = bounded_targets(
        [{"source": "DEP_TIME", "target": "DEP_TIME", "target_type": "NUMBER(10,7)"}],
        dest_types={"DEP_TIME": "NUMBER(9,6)"},
        dest_db="snowflake",
        source_kind="file",
        source_format="csv",
        dest_table_exists=False,
    )
    assert [t.target_type for t in targets] == ["NUMBER(10,7)"]
    assert targets[0].binds_live_ddl is False
