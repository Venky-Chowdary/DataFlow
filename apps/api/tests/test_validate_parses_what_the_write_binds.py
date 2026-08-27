"""Validate must refuse the values the write's coercion refuses.

The production failures these cover: a text column of decimals mapped onto a
MySQL ``INT`` passed Validate and the writer refused 4,917 cells starting at row
1, and a text column of ``yes``/``maybe`` mapped onto ``BOOLEAN`` did the same.
Both were decidable before a row moved: the fit scan now asks the write's own
parser (``resolve_transform`` → ``apply_transform``), not a bounds test that
truncated the value first.
"""

from __future__ import annotations

from datetime import date

import pytest

from connectors.writer_common import integer_fit_failure
from services.population_fit_scan import (
    CARRIER_BYTES,
    CARRIER_DOMAIN,
    CARRIER_INTEGER,
    CARRIER_TYPED,
    bounded_targets,
    scan_rows,
)


def _scan(
    target_type: str,
    source_type: str,
    values: list[str],
    *,
    dest_db: str = "mysql",
    source_kind: str = "",
    source_format: str = "",
):
    mappings = [
        {
            "source": "c",
            "target": "c",
            "target_type": target_type,
            "source_type": source_type,
            "confidence": 1.0,
        }
    ]
    targets, undecidable, safe = bounded_targets(
        mappings,
        dest_db=dest_db,
        source_types={"c": source_type},
        source_kind=source_kind,
        source_format=source_format,
    )
    report = scan_rows(
        [{"c": v} for v in values],
        targets,
        dest_db=dest_db,
        rows_total=len(values),
    )
    return targets, undecidable, safe, report


@pytest.mark.parametrize(
    "value",
    ["ABC-1", "12abc", "--3", "NaN", "Infinity"],
)
def test_unwritable_integer_text_is_named_not_deferred(value: str) -> None:
    """``integer_fit_failure`` returned ``None`` here — Validate called it fit."""
    reason = integer_fit_failure(value, "INT", dest_db="mysql")

    assert reason, f"{value!r} must not be reported as fitting INT"
    assert "not an integer" in reason


@pytest.mark.parametrize(
    "value,expected",
    [
        ("7", None),
        ("-2147483648", None),
        ("true", None),  # canonical boolean wire is what the write binds as 1
        ("1e3", None),
        ("$1,000", None),
    ],
)
def test_values_the_write_binds_stay_fit(value: str, expected: None) -> None:
    assert integer_fit_failure(value, "INT", dest_db="mysql") is expected


def test_text_of_decimals_into_int_blocks_at_validate() -> None:
    targets, _und, safe, report = _scan(
        "INT", "VARCHAR", ["22.433332", "22.05", "7"]
    )

    assert [t.carrier for t in targets] == [CARRIER_INTEGER]
    assert safe == ()
    finding = report.findings[0]
    assert finding.unfit_rows == 2
    assert finding.example_rows[:2] == (1, 2)
    assert "not an integer" in finding.unfit_reason


def test_non_canonical_boolean_text_blocks_at_validate() -> None:
    targets, _und, _safe, report = _scan("BOOLEAN", "VARCHAR", ["maybe", "true"])

    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert [t.transform for t in targets] == ["boolean"]
    assert report.findings[0].unfit_rows == 1
    assert "Invalid boolean" in report.findings[0].unfit_reason


def test_a_typed_source_wire_is_not_rescanned_for_its_parse() -> None:
    """Cost stays off an ordinary transfer: a DB numeric wire parses by
    construction, so the declaration decides it without a million parses."""
    targets, _und, safe, report = _scan(
        "DECIMAL(11,8)",
        "DECIMAL(11,8)",
        ["12.34567890"],
        source_kind="database",
        source_format="mysql",
    )

    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_a_boolean_typed_wire_is_still_not_rescanned() -> None:
    """Warehouse BOOLEAN wires parse by construction. File peek does not."""
    targets, _und, safe, report = _scan(
        "BOOLEAN",
        "BOOLEAN",
        ["true", "false"],
        source_kind="database",
        source_format="mysql",
    )

    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_file_inferred_boolean_still_scans_maybe() -> None:
    """Peek inferred BOOLEAN from true/false is not a domain — ``maybe`` lives later."""
    targets, _und, safe, report = _scan(
        "BOOLEAN",
        "BOOLEAN",
        ["true", "maybe"],
        source_kind="file",
        source_format="csv",
    )

    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert "maybe" in (report.findings[0].example_values[0] if report.findings[0].example_values else "maybe")
    assert report.findings[0].suggested_fix
    assert "silently coerce" in report.findings[0].suggested_fix


def test_invalid_calendar_day_stamps_a_fix_not_a_widen() -> None:
    _targets, _und, _safe, report = _scan(
        "DATE", "VARCHAR", ["2024-02-31"], dest_db="mysql"
    )
    assert report.findings[0].suggested_fix
    assert "invalid date" in report.findings[0].suggested_fix.lower()
    assert not report.findings[0].suggested_target_type


def test_invalid_time_stamps_a_fix_not_a_widen() -> None:
    _targets, _und, _safe, report = _scan(
        "TIME", "VARCHAR", ["25:61:00"], dest_db="postgresql"
    )
    assert report.findings[0].suggested_fix
    assert "invalid time" in report.findings[0].suggested_fix.lower()
    assert not report.findings[0].suggested_target_type


def test_invalid_uuid_stamps_varchar_widen() -> None:
    _targets, _und, _safe, report = _scan(
        "UUID", "VARCHAR", ["not-a-uuid"], dest_db="postgresql"
    )
    assert report.findings[0].suggested_fix
    assert report.findings[0].suggested_target_type == "VARCHAR(36)"
    assert "silently coerce" in report.findings[0].suggested_fix


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql"])
@pytest.mark.parametrize("source_type", ["VARCHAR", "DATE"])
def test_an_unreal_calendar_day_blocks_at_validate(
    dest_db: str, source_type: str
) -> None:
    """``2024-02-31`` used to pass Validate because DATE was undecidable or
    skipped as a typed wire. The write's temporal bind cannot hold it — PG
    errors, MySQL may store a zero date — so Validate must name it first."""
    targets, _und, safe, report = _scan(
        "DATE",
        source_type,
        ["2024-02-31", "2024-02-29", "2024-03-01"],
        dest_db=dest_db,
    )

    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (1,)
    assert "Invalid date" in report.findings[0].unfit_reason
    assert "2024-02-31" in report.findings[0].unfit_reason


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql"])
def test_a_non_leap_29_feb_blocks_and_a_leap_day_fits(dest_db: str) -> None:
    targets, _und, _safe, report = _scan(
        "DATE",
        "VARCHAR",
        ["2024-02-29", "2023-02-29"],
        dest_db=dest_db,
    )

    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (2,)
    assert "2023-02-29" in report.findings[0].unfit_reason


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql"])
def test_a_native_date_object_fits_a_date_carrier(dest_db: str) -> None:
    mappings = [
        {
            "source": "c",
            "target": "c",
            "target_type": "DATE",
            "source_type": "DATE",
            "confidence": 1.0,
        }
    ]
    targets, _und, safe = bounded_targets(
        mappings, dest_db=dest_db, source_types={"c": "DATE"}
    )
    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    report = scan_rows(
        [{"c": date(2024, 2, 29)}, {"c": date(2024, 3, 1)}],
        targets,
        dest_db=dest_db,
        rows_total=2,
    )
    assert report.findings == ()


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql"])
def test_a_date_transform_the_write_accepts_is_not_refused(
    dest_db: str,
) -> None:
    """``31/12/2024`` is unambiguous day-first. ``apply_transform(..., "date")``
    ISO-normalizes it; coercing the raw slash form would false-refuse a cell
    the write already binds."""
    mappings = [
        {
            "source": "c",
            "target": "c",
            "target_type": "DATE",
            "source_type": "VARCHAR",
            "transform": "date",
            "confidence": 1.0,
        }
    ]
    targets, _und, safe = bounded_targets(
        mappings, dest_db=dest_db, source_types={"c": "VARCHAR"}
    )
    report = scan_rows(
        [{"c": "31/12/2024"}, {"c": "2024-02-31"}],
        targets,
        dest_db=dest_db,
        rows_total=2,
    )
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (2,)
    assert "2024-02-31" in report.findings[0].unfit_reason


@pytest.mark.parametrize("dest_db", ["mysql", "postgresql"])
def test_slash_dates_without_a_date_transform_match_the_write(dest_db: str) -> None:
    """VARCHAR → DATE with no date transform is identity then
    ``coerce_sql_temporal``. That bind now uses the same Auto date parser as
    the write path: unambiguous ``31/12/2024`` lands; ``01/02/2024`` does not.
    Validate must not invent a calendar the write refuses.
    """
    _targets, _und, _safe, fit = _scan(
        "DATE", "VARCHAR", ["31/12/2024"], dest_db=dest_db
    )
    assert fit.findings == ()

    _targets, _und, _safe, unfit = _scan(
        "DATE", "VARCHAR", ["01/02/2024"], dest_db=dest_db
    )
    assert unfit.findings[0].unfit_rows == 1
    reason = unfit.findings[0].unfit_reason
    assert "01/02/2024" in reason or "Invalid date" in reason


def test_late_enum_member_blocks_at_validate_and_stamps_dest_widen() -> None:
    """MySQL non-strict ENUM stores an unknown label as '' — silent wipe.

    Preview of ``active``/``inactive`` is clean. Row 3 ``late`` is the write
    refuse. Validate must name it and offer dest-spelled ENUM, never VARCHAR.
    """
    targets, undecidable, safe, report = _scan(
        "ENUM('active','inactive')",
        "VARCHAR",
        ["active", "inactive", "late"],
        dest_db="mysql",
    )

    assert [t.carrier for t in targets] == [CARRIER_DOMAIN]
    assert undecidable == ()
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (3,)
    assert "late" in report.findings[0].unfit_reason
    assert report.findings[0].suggested_target_type == "ENUM('active','inactive','late')"
    assert "VARCHAR" not in (report.findings[0].suggested_target_type or "")
    assert "silently store" in report.findings[0].suggested_fix


def test_empty_enum_cell_is_the_mysql_error_member() -> None:
    """Blank is a nullability skip for NUMBER. ENUM treats '' as index 0."""
    _targets, _und, _safe, report = _scan(
        "ENUM('a','b')", "VARCHAR", [""], dest_db="mysql"
    )
    assert report.findings[0].unfit_rows == 1
    assert "empty string" in report.findings[0].unfit_reason.lower() or "error member" in (
        report.findings[0].unfit_reason.lower()
    )


def test_warehouse_enum_subset_skips_the_scan() -> None:
    targets, _und, safe, report = _scan(
        "ENUM('a','b','c')",
        "ENUM('a','b')",
        ["a", "b"],
        dest_db="mysql",
        source_kind="database",
        source_format="mysql",
    )
    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_warehouse_varchar_into_enum_still_scans() -> None:
    targets, _und, safe, _report = _scan(
        "ENUM('a','b')",
        "VARCHAR",
        ["a", "late"],
        dest_db="mysql",
        source_kind="database",
        source_format="mysql",
    )
    assert [t.carrier for t in targets] == [CARRIER_DOMAIN]
    assert safe == ()


def test_file_inferred_enum_still_scans_late_member() -> None:
    targets, _und, safe, report = _scan(
        "ENUM('a','b')",
        "ENUM('a','b')",
        ["a", "late"],
        dest_db="mysql",
        source_kind="file",
        source_format="csv",
    )
    assert [t.carrier for t in targets] == [CARRIER_DOMAIN]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1


def test_set_member_drop_blocks_at_validate() -> None:
    """MySQL IGNORE drops unknown SET members silently. Fail closed."""
    targets, _und, _safe, report = _scan(
        "SET('read','write')",
        "VARCHAR",
        ["read", "read,admin"],
        dest_db="mysql",
    )
    assert [t.carrier for t in targets] == [CARRIER_DOMAIN]
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (2,)
    assert "SET(" in (report.findings[0].suggested_target_type or "")
    assert "admin" in (report.findings[0].suggested_target_type or "")


def test_interval_family_mismatch_blocks_and_does_not_invent_varchar() -> None:
    targets, _und, safe, report = _scan(
        "INTERVAL DAY TO SECOND",
        "VARCHAR",
        ["P1DT2H", "P1Y2M"],
        dest_db="oracle",
    )
    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (2,)
    assert "interval family" in report.findings[0].unfit_reason.lower()
    assert not report.findings[0].suggested_target_type
    assert "varchar" not in (report.findings[0].suggested_fix or "").lower()


def test_invalid_interval_text_blocks_at_validate() -> None:
    _targets, _und, _safe, report = _scan(
        "INTERVAL", "VARCHAR", ["not-an-interval", "1 day"], dest_db="postgresql"
    )
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (1,)
    assert "interval" in report.findings[0].unfit_reason.lower()


def test_warehouse_interval_wire_is_not_rescanned() -> None:
    targets, _und, safe, report = _scan(
        "INTERVAL DAY TO SECOND",
        "INTERVAL DAY TO SECOND",
        ["P1DT2H"],
        dest_db="oracle",
        source_kind="database",
        source_format="oracle",
    )
    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_json_and_variant_stay_undecidable() -> None:
    """JSON/VARIANT cost is unbounded in the cell — other probes own them."""
    for dest_type in ("JSON", "JSONB", "VARIANT"):
        targets, undecidable, safe, _report = _scan(
            dest_type, "VARCHAR", ['{"a":1}', "not-json"], dest_db="snowflake"
        )
        assert targets == (), dest_type
        assert undecidable == ("c",), dest_type
        assert safe == (), dest_type


def test_out_of_range_year_blocks_and_does_not_invent_varchar() -> None:
    """Non-strict MySQL stores invalid YEAR as 0000 — silent wipe."""
    targets, _und, safe, report = _scan(
        "YEAR", "VARCHAR", ["1999", "1899", "2156"], dest_db="mysql"
    )
    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    assert report.findings[0].unfit_rows == 2
    assert report.findings[0].example_rows == (2, 3)
    assert "1899" in report.findings[0].unfit_reason
    assert not report.findings[0].suggested_target_type
    assert "0000" in report.findings[0].suggested_fix
    assert "varchar" not in (report.findings[0].suggested_fix or "").lower()


def test_empty_year_is_a_silent_wipe_not_nullability() -> None:
    """Write refuses empty YEAR (integer transform, then YEAR bind). Not a skip."""
    _targets, _und, _safe, report = _scan("YEAR", "VARCHAR", [""], dest_db="mysql")
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].suggested_fix
    assert "0000" in report.findings[0].suggested_fix


def test_warehouse_year_wire_is_not_rescanned() -> None:
    targets, _und, safe, report = _scan(
        "YEAR",
        "YEAR",
        ["1999", "2001"],
        dest_db="mysql",
        source_kind="database",
        source_format="mysql",
    )
    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_file_inferred_year_still_scans_out_of_range() -> None:
    targets, _und, safe, report = _scan(
        "YEAR",
        "YEAR",
        ["1999", "1899"],
        dest_db="mysql",
        source_kind="file",
        source_format="csv",
    )
    assert [t.carrier for t in targets] == [CARRIER_TYPED]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1


def test_binary_overflow_stamps_dest_widen_not_text() -> None:
    import base64

    fit = base64.b64encode(b"ab").decode("ascii")
    overflow = base64.b64encode(b"abcd").decode("ascii")
    targets, _und, safe, report = _scan(
        "BINARY(2)", "VARCHAR", [fit, overflow], dest_db="mysql"
    )
    assert [t.carrier for t in targets] == [CARRIER_BYTES]
    assert safe == ()
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].example_rows == (2,)
    assert report.findings[0].suggested_target_type == "BINARY(4)"
    assert "TEXT" not in (report.findings[0].suggested_target_type or "")
    assert "truncate" in report.findings[0].suggested_fix.lower()


def test_invalid_base64_into_binary_is_a_fix_not_a_widen() -> None:
    _targets, _und, _safe, report = _scan(
        "VARBINARY(16)", "VARCHAR", ["not-base64!!!"], dest_db="postgresql"
    )
    assert report.findings[0].unfit_rows == 1
    assert not report.findings[0].suggested_target_type
    assert "utf-8" in report.findings[0].suggested_fix.lower()


def test_bitstring_overflow_widens_bit_not_bytea() -> None:
    targets, _und, _safe, report = _scan(
        "BIT(8)", "VARCHAR", ["10101010", "101010101"], dest_db="postgresql"
    )
    assert [t.carrier for t in targets] == [CARRIER_BYTES]
    assert report.findings[0].unfit_rows == 1
    assert report.findings[0].suggested_target_type == "BIT(9)"
    assert "BYTEA" not in (report.findings[0].suggested_target_type or "")


def test_shorter_fixed_bit_is_a_pad_not_a_shrink() -> None:
    _targets, _und, _safe, report = _scan(
        "BIT(8)", "VARCHAR", ["101"], dest_db="postgresql"
    )
    assert report.findings[0].unfit_rows == 1
    assert not report.findings[0].suggested_target_type


def test_warehouse_binary_identical_width_skips_the_scan() -> None:
    targets, _und, safe, report = _scan(
        "VARBINARY(16)",
        "VARBINARY(16)",
        ["YWJj"],
        dest_db="mysql",
        source_kind="database",
        source_format="mysql",
    )
    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


def test_unbounded_bytea_stays_undecidable() -> None:
    targets, undecidable, safe, _report = _scan(
        "BYTEA", "VARCHAR", ["YWJj"], dest_db="postgresql"
    )
    assert targets == ()
    assert undecidable == ("c",)
    assert safe == ()
