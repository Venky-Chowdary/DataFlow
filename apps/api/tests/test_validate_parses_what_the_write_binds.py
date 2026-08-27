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
    """Calendar scanning must not pull BOOLEAN off the typed-skip cost path."""
    targets, _und, safe, report = _scan("BOOLEAN", "BOOLEAN", ["true", "false"])

    assert targets == ()
    assert safe == ("c",)
    assert report.findings == ()


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
