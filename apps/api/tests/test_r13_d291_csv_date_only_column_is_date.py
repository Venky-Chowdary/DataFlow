"""D291 — a CSV date-only column lands as DATE, not TIMESTAMP.

QA: SFTP CSV column ``d`` (``2024-01-01`` ...) with one bad cell
(``2024-13-45``) was planned and created in PostgreSQL as TIMESTAMP and every
value landed as ``2024-01-01 00:00:00``. The bad cell kept the profiler from
settling on DATE, and the semantic fallback chose the datetime parse. A
date-shaped column whose cells carry no clock is a calendar day; columns with
a time of day keep their timestamp carrier. The bad cell is still quarantined.

Planner level (the stamp MCP plan_transfer and engine automap share) for
PostgreSQL / MySQL / SQL Server; no SFTP server here for QA's exact route.
"""

from __future__ import annotations

import pytest

from services.transform_engine import infer_transform_for_mapping


@pytest.mark.parametrize(
    ("samples", "expected"),
    [
        (["2024-01-01", "2024-01-02", "2024-13-45", "", "2024-02-29"], "date"),
        (["2024-01-01 10:00:00", "2024-01-02 11:30", "2024-01-03 00:00:00",
          "2024-01-04 09:15:00", "bad"], "datetime"),
    ],
)
def test_clockless_date_text_infers_a_date_parse(samples, expected):
    assert infer_transform_for_mapping("d", "d", "VARCHAR", None, source_samples=samples) == expected


def test_one_clocked_cell_past_a_window_of_dates_keeps_the_timestamp():
    clean = [f"2024-01-{(i % 28) + 1:02d}" for i in range(40)]
    assert infer_transform_for_mapping("d", "d", "VARCHAR", None, source_samples=clean) == "date"
    assert infer_transform_for_mapping(
        "d", "d", "VARCHAR", None, source_samples=clean + ["2024-01-01 10:30:00"]
    ) == "datetime"


def test_date_transform_refuses_a_time_of_day_instead_of_dropping_it():
    from services.transform_engine import apply_transform

    assert apply_transform("2024-01-01", "date") == ("2024-01-01", None)
    assert apply_transform("2024-01-01 00:00:00", "date") == ("2024-01-01", None)
    value, error = apply_transform("2024-01-01 10:30:00", "date")
    assert value is None and "10:30:00" in error and "DATE column cannot hold" in error, error
    assert "QUARANTINE_ROW" in error


_D = ["2024-01-01", "2024-01-02", "2024-13-45", "", "2024-02-29"]


@pytest.mark.parametrize(
    ("dest", "carrier"), [("postgresql", "DATE"), ("mysql", "DATE"), ("sqlserver", "DATE")]
)
def test_csv_plan_creates_a_date_column(dest, carrier):
    """The planner MCP plan and engine automap share stamps DATE + a date parse."""
    from services.mapping_pipeline import run_mapping_pipeline

    samples = {"id": [str(i) for i in range(1, 6)], "d": _D, "note": ["a", "", "c", "東京", "  "]}
    out = run_mapping_pipeline(
        ["id", "d", "note"], [],
        source_schemas=[{"name": c, "inferred_type": "VARCHAR", "samples": v}
                        for c, v in samples.items()],
        source_samples=samples, destination_db_type=dest, source_db_type="sftp",
        file_format="csv", destination_table_exists=False, use_llm=False,
    )
    d = next(m for m in out["mappings"] if m["source"] == "d")
    assert str(d["target_type"]).upper().startswith(carrier), d
    assert d["transform"] == "date", d
