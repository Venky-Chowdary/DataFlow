"""ITEM 25 — Excel/CSV blank cells must not FAIL_JOB a nullable typed load.

fsi-2019.xlsx → Postgres failed with ~28k cell findings (Empty value cannot
coerce to decimal/datetime) under Migration Risk Contract abort. Spreadsheet
absence is SQL NULL on nullable columns — Airbyte-class empty→null — not silent
loss of a present value. DB→DB empty strings still require a Risk Contract.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from connectors.writer_common import (
    build_mapped_rows_with_details,
    reject_on_strict_policy,
)
from services.transform_engine import infer_transform_for_mapping


def test_file_empty_decimal_becomes_null_without_contract():
    mapped, errs, details = build_mapped_rows_with_details(
        headers=["Country", "Total", "Year"],
        data_rows=[
            ["Finland", "16.9", "2019"],
            ["Trailer", "", ""],  # blank score/year cells
        ],
        mappings=[
            {"source": "Country", "target": "country", "confidence": 0.99},
            {
                "source": "Total",
                "target": "total",
                "transform": "decimal",
                "confidence": 0.99,
            },
            {
                "source": "Year",
                "target": "year",
                "transform": "integer",
                "confidence": 0.99,
            },
        ],
        target_cols=["country", "total", "year"],
        column_types={"Country": "string", "Total": "decimal", "Year": "integer"},
        dest_types={"country": "text", "total": "decimal", "year": "integer"},
        error_policy="fail",
        dest_kind="postgresql",
        empty_cells_as_null=True,
    )
    assert errs == []
    assert details == []
    assert len(mapped) == 2
    assert mapped[1][0] == "Trailer"
    assert mapped[1][1] is None
    assert mapped[1][2] is None
    assert reject_on_strict_policy("fail", details, "PostgreSQL") is None


def test_db_empty_integer_still_quarantines_without_file_flag():
    """Regression: MySQL '' → INTEGER without empty_cells_as_null stays honest."""
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "age"],
        data_rows=[["1", ""], ["2", "30"]],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "age", "target": "age", "transform": "integer", "confidence": 0.99},
        ],
        target_cols=["id", "age"],
        column_types={"id": "string", "age": "string"},
        dest_types={"id": "string", "age": "integer"},
        error_policy="quarantine",
        dest_kind="postgresql",
        empty_cells_as_null=False,
    )
    assert len(mapped) == 1
    assert details
    assert any("empty" in str(d.get("reason") or "").lower() for d in details)


def test_empty_into_not_null_still_fails_under_file_flag():
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["age"],
        data_rows=[[""], ["3"]],
        mappings=[
            {
                "source": "age",
                "target": "age",
                "transform": "integer",
                "target_nullable": False,
            }
        ],
        target_cols=["age"],
        dest_types={"age": "integer"},
        error_policy="fail",
        empty_cells_as_null=True,
        destination_column_nullability={"age": False},
    )
    assert len(mapped) == 1
    assert details
    abort = reject_on_strict_policy("fail", details, "PostgreSQL")
    assert abort
    assert "cell finding" in abort.lower()


def test_year_field_does_not_invent_datetime_transform():
    assert (
        infer_transform_for_mapping("Year", "year", "INTEGER", "TIMESTAMP")
        == "integer"
    )
    assert (
        infer_transform_for_mapping("Year", "year", "string", "datetime") == "integer"
    )


def test_abort_message_counts_cells_and_rows():
    details = [
        {"row": 1, "policy": "fail", "execution_policy": "FAIL_JOB"},
        {"row": 1, "policy": "fail", "execution_policy": "FAIL_JOB"},
        {"row": 2, "policy": "fail", "execution_policy": "FAIL_JOB"},
    ]
    msg = reject_on_strict_policy("fail", details, "PostgreSQL")
    assert msg is not None
    assert "3 cell finding" in msg
    assert "2 row" in msg
