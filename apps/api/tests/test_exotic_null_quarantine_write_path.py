"""Exotic write-quarantine skips reader-null via _unfit_cell_absent.

BINARY treated extract SQL_NULL_SENTINEL as invalid base64 and held the
row out. BIT / ENUM / specialty used None+Missing only, so DuckDB null
and the extract token were present text. Empty string stays present
(unfit specialty / empty INTEGER still refuse).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.salesforce_writer import _normalize_salesforce_id_cells  # noqa: E402
from connectors.writer_common import (  # noqa: E402
    quarantine_unfit_binaries,
    quarantine_unfit_bitstrings,
    quarantine_unfit_enum_set,
    quarantine_unfit_specialty_types,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def _reader_nulls() -> tuple[object, ...]:
    return (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing, DF_MISSING_SENTINEL)


def test_binary_quarantine_keeps_reader_null_not_base64_holdout():
    details: list[dict] = []
    rows = [(wire,) for wire in _reader_nulls()] + [("not-base64!!!",)]
    out = quarantine_unfit_binaries(
        rows,
        ["blob"],
        ["VARBINARY(16)"],
        details,
        policy="quarantine",
    )
    assert [row[0] for row in out] == list(_reader_nulls())
    assert len(details) == 1
    assert "base64" in details[0]["reason"].lower()
    assert SQL_NULL_SENTINEL not in str(details[0].get("value") or "")


def test_bitstring_and_enum_quarantine_keep_reader_null():
    bit_details: list[dict] = []
    bit_out = quarantine_unfit_bitstrings(
        [(SQL_NULL_SENTINEL,), ("YWJj",), ("1010",)],
        ["flags"],
        ["BIT(4)"],
        bit_details,
        policy="quarantine",
    )
    assert bit_out == [(SQL_NULL_SENTINEL,), ("1010",)]
    assert bit_details and "0/1" in bit_details[0]["reason"]

    enum_details: list[dict] = []
    enum_out = quarantine_unfit_enum_set(
        [("__df_ddb_null__",), ("z",), ("a",)],
        ["status"],
        ["ENUM('a','b')"],
        enum_details,
        policy="quarantine",
    )
    assert enum_out == [("__df_ddb_null__",), ("a",)]
    assert enum_details and "ENUM" in enum_details[0]["reason"]


def test_specialty_quarantine_keeps_reader_null_empty_still_unfit():
    details: list[dict] = []
    out = quarantine_unfit_specialty_types(
        [
            (SQL_NULL_SENTINEL,),
            ("__df_ddb_null__",),
            ("",),
            ("POINT(0 0)",),
        ],
        ["geom"],
        ["GEOMETRY"],
        details,
        policy="quarantine",
    )
    assert out == [(SQL_NULL_SENTINEL,), ("__df_ddb_null__",), ("POINT(0 0)",)]
    assert details and "geography" in details[0]["reason"]
    assert details[0].get("value") == ""


def test_salesforce_id_normalize_skips_reader_null():
    details: list[dict] = []
    rows = [
        (SQL_NULL_SENTINEL,),
        (Missing,),
        ("not-an-id",),
    ]
    out = _normalize_salesforce_id_cells(
        rows,
        ["Id"],
        ["VARCHAR(18)"],
        details,
        policy="quarantine",
    )
    assert out[0][0] == SQL_NULL_SENTINEL
    assert out[1][0] is Missing
    assert details
    assert all(row[0] != "not-an-id" for row in out)
