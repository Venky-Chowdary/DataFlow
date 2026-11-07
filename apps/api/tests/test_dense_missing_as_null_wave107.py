"""Dense INSERT/COPY: DF_MISSING → SQL NULL; Gate-8 fingerprints match."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import materialize_missing_as_null_for_dense_write
from services.reconciliation import normalize_cell
from services.value_serializer import DF_MISSING_SENTINEL, SQL_NULL_SENTINEL


def test_materialize_missing_as_null_for_dense_write():
    rows = [
        ("1", True, "keep"),
        ("2", DF_MISSING_SENTINEL, "x"),
        ("3", None, DF_MISSING_SENTINEL),
    ]
    out = materialize_missing_as_null_for_dense_write(rows)
    assert out[0] == ("1", True, "keep")
    assert out[1] == ("2", None, "x")
    assert out[2] == ("3", None, None)


def test_materialize_noop_when_no_missing():
    rows = [("1", True), ("2", False)]
    assert materialize_missing_as_null_for_dense_write(rows) is rows


def test_materialize_dense_reader_null_token_not_identity():
    """Extract SQL_NULL_SENTINEL must not skip rewrite just because no DF_MISSING."""

    rows = [("1", SQL_NULL_SENTINEL, ""), ("2", "__df_ddb_null__", 0)]
    out = materialize_missing_as_null_for_dense_write(rows)
    assert out == [("1", None, ""), ("2", None, 0)]
    assert out is not rows
    assert SQL_NULL_SENTINEL not in out[0]


def test_materialize_noop_when_only_empty_or_space():
    rows = [("1", ""), ("2", "  ")]
    assert materialize_missing_as_null_for_dense_write(rows) is rows


def test_normalize_cell_missing_equates_sql_null():
    assert normalize_cell(DF_MISSING_SENTINEL) == normalize_cell(None)
    assert normalize_cell(DF_MISSING_SENTINEL) == normalize_cell(SQL_NULL_SENTINEL)
    assert normalize_cell("__DF_MISSING__") == normalize_cell(None)
