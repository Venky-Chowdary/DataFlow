"""A quarantine record must name the row that actually failed.

Rejected rows are the dead-letter queue an operator replays, so the row number
is the only handle they have on the failing record. Helpers that receive a
*subset* of a batch — sparse CDC rows, the survivors of a bind — numbered
rejects by their position in that subset, which points at a different source
row. Nothing is lost silently, which is why this went unnoticed: the row is
quarantined with its reason and a value sample, under someone else's number.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    bind_rows_keeping_numbers,
    bind_sql_mapped_rows_with_quarantine,
    resolve_row_number,
    split_dense_sparse_rows,
    split_dense_sparse_rows_with_numbers,
)
from services.value_serializer import DF_MISSING_SENTINEL  # noqa: E402


def test_split_reports_source_positions_for_both_halves():
    rows = [
        ("1", "a"),
        ("2", DF_MISSING_SENTINEL),
        ("3", "c"),
        ("4", DF_MISSING_SENTINEL),
    ]
    dense, sparse, dense_rows, sparse_rows = split_dense_sparse_rows_with_numbers(rows)
    assert dense == [("1", "a"), ("3", "c")]
    assert sparse == [("2", DF_MISSING_SENTINEL), ("4", DF_MISSING_SENTINEL)]
    # 1-based, matching every other quarantine record.
    assert dense_rows == [1, 3]
    assert sparse_rows == [2, 4]


def test_split_honours_a_batch_offset():
    rows = [("1", DF_MISSING_SENTINEL), ("2", "b")]
    _dense, _sparse, dense_rows, sparse_rows = split_dense_sparse_rows_with_numbers(
        rows, row_offset=500
    )
    assert sparse_rows == [501]
    assert dense_rows == [502]


def test_legacy_split_still_returns_two_lists():
    """The four writers sharing this keep their signature."""
    rows = [("1", "a"), ("2", DF_MISSING_SENTINEL)]
    dense, sparse = split_dense_sparse_rows(rows)
    assert dense == [("1", "a")]
    assert sparse == [("2", DF_MISSING_SENTINEL)]


def test_resolve_row_number_falls_back_to_the_position():
    """A caller holding a whole batch can let the position stand in."""
    assert resolve_row_number(None, 0) == 1
    assert resolve_row_number([7, 9], 1) == 9
    # Out of range must not raise — it degrades to the position.
    assert resolve_row_number([7], 5) == 6


def test_bind_attributes_rejects_to_the_source_row():
    """A bad cell in the second sparse row is source row 4, not sparse row 2."""
    rejected: list[dict] = []
    sparse = [("10", "11"), ("20", "not-a-number")]
    bound = bind_sql_mapped_rows_with_quarantine(
        sparse,
        ["id", "amount"],
        ["INTEGER", "INTEGER"],
        rejected,
        "quarantine",
        engine="postgresql",
        row_numbers=[2, 4],
    )
    assert len(bound) == 1
    assert len(rejected) == 1
    assert rejected[0]["row"] == 4


def test_bind_returns_the_numbers_of_the_rows_that_survived():
    """Binding drops what it quarantines, so the numbers must be re-derived.

    Carrying the original list forward would name rows that are no longer
    present, which is the same misattribution one layer later.
    """
    rejected: list[dict] = []
    rows = [("bad",), ("2",), ("also-bad",), ("4",)]
    bound, kept = bind_rows_keeping_numbers(
        rows,
        ["id"],
        ["INTEGER"],
        rejected,
        "quarantine",
        engine="postgresql",
        row_numbers=[10, 20, 30, 40],
    )
    assert len(bound) == 2
    assert kept == [20, 40]
    assert sorted(d["row"] for d in rejected) == [10, 30]


def test_bind_on_an_empty_batch_returns_no_numbers():
    rejected: list[dict] = []
    bound, kept = bind_rows_keeping_numbers(
        [], ["id"], ["INTEGER"], rejected, "quarantine", engine="postgresql"
    )
    assert bound == []
    assert kept == []


def test_bigquery_sparse_upsert_names_the_source_row():
    """The reported defect, end to end through the BigQuery sparse path."""
    from unittest.mock import MagicMock

    import connectors.bigquery_writer as bw

    rejected: list[dict] = []
    # A sparse row whose identity key is absent — the sparse upsert refuses it.
    sparse = [(DF_MISSING_SENTINEL, "x"), (None, "y")]
    written, skipped, checksum_rows = bw._bq_apply_sparse_upsert(
        MagicMock(),
        "proj.ds.tbl",
        ["id", "note"],
        ["id"],
        sparse,
        ["INT64", "STRING"],
        rejected_details=rejected,
        policy="quarantine",
        row_numbers=[6, 9],
    )
    assert written == 0
    assert rejected, "a sparse row with no identity must be quarantined"
    # Both rejects must name their source rows, never 0 and 1.
    assert {d["row"] for d in rejected} <= {6, 9}
    assert 0 not in {d["row"] for d in rejected}
