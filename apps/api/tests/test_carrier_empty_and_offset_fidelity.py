"""Two fidelity rules the file matrix at 100K found broken (Track B).

1. A carrier that spells NULL separately from ``""`` must keep the empty string
   the source stated; only delimited carriers (no NULL token in RFC 4180) may
   read a blank field as absence.
2. A declared offset-bearing timestamp column must keep its offset on export —
   folding it to a naive wire value moves the instant.
"""
from __future__ import annotations

import io

import pytest

from connectors.writer_common import to_delimited_value, to_json_value
from src.transfer.file_stream import _batch_iterator_for_type, _iter_csv_batches


def test_json_family_keeps_explicit_empty_string():
    raw = b'[{"id": 1, "note": ""}]'
    rows = next(iter(_batch_iterator_for_type("json", raw, 10)))
    assert rows[0]["note"] == ""

    jsonl = b'{"id": 1, "note": ""}\n'
    rows = next(iter(_batch_iterator_for_type("jsonl", jsonl, 10)))
    assert rows[0]["note"] == ""


def test_parquet_keeps_explicit_empty_string():
    pa = pytest.importorskip("pyarrow")
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    table = pa.table({"id": [1, 2], "note": ["", None]})
    pq.write_table(table, buf)
    rows = next(iter(_batch_iterator_for_type("parquet", buf.getvalue(), 10)))
    assert rows[0]["note"] == ""
    assert rows[1]["note"] is None


def test_delimited_blank_field_is_absence():
    raw = b"id,note\n1,\n"
    rows = next(iter(_iter_csv_batches(raw, 10)))
    assert rows[0]["note"] is None


@pytest.mark.parametrize(
    "declared",
    ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "timestamptz"],
)
def test_instant_survives_declared_tz_column(declared):
    """The offset column normalizes to the UTC instant, not the local wall clock.

    ``normalize_logical_type`` folds every timestamp flavour into ``datetime``,
    so resolving the temporal carrier from it alone made an offset-bearing column
    behave like a naive one: ``01:01:07+05:30`` was written as ``01:01:07``, a
    different instant. The carrier now comes from the declared type.
    """
    value = "2024-02-02T01:01:07+05:30"
    types = {"created_at": declared}
    assert str(to_json_value(value, "created_at", types)).startswith("2024-02-01 19:31:07")
    assert str(to_delimited_value(value, "created_at", types)).startswith(
        "2024-02-01 19:31:07"
    )


def test_naive_timestamp_column_stays_naive():
    value = "2024-02-02T01:01:07"
    out = str(to_json_value(value, "created_at", {"created_at": "TIMESTAMP"}))
    assert out.startswith("2024-02-02")
    assert "+" not in out
