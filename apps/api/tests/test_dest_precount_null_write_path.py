"""Dest identity keys use present_cell_text, not a second stringify.

Reader-wired SQL_NULL_SENTINEL used to look like a present PK, so Iceberg
key hits and artifact samples searched for the sentinel spelling. JSONL
"1" and catalog 1 must share one key. True and dest "true" share one key.
Empty / whitespace stay incomplete identity.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.dest_precount import (  # noqa: E402
    _iceberg_key_hits,
    _norm_dest_key,
    _row_values_for_cols,
    _unique_key_tuples,
    iceberg_target_sample,
    records_to_key_tuples,
)
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)


def test_norm_dest_key_matches_reader_wire():
    assert _norm_dest_key((None,)) is None
    assert _norm_dest_key(("",)) is None
    assert _norm_dest_key(("   ",)) is None
    assert _norm_dest_key((SQL_NULL_SENTINEL,)) is None
    assert _norm_dest_key((DF_MISSING_SENTINEL,)) is None
    assert _norm_dest_key((Missing,)) is None
    assert _norm_dest_key((0,)) == ("0",)
    assert _norm_dest_key((1,)) == ("1",)
    assert _norm_dest_key(("1",)) == ("1",)
    assert _norm_dest_key((True,)) == ("true",)
    assert _norm_dest_key((True,)) != (str(True),)


def test_row_values_skip_reader_null():
    assert _row_values_for_cols({"id": SQL_NULL_SENTINEL}, ["id"]) is None
    assert _row_values_for_cols({"id": None}, ["id"]) is None
    assert _row_values_for_cols({"id": ""}, ["id"]) is None
    assert _row_values_for_cols({"id": 1}, ["id"]) == (1,)


def test_unique_key_tuples_skip_and_dedup_wire():
    assert _unique_key_tuples(
        [(SQL_NULL_SENTINEL,), (None,), (1,), ("1",), (True,), ("true",)],
        1,
    ) == [(1,), (True,)]


def test_records_to_key_tuples_reader_null_is_unmeasured():
    assert records_to_key_tuples([{"id": SQL_NULL_SENTINEL}], ["id"]) is None
    assert records_to_key_tuples([{"id": ""}], ["id"]) is None


def test_records_to_key_tuples_bool_and_dest_true_are_one_key():
    assert records_to_key_tuples([{"id": True}, {"id": "true"}], ["id"]) is None
    assert records_to_key_tuples([{"id": 1}, {"id": "1"}], ["id"]) is None
    assert records_to_key_tuples([{"id": 1}, {"id": 2}], ["id"]) == [(1,), (2,)]


def test_iceberg_key_hits_bool_matches_dest_true(monkeypatch):
    monkeypatch.setattr(
        "services.dest_precount._iceberg_snapshot_rows",
        lambda *_a, **_k: [{"id": True, "name": "kept"}],
    )
    assert _iceberg_key_hits(
        {},
        schema="",
        table_name="jobs",
        cols=["id"],
        keys=[("true",), (SQL_NULL_SENTINEL,)],
    ) == 1


def test_iceberg_sample_filter_skips_reader_null(monkeypatch):
    monkeypatch.setattr(
        "services.dest_precount._iceberg_snapshot_rows",
        lambda *_a, **_k: [
            {"id": SQL_NULL_SENTINEL, "name": "null"},
            {"id": True, "name": "kept"},
            {"id": 2, "name": "other"},
        ],
    )
    rows = iceberg_target_sample(
        {},
        schema="",
        table_name="jobs",
        columns=["id", "name"],
        sort_key="id",
        key_values=["true", SQL_NULL_SENTINEL],
    )
    assert rows == [{"id": True, "name": "kept"}]
