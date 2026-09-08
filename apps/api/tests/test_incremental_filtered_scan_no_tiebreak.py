"""Incremental read on a cursor with no unique tie-break pages one filtered scan.

A strict ``WHERE cursor > page_max`` seek skips every row that shares the
page-edge cursor value. Without a primary key or enforced unique key the
engine must refuse the seek and page ``WHERE cursor > run_watermark`` on one
held cursor instead — on the read path, the decision owner and the SQLite
reader alike.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from connectors.sql_snapshot_scan import FILTERED_SCAN_SOURCES, scan_filter_value
from connectors.sqlite_reader import read_table_scan_batch
from services.keyset_pagination import (
    cursor_unique_evidence,
    decide_keyset_pagination,
    incremental_read_needs_filtered_scan,
)


def _decide(**overrides):
    kwargs = dict(
        src_type="postgresql",
        keyset_order_cols=["updated_seq"],
        keyset_col="updated_seq",
        keyset_tiebreak="",
        incremental=True,
        offset=0,
        chunk_index=0,
        cursor_after="100",
        snapshot_scan=True,
        cursor_is_unique=False,
    )
    kwargs.update(overrides)
    return decide_keyset_pagination(**kwargs)


def test_cursor_only_incremental_seek_is_refused_without_unique_evidence():
    decision = _decide()
    assert decision.use_keyset is False
    assert decision.pagination_mode == "filtered_scan"
    assert "updated_seq" in decision.seek_refused_reason


def test_unique_cursor_still_seeks():
    decision = _decide(cursor_is_unique=True)
    assert decision.use_keyset is True
    assert decision.pagination_mode == "keyset"
    assert decision.seek_refused_reason == ""


def test_tiebreak_still_seeks():
    decision = _decide(keyset_order_cols=["updated_seq", "id"], keyset_tiebreak="id")
    assert decision.use_keyset is True
    assert decision.seek_refused_reason == ""


def test_full_refresh_keyset_is_unchanged():
    decision = _decide(incremental=False, cursor_after=None)
    assert decision.use_keyset is True
    assert decision.seek_refused_reason == ""


@pytest.mark.parametrize(
    ("pk", "uks", "nulls", "expected"),
    [
        (["updated_seq"], [], {}, True),
        (["id"], [], {}, False),
        (["updated_seq", "id"], [], {}, False),
        ([], [{"columns": ["updated_seq"]}], {"updated_seq": False}, True),
        ([], [{"columns": ["updated_seq"]}], {}, False),
        ([], [{"columns": ["updated_seq"], "enforced": False}], {"updated_seq": False}, False),
        ([], [{"columns": ["updated_seq", "id"]}], {"updated_seq": False, "id": False}, False),
    ],
)
def test_cursor_unique_evidence(pk, uks, nulls, expected):
    assert (
        cursor_unique_evidence(
            "updated_seq", primary_key_columns=pk, unique_keys=uks, nullable=nulls
        )
        is expected
    )


@pytest.mark.parametrize("src_type", sorted(FILTERED_SCAN_SOURCES))
def test_filtered_scan_required_for_every_sql_scan_source(src_type):
    reason = incremental_read_needs_filtered_scan(
        src_type=src_type,
        incremental=True,
        cursor_column="updated_seq",
        tiebreak_column="",
        cursor_is_unique=False,
        callable_source=False,
    )
    assert "updated_seq" in reason


@pytest.mark.parametrize(
    "kwargs",
    [
        {"incremental": False},
        {"tiebreak_column": "id"},
        {"cursor_is_unique": True},
        {"callable_source": True},
        {"cursor_column": ""},
        {"src_type": "snowflake"},
    ],
)
def test_filtered_scan_not_required(kwargs):
    base = dict(
        src_type="postgresql",
        incremental=True,
        cursor_column="updated_seq",
        tiebreak_column="",
        cursor_is_unique=False,
        callable_source=False,
    )
    base.update(kwargs)
    assert incremental_read_needs_filtered_scan(**base) == ""


def test_scan_filter_value_decodes_run_watermark_only():
    assert scan_filter_value("updated_seq", "42") == "42"
    assert scan_filter_value("updated_seq", None) is None
    assert scan_filter_value("", "42") is None


def _read_all(db: Path, watermark: str | None, page: int) -> tuple[list, int | None]:
    state: dict = {}
    rows: list = []
    total = None
    offset = 0
    while True:
        batch = read_table_scan_batch(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table="t",
            offset=offset,
            limit=page,
            scan_state=state,
            filter_column="updated_seq",
            filter_after=watermark,
        )
        if total is None:
            total = batch.total_rows
        if not batch.rows:
            break
        rows.extend(batch.rows)
        offset += len(batch.rows)
    return rows, total


def test_sqlite_filtered_scan_keeps_every_tied_row_across_page_edges(tmp_path: Path):
    db = tmp_path / "src.db"
    # 40 rows over 4 distinct cursor values: every page edge lands mid-tie.
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (updated_seq INTEGER, name TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [(1 + i // 10, f"r{i}") for i in range(40)],
        )

    rows, total = _read_all(db, "1", page=7)
    assert total == 30
    assert len(rows) == 30
    assert sorted(r[1] for r in rows) == sorted(f"r{i}" for i in range(10, 40))
    assert [r[0] for r in rows] == sorted(r[0] for r in rows)


def test_sqlite_filtered_scan_first_run_reads_everything(tmp_path: Path):
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (updated_seq INTEGER, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(i % 3, f"r{i}") for i in range(9)])

    rows, total = _read_all(db, None, page=4)
    assert total == 9
    assert len(rows) == 9


def test_sqlite_filtered_scan_at_watermark_reads_nothing(tmp_path: Path):
    db = tmp_path / "src.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (updated_seq INTEGER, name TEXT)")
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(5, f"r{i}") for i in range(6)])

    rows, total = _read_all(db, "5", page=4)
    assert total == 0
    assert rows == []


def test_dispatcher_refuses_filtered_scan_without_algorithm():
    from src.transfer.batch_readers import _read_batch_impl

    with pytest.raises(ValueError, match="unique tie-break"):
        _read_batch_impl(
            "snowflake",
            {"host": "h", "port": 1, "database": "d"},
            "t",
            None,
            0,
            10,
            scan_state={},
            scan_filter=("updated_seq", "1"),
        )
    with pytest.raises(ValueError, match="scan_state"):
        _read_batch_impl(
            "postgresql",
            {"host": "h", "port": 1, "database": "d"},
            "t",
            None,
            0,
            10,
            scan_filter=("updated_seq", "1"),
        )


def test_generic_sql_filtered_scan_keeps_tied_rows_across_page_edges(tmp_path: Path):
    pytest.importorskip("sqlalchemy")
    from connectors.generic_sql import read_table_scan_batch as generic_scan

    db = tmp_path / "gen.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t (updated_seq INTEGER, name TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [(1 + i // 10, f"r{i}") for i in range(40)],
        )

    state: dict = {}
    rows: list = []
    total = None
    offset = 0
    while True:
        batch = generic_scan(
            host="",
            port=0,
            database=str(db),
            username="",
            password="",
            schema="",
            connection_string=f"sqlite:///{db}",
            ssl=False,
            table="t",
            offset=offset,
            limit=7,
            scan_state=state,
            filter_column="updated_seq",
            filter_after="2",
            type="sqlite",
        )
        if total is None:
            total = batch.total_rows
        if not batch.rows:
            break
        rows.extend(batch.rows)
        offset += len(batch.rows)

    assert total == 20
    assert len(rows) == 20
    assert sorted(r[1] for r in rows) == sorted(f"r{i}" for i in range(20, 40))
