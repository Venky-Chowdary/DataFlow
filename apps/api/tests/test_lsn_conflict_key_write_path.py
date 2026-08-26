"""LSN / conflict-key lookup uses _is_nullish_conflict_key.

filter_stale_lsn_rows only skipped Python None / \"\". After extract emits
SQL_NULL_SENTINEL, the lookup bound ``col = '__DF_SQL_NULL__'``, missed dest
NULL PKs, and fail-opened the LSN gate on at-least-once redelivery.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.postgresql_writer import (  # noqa: E402
    _redshift_filter_stale_lsn_rows,
    _redshift_stage_delete,
)
from connectors.writer_common import (  # noqa: E402
    DF_LSN_COL,
    _conflict_key_identity,
    _is_nullish_conflict_key,
    filter_stale_lsn_rows,
)
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402

COLS = ["id", "v", DF_LSN_COL]


class _Cursor:
    def __init__(self, dest_rows: list[tuple] | None = None, fetchone_lsn: str | None = None):
        self.dest_rows = dest_rows or []
        self.fetchone_lsn = fetchone_lsn
        self.stmts: list[str] = []
        self.params: list[object] = []

    def execute(self, stmt: object, params: object = None) -> None:
        self.stmts.append(str(stmt))
        self.params.append(params)

    def fetchall(self):
        return list(self.dest_rows)

    def fetchone(self):
        if self.fetchone_lsn is None:
            return None
        return (self.fetchone_lsn,)

    def executemany(self, query: object, params: object = None) -> None:
        return None


class _Frag:
    def __init__(self, text: str = ""):
        self.text = text

    def format(self, *args: object) -> "_Frag":
        return _Frag(" ".join([self.text, *[str(a) for a in args]]))

    def join(self, parts: object) -> "_Frag":
        return _Frag(self.text.join(str(p) for p in parts))

    def __str__(self) -> str:
        return self.text


class _SQL:
    @staticmethod
    def SQL(text: str) -> _Frag:
        return _Frag(text)

    @staticmethod
    def Identifier(name: str) -> str:
        return name

    @staticmethod
    def Placeholder() -> str:
        return "%s"


def test_conflict_key_identity_collapses_reader_null_and_blank():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", "", "  "):
        assert _is_nullish_conflict_key(wire) is True
        assert _conflict_key_identity(wire) is None
    assert _conflict_key_identity(0) == 0
    assert _conflict_key_identity(False) is False
    assert _conflict_key_identity("1") == "1"


def test_filter_stale_lsn_reader_null_uses_is_null_and_skips_stale():
    cur = _Cursor(dest_rows=[(None, "0/300")])
    out, skipped = filter_stale_lsn_rows(
        cur,
        "orders",
        "public",
        ["id"],
        [(SQL_NULL_SENTINEL, "stale", "0/100")],
        COLS,
    )
    assert skipped == 1
    assert out == []
    assert "IS NULL" in cur.stmts[0]
    assert SQL_NULL_SENTINEL not in (cur.params[0] or [])
    assert "__DF_SQL_NULL__" not in cur.stmts[0]


def test_filter_stale_lsn_reader_null_writes_when_incoming_newer():
    cur = _Cursor(dest_rows=[(None, "0/100")])
    row = (SQL_NULL_SENTINEL, "fresh", "0/200")
    out, skipped = filter_stale_lsn_rows(
        cur, "orders", "public", ["id"], [row], COLS
    )
    assert skipped == 0
    assert out == [row]
    assert "IS NULL" in cur.stmts[0]


def test_filter_stale_lsn_python_none_matches_dest_null():
    cur = _Cursor(dest_rows=[(None, "0/300")])
    out, skipped = filter_stale_lsn_rows(
        cur, "orders", "public", ["id"], [(None, "stale", "0/100")], COLS
    )
    assert skipped == 1
    assert out == []
    assert "IS NULL" in cur.stmts[0]
    assert cur.params[0] == []


def test_filter_stale_lsn_present_key_still_binds_equality():
    cur = _Cursor(dest_rows=[("1", "0/300")])
    out, skipped = filter_stale_lsn_rows(
        cur, "orders", "public", ["id"], [("1", "stale", "0/100")], COLS
    )
    assert skipped == 1
    assert out == []
    assert "IS NULL" not in cur.stmts[0]
    assert cur.params[0] == ["1"]


def test_filter_stale_lsn_zero_and_false_are_present_keys():
    cur = _Cursor(dest_rows=[(0, "0/300")])
    out, skipped = filter_stale_lsn_rows(
        cur, "orders", "public", ["id"], [(0, "stale", "0/100")], COLS
    )
    assert skipped == 1
    assert "IS NULL" not in cur.stmts[0]
    assert cur.params[0] == [0]

    cur2 = _Cursor(dest_rows=[(False, "0/300")])
    out2, skipped2 = filter_stale_lsn_rows(
        cur2, "orders", "public", ["id"], [(False, "stale", "0/100")], COLS
    )
    assert skipped2 == 1
    assert "IS NULL" not in cur2.stmts[0]
    assert cur2.params[0] == [False]


def test_redshift_lsn_lookup_reader_null_is_is_null():
    cur = _Cursor(fetchone_lsn="0/300")
    out = _redshift_filter_stale_lsn_rows(
        cur,
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=COLS,
        conflict_cols=["id"],
        batch=[(SQL_NULL_SENTINEL, "stale", "0/100")],
    )
    assert out == []
    assert "IS NULL" in cur.stmts[0]
    assert SQL_NULL_SENTINEL not in (cur.params[0] or [])


def test_redshift_stage_delete_refuses_reader_null_conflict_key():
    cur = _Cursor(fetchone_lsn=None)
    with pytest.raises(ValueError, match="null/empty conflict key"):
        _redshift_stage_delete(
            cur,
            _SQL,
            schema="public",
            table_name="orders",
            target_cols=COLS,
            conflict_cols=["id"],
            batch=[(SQL_NULL_SENTINEL, "fresh", "0/200")],
        )


def test_generic_sql_delete_by_keys_refuses_reader_null():
    from connectors.generic_sql import _delete_by_keys

    with pytest.raises(ValueError, match="null/empty conflict key"):
        _delete_by_keys(None, None, [{"id": SQL_NULL_SENTINEL}], ["id"])
    with pytest.raises(ValueError, match="null/empty conflict key"):
        _delete_by_keys(None, None, [{"id": ""}], ["id"])
