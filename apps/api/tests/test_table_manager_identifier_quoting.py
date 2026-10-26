"""Destination drop/delete paths must quote identifiers, not interpolate them.

A destination table or key-column name containing the dialect's quote
character used to close the quoted identifier and splice SQL into
``DROP TABLE`` / ``DELETE FROM`` (e.g. ``x`, `victim`` dropped a second
table). Every identifier now goes through ``quote_sql_identifier``.
"""

from __future__ import annotations

import contextlib
import sqlite3
from typing import Any, Self

import pytest

from connectors.table_manager import delete_by_primary_keys, drop_table
from services.cdc_snapshot_window import _PK_SEP


def _tables(path: str) -> set[str]:
    conn = sqlite3.connect(path)
    try:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "dest.db")
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE "x" (id TEXT PRIMARY KEY, region TEXT)')
    conn.execute('CREATE TABLE "sentinel" (id TEXT PRIMARY KEY, region TEXT)')
    conn.executemany('INSERT INTO "sentinel" VALUES (?, ?)', [("1", "us"), ("2", "eu")])
    conn.commit()
    conn.close()
    return path


@pytest.mark.parametrize(
    "payload",
    ["x`, `sentinel", 'x", "sentinel', "x`; DROP TABLE `sentinel"],
)
def test_sqlite_drop_cannot_reach_a_second_table(db_path, payload):
    with contextlib.suppress(Exception):
        drop_table("sqlite", {"database": db_path}, payload)
    assert "sentinel" in _tables(db_path)
    assert "x" in _tables(db_path)


def test_sqlite_drop_handles_quote_char_in_a_real_table_name(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute('CREATE TABLE "we`ird""name" (id TEXT)')
    conn.commit()
    conn.close()
    assert drop_table("sqlite", {"database": db_path}, 'we`ird"name') is True
    assert _tables(db_path) == {"x", "sentinel"}


def _sentinel_ids(path: str) -> list[str]:
    conn = sqlite3.connect(path)
    try:
        return [r[0] for r in conn.execute('SELECT id FROM "sentinel" ORDER BY id')]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "table,pk",
    [
        ('sentinel" WHERE 1=1 --', "id"),
        ("sentinel", "id\" IN ('1') OR 1=1 OR \"id"),
    ],
)
def test_sqlite_delete_cannot_widen_the_predicate(db_path, table, pk):
    with contextlib.suppress(Exception):
        delete_by_primary_keys("sqlite", {"database": db_path}, table, pk, ["nope"])
    assert _sentinel_ids(db_path) == ["1", "2"]


def test_sqlite_composite_delete_quotes_table(db_path):
    with contextlib.suppress(Exception):
        delete_by_primary_keys(
            "sqlite",
            {"database": db_path},
            'sentinel" WHERE 1=1 --',
            ["id", "region"],
            [_PK_SEP.join(("nope", "nope"))],
        )
    assert _sentinel_ids(db_path) == ["1", "2"]


class _FakeCursor:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink
        self.rowcount = 0

    def execute(self, query: str, params: Any = None) -> None:
        self._sink.append(query)

    def fetchall(self) -> list[Any]:
        return []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class _FakeConn:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink
        self.autocommit = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self._sink)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.fixture()
def mysql_sql(monkeypatch):
    sink: list[str] = []
    from connectors import mysql_conn

    monkeypatch.setattr(mysql_conn, "get_connection", lambda **_: _FakeConn(sink))
    return sink


def test_mysql_drop_escapes_backticks(mysql_sql):
    assert drop_table("mysql", {}, "x`, `sentinel") is True
    assert mysql_sql == ["DROP TABLE IF EXISTS `x``, ``sentinel`"]


def test_mysql_delete_escapes_table_and_pk(mysql_sql):
    delete_by_primary_keys("mysql", {}, "x` WHERE 1=1 -- ", "id`) OR 1=1 --", ["1"])
    assert mysql_sql == ["DELETE FROM `x`` WHERE 1=1 -- ` WHERE `id``) OR 1=1 --` IN (%s)"]


def test_mysql_composite_delete_escapes_table(mysql_sql):
    delete_by_primary_keys(
        "mysql", {}, "x` WHERE 1=1 -- ", ["id", "re`gion"], [_PK_SEP.join(("1", "us"))]
    )
    assert len(mysql_sql) == 1
    assert mysql_sql[0].startswith("DELETE FROM `x`` WHERE 1=1 -- ` WHERE ")
    assert "`re``gion` = %s" in mysql_sql[0]


def test_snowflake_drop_escapes_double_quotes(monkeypatch):
    sink: list[str] = []
    from connectors import snowflake_conn

    monkeypatch.setattr(snowflake_conn, "get_connection", lambda **_: _FakeConn(sink))
    cfg = {"warehouse": 'wh"; DROP TABLE t; --'}
    assert drop_table("snowflake", cfg, 'x", "sentinel') is True
    statements = [q for q in sink if q.startswith(("USE ", "DROP "))]
    assert statements == [
        'USE WAREHOUSE "wh""; DROP TABLE t; --"',
        'DROP TABLE IF EXISTS "x"", ""sentinel"',
    ]
