"""Sample refine must escape embedded quote characters in identifiers."""

from __future__ import annotations

import pytest

from services.schema_introspect import _refine_columns_by_samples


class _Cursor:
    def __init__(self, sink: list[str]):
        self._sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self._sink.append(sql)

    def fetchall(self):
        return []


class _Conn:
    def __init__(self):
        self.sql: list[str] = []

    def cursor(self):
        return _Cursor(self.sql)


def _text_col(name: str) -> dict:
    return {"name": name, "inferred_type": "TEXT", "data_type": "text"}


@pytest.mark.parametrize(
    "quote_char, col, schema, table, expected",
    [
        (
            '"',
            'a" FROM pg_shadow --',
            'sc"h',
            't"bl',
            'SELECT "a"" FROM pg_shadow --" FROM "sc""h"."t""bl" LIMIT %s',
        ),
        (
            "`",
            "a` FROM mysql.user -- ",
            "d`b",
            "t`bl",
            "SELECT `a`` FROM mysql.user -- ` FROM `d``b`.`t``bl` LIMIT %s",
        ),
    ],
)
def test_embedded_quotes_are_doubled(quote_char, col, schema, table, expected):
    conn = _Conn()
    _refine_columns_by_samples(conn, [_text_col(col)], table, schema, quote_char=quote_char)
    assert conn.sql == [expected]


def test_unqualified_table_is_quoted():
    conn = _Conn()
    _refine_columns_by_samples(conn, [_text_col("x")], 'o"rders', "")
    assert conn.sql == ['SELECT "x" FROM "o""rders" LIMIT %s']
