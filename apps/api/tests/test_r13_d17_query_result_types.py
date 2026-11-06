"""D17 (remaining): run_query result types tell the truth.

* SQLite ``count(distinct name) AS name`` was labelled TEXT: the base table's
  catalog type was applied to an aggregate because its alias spelled a column.
* Snowflake TO_VARCHAR outputs were labelled from the raw cursor type code
  ("2") — or, after inference, INTEGER/TIMESTAMP.
* Snowflake ``LATERAL FLATTEN`` output columns (VALUE, KEY ...) were refused
  as identifiers not in the introspected schema.
"""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest


@pytest.fixture
def sqlite_conn(tmp_path):
    db = tmp_path / "q.db"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t (id INTEGER, name TEXT, amt REAL)")
        c.executemany("INSERT INTO t VALUES (?,?,?)", [(1, "a", 1.5), (2, "b", None), (3, "a", 2.0)])
    return SimpleNamespace(
        type="sqlite", host="", port=0, database=str(db), username="", password="",
        connection_string="", schema="", warehouse="", ssl=False, extra={},
    )


@pytest.mark.parametrize(
    "sql,col,expected",
    [
        ("SELECT count(distinct name) AS name FROM t", "name", "INTEGER"),
        ("SELECT count(distinct name) AS id FROM t", "id", "INTEGER"),
        ("SELECT name FROM t", "name", "TEXT"),
        ("SELECT name AS label FROM t", "label", "TEXT"),
        ("SELECT * FROM t", "amt", "REAL"),
        ("SELECT upper(name) AS name FROM t", "name", None),
    ],
)
def test_sqlite_catalog_types_apply_only_to_passthrough_columns(sqlite_conn, sql, col, expected):
    from src.routers.query_router import QueryExecuteRequest, _run_sql_query

    _rows, _cols, schema, _ = _run_sql_query(sqlite_conn, QueryExecuteRequest(connector_id="x", query=sql, limit=10))
    if expected is None:
        assert schema[col] != "INTEGER", schema
    else:
        assert schema[col] == expected, schema


@pytest.mark.parametrize(
    "code,precision,scale,expected",
    [
        (2, None, None, "VARCHAR"),       # TO_VARCHAR(...)
        (0, 38, 0, "DECIMAL(38,0)"),      # DATEDIFF / COUNT -> NUMBER(38,0)
        (0, 12, 2, "DECIMAL(12,2)"),
        (3, None, None, "DATE"),
        (8, None, None, "TIMESTAMP"),
        (7, None, None, "TIMESTAMPTZ"),
        (5, None, None, "JSON"),          # VARIANT doc:k
        (13, None, None, "BOOLEAN"),
    ],
)
def test_snowflake_cursor_codes_are_read_as_snowflake_types(code, precision, scale, expected):
    from services.decimal_observe import cursor_declared_carriers

    desc = (("C", code, None, None, precision, scale, True),)
    assert cursor_declared_carriers(["C"], desc, dialect="snowflake") == {"C": expected}


def test_snowflake_query_labels_to_varchar_as_text(monkeypatch):
    """The Snowflake playground path labels from the cursor, through the same owner."""
    import src.routers.query_router as qr

    class _Cur:
        description = [("S", 2, None, None, None, None, True), ("N", 0, None, None, 38, 0, True)]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return [("2024-01-01", 7)]

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            pass

    import connectors.snowflake_conn as sc

    monkeypatch.setattr(sc, "get_connection", lambda **_k: _Conn())
    connector = SimpleNamespace(type="snowflake", host="acct", username="u", password="p", database="D",
                                schema="PUBLIC", warehouse="W", connection_string="", auth_role="", role="",
                                private_key="", auth_mode="")
    _rows, _cols, schema, _ = qr._run_snowflake_query(
        connector, qr.QueryExecuteRequest(connector_id="x", query="SELECT to_varchar(d) AS s, datediff(day, a, b) AS n FROM t", limit=10)
    )
    assert schema == {"S": "VARCHAR", "N": "DECIMAL(38,0)"}


def test_flatten_output_columns_are_query_defined():
    from services.copilot_sql_guard import assert_identifiers_allowed

    assert_identifiers_allowed(
        "SELECT f.value:s, f.key FROM orders, LATERAL FLATTEN(input => doc:arr) f",
        allowed={"orders", "doc"},
        dialect="snowflake",
    )
    with pytest.raises(ValueError, match="identifiers not in introspected"):
        assert_identifiers_allowed("SELECT secret_col FROM orders", allowed={"orders", "doc"}, dialect="snowflake")
