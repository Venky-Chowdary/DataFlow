"""verify_pg_route_key_uniqueness must quote route key columns as identifiers.

A mapped target column name containing a double quote used to close the
identifier and smuggle a stacked statement into the Gate-8 probe.
"""

from __future__ import annotations

import pytest

import connectors.postgresql_conn as pg_conn
from services.reconciliation import verify_pg_route_key_uniqueness


class _Cursor:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self._sink.append(sql)

    def fetchall(self) -> list[tuple]:
        return []


class _Conn:
    def __init__(self, sink: list[str]) -> None:
        self._sink = sink

    def cursor(self) -> _Cursor:
        return _Cursor(self._sink)

    def close(self) -> None:
        return None


@pytest.fixture
def executed(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    sink: list[str] = []
    monkeypatch.setattr(pg_conn, "get_connection", lambda **_: _Conn(sink))
    return sink


def _run(route_tables: list[tuple[str, list[str]]]) -> list[tuple] | None:
    return verify_pg_route_key_uniqueness(
        host="h",
        port=5432,
        database="d",
        username="u",
        password="p",
        schema="public",
        connection_string="",
        ssl=False,
        route_tables=route_tables,
    )


def test_embedded_quote_in_key_column_stays_inside_identifier(executed: list[str]) -> None:
    hostile = 'x"); DELETE FROM audit_trail;--'
    assert _run([("orders_a", [hostile]), ("orders_b", [hostile])]) == []
    (sql,) = executed
    assert '"x""); DELETE FROM audit_trail;--"' in sql
    assert '"x"); DELETE' not in sql
    assert sql.count("SELECT ") == 3


def test_plain_composite_key_columns_quoted(executed: list[str]) -> None:
    assert _run([("r1", ["tenant_id", "id"]), ("r2", ["tenant_id", "id"])]) == []
    (sql,) = executed
    assert 'SELECT "tenant_id", "id" FROM "public"."r1"' in sql
    assert 'SELECT "tenant_id", "id" FROM "public"."r2"' in sql
