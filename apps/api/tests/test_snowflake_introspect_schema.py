"""Snowflake introspect must survive missing schemas without traceback spam."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services.schema_introspect import _introspect_snowflake, _snowflake_resolve_schema


class _ProgError(Exception):
    """Stand-in for snowflake.connector.errors.ProgrammingError 002043."""


def test_resolve_schema_falls_back_when_requested_missing() -> None:
    cur = MagicMock()

    def execute(sql: str, *args):
        sql_u = sql.upper()
        if "USE SCHEMA" in sql_u and "MISSING" in sql_u:
            raise _ProgError("002043 (02000): Object does not exist")
        if "INFORMATION_SCHEMA.SCHEMATA" in sql_u:
            return None
        return None

    cur.execute.side_effect = execute

    resolved, available, warning = _snowflake_resolve_schema(cur, "MISSING")
    assert resolved == "PUBLIC"
    # USE SCHEMA PUBLIC succeeded — do not list every schema on the cold warehouse.
    assert available == []
    assert warning and "MISSING" in warning and "PUBLIC" in warning


def test_resolve_schema_uses_exact_match() -> None:
    cur = MagicMock()

    resolved, available, warning = _snowflake_resolve_schema(cur, "MART")
    assert resolved == "MART"
    assert warning is None
    assert available == []
    executed = " ".join(str(c.args[0]) for c in cur.execute.call_args_list)
    assert "USE SCHEMA" in executed.upper()
    assert "MART" in executed.upper()


def test_introspect_snowflake_returns_actionable_error_on_bad_database() -> None:
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    def execute(sql: str, *args):
        if "USE DATABASE" in sql.upper():
            raise _ProgError("002043 (02000): Object does not exist")

    cur.execute.side_effect = execute

    with (
        patch("connectors.snowflake_conn.get_connection", return_value=conn),
        patch("connectors.snowflake_conn.normalize_account", return_value="xy12345"),
    ):
        out = _introspect_snowflake(
            host="xy12345",
            database="NO_SUCH_DB",
            username="u",
            password="p",
            schema="PUBLIC",
            warehouse="WH",
        )
    assert out["ok"] is False
    assert "NO_SUCH_DB" in out["error"]
    assert out["tables"] == []


def test_introspect_snowflake_ok_with_schema_fallback() -> None:
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False

    calls: list[str] = []

    def execute(sql: str, *args):
        calls.append(sql)
        sql_u = sql.upper()
        if "USE SCHEMA" in sql_u and "BAD_SCHEMA" in sql_u:
            raise _ProgError("002043 (02000): Object does not exist")
        return None

    cur.execute.side_effect = execute

    def fetchall():
        last = calls[-1].upper() if calls else ""
        if "INFORMATION_SCHEMA.TABLES" in last and "TABLE_NAME" in last:
            return [("CUSTOMERS",), ("ORDERS",)]
        if "INFORMATION_SCHEMA.COLUMNS" in last:
            return [
                ("ID", "NUMBER", "NO", None, 38, 0, None),
                ("NAME", "TEXT", "YES", 50, None, None, None),
                ("BLOB", "BINARY", "YES", 16, None, None, None),
            ]
        return []

    cur.fetchall.side_effect = fetchall

    with (
        patch("connectors.snowflake_conn.get_connection", return_value=conn),
        patch("connectors.snowflake_conn.normalize_account", return_value="xy12345"),
    ):
        out = _introspect_snowflake(
            host="xy12345",
            database="DATAFLOW",
            username="u",
            password="p",
            schema="BAD_SCHEMA",
            warehouse="",
        )
    assert out["ok"] is True
    assert out["schema"] == "PUBLIC"
    assert "CUSTOMERS" in out["tables"]
    assert out.get("warnings")
    assert "BAD_SCHEMA" in out["warnings"][0]
    by_name = {c["name"]: c["inferred_type"] for c in out["columns"]}
    assert by_name["NAME"] == "VARCHAR(50)"
    assert by_name["BLOB"] == "BINARY(16)"
    assert by_name["ID"].startswith("DECIMAL") or by_name["ID"] == "DECIMAL(38,0)"
