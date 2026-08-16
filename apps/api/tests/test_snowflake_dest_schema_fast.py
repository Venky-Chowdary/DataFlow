"""Destination Snowflake schema probe must not scan SNOWFLAKE_SAMPLE_DATA catalogs.

Named hole: Transfer Studio sat on "Checking destination… Looking up
SNOWFLAKE_SAMPLE_DATA.datawrap_customer" while information_schema.columns and
table_constraints federated every TPCH schema after a warehouse resume.
Dest Map needs DESC TABLE + a statement timeout — not a row sample, not COUNT(*).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from connectors.snowflake_conn import snowflake_physical_column_rows
from services.schema_introspect import _introspect_snowflake


class _Missing(Exception):
    def __str__(self) -> str:
        return "002003 (02000): Object 'DATAWRAP_CUSTOMER' does not exist"


def test_physical_columns_use_desc_not_information_schema():
    cur = MagicMock()
    executed: list[str] = []

    def execute(sql, *args):
        executed.append(sql)
        return None

    cur.execute.side_effect = execute
    cur.fetchall.return_value = [
        ("C_CUSTKEY", "NUMBER(38,0)", "COLUMN", "N", None, "Y", "N"),
        ("C_NAME", "VARCHAR(25)", "COLUMN", "Y", None, "N", "N"),
    ]
    rows = snowflake_physical_column_rows(cur, "TPCH_SF1", "CUSTOMER")
    assert [r[0] for r in rows] == ["C_CUSTKEY", "C_NAME"]
    assert any("DESC TABLE" in s.upper() for s in executed)
    assert not any("INFORMATION_SCHEMA.COLUMNS" in s.upper() for s in executed)


def test_missing_table_desc_does_not_scan_information_schema():
    cur = MagicMock()
    executed: list[str] = []

    def execute(sql, *args):
        executed.append(sql)
        if "DESC TABLE" in sql.upper():
            raise _Missing()
        return None

    cur.execute.side_effect = execute
    rows = snowflake_physical_column_rows(cur, "PUBLIC", "DATAWRAP_CUSTOMER")
    assert rows == []
    assert any("DESC TABLE" in s.upper() for s in executed)
    assert not any("INFORMATION_SCHEMA" in s.upper() for s in executed)


def test_named_dest_introspect_skips_constraints_join_and_sets_timeout():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    executed: list[str] = []

    def execute(sql, *args):
        executed.append(sql)
        return None

    cur.execute.side_effect = execute
    cur.fetchall.return_value = [
        ("ID", "NUMBER(38,0)", "COLUMN", "N", None, "Y", "N"),
    ]

    with (
        patch("connectors.snowflake_conn.get_connection", return_value=conn),
        patch("connectors.snowflake_conn.normalize_account", return_value="acct"),
    ):
        out = _introspect_snowflake(
            host="acct",
            database="SNOWFLAKE_SAMPLE_DATA",
            username="u",
            password="p",
            schema="PUBLIC",
            warehouse="COMPUTE_WH",
            table="datawrap_customer",
            strict_namespace=True,
        )
    assert out["ok"] is True
    assert [c["name"] for c in out["columns"]] == ["ID"]
    joined = "\n".join(executed).upper()
    assert "STATEMENT_TIMEOUT_IN_SECONDS" in joined
    assert "DESC TABLE" in joined
    assert "TABLE_CONSTRAINTS" not in joined
    assert "KEY_COLUMN_USAGE" not in joined
    assert any("SNOWFLAKE_SAMPLE_DATA" in w for w in (out.get("warnings") or []))
