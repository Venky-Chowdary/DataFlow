"""Cross-engine: wrong schema/DB must still load columns — never empty→create-new.

Covers PostgreSQL, MySQL, Snowflake, SQL Server, Oracle, BigQuery recovery paths.
Shared failure modes that made Map invent identity CREATE when the destination
object already existed (enterprise proof bar — not MySQL→Postgres only).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from services.mapping_pipeline import run_mapping_pipeline
from services.schema_introspect import (
    _introspect_bigquery,
    _introspect_mysql,
    _introspect_oracle,
    _introspect_postgresql,
    _introspect_snowflake,
    _introspect_sqlserver,
)


def test_pipeline_existing_empty_targets_never_create_new_across_dest_families():
    for dest in ("postgresql", "mysql", "snowflake", "sqlserver", "oracle", "bigquery"):
        result = run_mapping_pipeline(
            ["id", "title"],
            [],
            destination_db_type=dest,
            destination_table_exists=True,
            use_llm=False,
        )
        mappings = result["mappings"]
        assert mappings, dest
        assert all(m.get("create_new") is False for m in mappings), dest
        assert all(m.get("assignment_strategy") == "pending_dest_schema" for m in mappings), dest
        assert all("New destination table" not in (m.get("reasoning") or "") for m in mappings), dest


def test_pg_cross_schema_recovery():
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],
        [],
        [("public", "jobs")],
        [("id", "text", "YES"), ("title", "text", "YES")],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.postgresql_conn.get_connection", return_value=conn), patch(
        "services.schema_introspect._refine_columns_by_samples",
        side_effect=lambda _c, cols, *_a, **_k: cols,
    ):
        result = _introspect_postgresql(
            host="h", port=5432, database="db", username="u", password="p",
            schema="wrong", connection_string="", ssl=True, table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "public"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_mysql_cross_database_recovery():
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],
        [],
        [],
        [("app_prod", "jobs")],
        [("id", "varchar(36)", "YES"), ("title", "text", "YES")],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.mysql_conn.get_connection", return_value=conn), patch(
        "services.schema_introspect._refine_columns_by_samples",
        side_effect=lambda _c, cols, *_a, **_k: cols,
    ):
        result = _introspect_mysql(
            host="h", port=3306, database="wrong_db", username="u", password="p",
            schema="", connection_string="", ssl=True, table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "app_prod"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_snowflake_cross_schema_recovery():
    cur = MagicMock()
    cur.fetchall.side_effect = [
        [],
        [],
        [("PUBLIC", "JOBS")],
        [("ID", "NUMBER", "YES"), ("TITLE", "TEXT", "YES")],
    ]
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    with patch("connectors.snowflake_conn.get_connection", return_value=conn), patch(
        "connectors.snowflake_conn.normalize_account", return_value="acct",
    ), patch(
        "services.schema_introspect._snowflake_resolve_schema",
        return_value=("WRONG", [], None),
    ), patch(
        "connectors.snowflake_conn.resolve_or_fold_snowflake_table",
        side_effect=lambda _c, _s, t: t,
    ):
        result = _introspect_snowflake(
            host="acct", database="DB", username="u", password="p",
            schema="WRONG", warehouse="WH", connection_string="", table="JOBS",
        )
    assert result["ok"] is True
    assert str(result["schema"]).upper() == "PUBLIC"
    assert [c["name"] for c in result["columns"]] == ["ID", "TITLE"]


def test_sqlserver_cross_schema_recovery():
    pytest.importorskip("sqlalchemy")

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql_u = str(stmt).upper()
        if "INFORMATION_SCHEMA.TABLES" in sql_u and "LOWER(TABLE_NAME)" in sql_u:
            return FakeResult([("dbo", "jobs")])
        if "INFORMATION_SCHEMA.TABLES" in sql_u:
            return FakeResult([])
        if "INFORMATION_SCHEMA.COLUMNS" in sql_u:
            if params and params.get("schema") == "dbo":
                return FakeResult([
                    ("id", "varchar", None, None, "YES"),
                    ("title", "nvarchar", None, None, "YES"),
                ])
            return FakeResult([])
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine):
        result = _introspect_sqlserver(
            host="h", port=1433, database="db", username="u", password="p",
            schema="wrong", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "dbo"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]


def test_oracle_cross_owner_recovery():
    pytest.importorskip("sqlalchemy")

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    def execute(stmt, params=None):
        sql = str(stmt).upper().replace("\n", " ")
        if "FROM ALL_TABLES" in sql and "UPPER(TABLE_NAME)" in sql:
            return FakeResult([("APP", "JOBS")])
        if "FROM ALL_TABLES" in sql:
            return FakeResult([])
        if "FROM ALL_TAB_COLUMNS" in sql:
            if params and params.get("owner") == "APP":
                return FakeResult([
                    ("ID", "VARCHAR2", None, None, "Y"),
                    ("TITLE", "VARCHAR2", None, None, "Y"),
                ])
            return FakeResult([])
        return FakeResult([])

    conn = MagicMock()
    conn.execute.side_effect = execute
    conn_cm = MagicMock()
    conn_cm.__enter__.return_value = conn
    conn_cm.__exit__.return_value = False
    engine = MagicMock()
    engine.connect.return_value = conn_cm

    with patch("connectors.generic_sql._engine", return_value=engine):
        result = _introspect_oracle(
            host="h", port=1521, database="ORCL", username="wrong_user", password="p",
            schema="WRONG", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "APP"
    assert [c["name"] for c in result["columns"]] == ["ID", "TITLE"]


def test_bigquery_cross_dataset_recovery():
    field = SimpleNamespace(name="id", field_type="STRING", mode="NULLABLE")
    field2 = SimpleNamespace(name="title", field_type="STRING", mode="NULLABLE")
    good_table = SimpleNamespace(schema=[field, field2])

    client = MagicMock()
    client.list_tables.return_value = []

    def get_table(ref: str):
        if ref.endswith(".analytics.jobs"):
            return good_table
        raise Exception("not found")

    client.get_table.side_effect = get_table
    client.list_datasets.return_value = [SimpleNamespace(dataset_id="analytics")]

    with patch("connectors.bigquery_conn.get_client", return_value=client):
        result = _introspect_bigquery(
            database="myproj", schema="wrong_ds", connection_string="", table="jobs",
        )
    assert result["ok"] is True
    assert result["schema"] == "analytics"
    assert [c["name"] for c in result["columns"]] == ["id", "title"]
