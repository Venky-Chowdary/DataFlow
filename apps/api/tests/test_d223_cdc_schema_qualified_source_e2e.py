"""QA D223: SQL Server CDC on a schema-qualified source name was blocked.

QA (production 087ac4bf): ``source_table="dbo.qah_cdc0010"`` with
``sync_mode="cdc"`` was checked as ``dbo.dbo.qah_cdc0010`` and blocked as
"change data capture is not enabled", although native CDC was on. The plain
``qah_cdc0010`` worked. The connector's schema was prepended to a name that
already carried it.

Route: MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job, on a real
SQL Server 2022 with the Agent running and native CDC enabled, into a real
PostgreSQL. Both spellings must stage, run and land the same rows.
"""

from __future__ import annotations

import os
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)


def _ms():
    import pyodbc

    h.mssql_conn().close()
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=127.0.0.1,1433;DATABASE=dataflow;"
        f"UID={os.environ.get('DATAFLOW_TEST_MSSQL_USER', 'sa')};"
        f"PWD={os.environ['DATAFLOW_TEST_MSSQL_PASSWORD']};TrustServerCertificate=yes",
        timeout=10, autocommit=True,
    )


def _agent_running() -> bool:
    conn = _ms()
    try:
        rows = conn.execute(
            "SELECT status_desc FROM sys.dm_server_services "
            "WHERE servicename LIKE 'SQL Server Agent%'"
        ).fetchall()
    finally:
        conn.close()
    return bool(rows) and str(rows[0][0]).lower() == "running"


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    monkeypatch.setenv("DATAFLOW_TRAINING", "off")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


_ROWS = [(1, "alpha"), (2, None), (3, ""), (4, "  "), (5, "naïve 中文 😀")]


def _create_cdc_source(table: str) -> None:
    conn = _ms()
    try:
        conn.execute(
            "IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = DB_NAME() "
            "AND is_cdc_enabled = 1) EXEC sys.sp_cdc_enable_db"
        )
        conn.execute(f"CREATE TABLE dbo.[{table}] (id INT NOT NULL PRIMARY KEY, note NVARCHAR(40) NULL)")
        conn.cursor().executemany(f"INSERT INTO dbo.[{table}] (id, note) VALUES (?, ?)", _ROWS)
        conn.execute(
            "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', "
            f"@source_name = N'{table}', @role_name = NULL, @supports_net_changes = 0"
        )
    finally:
        conn.close()


def _drop_cdc_source(table: str) -> None:
    conn = _ms()
    try:
        try:
            conn.execute(
                "EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', "
                f"@source_name = N'{table}', @capture_instance = N'all'"
            )
        except Exception:  # noqa: BLE001, S110 - may not be CDC-enabled
            pass
        conn.execute(f"DROP TABLE IF EXISTS dbo.[{table}]")
    finally:
        conn.close()


def _connectors(token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"d223-s-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("sqlserver"), "schema": "dbo"})
    dst = create_connector({"name": f"d223-d-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
    return src, dst


@pytest.mark.parametrize("qualified", [True, False], ids=["dbo.table", "table"])
def test_sqlserver_cdc_accepts_a_schema_qualified_source_name(client, qualified):
    if not _agent_running():
        pytest.skip("SQL Server Agent is not running (native CDC capture job)")
    token = uuid.uuid4().hex[:8]
    table, dst_t = f"qah_cdc_{token}", f"d223_d_{token}"
    _create_cdc_source(table)
    try:
        src, dst = _connectors(token)
        args = {
            "source_connector_id": src.id,
            "source_table": f"dbo.{table}" if qualified else table,
            "dest_connector_id": dst.id, "dest_table": dst_t,
            "sync_mode": "cdc", "primary_key": "id",
        }
        ok, staged = h.mcp_call(client, "start_transfer", args)
        assert ok, {"error": staged}
        text = str(staged)
        assert "dbo.dbo." not in text, text[:2000]
        assert not h.blockers_of(staged), h.blockers_of(staged)
        job = h.confirm(client, staged["ack_id"], reason="d223")
        assert "dbo.dbo." not in str(job), str(job)[:2000]
        # The run read SQL Server's native capture, not a primary-key poll.
        assert "sqlserver_native" in str(job), str(job)[:2000]
        got = sorted(h.query("postgresql", f'SELECT id, note FROM "{dst_t}"'))
        assert got == sorted(_ROWS), got
    finally:
        _drop_cdc_source(table)
        h.drop_table("postgresql", dst_t)


@pytest.mark.parametrize(
    ("dialect", "schema", "table", "want"),
    [
        ("sqlserver", "dbo", "dbo.orders", ("dbo", "orders")),
        ("sqlserver", "dbo", "sales.orders", ("sales", "orders")),
        ("sqlserver", "dbo", "orders", ("dbo", "orders")),
        ("sqlserver", "dbo", "[dbo].[orders]", ("dbo", "orders")),
        ("sqlserver", "", "dbo.orders", ("dbo", "orders")),
        ("postgresql", "public", "public.orders", ("public", "orders")),
        ("postgresql", "public", '"public"."orders"', ("public", "orders")),
        ("oracle", "APP", "APP.ORDERS", ("APP", "ORDERS")),
        ("oracle", "APP", "ORDERS", ("APP", "ORDERS")),
        ("mysql", "shop", "shop.orders", ("shop", "orders")),
        ("mysql", "shop", "`shop`.`orders`", ("shop", "orders")),
    ],
)
def test_cdc_reader_names_split_a_qualified_table_once(dialect, schema, table, want):
    """Every CDC reader resolves ``(schema, table)`` through the same split."""
    from services.cdc_table_name import split_cdc_table

    assert split_cdc_table(table, schema, dialect=dialect) == want
