"""M18 — views appear in the connector object listing, labelled as views.

PostgreSQL already did (tests/test_m18_pg_listing_views.py). MySQL, SQLite and
SQL Server listed base tables only, so a view could not be picked at all, and
the MCP ``list_connector_objects`` result dropped the per-object kind. Live
PostgreSQL, MySQL and SQL Server legs skip when the service is not up.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from src.transfer.endpoint_intelligence import introspect_endpoint
from src.transfer.models import EndpointConfig
from tests.helpers.live_env import mysql_creds, mysql_up, pg_creds, pg_up


def _kinds(info: dict, *names: str) -> dict[str, str]:
    return {o["name"]: o["type"] for o in info["objects"] if o["name"] in names}


def test_sqlite_listing_labels_views(tmp_path):
    db = tmp_path / "m18.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)")
        conn.execute("CREATE VIEW orders_v AS SELECT id FROM orders")
    info = introspect_endpoint(EndpointConfig(kind="database", format="sqlite", database=str(db)))
    assert _kinds(info, "orders", "orders_v") == {"orders": "table", "orders_v": "view"}


@pytest.mark.skipif(not mysql_up(), reason="MySQL not available")
def test_mysql_listing_labels_views():
    import pymysql

    creds = mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    table, view = f"m18_t_{suffix}", f"m18_v_{suffix}"
    conn = pymysql.connect(
        host=creds["host"], port=int(creds["port"]), user=creds["username"],
        password=creds["password"], database=creds["database"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE `{table}` (id INT PRIMARY KEY)")
            cur.execute(f"CREATE VIEW `{view}` AS SELECT id FROM `{table}`")
        info = introspect_endpoint(
            EndpointConfig(
                kind="database", format="mysql", host=str(creds["host"]),
                port=int(creds["port"]), database=str(creds["database"]),
                username=str(creds["username"]), password=str(creds["password"]),
            )
        )
        assert _kinds(info, table, view) == {table: "table", view: "view"}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP VIEW IF EXISTS `{view}`")
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        conn.close()


@pytest.mark.skipif(not pg_up(), reason="PostgreSQL not available")
def test_postgres_listing_labels_views():
    import psycopg2

    creds = pg_creds()
    suffix = uuid.uuid4().hex[:8]
    table, view = f"m18_t_{suffix}", f"m18_v_{suffix}"
    # A private schema: the shared test database accumulates thousands of
    # tables, and the listing is bounded (alphabetical first page, M7), so
    # objects in ``public`` can fall off the page.
    schema = f"m18_s_{suffix}"
    conn = psycopg2.connect(
        host=creds["host"], port=int(creds["port"]), dbname=creds["database"],
        user=creds["username"], password=creds["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(f'CREATE TABLE "{schema}"."{table}" (id INT PRIMARY KEY)')
            cur.execute(f'CREATE VIEW "{schema}"."{view}" AS SELECT id FROM "{schema}"."{table}"')
        info = introspect_endpoint(
            EndpointConfig(
                kind="database", format="postgresql", host=str(creds["host"]),
                port=int(creds["port"]), database=str(creds["database"]), schema=schema,
                username=str(creds["username"]), password=str(creds["password"]),
            )
        )
        assert _kinds(info, table, view) == {table: "table", view: "view"}
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        conn.close()


def _mssql_cfg() -> dict | None:
    host = os.environ.get("DATAFLOW_TEST_MSSQL_HOST")
    password = os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD")
    if not (host and password):
        return None
    return {
        "type": "sqlserver", "host": host,
        "port": int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT") or 1433),
        "database": os.environ.get("DATAFLOW_TEST_MSSQL_DB") or "dataflow",
        "username": os.environ.get("DATAFLOW_TEST_MSSQL_USER") or "sa",
        "password": password, "schema": "dbo", "ssl": False,
        "trust_server_certificate": True,
    }


def test_sqlserver_listing_labels_views():
    cfg = _mssql_cfg()
    if cfg is None:
        pytest.skip("SQL Server not configured")
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from services.schema_introspect import introspect_schema

    try:
        engine = _engine(cfg)
        with engine.connect() as probe:
            probe.execute(sa.text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001 - service not reachable
        pytest.skip(f"SQL Server not reachable: {exc}")
    suffix = uuid.uuid4().hex[:8]
    table, view = f"m18_t_{suffix}", f"m18_v_{suffix}"
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE TABLE dbo.{table} (id INT PRIMARY KEY)"))
        conn.execute(sa.text(f"CREATE VIEW dbo.{view} AS SELECT id FROM dbo.{table}"))
    try:
        info = introspect_schema(
            "sqlserver", host=cfg["host"], port=cfg["port"], database=cfg["database"],
            username=cfg["username"], password=cfg["password"], schema="dbo",
            ssl=False, table="", trust_server_certificate=True,
        )
        assert info["ok"], info
        assert {n: info["table_types"][n] for n in (table, view)} == {
            table: "table", view: "view",
        }
    finally:
        with engine.begin() as conn:
            conn.execute(sa.text(f"DROP VIEW IF EXISTS dbo.{view}"))
            conn.execute(sa.text(f"DROP TABLE IF EXISTS dbo.{table}"))


def test_mcp_list_connector_objects_exposes_object_kinds(monkeypatch):
    from src.ai.copilot import schema_tools

    monkeypatch.setattr(
        schema_tools, "_safe_connector", lambda *a, **k: ({"id": "c1", "name": "db", "type": "sqlite"}, None)
    )
    monkeypatch.setattr(
        schema_tools, "_endpoint_from_connector", lambda conn, **k: object()
    )
    import src.transfer.endpoint_intelligence as intel

    monkeypatch.setattr(
        intel,
        "introspect_endpoint",
        lambda endpoint: {
            "connected": True,
            "objects": [
                {"name": "orders", "type": "table"},
                {"name": "orders_v", "type": "view"},
            ],
        },
    )
    out = schema_tools.list_connector_objects(connector_name="db")
    assert out.success, out.error
    assert out.output["objects"] == ["orders", "orders_v"]
    assert out.output["object_kinds"] == {"orders": "table", "orders_v": "view"}
