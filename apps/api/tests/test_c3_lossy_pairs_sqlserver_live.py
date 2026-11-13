"""C3 live proof on SQL Server (CI ``cdc-sqlserver`` job: SQL Server, MySQL, PostgreSQL).

Real engine path, Validate on, from a live SQL Server source:

* ``tinyint`` holding 255 into an existing MySQL signed ``TINYINT`` is refused
  at Validate; create-new lands ``tinyint unsigned`` and keeps 255.
* ``datetime2(7)`` values that use the 7th digit into an existing PostgreSQL
  ``TIMESTAMP(6)`` are refused at Validate (PostgreSQL keeps microseconds).

Skips without a reachable SQL Server (``DATAFLOW_SQLSERVER_*`` /
``DATAFLOW_TEST_MSSQL_PASSWORD``, as the other SQL Server live modules).
"""

from __future__ import annotations

import ipaddress
import os
import socket

import pytest

from services.brand_env import getenv_brand
from tests.c3_live_helpers import (
    mysql_endpoint,
    mysql_sql,
    pg_endpoint,
    pg_sql,
    refused_at_validate,
    run_transfer,
    sqlalchemy_sql,
    token,
)
from tests.test_commit_safe_review_round2 import isolated  # noqa: F401 — fixture


def _loopback(host: str) -> bool:
    name = str(host or "").strip().strip("[]").lower()
    if name in {"localhost", "localhost."}:
        return True
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def _cfg() -> dict:
    host = getenv_brand("SQLSERVER_HOST", "localhost")
    cfg = {
        "type": "sqlserver",
        "host": host,
        "port": int(getenv_brand("SQLSERVER_PORT", "1433") or 1433),
        "database": getenv_brand("SQLSERVER_DATABASE", "dataflow"),
        "username": getenv_brand("SQLSERVER_USER", "sa"),
        "password": getenv_brand(
            "SQLSERVER_PASSWORD", os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD", "")
        ),
    }
    if _loopback(host):
        # The lab container's certificate is self-signed (loopback only).
        cfg["trust_server_certificate"] = True
    return cfg


def _ready() -> bool:
    cfg = _cfg()
    if not cfg["password"]:
        return False
    try:
        with socket.create_connection((cfg["host"], cfg["port"]), timeout=2):
            pass
        return sqlalchemy_sql(cfg, "SELECT 1", fetch=True) == [(1,)]
    except Exception:  # noqa: BLE001 — not reachable: the module skips
        return False


pytestmark = pytest.mark.skipif(
    not _ready(), reason="SQL Server not reachable (set DATAFLOW_SQLSERVER_* / DATAFLOW_TEST_MSSQL_PASSWORD)"
)


def _endpoint(table: str):
    from src.transfer.models import EndpointConfig

    cfg = _cfg()
    extra = {"trust_server_certificate": True} if cfg.get("trust_server_certificate") else {}
    return EndpointConfig(
        kind="database", format="sqlserver", host=cfg["host"], port=cfg["port"],
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema="dbo", table=table, extra=extra,
    )


@pytest.fixture()
def mssql_table():
    created: list[str] = []

    def make(ddl_cols: str, rows: str) -> str:
        table = f"c3ms_{token()}"
        sqlalchemy_sql(_cfg(), f"CREATE TABLE dbo.{table} ({ddl_cols})")
        sqlalchemy_sql(_cfg(), f"INSERT INTO dbo.{table} VALUES {rows}")
        created.append(table)
        return table

    try:
        yield make
    finally:
        for table in created:
            sqlalchemy_sql(_cfg(), f"IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.{table}")
            mysql_sql(f"DROP TABLE IF EXISTS {table}")
            pg_sql(f'DROP TABLE IF EXISTS public."{table}"')


def test_live_sqlserver_tinyint_into_signed_mysql_tinyint_is_refused(isolated, mssql_table):  # noqa: F811
    table = mssql_table("id INT PRIMARY KEY, small TINYINT", "(1, 255), (2, 7)")
    mysql_sql(f"CREATE TABLE {table} (id INT PRIMARY KEY, small TINYINT)")
    result = run_transfer(isolated, _endpoint(table), mysql_endpoint(table))
    assert refused_at_validate(result, "small"), result.error
    assert mysql_sql(f"SELECT count(*) FROM {table}", fetch=True)[0][0] == 0


def test_live_sqlserver_tinyint_create_new_keeps_255(isolated, mssql_table):  # noqa: F811
    table = mssql_table("id INT PRIMARY KEY, small TINYINT", "(1, 255), (2, 7)")
    result = run_transfer(isolated, _endpoint(table), mysql_endpoint(table))
    assert result.success, result.error
    column = mysql_sql(
        "SELECT column_type FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = 'small'",
        (table,), fetch=True,
    )
    assert "unsigned" in str(column).lower(), column
    assert mysql_sql(f"SELECT id, small FROM {table} ORDER BY id", fetch=True) == ((1, 255), (2, 7))


def test_live_sqlserver_datetime2_seventh_digit_into_pg_micros_is_refused(isolated, mssql_table):  # noqa: F811
    table = mssql_table(
        "id INT PRIMARY KEY, at DATETIME2(7)",
        "(1, '2024-01-01 10:00:00.1234567'), (2, '2024-01-01 10:00:01.0000001')",
    )
    pg_sql(f'CREATE TABLE public."{table}" (id INT PRIMARY KEY, at TIMESTAMP(6))')
    result = run_transfer(isolated, _endpoint(table), pg_endpoint(table))
    assert refused_at_validate(result, "at"), result.error
    assert pg_sql(f'SELECT count(*) FROM public."{table}"', fetch=True)[0][0] == 0
