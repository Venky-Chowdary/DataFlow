"""SQL Server CREATE TABLE must not emit unsupported IF NOT EXISTS."""

from __future__ import annotations

import importlib.util
import socket

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mssql


def test_mssql_dialect_if_not_exists_is_unsupported_syntax() -> None:
    """Document why generic_sql must omit if_not_exists for mssql."""
    t = sa.Table("t", sa.MetaData(), sa.Column("id", sa.BigInteger))
    with_if = str(
        sa.schema.CreateTable(t, if_not_exists=True).compile(dialect=mssql.dialect())
    )
    plain = str(sa.schema.CreateTable(t).compile(dialect=mssql.dialect()))
    assert "IF NOT EXISTS" in with_if.upper()
    assert "IF NOT EXISTS" not in plain.upper()


@pytest.mark.skipif(importlib.util.find_spec("pymssql") is None, reason="pymssql not installed")
def test_verify_sqlserver_table_live_edge() -> None:
    """Live read-back against Azure SQL Edge / SQL Server when :1433 answers."""
    try:
        socket.create_connection(("127.0.0.1", 1433), timeout=1).close()
    except OSError:
        pytest.skip("SQL Server not listening on 1433")

    import pymssql

    from services.reconciliation import verify_sqlserver_table

    try:
        conn = pymssql.connect(
            server="127.0.0.1",
            port=1433,
            user="sa",
            password="DataFlow_CDC_2022!",
            database="dataflow",
            login_timeout=3,
            timeout=5,
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server auth failed: {exc}")

    cur = conn.cursor()
    try:
        cur.execute("IF OBJECT_ID(N'dbo.df_verify_probe', N'U') IS NOT NULL DROP TABLE dbo.df_verify_probe")
        cur.execute(
            "CREATE TABLE dbo.df_verify_probe (id BIGINT NULL, amount NUMERIC(18,2) NULL)"
        )
        cur.execute(
            "INSERT INTO dbo.df_verify_probe (id, amount) VALUES (1, 10.00), (2, 20.50)"
        )
    finally:
        cur.close()
        conn.close()

    count, checksum = verify_sqlserver_table(
        host="127.0.0.1",
        port=1433,
        database="dataflow",
        username="sa",
        password="DataFlow_CDC_2022!",
        connection_string="",
        schema="dbo",
        table_name="df_verify_probe",
        target_columns=["id", "amount"],
    )
    assert count == 2
    assert checksum
