"""Real SQL Server Change Tracking CDC integration (docker-compose ``sqlserver``).

Requires:
  - compose service healthy on localhost:1433 (or env override)
  - ``tests/sqlserver_ct_init.sql`` applied once
  - ``pymssql`` or ``pyodbc`` available via generic_sql
  - ``DATAFLOW_SQLSERVER_ENABLE=1`` for non-local hosts
"""

from __future__ import annotations

import os
from services.brand_env import getenv_brand
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sqlserver_change_stream import (  # noqa: E402
    SqlServerChangeTrackingCdc,
    decode_sqlserver_resume_token,
)


def _sqlserver_cfg() -> dict:
    return {
        "host": getenv_brand("SQLSERVER_HOST", "localhost"),
        "port": int(getenv_brand("SQLSERVER_PORT", "1433") or 1433),
        "database": getenv_brand("SQLSERVER_DATABASE", "dataflow"),
        "username": getenv_brand("SQLSERVER_USER", "sa"),
        "password": getenv_brand("SQLSERVER_PASSWORD", "DataFlow_CDC_2022!"),
        "connection_string": "",
        "ssl": False,
    }


CFG = _sqlserver_cfg()


def _sqlserver_ct_ready() -> bool:
    host, port = CFG["host"], int(CFG["port"])
    try:
        with socket.create_connection((host, port), timeout=2):
            pass
    except OSError:
        return False
    if getenv_brand("SQLSERVER_ENABLE", "").strip().lower() not in {
        "1",
        "true",
        "yes",
    } and host == "localhost":
        # Default-localhost integration still allowed without explicit enable.
        pass
    elif not getenv_brand("SQLSERVER_ENABLE", "").strip():
        return False
    try:
        cdc = SqlServerChangeTrackingCdc(
            CFG, table="cdc_orders", primary_key="id", schema="dbo"
        )
        return cdc.is_available()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _sqlserver_ct_ready(),
    reason="SQL Server with Change Tracking not reachable (set DATAFLOW_SQLSERVER_ENABLE=1 + host/user/pass)",
)


def test_sqlserver_ct_snapshot_and_poll_real():
    table = "cdc_orders_" + uuid.uuid4().hex[:8]
    cdc_setup = SqlServerChangeTrackingCdc(
        CFG, table="cdc_orders", primary_key="id", schema="dbo"
    )
    with cdc_setup._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}];
                CREATE TABLE dbo.[{table}] (
                    id INT NOT NULL PRIMARY KEY,
                    amount DECIMAL(12,2) NOT NULL
                );
                ALTER TABLE dbo.[{table}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
                INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10.00), (2, 20.00);
                """
            )
        conn.commit()

    cdc = SqlServerChangeTrackingCdc(
        CFG, table=table, primary_key="id", schema="dbo", batch_size=100
    )
    try:
        batches = list(cdc.snapshot())
        inserts = [r for b in batches for r in b.inserts]
        assert len(inserts) == 2

        with cdc._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE dbo.[{table}] SET amount = 99.00 WHERE id = 1")
                cur.execute(f"DELETE FROM dbo.[{table}] WHERE id = 2")
            conn.commit()

        changes = list(cdc.poll())
        assert any(str(r.get("id")) == "3" for b in changes for r in b.inserts)
        assert any(str(r.get("id")) == "1" for b in changes for r in b.updates)
        assert any("2" in b.deletes for b in changes)
    finally:
        with cdc._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}]")
            conn.commit()


def test_sqlserver_ct_resume_token_roundtrip():
    """A new SqlServerChangeTrackingCdc resumed from a snapshot token must stream."""
    table = "cdc_resume_" + uuid.uuid4().hex[:8]
    cdc_setup = SqlServerChangeTrackingCdc(
        CFG, table="cdc_orders", primary_key="id", schema="dbo"
    )
    with cdc_setup._conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}];
                CREATE TABLE dbo.[{table}] (
                    id INT NOT NULL PRIMARY KEY,
                    amount DECIMAL(12,2) NOT NULL
                );
                ALTER TABLE dbo.[{table}] ENABLE CHANGE_TRACKING WITH (TRACK_COLUMNS_UPDATED = ON);
                INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10.00);
                """
            )
        conn.commit()

    cdc = SqlServerChangeTrackingCdc(
        CFG, table=table, primary_key="id", schema="dbo", batch_size=100
    )
    try:
        batches = list(cdc.snapshot())
        token = batches[-1].resume_token
        assert decode_sqlserver_resume_token(token)["phase"] == "streaming"
        cdc.close()

        with cdc._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (2, 20.00)")
            conn.commit()

        cdc2 = SqlServerChangeTrackingCdc(
            CFG,
            table=table,
            primary_key="id",
            schema="dbo",
            batch_size=100,
            resume_token=token,
        )
        changes = list(cdc2.poll())
        assert any(str(r.get("id")) == "2" for b in changes for r in b.inserts)
    finally:
        cdc.close()
        cdc2 = locals().get("cdc2")
        if cdc2:
            cdc2.close()
        with cdc._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"IF OBJECT_ID('dbo.{table}') IS NOT NULL DROP TABLE dbo.[{table}]")
            conn.commit()


def test_sqlserver_ct_metadata_honesty():
    """At-least-once must be exposed in CDC metadata; exactly-once must not be claimed."""
    cdc = SqlServerChangeTrackingCdc(
        CFG, table="cdc_orders", primary_key="id", schema="dbo"
    )
    meta = cdc.cdc_metadata()
    assert meta.get("delivery") == "at-least-once"
    assert "exactly-once" not in str(meta).lower()
