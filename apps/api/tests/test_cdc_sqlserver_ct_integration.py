"""Real SQL Server Change Tracking CDC integration (docker-compose ``sqlserver``).

Requires:
  - compose service healthy on localhost:1433
  - ``tests/sqlserver_ct_init.sql`` applied once
  - ``pymssql`` or ``pyodbc`` available via generic_sql
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 1433,
    "database": "dataflow",
    "username": "sa",
    "password": "DataFlow_CDC_2022!",
    "connection_string": "",
    "ssl": False,
}


def _sqlserver_ct_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 1433), timeout=1):
            pass
    except OSError:
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
    reason="SQL Server with Change Tracking not reachable on localhost:1433",
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
