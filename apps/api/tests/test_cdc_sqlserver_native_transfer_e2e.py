"""Live SQL Server native CDC → sqlite transfer when :1433 is up.

Skips cleanly when the port is closed or native CDC is not enabled.
Delivery remains at-least-once upsert. Not leftover MERGE.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import sys
import time
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sqlserver_cdc_native import SqlServerNativeCdc  # noqa: E402
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 1433,
    "database": "dataflow",
    "username": "sa",
    "password": "Datawrap_CDC_2022!",
    "connection_string": "",
    "ssl": False,
}


def _sqlserver_native_ready() -> bool:
    try:
        with socket.create_connection(("localhost", 1433), timeout=1):
            pass
    except OSError:
        return False
    try:
        cdc = SqlServerNativeCdc(
            CFG, table="cdc_native_orders", primary_key="id", schema="dbo"
        )
        return cdc.is_available()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _sqlserver_native_ready(),
    reason="SQL Server native CDC (cdc_native_orders) not reachable on localhost:1433",
)


def _enable_cdc_on_table(cur, table: str) -> None:
    cur.execute(
        f"""
        IF OBJECT_ID('dbo.[{table}]') IS NOT NULL DROP TABLE dbo.[{table}];
        CREATE TABLE dbo.[{table}] (
            id INT NOT NULL PRIMARY KEY,
            amount DECIMAL(12,2) NOT NULL
        );
        """
    )
    cur.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_cdc_enabled = 1
        )
            EXEC sys.sp_cdc_enable_db;
        """
    )
    cur.execute(
        f"""
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = N'{table}',
            @role_name = NULL,
            @supports_net_changes = 0;
        """
    )
    cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (1, 10.00), (2, 20.00)")


def _dest_rows(path: Path, table: str) -> list[tuple]:
    con = sqlite3.connect(str(path))
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        if "_df_lsn" in cols:
            return list(
                con.execute(f'SELECT id, amount, "_df_lsn" FROM "{table}" ORDER BY id')
            )
        return list(con.execute(f'SELECT id, amount FROM "{table}" ORDER BY id'))
    finally:
        con.close()


def test_sqlserver_native_transfer_snapshot_resume_delete(tmp_path: Path) -> None:
    table = "cdc_sx_" + uuid.uuid4().hex[:8]
    dest_path = tmp_path / "sqlserver_native_dest.db"
    job_id = "mssql-native-e2e-" + uuid.uuid4().hex[:8]
    holder = f"e2e-{table}"
    cfg = {**CFG, "lease_holder_id": holder, "job_id": holder}
    bootstrap = SqlServerNativeCdc(
        cfg, table="cdc_native_orders", primary_key="id", schema="dbo"
    )
    with bootstrap._conn() as conn:
        with conn.cursor() as cur:
            _enable_cdc_on_table(cur, table)
        conn.commit()

    src = EndpointConfig(
        kind="database", format="sqlserver", table=table, schema="dbo", **CFG
    )
    dst = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table=table,
    )
    mappings = [
        {"source": "id", "target": "id"},
        {"source": "amount", "target": "amount"},
    ]
    schema = {"id": "INTEGER", "amount": "NUMERIC(12,2)"}
    stream = [
        {
            "name": table,
            "selected": True,
            "snapshot_mode": "initial",
            "primary_key": "id",
            "sync_mode": "cdc",
        }
    ]

    try:
        rows1, ddl1, summary1, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
            limit=2,
        )
        assert any("CDC(sqlserver_native)" in line for line in ddl1), ddl1
        assert rows1 == 2, (rows1, summary1)

        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO dbo.[{table}] (id, amount) VALUES (3, 30.00)")
                cur.execute(f"UPDATE dbo.[{table}] SET amount = 99.00 WHERE id = 1")
                cur.execute(f"DELETE FROM dbo.[{table}] WHERE id = 2")
            conn.commit()
        runner = SqlServerNativeCdc(
            cfg, table=table, primary_key="id", schema="dbo"
        )
        runner.force_cdc_scan()
        time.sleep(0.4)

        rows2, ddl2, summary2, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
        )
        assert any("CDC(sqlserver_native)" in line for line in ddl2), ddl2
        dest2 = _dest_rows(dest_path, table)
        ids = [int(r[0]) for r in dest2]
        amounts = {int(r[0]): float(r[1]) for r in dest2}
        assert 2 not in ids, dest2
        assert 1 in ids and 3 in ids, dest2
        assert amounts[1] == 99.0, dest2
        assert amounts[3] == 30.0, dest2
        artifact = {
            "test": "test_sqlserver_native_transfer_snapshot_resume_delete",
            "delivery": "at-least-once upsert",
            "capture": "CDC(sqlserver_native)",
            "snapshot_rows": rows1,
            "resume_rows": rows2,
            "dest_ids": ids,
            "watermark": (summary2.get("cdc") or {}).get("watermark"),
            "note": "Not leftover MERGE. Not dest-owned exactly-once.",
        }
        out_dir = Path(os.environ.get("DATAFLOW_PROOF_DIR", "/opt/cursor/artifacts/cdc-merge-live"))
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "sqlserver-native-transfer.json").write_text(
                json.dumps(artifact, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError:
            pass
    finally:
        with bootstrap._conn() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(
                        f"""
                        EXEC sys.sp_cdc_disable_table
                            @source_schema = N'dbo',
                            @source_name = N'{table}',
                            @capture_instance = N'all';
                        """
                    )
                except Exception:
                    pass
                cur.execute(
                    f"IF OBJECT_ID('dbo.[{table}]') IS NOT NULL DROP TABLE dbo.[{table}]"
                )
            conn.commit()
