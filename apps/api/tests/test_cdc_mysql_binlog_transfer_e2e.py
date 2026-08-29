"""Live MySQL binlog → sqlite CDC transfer (snapshot + file:pos resume).

Proves ``run_cdc_database_transfer`` uses ``CDC(binlog)`` — not query
downgrade — then resumes from the persisted file:pos watermark and applies
insert / update / delete. Delivery remains at-least-once upsert.

Skips when ROW binlog + ``pymysqlreplication`` are not reachable.
"""

from __future__ import annotations

import json
import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_conn import get_connection  # noqa: E402
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

CFG = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}


def _connect():
    return get_connection(
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        connection_string="",
        ssl=False,
    )


def _mysql_binlog_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401
    except ImportError:
        return False
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        return False
    try:
        conn = _connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row) and str(row[1]).upper() == "ROW"
        finally:
            conn.close()
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _mysql_binlog_ready(),
    reason="MySQL with ROW binlog + pymysqlreplication not reachable on localhost:3306",
)


def _exec(sql: str) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _dest_rows(path: Path, table: str) -> list[tuple]:
    con = sqlite3.connect(str(path))
    try:
        cols = [r[1] for r in con.execute(f'PRAGMA table_info("{table}")')]
        lsn_col = "_df_lsn" if "_df_lsn" in cols else None
        if lsn_col:
            return list(
                con.execute(
                    f'SELECT id, amount, "{lsn_col}" FROM "{table}" ORDER BY id'
                )
            )
        return list(con.execute(f'SELECT id, amount FROM "{table}" ORDER BY id'))
    finally:
        con.close()


def test_mysql_binlog_transfer_snapshot_resume_delete(tmp_path: Path) -> None:
    src_table = "cdc_xfer_" + uuid.uuid4().hex[:8]
    dest_path = tmp_path / "mysql_binlog_dest.db"
    job_id = "mysql-binlog-e2e-" + uuid.uuid4().hex[:8]
    _exec(f"DROP TABLE IF EXISTS {src_table}")
    _exec(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, amount DECIMAL(10,2))")
    _exec(f"INSERT INTO {src_table} (id, amount) VALUES (1, 10.00), (2, 20.00)")

    src = EndpointConfig(kind="database", format="mysql", table=src_table, **CFG)
    dst = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(dest_path),
        table=src_table,
    )
    mappings = [
        {"source": "id", "target": "id", "source_type": "INT", "target_type": "INTEGER"},
        {
            "source": "amount",
            "target": "amount",
            "source_type": "DECIMAL",
            "target_type": "NUMERIC",
        },
    ]
    schema = {"id": "INTEGER", "amount": "NUMERIC(10,2)"}
    stream = [
        {
            "name": src_table,
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
        assert any("CDC(binlog)" in line for line in ddl1), ddl1
        assert not any("downgraded" in line.lower() for line in ddl1), ddl1
        assert rows1 == 2, f"expected 2 snapshot rows, got {rows1} summary={summary1}"
        dest1 = _dest_rows(dest_path, src_table)
        assert [int(r[0]) for r in dest1] == [1, 2], dest1
        assert all(r[2] for r in dest1 if len(r) > 2), f"missing _df_lsn: {dest1}"

        _exec(f"INSERT INTO {src_table} (id, amount) VALUES (3, 30.00)")
        _exec(f"UPDATE {src_table} SET amount = 99.00 WHERE id = 1")
        _exec(f"DELETE FROM {src_table} WHERE id = 2")

        rows2, ddl2, summary2, _ = run_cdc_database_transfer(
            src,
            dst,
            mappings,
            schema,
            sync_mode="cdc",
            stream_contracts=stream,
            job_id=job_id,
        )
        assert any("CDC(binlog)" in line for line in ddl2), ddl2
        dest2 = _dest_rows(dest_path, src_table)
        ids = [int(r[0]) for r in dest2]
        amounts = {int(r[0]): float(r[1]) for r in dest2}
        assert 2 not in ids, f"delete of id=2 not applied: {dest2}"
        assert 1 in ids and 3 in ids, dest2
        assert amounts[1] == 99.0, dest2
        assert amounts[3] == 30.0, dest2
        assert all(r[2] for r in dest2 if len(r) > 2), f"missing _df_lsn: {dest2}"
        cdc2 = summary2.get("cdc") or {}
        artifact = {
            "test": "test_mysql_binlog_transfer_snapshot_resume_delete",
            "delivery": "at-least-once upsert",
            "capture": "CDC(binlog)",
            "snapshot_rows": rows1,
            "resume_rows": rows2,
            "dest_ids": ids,
            "dest_amounts": amounts,
            "watermark": cdc2.get("watermark") or summary2.get("watermark"),
            "note": "Not leftover MERGE. Not dest-owned exactly-once.",
        }
        out_dir = Path(os.environ.get("DATAFLOW_PROOF_DIR", "/opt/cursor/artifacts/cdc-merge-live"))
        try:
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "mysql-binlog-transfer.json").write_text(
                json.dumps(artifact, indent=2, default=str),
                encoding="utf-8",
            )
        except OSError:
            pass
    finally:
        _exec(f"DROP TABLE IF EXISTS {src_table}")
