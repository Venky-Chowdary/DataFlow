"""Live MySQL binlog → PostgreSQL dest-owned DELETE + idempotent replay.

CDC apply still uses dest-engine ``DELETE WHERE pk IN (...)`` (PostgreSQL),
not leftover MERGE and not dest-owned exactly-once. Replay of the same
delete must leave dest ``{1:99, 3:30}``. Stale LSN must not wipe a newer row.

Skips when ROW binlog or Postgres auth is not reachable.
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

from connectors.mysql_conn import get_connection  # noqa: E402
from connectors.table_manager import delete_by_primary_keys  # noqa: E402
from src.transfer.cdc_transfer import run_cdc_database_transfer  # noqa: E402
from src.transfer.models import EndpointConfig  # noqa: E402

MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}

PG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
    "schema": "public",
}


def _mysql_connect():
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
        conn = _mysql_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                return bool(row) and str(row[1]).upper() == "ROW"
        finally:
            conn.close()
    except Exception:
        return False


def _pg_ready() -> bool:
    try:
        import psycopg2

        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=3,
        )
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not (_mysql_binlog_ready() and _pg_ready()),
    reason="MySQL ROW binlog or PostgreSQL dataflow/dataflow not reachable",
)


def _mysql_exec(sql: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _pg_rows(table: str) -> list[tuple]:
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT id, amount, {lsn} FROM {tbl} ORDER BY id").format(
                    lsn=sql.Identifier("_df_lsn"),
                    tbl=sql.Identifier("public", table),
                )
            )
            return list(cur.fetchall())
    finally:
        conn.close()


def _pg_drop(table: str) -> None:
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier("public", table)))
        conn.commit()
    finally:
        conn.close()


def test_mysql_binlog_postgres_dest_owned_delete_replay() -> None:
    src_table = "cdc_pg_" + uuid.uuid4().hex[:8]
    dest_table = src_table
    job_id = "mysql-pg-dest-owned-" + uuid.uuid4().hex[:8]
    _mysql_exec(f"DROP TABLE IF EXISTS {src_table}")
    _pg_drop(dest_table)
    _mysql_exec(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, amount DECIMAL(10,2))")
    _mysql_exec(f"INSERT INTO {src_table} (id, amount) VALUES (1, 10.00), (2, 20.00)")

    src = EndpointConfig(kind="database", format="mysql", table=src_table, **MYSQL)
    dst = EndpointConfig(kind="database", format="postgresql", table=dest_table, **PG)
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
        assert rows1 == 2, f"snapshot rows={rows1} summary={summary1}"
        dest1 = _pg_rows(dest_table)
        assert [int(r[0]) for r in dest1] == [1, 2], dest1

        _mysql_exec(f"INSERT INTO {src_table} (id, amount) VALUES (3, 30.00)")
        _mysql_exec(f"UPDATE {src_table} SET amount = 99.00 WHERE id = 1")
        _mysql_exec(f"DELETE FROM {src_table} WHERE id = 2")

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
        dest2 = _pg_rows(dest_table)
        ids = [int(r[0]) for r in dest2]
        amounts = {int(r[0]): float(r[1]) for r in dest2}
        assert 2 not in ids, f"dest-owned delete of id=2 missing: {dest2}"
        assert amounts == {1: 99.0, 3: 30.0}, dest2

        dest_cfg = {**PG, "type": "postgresql"}
        replay = delete_by_primary_keys(
            db_type="postgresql",
            cfg=dest_cfg,
            table_name=dest_table,
            primary_key_column="id",
            keys=["2"],
            schema="public",
        )
        assert replay == 0, f"idempotent dest DELETE must be 0, got {replay}"
        dest3 = _pg_rows(dest_table)
        assert {int(r[0]): float(r[1]) for r in dest3} == {1: 99.0, 3: 30.0}, dest3

        newer_lsn = str(dest3[0][2] or "")
        stale = delete_by_primary_keys(
            db_type="postgresql",
            cfg=dest_cfg,
            table_name=dest_table,
            primary_key_column="id",
            keys=["1"],
            schema="public",
            incoming_lsn="0/1",
            lsn_column="_df_lsn",
        )
        assert stale == 0, f"stale LSN must not wipe dest id=1 (lsn={newer_lsn})"
        dest4 = _pg_rows(dest_table)
        assert {int(r[0]): float(r[1]) for r in dest4} == {1: 99.0, 3: 30.0}, dest4

        artifact = {
            "test": "test_mysql_binlog_postgres_dest_owned_delete_replay",
            "delivery": "at-least-once upsert",
            "capture": "CDC(binlog)",
            "dest": "postgresql dest-owned DELETE",
            "snapshot_rows": rows1,
            "resume_rows": rows2,
            "dest_amounts": {1: 99.0, 3: 30.0},
            "id_2_gone": True,
            "replay_delete_id_2": 0,
            "stale_lsn_delete_id_1": 0,
            "watermark": (summary2.get("cdc") or {}).get("watermark")
            or summary2.get("watermark"),
            "note": "Dest-engine PostgreSQL DELETE. Not leftover MERGE. Not dest-owned exactly-once.",
        }
        out = Path("/opt/cursor/artifacts/cdc-dest-owned-merge/mysql-pg-dest-owned-delete.json")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(__import__("json").dumps(artifact, indent=2) + "\n", encoding="utf-8")
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS {src_table}")
        _pg_drop(dest_table)
