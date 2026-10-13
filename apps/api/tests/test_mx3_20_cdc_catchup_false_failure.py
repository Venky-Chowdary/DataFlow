"""MX3-20: a MySQL→PG CDC catch-up that applied every change ended ``failed``.

Two faults, both on the live route:

1. The binlog reader advanced its consumed position on XID commits only. A
   DDL statement is logged as an implicit-commit QueryEvent with no XID, so a
   trailing DDL on any other table (the engine's own
   ``CREATE TABLE IF NOT EXISTS dataflow_signal``) left the reader "behind"
   the poll-start head forever and the drain raised ``CdcStreamBehind``.
2. The plan stamps a ``datetime`` carrier transform on a DATETIME column.
   The CDC value proof treated any non-identity transform as a value change
   and declined the scan, so reconciliation fell back to the COUNT diagnostic,
   which by design never passes.

Live tests need MySQL (ROW binlog) on 127.0.0.1:3307 and PostgreSQL on
localhost:5432 with ``dataflow/dataflow``; they skip otherwise.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime

import pytest

MYSQL = {
    "type": "mysql",
    "host": "127.0.0.1",
    "port": 3307,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}
PG = {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "schema": "public",
}


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


def _mysql():
    import pymysql

    return pymysql.connect(
        host="127.0.0.1", port=3307, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True, connect_timeout=3,
    )


def _pg():
    import psycopg2

    conn = psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow",
        password="dataflow", connect_timeout=3,
    )
    conn.autocommit = True
    return conn


def _mysql_binlog_ready() -> bool:
    if not _port_open("127.0.0.1", 3307):
        return False
    try:
        import pymysqlreplication  # noqa: F401

        conn = _mysql()
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
    if not _port_open("localhost", 5432):
        return False
    try:
        _pg().close()
        return True
    except Exception:
        return False


needs_mysql = pytest.mark.skipif(
    not _mysql_binlog_ready(), reason="MySQL ROW binlog not reachable on 127.0.0.1:3307"
)
needs_pg = pytest.mark.skipif(
    not _pg_ready(), reason="PostgreSQL dataflow/dataflow not reachable on localhost:5432"
)


@needs_mysql
def test_trailing_ddl_on_another_table_does_not_leave_the_reader_behind() -> None:
    from connectors.mysql_change_stream import MySqlChangeStreamCdc

    table = "mx320_src_" + uuid.uuid4().hex[:8]
    other = "mx320_other_" + uuid.uuid4().hex[:8]
    conn = _mysql()
    cur = conn.cursor()
    cur.execute(f"CREATE TABLE `{table}` (id INT PRIMARY KEY, qty INT)")
    cur.execute(f"INSERT INTO `{table}` VALUES (1, 1)")
    holder = f"mx320-{table}"
    cfg = {**MYSQL, "lease_holder_id": holder, "job_id": holder}
    cdc = MySqlChangeStreamCdc(cfg, table=table, primary_key="id", max_wait_seconds=8.0)
    resumed = None
    try:
        resume = list(cdc.snapshot())[-1].resume_token
        cdc.close()
        cur.execute(f"INSERT INTO `{table}` VALUES (2, 2)")
        # Last event before the head: a DDL on a table this job does not read.
        cur.execute(f"CREATE TABLE `{other}` (id INT PRIMARY KEY)")
        resumed = MySqlChangeStreamCdc(
            cfg, table=table, primary_key="id", resume_token=resume, max_wait_seconds=8.0
        )
        changes = list(resumed.poll())
        inserts = [r for b in changes for r in b.inserts]
        assert any(str(r.get("id")) == "2" for r in inserts), inserts
        assert resumed.capture_has_pending() is False
    finally:
        for reader in (cdc, resumed):
            if reader is not None:
                reader.close()
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(f"DROP TABLE IF EXISTS `{other}`")
        conn.close()


@needs_mysql
@needs_pg
def test_datetime_carrier_transform_is_value_proven_mysql_to_pg() -> None:
    from services.cdc_value_digest import prove_cdc_values

    table = "mx320_vals_" + uuid.uuid4().hex[:8]
    m = _mysql()
    mc = m.cursor()
    p = _pg()
    pc = p.cursor()
    mc.execute(
        f"CREATE TABLE `{table}` (id INT PRIMARY KEY, name VARCHAR(40), updated_at DATETIME(6))"
    )
    pc.execute(
        f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT, updated_at TIMESTAMP)'
    )
    rows = [(i, f"n{i}", datetime(2026, 1, 1, 0, 0, i)) for i in range(1, 6)]
    mc.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", rows)
    pc.executemany(f'INSERT INTO "{table}" VALUES (%s, %s, %s)', rows)
    mappings = [
        {"source": "id", "target": "id", "transform": "integer"},
        {"source": "name", "target": "name", "transform": "none"},
        {"source": "updated_at", "target": "updated_at", "transform": "datetime"},
    ]
    dest_types = {"id": "INTEGER", "name": "TEXT", "updated_at": "TIMESTAMP"}
    try:
        proof = prove_cdc_values(
            source_type="mysql", source_cfg=dict(MYSQL), source_table=table,
            dest_type="postgresql", dest_cfg=dict(PG), dest_table=table,
            mappings=mappings, dest_types=dest_types,
        )
        assert proof is not None, "a datetime carrier must not decline the value scan"
        assert proof.matched, proof

        # A real divergence still fails: shift one timestamp on the destination.
        pc.execute(f"UPDATE \"{table}\" SET updated_at = updated_at + interval '1 hour' WHERE id = 3")
        bad = prove_cdc_values(
            source_type="mysql", source_cfg=dict(MYSQL), source_table=table,
            dest_type="postgresql", dest_cfg=dict(PG), dest_table=table,
            mappings=mappings, dest_types=dest_types,
        )
        assert bad is not None and not bad.matched and bad.missing == 1
    finally:
        mc.execute(f"DROP TABLE IF EXISTS `{table}`")
        pc.execute(f'DROP TABLE IF EXISTS "{table}"')
        m.close()
        p.close()


def test_value_changing_transform_still_declines_the_scan() -> None:
    from services.cdc_value_digest import _identity_pairs

    assert _identity_pairs([{"source": "a", "target": "a", "transform": "uppercase"}]) is None
    assert _identity_pairs(
        [
            {"source": "a", "target": "a", "transform": "datetime"},
            {"source": "b", "target": "b", "transform": "integer"},
        ]
    ) == [("a", "a"), ("b", "b")]
