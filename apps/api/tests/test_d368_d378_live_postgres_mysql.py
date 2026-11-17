"""D368 and D378 on the servers CI starts.

The sqlite proofs stay. These run when Postgres or MySQL answers on
127.0.0.1 and skip otherwise, so a laptop without the servers is not a
false failure and the CI service containers are a real dialect.
"""

from __future__ import annotations

import uuid

import pytest

pytestmark = pytest.mark.timeout(60)


def _pg():
    import psycopg2
    from conftest import pg_test_cfg

    cfg = pg_test_cfg()
    try:
        conn = psycopg2.connect(
            host=cfg["host"], port=cfg["port"], dbname=cfg["database"],
            user=cfg["username"], password=cfg["password"], connect_timeout=2,
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = False
    return conn


def _mysql():
    import pymysql

    try:
        conn = pymysql.connect(
            host="127.0.0.1", port=3306, user="dataflow", password="dataflow",
            database="dataflow", connect_timeout=2, autocommit=False,
        )
    except Exception as exc:
        pytest.skip(f"MySQL not reachable: {exc}")
    return conn


def test_postgres_restore_rolls_back_the_aborted_transaction_first():
    from connectors.column_widen_restore import _RESTORE_BUCKET, settle_widen_restore

    conn = _pg()
    name = f"d368_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{name}" (id int)')
        conn.commit()
        cur.execute(f'INSERT INTO "{name}" VALUES (1)')
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM this_table_is_not_there")
        token = _RESTORE_BUCKET.set([f'ALTER TABLE "{name}" ADD COLUMN restored int'])
        try:
            settle_widen_restore(conn, rows_written=0, failed=True)
        finally:
            _RESTORE_BUCKET.reset(token)
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = %s AND column_name = 'restored'",
            (name,),
        )
        assert cur.fetchone() is not None
        cur.execute(f'SELECT count(*) FROM "{name}"')
        assert cur.fetchone()[0] == 0
    finally:
        conn.rollback()
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.commit()
        conn.close()


def test_mysql_restore_does_not_keep_the_failed_rows():
    from connectors.column_widen_restore import _RESTORE_BUCKET, settle_widen_restore

    conn = _mysql()
    name = f"d368_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE `{name}` (id INT)")
        conn.commit()
        cur.execute(f"INSERT INTO `{name}` VALUES (1)")
        token = _RESTORE_BUCKET.set([f"ALTER TABLE `{name}` ADD COLUMN restored INT"])
        try:
            settle_widen_restore(conn, rows_written=0, failed=True)
        finally:
            _RESTORE_BUCKET.reset(token)
        cur.execute(f"SELECT COUNT(*) FROM `{name}`")
        assert cur.fetchone()[0] == 0
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = 'restored'",
            (name,),
        )
        assert cur.fetchone() is not None
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        conn.commit()
        conn.close()


def test_postgres_drops_an_empty_created_table_and_keeps_a_filled_one(monkeypatch):
    from services.batch_undo import note_dropped_empty_create
    from services.error_handling import TransferCancelled

    monkeypatch.setenv("DATAFLOW_EGRESS_ALLOWLIST", "127.0.0.0/8")
    conn = _pg()
    from conftest import pg_test_cfg

    cfg = {"type": "postgresql", **pg_test_cfg()}
    empty = f"d378e_{uuid.uuid4().hex[:8]}"
    filled = f"d378f_{uuid.uuid4().hex[:8]}"
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{empty}" (id int)')
        cur.execute(f'CREATE TABLE "{filled}" (id int)')
        cur.execute(f'INSERT INTO "{filled}" VALUES (1)')
        conn.commit()
        gone = TransferCancelled("cancelled", rows_written=0)
        note_dropped_empty_create(gone, cfg, empty, None)
        kept = TransferCancelled("cancelled", rows_written=0)
        note_dropped_empty_create(kept, cfg, filled, None)
        cur.execute(
            "SELECT tablename FROM pg_tables WHERE tablename IN (%s, %s)",
            (empty, filled),
        )
        names = {row[0] for row in cur.fetchall()}
        assert empty not in names
        assert filled in names
        assert gone.dropped_created_table == empty
        assert kept.dropped_created_table == ""
    finally:
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{empty}"')
        cur.execute(f'DROP TABLE IF EXISTS "{filled}"')
        conn.commit()
        conn.close()
