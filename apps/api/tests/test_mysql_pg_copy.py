"""MySQL → PostgreSQL COPY FROM STDIN — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_mysql_pg import mysql_type_is_copy_safe  # noqa: E402


def test_mysql_copy_safe_types():
    assert mysql_type_is_copy_safe("varchar(32)") is True
    assert mysql_type_is_copy_safe("BIGINT") is True
    assert mysql_type_is_copy_safe("date") is True
    assert mysql_type_is_copy_safe("datetime") is True
    assert mysql_type_is_copy_safe("json") is False
    assert mysql_type_is_copy_safe("blob") is False
    assert mysql_type_is_copy_safe("timestamp") is False
    assert mysql_type_is_copy_safe("varbinary(16)") is False


def test_live_mysql_to_pg_dest_count_and_pk_ranges(monkeypatch):
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 or PostgreSQL 5432 not reachable")
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    pymysql = pytest.importorskip("pymysql")
    psycopg2 = pytest.importorskip("psycopg2")
    from services.copy_mysql_pg import copy_mysql_to_postgres

    tag = uuid.uuid4().hex[:8]
    src = f"mysql_pg_src_{tag}"
    dest = f"mysql_pg_dst_{tag}"
    my = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    pg = psycopg2.connect(
        host="localhost", port=5432, user="dataflow", password="dataflow", dbname="dataflow",
    )
    pg.autocommit = True
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(
                f"CREATE TABLE `{src}` (id bigint PRIMARY KEY, label varchar(32))"
            )
            cur.execute(
                f"""
                INSERT INTO `{src}` (id, label)
                WITH RECURSIVE n AS (
                  SELECT 1 AS seq
                  UNION ALL
                  SELECT seq + 1 FROM n WHERE seq < 800
                )
                SELECT seq, CONCAT('r', seq) FROM n
                """
            )
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        result = copy_mysql_to_postgres(
            source_cfg={
                "host": "localhost", "port": 3306, "database": "dataflow",
                "username": "dataflow", "password": "dataflow",
            },
            source_table=src,
            dest_cfg={
                "host": "localhost", "port": 5432, "database": "dataflow",
                "username": "dataflow", "password": "dataflow",
            },
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("shard_mode") == "pk"
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
            cur.execute(f'SELECT id, label FROM public."{dest}" WHERE id = 1')
            row = cur.fetchone()
            assert row == (1, "r1")
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        my.close()
        pg.close()
