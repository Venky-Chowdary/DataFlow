"""MySQL → MySQL identity bulk — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import socket
import sys
import uuid
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.mysql_load_data import load_data_text_value  # noqa: E402
from services.copy_mysql_mysql import (  # noqa: E402
    copy_mysql_to_mysql,
    fast_load_data_text_value,
    mysql_same_instance,
)


def test_mysql_same_instance_normalizes_loopback():
    src = {"host": "localhost", "port": 3306}
    dest = {"host": "127.0.0.1", "port": 3306}
    assert mysql_same_instance(src, dest) is True
    assert mysql_same_instance(src, {"host": "127.0.0.1", "port": 3307}) is False
    assert mysql_same_instance({"host": "", "port": 3306}, dest) is False


def test_fast_load_data_text_matches_canonical():
    samples = [
        None,
        0,
        1,
        "",
        "EMP0000001",
        "has\ttab",
        "has\nnewline",
        "back\\slash",
        date(2026, 9, 2),
        True,
        False,
    ]
    for value in samples:
        assert fast_load_data_text_value(value) == load_data_text_value(value), value


def _mysql_or_skip():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 not reachable")


def _cfg() -> dict:
    return {
        "host": "localhost",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }


def test_live_mysql_to_mysql_insert_select_dest_count(monkeypatch):
    _mysql_or_skip()
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_MYSQL_MYSQL_INSERT_SELECT", raising=False)
    pymysql = pytest.importorskip("pymysql")
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_mysql_src_{tag}"
    dest = f"mysql_mysql_dst_{tag}"
    cfg = _cfg()
    my = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
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
        result = copy_mysql_to_mysql(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "insert_select"
        assert result.source_snapshot.get("same_instance") is True
        assert result.source_snapshot.get("shard_mode") == "pk"
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 800
            cur.execute(f"SELECT id, label FROM `{dest}` WHERE id = 1")
            assert cur.fetchone() == (1, "r1")
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()


def test_live_mysql_to_mysql_fifo_escapes_and_nulls(monkeypatch):
    _mysql_or_skip()
    monkeypatch.setenv("DATAFLOW_MYSQL_MYSQL_INSERT_SELECT", "0")
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "1")
    pymysql = pytest.importorskip("pymysql")
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_mysql_esc_{tag}"
    dest = f"mysql_mysql_esc_dst_{tag}"
    cfg = _cfg()
    my = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    rows = [
        (1, "has\ttab"),
        (2, "has\nnewline"),
        (3, "back\\slash"),
        (4, None),
    ]
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{src}` (id bigint PRIMARY KEY, label varchar(64))"
            )
            cur.executemany(f"INSERT INTO `{src}` (id, label) VALUES (%s, %s)", rows)
        result = copy_mysql_to_mysql(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(64)"],
            replace_destination=True,
        )
        assert result.source_rows == 4
        assert result.target_rows == 4
        assert result.source_snapshot.get("copy_split") == "load_data_fifo"
        with my.cursor() as cur:
            cur.execute(f"SELECT id, label FROM `{dest}` ORDER BY id")
            assert list(cur.fetchall()) == rows
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()


def test_live_mysql_to_mysql_resume_skips_complete_range(monkeypatch):
    _mysql_or_skip()
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_MYSQL_MYSQL_INSERT_SELECT", raising=False)
    pymysql = pytest.importorskip("pymysql")
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_mysql_resume_{tag}"
    dest = f"mysql_mysql_resume_dst_{tag}"
    cfg = _cfg()
    my = pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )
    try:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{src}` (id bigint PRIMARY KEY, label varchar(32))"
            )
            cur.execute("SET SESSION cte_max_recursion_depth = 10000")
            cur.execute(
                f"""
                INSERT INTO `{src}` (id, label)
                WITH RECURSIVE n AS (
                  SELECT 1 AS seq
                  UNION ALL
                  SELECT seq + 1 FROM n WHERE seq < 8000
                )
                SELECT seq, CONCAT('r', seq) FROM n
                """
            )
        first = copy_mysql_to_mysql(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        with my.cursor() as cur:
            lo = victim["lo"]
            assert lo is not None
            cur.execute(f"DELETE FROM `{dest}` WHERE `id` = %s", (lo,))
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 7999
        second = copy_mysql_to_mysql(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_rows == 8000
        assert second.target_rows == 8000
        actions = [p["action"] for p in second.source_snapshot["partition_proof"]]
        assert actions.count("skip") == 3
        assert actions.count("reload") == 1
        assert second.source_snapshot.get("partitions_skipped") == 3
        with my.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 8000
    finally:
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
