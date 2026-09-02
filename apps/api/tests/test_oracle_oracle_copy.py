"""Oracle → Oracle identity bulk — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_oracle_oracle import (  # noqa: E402
    copy_oracle_to_oracle,
    oracle_family_name,
    oracle_same_instance,
)


def test_oracle_same_instance_requires_service():
    src = {"host": "localhost", "port": 1521, "database": "XEPDB1"}
    dest = {"host": "127.0.0.1", "port": 1521, "service_name": "XEPDB1"}
    assert oracle_same_instance(src, dest) is True
    assert oracle_same_instance(src, {"host": "127.0.0.1", "port": 1521, "database": "FREEPDB1"}) is False
    assert oracle_same_instance({"host": "localhost", "port": 1521, "database": ""}, dest) is False


def test_oracle_family_aliases():
    assert oracle_family_name("oracle_db") == "oracle"
    assert oracle_family_name("oracledb") == "oracle"
    assert oracle_family_name("mysql") == "mysql"


def _oracle_password() -> str:
    env = (os.environ.get("DATAFLOW_ORACLE_PASSWORD") or os.environ.get("ORA_PASSWORD") or "").strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _oracle_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("Oracle 1521 not reachable")


def _cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _connect():
    _oracle_or_skip()
    oracledb = pytest.importorskip("oracledb")
    try:
        conn = oracledb.connect(
            user="dataflow",
            password=_oracle_password(),
            dsn="127.0.0.1:1521/XEPDB1",
        )
    except Exception as exc:
        pytest.skip(f"Oracle auth failed: {exc}")
    return conn


def _drop(cur, table: str) -> None:
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f'{table} PURGE\'; EXCEPTION WHEN OTHERS THEN '
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _seed(cur, table: str, rows: int) -> None:
    _drop(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def test_live_oracle_insert_select_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_CP_SRC_{tag}"
    dest = f"ORA_CP_DST_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 800)
        conn.commit()
        result = copy_oracle_to_oracle(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "insert_select_append"
        assert result.source_snapshot.get("same_instance") is True
        assert result.source_snapshot.get("shard_mode") == "pk"
        assert result.source_snapshot.get("oracle_lock") == "share"
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
        cur.execute(f"SELECT ID, LABEL FROM {dest} WHERE ID = 1")
        row = cur.fetchone()
        assert int(row[0]) == 1
        assert str(row[1]) == "r1"
    finally:
        cur = conn.cursor()
        _drop(cur, src)
        _drop(cur, dest)
        conn.close()


def test_live_oracle_occupied_without_pk_declines():
    conn = _connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_NPK_SRC_{tag}"
    dest = f"ORA_NPK_DST_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _drop(cur, src)
        _drop(cur, dest)
        cur.execute(f"CREATE TABLE {src} (ID NUMBER NOT NULL, LABEL VARCHAR2(32))")
        cur.execute(f"CREATE TABLE {dest} (ID NUMBER NOT NULL, LABEL VARCHAR2(32))")
        cur.execute(f"INSERT INTO {src} (ID, LABEL) VALUES (1, 'a')")
        cur.execute(f"INSERT INTO {src} (ID, LABEL) VALUES (2, 'b')")
        cur.execute(f"INSERT INTO {dest} (ID, LABEL) VALUES (1, 'old')")
        conn.commit()
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_oracle_to_oracle(
                source_cfg=cfg,
                source_table=src,
                dest_cfg=cfg,
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
    finally:
        cur = conn.cursor()
        _drop(cur, src)
        _drop(cur, dest)
        conn.close()


def test_live_oracle_same_table_refused():
    conn = _connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SAME_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 2)
        conn.commit()
        with pytest.raises(FastPathUnavailable, match="same Oracle table"):
            copy_oracle_to_oracle(
                source_cfg=cfg,
                source_table=src,
                dest_cfg=cfg,
                dest_table=src,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=True,
            )
    finally:
        cur = conn.cursor()
        _drop(cur, src)
        conn.close()


def test_live_oracle_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_RS_SRC_{tag}"
    dest = f"ORA_RS_DST_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 8000)
        conn.commit()
        first = copy_oracle_to_oracle(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        lo = victim["lo"]
        assert lo is not None
        cur.execute(f"DELETE FROM {dest} WHERE ID = :1", [lo])
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 7999
        second = copy_oracle_to_oracle(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_rows == 8000
        assert second.target_rows == 8000
        actions = [p["action"] for p in second.source_snapshot["partition_proof"]]
        assert actions.count("skip") == 3
        assert actions.count("reload") == 1
        assert second.source_snapshot.get("partitions_skipped") == 3
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 8000
    finally:
        cur = conn.cursor()
        _drop(cur, src)
        _drop(cur, dest)
        conn.close()


def test_live_oracle_stream_insert_select_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_ORACLE_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_ST_SRC_{tag}"
    dest = f"ORA_ST_DST_{tag}"
    try:
        cur = conn.cursor()
        _seed(cur, src, 800)
        conn.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        endpoint = {
            "format": "oracle",
            "host": "127.0.0.1",
            "port": 1521,
            "database": "XEPDB1",
            "schema": "DATAFLOW",
            "username": "dataflow",
            "password": _oracle_password(),
        }
        source = EndpointConfig.from_dict("database", {**endpoint, "table": src})
        destination = EndpointConfig.from_dict("database", {**endpoint, "table": dest})
        mappings = [
            {"source": "id", "target": "id", "type": "integer", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "integer", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "insert_select_oracle_same_instance"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("INSERT SELECT" in line for line in ddl_log)
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        cur = conn.cursor()
        _drop(cur, src)
        _drop(cur, dest)
        conn.close()
