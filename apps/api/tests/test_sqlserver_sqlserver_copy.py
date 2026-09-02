"""SQL Server → SQL Server identity bulk — dest COUNT(*), PK-range proof."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlserver_sqlserver import (  # noqa: E402
    copy_sqlserver_to_sqlserver,
    sqlserver_family_name,
    sqlserver_same_instance,
)


def test_sqlserver_same_instance_normalizes_loopback():
    src = {"host": "localhost", "port": 1433}
    dest = {"host": "127.0.0.1", "port": 1433}
    assert sqlserver_same_instance(src, dest) is True
    assert sqlserver_same_instance(src, {"host": "127.0.0.1", "port": 1434}) is False
    assert sqlserver_same_instance({"host": "", "port": 1433}, dest) is False


def test_sqlserver_family_aliases():
    assert sqlserver_family_name("mssql") == "sqlserver"
    assert sqlserver_family_name("azure_sql") == "sqlserver"
    assert sqlserver_family_name("mysql") == "mysql"


def _sqlserver_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
    except OSError:
        pytest.skip("SQL Server 1433 not reachable")


def _cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "DataFlow_CDC_2022!",
        "schema": "dbo",
        "trust_server_certificate": True,
        "encrypt": "yes",
    }


def _connect():
    _sqlserver_or_skip()
    pymssql = pytest.importorskip("pymssql")
    try:
        conn = pymssql.connect(
            server="127.0.0.1",
            port=1433,
            user="sa",
            password="DataFlow_CDC_2022!",
            database="dataflow",
            login_timeout=3,
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server auth failed: {exc}")
    return conn


def _seed(cur, table: str, rows: int) -> None:
    cur.execute(f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]")
    cur.execute(
        f"CREATE TABLE dbo.[{table}] (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
    )
    cur.execute(
        f"""
        WITH n AS (
          SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq
          FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO dbo.[{table}] (id, label)
        SELECT seq, CONCAT(N'r', seq) FROM n
        """
    )


def test_live_sqlserver_insert_select_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_SQLSERVER_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_copy_src_{tag}"
    dest = f"ss_copy_dst_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 800)
        result = copy_sqlserver_to_sqlserver(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "insert_select"
        assert result.source_snapshot.get("same_instance") is True
        assert result.source_snapshot.get("shard_mode") == "pk"
        assert result.source_snapshot.get("sqlserver_isolation") in {"snapshot", "holdlock"}
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] WHERE id = 1")
        row = cur.fetchone()
        assert int(row[0]) == 1
        assert str(row[1]) == "r1"
    finally:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]")
        conn.close()


def test_live_sqlserver_occupied_without_pk_declines():
    conn = _connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_nopk_src_{tag}"
    dest = f"ss_nopk_dst_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]")
        cur.execute(f"CREATE TABLE dbo.[{src}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)")
        cur.execute(f"CREATE TABLE dbo.[{dest}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)")
        cur.execute(f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, N'a'), (2, N'b')")
        cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'old')")
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_sqlserver_to_sqlserver(
                source_cfg=cfg,
                source_table=src,
                dest_cfg=cfg,
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]")
        conn.close()


def test_live_sqlserver_same_table_refused():
    conn = _connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_same_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 2)
        with pytest.raises(FastPathUnavailable, match="same SQL Server table"):
            copy_sqlserver_to_sqlserver(
                source_cfg=cfg,
                source_table=src,
                dest_cfg=cfg,
                dest_table=src,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=True,
            )
    finally:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        conn.close()


def test_live_sqlserver_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_SQLSERVER_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_resume_src_{tag}"
    dest = f"ss_resume_dst_{tag}"
    cfg = _cfg()
    try:
        cur = conn.cursor()
        _seed(cur, src, 8000)
        first = copy_sqlserver_to_sqlserver(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        lo = victim["lo"]
        assert lo is not None
        cur.execute(f"DELETE FROM dbo.[{dest}] WHERE id = %s", (lo,))
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 7999
        second = copy_sqlserver_to_sqlserver(
            source_cfg=cfg,
            source_table=src,
            dest_cfg=cfg,
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_rows == 8000
        assert second.target_rows == 8000
        actions = [p["action"] for p in second.source_snapshot["partition_proof"]]
        assert actions.count("skip") == 3
        assert actions.count("reload") == 1
        assert second.source_snapshot.get("partitions_skipped") == 3
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 8000
    finally:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]")
        conn.close()


def test_live_sqlserver_stream_insert_select_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_SQLSERVER_INSERT_SELECT", raising=False)
    conn = _connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_stream_src_{tag}"
    dest = f"ss_stream_dst_{tag}"
    try:
        cur = conn.cursor()
        _seed(cur, src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        endpoint = {
            "format": "sqlserver",
            "host": "127.0.0.1",
            "port": 1433,
            "database": "dataflow",
            "schema": "dbo",
            "username": "sa",
            "password": "DataFlow_CDC_2022!",
            "trust_server_certificate": True,
            "encrypt": "yes",
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
        assert summary.get("load_method") == "insert_select_sqlserver_same_instance"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("INSERT SELECT" in line for line in ddl_log)
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID(N'dbo.{src}', 'U') IS NOT NULL DROP TABLE dbo.[{src}]")
        cur.execute(f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]")
        conn.close()
