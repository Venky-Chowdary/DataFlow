"""SQL Server → Oracle SELECT + executemany — dest COUNT(*), PK-range proof."""

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
from services.copy_mysql_oracle import python_bind_for_ora_ddl  # noqa: E402
from services.copy_sqlserver_oracle import copy_sqlserver_to_oracle  # noqa: E402
from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe  # noqa: E402


def test_sqlserver_copy_safe_types_for_oracle():
    assert sqlserver_type_is_copy_safe("NVARCHAR(32)") is True
    assert sqlserver_type_is_copy_safe("BIGINT") is True
    assert sqlserver_type_is_copy_safe("DATE") is True
    assert sqlserver_type_is_copy_safe("VARBINARY(16)") is False
    assert sqlserver_type_is_copy_safe("XML") is False


def test_varchar2_python_empty_string_counts_as_null():
    coerced = [0]
    conv = python_bind_for_ora_ddl("VARCHAR2(32)", coerced)
    assert conv(None) is None
    assert coerced[0] == 0
    assert conv("") is None
    assert coerced[0] == 1
    assert conv("x") == "x"


def _oracle_password() -> str:
    env = (
        os.environ.get("DATAFLOW_ORACLE_PASSWORD")
        or os.environ.get("ORA_PASSWORD")
        or ""
    ).strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _ss_ora_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("SQL Server 1433 or Oracle 1521 not reachable")


def _ss_cfg() -> dict:
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


def _ora_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _ss_connect():
    _ss_ora_or_skip()
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(
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


def _ora_connect():
    _ss_ora_or_skip()
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


def _seed_ss(cur, table: str, rows: int) -> None:
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


def _drop_ora(cur, table: str) -> None:
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _drop_ss(cur, table: str) -> None:
    cur.execute(
        f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]"
    )


def test_live_sqlserver_oracle_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_ORACLE_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ss_ora_src_{tag.lower()}"
    dest = f"SS_ORA_DST_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        ora_cur = ora.cursor()
        _drop_ora(ora_cur, dest)
        ora.commit()
        result = copy_sqlserver_to_oracle(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("shard_mode") == "pk"
        parts = result.source_snapshot.get("partition_proof") or []
        assert parts
        assert sum(int(p["source_count"]) for p in parts) == 800
        assert all(int(p["source_count"]) == int(p["dest_count"]) for p in parts)
        ora_cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(ora_cur.fetchone()[0]) == 800
        ora_cur.execute(f"SELECT ID, LABEL FROM {dest} WHERE ID = 1")
        row = ora_cur.fetchone()
        assert int(row[0]) == 1
        assert str(row[1]) == "r1"
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        ss.close()
        ora.close()


def test_live_sqlserver_oracle_empty_string_becomes_null():
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ss_ora_null_{tag.lower()}"
    dest = f"SS_ORA_NULL_{tag}"
    try:
        cur = ss.cursor()
        _drop_ss(cur, src)
        cur.execute(
            f"CREATE TABLE dbo.[{src}] (id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, NULL), (2, N''), (3, N'x')"
        )
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        result = copy_sqlserver_to_oracle(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 3
        assert int(result.source_snapshot.get("empty_string_as_null_cells") or 0) >= 1
        ora_cur = ora.cursor()
        ora_cur.execute(f"SELECT ID, LABEL FROM {dest} ORDER BY ID")
        rows = list(ora_cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        ora_cur.execute(f"SELECT COUNT(*) FROM {dest} WHERE LABEL IS NULL")
        assert int(ora_cur.fetchone()[0]) == 2
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        ss.close()
        ora.close()


def test_live_sqlserver_oracle_occupied_without_pk_declines():
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ss_ora_nopk_{tag.lower()}"
    dest = f"SS_ORA_NOPK_{tag}"
    try:
        cur = ss.cursor()
        _drop_ss(cur, src)
        cur.execute(
            f"CREATE TABLE dbo.[{src}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, N'a'), (2, N'b')")
        ora_cur = ora.cursor()
        _drop_ora(ora_cur, dest)
        ora_cur.execute(
            f"CREATE TABLE {dest} (ID NUMBER NOT NULL, LABEL VARCHAR2(32))"
        )
        ora_cur.execute(
            f"INSERT INTO {dest} (ID, LABEL) SELECT 1, 'old' FROM dual"
        )
        ora.commit()
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_sqlserver_to_oracle(
                source_cfg=_ss_cfg(),
                source_table=src,
                dest_cfg=_ora_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        ss.close()
        ora.close()


def test_live_sqlserver_oracle_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_ORACLE_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ss_ora_resume_{tag.lower()}"
    dest = f"SS_ORA_RESUME_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 8000)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        first = copy_sqlserver_to_oracle(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_ora_cfg(),
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
        cur = ora.cursor()
        cur.execute(f"DELETE FROM {dest} WHERE ID = :1", [lo])
        ora.commit()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 7999
        second = copy_sqlserver_to_oracle(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_ora_cfg(),
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
        _drop_ss(ss.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        ss.close()
        ora.close()


def test_live_sqlserver_oracle_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_ORACLE_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ss_ora_stream_{tag.lower()}"
    dest = f"SS_ORA_STREAM_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-ora-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "NVARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "NVARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlserver_executemany_oracle"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("executemany" in line for line in ddl_log)
        cur = ora.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {dest}")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ora(ora.cursor(), dest)
        ora.commit()
        ss.close()
        ora.close()
