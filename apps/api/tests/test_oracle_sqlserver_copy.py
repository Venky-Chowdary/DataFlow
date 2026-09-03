"""Oracle → SQL Server SELECT + fast_executemany — dest COUNT(*), PK-range proof."""

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
from services.copy_oracle_pg import oracle_type_is_copy_safe  # noqa: E402
from services.copy_oracle_sqlserver import copy_oracle_to_sqlserver  # noqa: E402


def test_oracle_copy_safe_types_for_sqlserver():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("CLOB") is False


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


def _ora_ss_or_skip():
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
    _ora_ss_or_skip()
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
    _ora_ss_or_skip()
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


def _seed_ora(cur, table: str, rows: int) -> None:
    _drop_ora(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def test_live_oracle_sqlserver_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SS_SRC_{tag}"
    dest = f"ora_ss_dst_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        result = copy_oracle_to_sqlserver(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
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
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] WHERE id = 1")
        row = cur.fetchone()
        assert int(row[0]) == 1
        assert str(row[1]) == "r1"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        ss.close()
        ora.close()


def test_live_oracle_sqlserver_varchar2_empty_is_null():
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SS_NULL_{tag}"
    dest = f"ora_ss_null_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _drop_ora(cur, src)
        cur.execute(
            f"CREATE TABLE {src} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(
            f"INSERT INTO {src} (ID, LABEL) "
            "SELECT 1, NULL FROM dual UNION ALL "
            "SELECT 2, '' FROM dual UNION ALL "
            "SELECT 3, 'x' FROM dual"
        )
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        result = copy_oracle_to_sqlserver(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 3
        assert result.source_snapshot.get("varchar2_empty_stored_as_null") is True
        cur = ss.cursor()
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        ss.close()
        ora.close()


def test_live_oracle_sqlserver_occupied_without_pk_declines():
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SS_NOPK_{tag}"
    dest = f"ora_ss_nopk_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _drop_ora(cur, src)
        cur.execute(
            f"CREATE TABLE {src} (ID NUMBER NOT NULL, LABEL VARCHAR2(32))"
        )
        cur.execute(
            f"INSERT INTO {src} (ID, LABEL) "
            "SELECT 1, 'a' FROM dual UNION ALL SELECT 2, 'b' FROM dual"
        )
        ora.commit()
        ss_cur = ss.cursor()
        _drop_ss(ss_cur, dest)
        ss_cur.execute(
            f"CREATE TABLE dbo.[{dest}] (id BIGINT NOT NULL, label NVARCHAR(32) NULL)"
        )
        ss_cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'old')")
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_oracle_to_sqlserver(
                source_cfg=_ora_cfg(),
                source_table=src,
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        ss.close()
        ora.close()


def test_live_oracle_sqlserver_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SS_RESUME_{tag}"
    dest = f"ora_ss_resume_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 8000)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        first = copy_oracle_to_sqlserver(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_ss_cfg(),
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
        ss_cur = ss.cursor()
        ss_cur.execute(f"DELETE FROM dbo.[{dest}] WHERE id = %s", (lo,))
        ss_cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(ss_cur.fetchone()[0]) == 7999
        second = copy_oracle_to_sqlserver(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_ss_cfg(),
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
        ss_cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(ss_cur.fetchone()[0]) == 8000
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        ss.close()
        ora.close()


def test_live_oracle_sqlserver_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_SS_STREAM_{tag}"
    dest = f"ora_ss_stream_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-ss-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "VARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_oracle_fast_executemany_sqlserver"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("fast_executemany" in line for line in ddl_log)
        ss_cur = ss.cursor()
        ss_cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(ss_cur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_ss(ss.cursor(), dest)
        ss.close()
        ora.close()
