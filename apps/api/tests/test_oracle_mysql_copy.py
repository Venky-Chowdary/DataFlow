"""Oracle → MySQL SELECT + STRICT LOAD DATA — dest COUNT(*), PK-range proof."""

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
from services.copy_mysql_mysql import fast_load_data_text_value  # noqa: E402
from services.copy_oracle_mysql import copy_oracle_to_mysql  # noqa: E402
from services.copy_oracle_pg import oracle_type_is_copy_safe  # noqa: E402


def test_oracle_types_safe_for_mysql_load():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("CLOB") is False


def test_load_data_null_encodes_as_backslash_n():
    assert fast_load_data_text_value(None) == "\\N"
    assert fast_load_data_text_value("") == ""


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


def _ora_mysql_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1):
            pass
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 or Oracle 1521 not reachable")


def _mysql_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
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


def _mysql_connect():
    _ora_mysql_or_skip()
    pymysql = pytest.importorskip("pymysql")
    return pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )


def _ora_connect():
    _ora_mysql_or_skip()
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


def _seed_ora(cur, table: str, rows: int) -> None:
    _drop_ora(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def test_live_oracle_mysql_dest_count(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_MYSQL_COPY", raising=False)
    my = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MY_SRC_{tag}"
    dest = f"ora_my_dst_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        with my.cursor() as mcur:
            mcur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        result = copy_oracle_to_mysql(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
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
        with my.cursor() as mcur:
            mcur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(mcur.fetchone()[0]) == 800
            mcur.execute(f"SELECT id, label FROM `{dest}` WHERE id = 1")
            row = mcur.fetchone()
            assert int(row[0]) == 1
            assert str(row[1]) == "r1"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
        ora.close()


def test_live_oracle_mysql_varchar2_empty_is_null():
    my = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MY_NULL_{tag}"
    dest = f"ora_my_null_d_{tag.lower()}"
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
        with my.cursor() as mcur:
            mcur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        result = copy_oracle_to_mysql(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 3
        assert result.source_snapshot.get("varchar2_empty_stored_as_null") is True
        with my.cursor() as mcur:
            mcur.execute(f"SELECT id, label FROM `{dest}` ORDER BY id")
            rows = list(mcur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
        ora.close()


def test_live_oracle_mysql_occupied_without_pk_declines():
    my = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MY_NOPK_{tag}"
    dest = f"ora_my_nopk_d_{tag.lower()}"
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
        with my.cursor() as mcur:
            mcur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            mcur.execute(
                f"CREATE TABLE `{dest}` (id bigint NOT NULL, label varchar(32))"
            )
            mcur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'old')")
        with pytest.raises(FastPathUnavailable, match="non-empty"):
            copy_oracle_to_mysql(
                source_cfg=_ora_cfg(),
                source_table=src,
                dest_cfg=_mysql_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mysql_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
        ora.close()


def test_live_oracle_mysql_resume_skips_complete_range(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_MYSQL_COPY", raising=False)
    my = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MY_RESUME_{tag}"
    dest = f"ora_my_resume_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 8000)
        ora.commit()
        with my.cursor() as mcur:
            mcur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        first = copy_oracle_to_mysql(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert first.source_rows == 8000
        parts = first.source_snapshot["partition_proof"]
        assert len(parts) == 4
        victim = parts[2]
        lo = victim["lo"]
        assert lo is not None
        with my.cursor() as mcur:
            mcur.execute(f"DELETE FROM `{dest}` WHERE id = %s", (lo,))
            mcur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(mcur.fetchone()[0]) == 7999
        second = copy_oracle_to_mysql(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mysql_cfg(),
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
        with my.cursor() as mcur:
            mcur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(mcur.fetchone()[0]) == 8000
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
        ora.close()


def test_live_oracle_mysql_stream_load_method(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MYSQL_COPY_WORKERS", "4")
    monkeypatch.delenv("DATAFLOW_ORACLE_MYSQL_COPY", raising=False)
    my = _mysql_connect()
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MY_STREAM_{tag}"
    dest = f"ora_my_stream_d_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        with my.cursor() as mcur:
            mcur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-my-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": dest}
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
        assert summary.get("load_method") == "select_oracle_load_data_mysql"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("LOAD DATA" in line for line in ddl_log)
        with my.cursor() as mcur:
            mcur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(mcur.fetchone()[0]) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        with my.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        my.close()
        ora.close()
