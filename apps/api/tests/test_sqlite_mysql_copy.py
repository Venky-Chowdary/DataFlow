"""SQLite SELECT TSV → MySQL STRICT LOAD DATA — dest COUNT(*)."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlite_mysql import (  # noqa: E402
    copy_sqlite_to_mysql,
    sqlite_mysql_copy_enabled,
    sqlite_mysql_type_is_copy_safe,
    sqlite_value_to_load_data,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _mysql_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 not reachable")


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _mysql_cfg() -> dict:
    return {
        "type": "mysql",
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
    }


def _mysql_connect():
    _mysql_or_skip()
    pymysql = pytest.importorskip("pymysql")
    try:
        return pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"MySQL auth failed: {exc}")


def _seed(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(
        f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
    )
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


def _dest_count(table: str) -> int:
    n = destination_row_count("mysql", _mysql_cfg(), schema="", table_name=table)
    assert n is not None
    return int(n)


def _drop_mysql(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def test_sqlite_mysql_copy_safe_types():
    assert sqlite_mysql_type_is_copy_safe("INTEGER") is True
    assert sqlite_mysql_type_is_copy_safe("TEXT") is True
    assert sqlite_mysql_type_is_copy_safe("DATE") is True
    assert sqlite_mysql_type_is_copy_safe("REAL") is True
    assert sqlite_mysql_type_is_copy_safe("BOOLEAN") is True
    assert sqlite_mysql_type_is_copy_safe("DATETIME") is True
    assert sqlite_mysql_type_is_copy_safe("TIMESTAMP") is False
    assert sqlite_mysql_type_is_copy_safe("JSON") is False
    assert sqlite_mysql_type_is_copy_safe("BLOB") is False


def test_sqlite_mysql_date_iso_to_load_data():
    assert sqlite_value_to_load_data("2020-01-02", "DATE") == "2020-01-02"
    assert sqlite_value_to_load_data(date(2020, 1, 2), "DATE") == "2020-01-02"
    assert sqlite_value_to_load_data("2020-01-02", "TEXT") == "2020-01-02"
    assert sqlite_value_to_load_data(None, "DATE") == "\\N"
    assert sqlite_value_to_load_data("", "TEXT") == ""
    with pytest.raises(FastPathUnavailable, match="not ISO"):
        sqlite_value_to_load_data("not-a-date", "DATE")
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_value_to_load_data("2020-01-02 12:00:00", "DATE")
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        sqlite_value_to_load_data(b"x", "TEXT")
    assert sqlite_value_to_load_data("2020-01-02 12:00:00", "DATETIME") == (
        "2020-01-02 12:00:00"
    )
    assert sqlite_value_to_load_data(None, "DATETIME") == "\\N"
    with pytest.raises(FastPathUnavailable, match="unix"):
        sqlite_value_to_load_data(1711929600, "DATETIME")
    with pytest.raises(FastPathUnavailable, match="tz-aware"):
        sqlite_value_to_load_data("2020-01-02T12:00:00Z", "DATETIME")
    with pytest.raises(FastPathUnavailable, match="date-only"):
        sqlite_value_to_load_data("2020-01-02", "DATETIME")
    with pytest.raises(FastPathUnavailable, match="TIMESTAMP"):
        sqlite_value_to_load_data("2020-01-02 12:00:00", "TIMESTAMP")
    assert sqlite_value_to_load_data(1, "BOOLEAN") == "1"
    assert sqlite_value_to_load_data(0, "BOOLEAN") == "0"
    assert sqlite_value_to_load_data(None, "BOOLEAN") == "\\N"
    with pytest.raises(FastPathUnavailable, match="0/1"):
        sqlite_value_to_load_data("true", "BOOLEAN")
    with pytest.raises(FastPathUnavailable, match="0/1"):
        sqlite_value_to_load_data(2, "BOOLEAN")


def test_sqlite_mysql_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_MYSQL_COPY", "0")
    assert sqlite_mysql_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_mysql(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_mysql_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            mysql_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlite_mysql_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_mysql(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_mysql_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            mysql_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_sqlite_mysql_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_MYSQL_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_dst_{tag}"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("mysql_write") == "insert"
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        _drop_mysql(dest)


def test_live_sqlite_mysql_datetime_iso_dest_count(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_dt_{tag}"
    conn = sqlite3.connect(src)
    conn.execute(
        'CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, updated_at DATETIME)'
    )
    conn.execute(
        'INSERT INTO "src_t" (id, updated_at) VALUES (1, ?), (2, NULL)',
        ("2024-11-01 08:00:00",),
    )
    conn.commit()
    conn.close()
    mysql = _mysql_connect()
    try:
        result = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("updated_at", "updated_at")],
            mysql_ddls=["BIGINT", "DATETIME(6)"],
            replace_destination=True,
        )
        assert result.source_rows == 2
        assert result.source_checksum == "dest_count:2"
        assert _dest_count(dest) == 2
        with mysql.cursor() as cur:
            cur.execute(f"SELECT id, updated_at FROM `{dest}` ORDER BY id")
            rows = cur.fetchall()
        assert rows[0][0] == 1
        assert rows[0][1] == datetime(2024, 11, 1, 8, 0, 0)
        assert rows[1] == (2, None)
    finally:
        mysql.close()
        _drop_mysql(dest)


def test_live_sqlite_mysql_boolean_0_1_dest_count(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_bool_{tag}"
    conn = sqlite3.connect(src)
    conn.execute(
        'CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, flag BOOLEAN)'
    )
    conn.execute(
        'INSERT INTO "src_t" (id, flag) VALUES (1, 1), (2, 0), (3, NULL)'
    )
    conn.commit()
    conn.close()
    mysql = _mysql_connect()
    try:
        result = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("flag", "flag")],
            mysql_ddls=["BIGINT", "BOOLEAN"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert result.source_checksum == "dest_count:3"
        assert _dest_count(dest) == 3
        with mysql.cursor() as cur:
            cur.execute(f"SELECT id, flag FROM `{dest}` ORDER BY id")
            rows = cur.fetchall()
        assert rows[0] == (1, 1)
        assert rows[1] == (2, 0)
        assert rows[2] == (3, None)
    finally:
        mysql.close()
        _drop_mysql(dest)


def test_live_sqlite_mysql_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_null_{tag}"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    mysql = _mysql_connect()
    try:
        result = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        with mysql.cursor() as cur:
            cur.execute(f"SELECT id, label FROM `{dest}` ORDER BY id")
            rows = cur.fetchall()
        assert rows[0] == (1, None)
        assert rows[1] == (2, "")
        assert rows[2] == (3, "x")
    finally:
        mysql.close()
        _drop_mysql(dest)


def test_live_sqlite_mysql_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_skip_{tag}"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _drop_mysql(dest)


def test_live_sqlite_mysql_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_occ_{tag}"
    _seed(src, "src_t", 800)
    mysql = _mysql_connect()
    try:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.executemany(
                f"INSERT INTO `{dest}` (id, label) VALUES (%s, %s)",
                [(1, "g"), (2, "g")],
            )
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied MySQL dest"):
            copy_sqlite_to_mysql(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_mysql_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mysql_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        mysql.close()
        _drop_mysql(dest)


def test_live_sqlite_mysql_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_ow_{tag}"
    _seed(src, "src_t", 800)
    mysql = _mysql_connect()
    try:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'ghost')")
        result = copy_sqlite_to_mysql(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mysql_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        mysql.close()
        _drop_mysql(dest)


def test_live_sqlite_mysql_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_MYSQL_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mysql_str_{tag}"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-mysql-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlite_load_data_mysql"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest) == 800
    finally:
        _drop_mysql(dest)
