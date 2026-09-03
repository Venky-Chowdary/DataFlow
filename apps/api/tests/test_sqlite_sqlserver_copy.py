"""SQLite SELECT → SQL Server fast_executemany — dest COUNT(*)."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlite_sqlserver import (  # noqa: E402
    copy_sqlite_to_sqlserver,
    sqlite_sqlserver_copy_enabled,
    sqlite_sqlserver_type_is_copy_safe,
    sqlite_value_to_sqlserver,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _ss_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
    except OSError:
        pytest.skip("SQL Server 1433 not reachable")


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


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


def _ss_connect():
    _ss_or_skip()
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


def _drop_ss(table: str) -> None:
    conn = _ss_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]"
        )
    finally:
        conn.close()


def _dest_count(table: str) -> int:
    n = destination_row_count("sqlserver", _ss_cfg(), schema="dbo", table_name=table)
    assert n is not None
    return int(n)


def test_sqlite_sqlserver_copy_safe_types():
    assert sqlite_sqlserver_type_is_copy_safe("INTEGER") is True
    assert sqlite_sqlserver_type_is_copy_safe("TEXT") is True
    assert sqlite_sqlserver_type_is_copy_safe("DATE") is True
    assert sqlite_sqlserver_type_is_copy_safe("REAL") is True
    assert sqlite_sqlserver_type_is_copy_safe("BOOLEAN") is True
    assert sqlite_sqlserver_type_is_copy_safe("DATETIME") is False
    assert sqlite_sqlserver_type_is_copy_safe("TIMESTAMP") is False
    assert sqlite_sqlserver_type_is_copy_safe("JSON") is False
    assert sqlite_sqlserver_type_is_copy_safe("BLOB") is False


def test_sqlite_sqlserver_date_iso_bind():
    assert sqlite_value_to_sqlserver("2020-01-02", "DATE") == date(2020, 1, 2)
    assert sqlite_value_to_sqlserver(date(2020, 1, 2), "DATE") == date(2020, 1, 2)
    assert sqlite_value_to_sqlserver("2020-01-02", "NVARCHAR(32)") == "2020-01-02"
    assert sqlite_value_to_sqlserver(None, "DATE") is None
    assert sqlite_value_to_sqlserver("", "NVARCHAR(32)") == ""
    with pytest.raises(FastPathUnavailable, match="not ISO"):
        sqlite_value_to_sqlserver("not-a-date", "DATE")
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_value_to_sqlserver("2020-01-02 12:00:00", "DATETIME")
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        sqlite_value_to_sqlserver(b"x", "NVARCHAR(32)")


def test_sqlite_sqlserver_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_SQLSERVER_COPY", "0")
    assert sqlite_sqlserver_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_sqlserver(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_ss_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            sqlserver_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_sqlite_sqlserver_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_sqlserver(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_ss_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            sqlserver_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_sqlite_sqlserver_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_SQLSERVER_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_dst_{tag}"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_sqlserver(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("sqlserver_write") == "insert"
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(dest)


def test_live_sqlite_sqlserver_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_null_{tag}"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    ss = _ss_connect()
    try:
        result = copy_sqlite_to_sqlserver(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        cur = ss.cursor()
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        ss.close()
        _drop_ss(dest)


def test_live_sqlite_sqlserver_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_skip_{tag}"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_sqlserver(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_sqlserver(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(dest)


def test_live_sqlite_sqlserver_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_occ_{tag}"
    _seed(src, "src_t", 800)
    ss = _ss_connect()
    try:
        cur = ss.cursor()
        cur.execute(
            f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]"
        )
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'g'), (2, N'g')"
        )
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied SQL Server dest"):
            copy_sqlite_to_sqlserver(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        ss.close()
        _drop_ss(dest)


def test_live_sqlite_sqlserver_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_ow_{tag}"
    _seed(src, "src_t", 800)
    ss = _ss_connect()
    try:
        cur = ss.cursor()
        cur.execute(
            f"IF OBJECT_ID(N'dbo.{dest}', 'U') IS NOT NULL DROP TABLE dbo.[{dest}]"
        )
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'ghost')")
        result = copy_sqlite_to_sqlserver(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlserver_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        ss.close()
        _drop_ss(dest)


def test_live_sqlite_sqlserver_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_SQLSERVER_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ss_str_{tag}"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-ss-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlite_fast_executemany_sqlserver"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("SQL Server" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(dest)
