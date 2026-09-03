"""SQL Server HOLDLOCK SELECT → SQLite executemany — dest COUNT(*)."""

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
from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe  # noqa: E402
from services.copy_sqlserver_sqlite import (  # noqa: E402
    copy_sqlserver_to_sqlite,
    sqlserver_sqlite_copy_enabled,
    sqlserver_value_to_sqlite,
)
from services.copy_sqlite_sqlserver import copy_sqlite_to_sqlserver  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _ss_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
    except OSError:
        pytest.skip("SQL Server 1433 not reachable")


def _sqlite_cfg(path: Path | str, table: str) -> dict:
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


def _seed_sqlite(path: Path, table: str, rows: int) -> None:
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


def _ss_count(table: str) -> int:
    n = destination_row_count("sqlserver", _ss_cfg(), schema="dbo", table_name=table)
    assert n is not None
    return int(n)


def _sqlite_count(path: Path, table: str) -> int:
    n = destination_row_count(
        "sqlite", _sqlite_cfg(path, table), schema="", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_ss_from_sqlite(src: Path, src_table: str, dest: str, rows: int) -> None:
    _seed_sqlite(src, src_table, rows)
    _drop_ss(dest)
    result = copy_sqlite_to_sqlserver(
        source_cfg=_sqlite_cfg(src, src_table),
        source_table=src_table,
        dest_cfg=_ss_cfg(),
        dest_table=dest,
        pairs=[("id", "id"), ("label", "label")],
        sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _ss_count(dest) == rows


def test_sqlserver_sqlite_copy_safe_types():
    assert sqlserver_type_is_copy_safe("NVARCHAR(32)") is True
    assert sqlserver_type_is_copy_safe("BIGINT") is True
    assert sqlserver_type_is_copy_safe("DATE") is True
    assert sqlserver_type_is_copy_safe("DATETIME") is True
    assert sqlserver_type_is_copy_safe("VARBINARY") is False
    assert sqlserver_type_is_copy_safe("XML") is False
    assert sqlserver_type_is_copy_safe("DATETIMEOFFSET") is False
    assert sqlserver_type_is_copy_safe("UNIQUEIDENTIFIER") is False


def test_sqlserver_sqlite_date_binds_as_text():
    assert sqlserver_value_to_sqlite(date(2020, 1, 2)) == "2020-01-02"
    assert sqlserver_value_to_sqlite(None) is None
    assert sqlserver_value_to_sqlite("") == ""
    with pytest.raises(FastPathUnavailable, match="binary"):
        sqlserver_value_to_sqlite(b"x")


def test_sqlserver_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLSERVER_SQLITE_COPY", "0")
    assert sqlserver_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlserver_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(":memory:", "orders"),
            dest_table="orders",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlserver_sqlite_blob_dest_declines(tmp_path):
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("payload", "payload")],
            sqlite_ddls=["BLOB"],
            replace_destination=True,
        )


def test_live_sqlserver_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLSERVER_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_mid_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_dst"
    try:
        _seed_ss_from_sqlite(src, "src_t", ss_mid, 800)
        result = copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=ss_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ss(ss_mid)


def test_live_sqlserver_sqlite_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_null_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_null_dst"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        _drop_ss(ss_mid)
        copy_sqlite_to_sqlserver(
            source_cfg=_sqlite_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ss_cfg(),
            dest_table=ss_mid,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        result = copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=ss_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _sqlite_count(dest_db, dest) == 3
        out = sqlite3.connect(dest_db)
        try:
            rows = out.execute(f'SELECT id, label FROM "{dest}" ORDER BY id').fetchall()
        finally:
            out.close()
        assert rows == [(1, None), (2, ""), (3, "x")]
    finally:
        _drop_ss(ss_mid)


def test_live_sqlserver_sqlite_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_skip_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_skip_dst"
    try:
        _seed_ss_from_sqlite(src, "src_t", ss_mid, 800)
        first = copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=ss_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=ss_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ss(ss_mid)


def test_live_sqlserver_sqlite_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_occ_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_occ_dst"
    try:
        _seed_ss_from_sqlite(src, "src_t", ss_mid, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.executemany(
            f'INSERT INTO "{dest}" (id, label) VALUES (?, ?)',
            [(1, "ghost"), (2, "ghost")],
        )
        conn.commit()
        conn.close()
        with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
            copy_sqlserver_to_sqlite(
                source_cfg=_ss_cfg(),
                source_schema="dbo",
                source_table=ss_mid,
                dest_cfg=_sqlite_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _sqlite_count(dest_db, dest) == 2
    finally:
        _drop_ss(ss_mid)


def test_live_sqlserver_sqlite_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_ow_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_ow_dst"
    try:
        _seed_ss_from_sqlite(src, "src_t", ss_mid, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.execute(f'INSERT INTO "{dest}" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_sqlserver_to_sqlite(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=ss_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ss(ss_mid)


def test_live_sqlserver_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLSERVER_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ss_mid = f"ss_sqlite_stream_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ss_sqlite_stream_dst"
    try:
        _seed_ss_from_sqlite(src, "src_t", ss_mid, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": ss_mid}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_sqlite_cfg(dest_db, dest), "format": "sqlite"}
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
        assert summary.get("load_method") == "select_sqlserver_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("SQLite" in line for line in ddl_log)
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ss(ss_mid)
