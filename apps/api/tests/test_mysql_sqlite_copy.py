"""MySQL snapshot SELECT → SQLite executemany — dest COUNT(*)."""

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
from services.copy_mysql_sqlite import (  # noqa: E402
    copy_mysql_to_sqlite,
    mysql_sqlite_copy_enabled,
    mysql_value_to_sqlite,
)
from services.copy_sqlite_mysql import copy_sqlite_to_mysql  # noqa: E402
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


def _drop_table(path: Path, table: str) -> None:
    if not path.exists():
        return
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.commit()
    conn.close()


def _drop_mysql(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def _dest_count(path: Path | str, table: str) -> int:
    n = destination_row_count("sqlite", _cfg(path, table), schema="", table_name=table)
    assert n is not None
    return int(n)


def _seed_mysql_from_sqlite(src_db: Path, table: str, mysql_table: str, rows: int) -> None:
    _seed_sqlite(src_db, table, rows)
    result = copy_sqlite_to_mysql(
        source_cfg=_cfg(src_db, table),
        source_table=table,
        dest_cfg=_mysql_cfg(),
        dest_table=mysql_table,
        pairs=[("id", "id"), ("label", "label")],
        mysql_ddls=["BIGINT", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_mysql_sqlite_python_date_to_iso_text():
    assert mysql_value_to_sqlite(None) is None
    assert mysql_value_to_sqlite("x") == "x"
    assert mysql_value_to_sqlite("") == ""
    assert mysql_value_to_sqlite(True) == 1
    assert mysql_value_to_sqlite(date(2020, 1, 2)) == "2020-01-02"
    assert mysql_value_to_sqlite(datetime(2020, 1, 2)) == "2020-01-02"
    assert mysql_value_to_sqlite(datetime(2020, 1, 2, 12, 30, 0)) == "2020-01-02 12:30:00"
    with pytest.raises(FastPathUnavailable, match="binary"):
        mysql_value_to_sqlite(b"x")


def test_mysql_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_MYSQL_SQLITE_COPY", "0")
    assert mysql_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table="missing",
            dest_cfg=_cfg(tmp_path / "dst.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_mysql_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table="missing",
            dest_cfg=_cfg(":memory:", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_mysql_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_MYSQL_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_src_{tag}"
    dest = "dst_t"
    try:
        _seed_mysql_from_sqlite(src_db, "src_t", src, 800)
        result = copy_mysql_to_sqlite(
            source_cfg={**_mysql_cfg(), "table": src},
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("mysql_read") == "consistent_snapshot"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)


def test_live_mysql_sqlite_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_null_{tag}"
    dest = "dst_t"
    conn = sqlite3.connect(src_db)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        copy_sqlite_to_mysql(
            source_cfg=_cfg(src_db, "src_t"),
            source_table="src_t",
            dest_cfg=_mysql_cfg(),
            dest_table=src,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        result = copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest_db, dest) == 3
        dest_conn = sqlite3.connect(dest_db)
        rows = dest_conn.execute('SELECT id, label FROM "dst_t" ORDER BY id').fetchall()
        dest_conn.close()
        assert rows[0] == (1, None)
        assert rows[1] == (2, "")
        assert rows[2] == (3, "x")
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)


def test_live_mysql_sqlite_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_skip_{tag}"
    dest = "dst_t"
    try:
        _seed_mysql_from_sqlite(src_db, "src_t", src, 800)
        first = copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)


def test_live_mysql_sqlite_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_occ_{tag}"
    dest = "dst_t"
    try:
        _seed_mysql_from_sqlite(src_db, "src_t", src, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute('CREATE TABLE "dst_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
        conn.executemany(
            'INSERT INTO "dst_t" (id, label) VALUES (?, ?)',
            [(1, "g"), (2, "g")],
        )
        conn.commit()
        conn.close()
        assert _dest_count(dest_db, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
            copy_mysql_to_sqlite(
                source_cfg=_mysql_cfg(),
                source_table=src,
                dest_cfg=_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest_db, dest) == 2
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)


def test_live_mysql_sqlite_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_ow_{tag}"
    dest = "dst_t"
    try:
        _seed_mysql_from_sqlite(src_db, "src_t", src, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute('CREATE TABLE "dst_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
        conn.execute('INSERT INTO "dst_t" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_mysql_to_sqlite(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)


def test_live_mysql_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_MYSQL_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mysql_sqlite_str_{tag}"
    dest = "dst_t"
    try:
        _seed_mysql_from_sqlite(src_db, "src_t", src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mysql-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_cfg(dest_db, dest), "format": "sqlite"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_mysql_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mysql(src)
        _drop_table(dest_db, dest)
