"""Mongo snapshot find → SQLite executemany — dest COUNT(*)."""

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
from services.copy_mongo_sqlite import (  # noqa: E402
    copy_mongo_to_sqlite,
    mongo_sqlite_copy_enabled,
    python_to_sqlite,
)
from services.copy_sqlite_mongo import copy_sqlite_to_mongo  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
    }


def _mongo_coll(name: str):
    _mongo_or_skip()
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(
        "mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000
    )
    try:
        client.admin.command("ping")
    except Exception as exc:
        client.close()
        pytest.skip(f"MongoDB ping failed: {exc}")
    return client, client["dataflow"][name]


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


def _drop_mongo(name: str) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
    finally:
        client.close()


def _dest_count(path: Path | str, table: str) -> int:
    n = destination_row_count("sqlite", _cfg(path, table), schema="", table_name=table)
    assert n is not None
    return int(n)


def _seed_mongo_from_sqlite(src_db: Path, table: str, collection: str, rows: int) -> None:
    _seed_sqlite(src_db, table, rows)
    result = copy_sqlite_to_mongo(
        source_cfg=_cfg(src_db, table),
        source_table=table,
        dest_cfg=_mongo_cfg(collection),
        dest_table=collection,
        pairs=[("id", "id"), ("label", "label")],
        mongo_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_mongo_sqlite_python_date_to_iso_text():
    assert python_to_sqlite(None) is None
    assert python_to_sqlite("x") == "x"
    assert python_to_sqlite("") == ""
    assert python_to_sqlite(date(2020, 1, 2)) == "2020-01-02"
    assert python_to_sqlite(datetime(2020, 1, 2)) == "2020-01-02"
    with pytest.raises(FastPathUnavailable, match="time component"):
        python_to_sqlite(datetime(2020, 1, 2, 12, 0))
    with pytest.raises(FastPathUnavailable, match="binary"):
        python_to_sqlite(b"x")


def test_mongo_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_MONGO_SQLITE_COPY", "0")
    assert mongo_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_cfg(tmp_path / "dst.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_mongo_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_cfg(":memory:", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_mongo_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_MONGO_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_src_{tag}"
    dest = "dst_t"
    try:
        _seed_mongo_from_sqlite(src_db, "src_t", src, 800)
        result = copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mongo(src)
        _drop_table(dest_db, dest)


def test_live_mongo_sqlite_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_null_{tag}"
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
        copy_sqlite_to_mongo(
            source_cfg=_cfg(src_db, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(src),
            dest_table=src,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        result = copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg(src),
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
        _drop_mongo(src)
        _drop_table(dest_db, dest)


def test_live_mongo_sqlite_nested_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_nest_{tag}"
    mongo, coll = _mongo_coll(src)
    try:
        coll.drop()
        coll.insert_one({"id": 1, "label": {"nested": True}})
        with pytest.raises(FastPathUnavailable, match="nested"):
            copy_mongo_to_sqlite(
                source_cfg=_mongo_cfg(src),
                source_table=src,
                dest_cfg=_cfg(dest_db, "dst_t"),
                dest_table="dst_t",
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=True,
            )
    finally:
        coll.drop()
        mongo.close()


def test_live_mongo_sqlite_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_skip_{tag}"
    dest = "dst_t"
    try:
        _seed_mongo_from_sqlite(src_db, "src_t", src, 800)
        first = copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg(src),
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
        _drop_mongo(src)
        _drop_table(dest_db, dest)


def test_live_mongo_sqlite_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_occ_{tag}"
    dest = "dst_t"
    try:
        _seed_mongo_from_sqlite(src_db, "src_t", src, 800)
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
            copy_mongo_to_sqlite(
                source_cfg=_mongo_cfg(src),
                source_table=src,
                dest_cfg=_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest_db, dest) == 2
    finally:
        _drop_mongo(src)
        _drop_table(dest_db, dest)


def test_live_mongo_sqlite_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_ow_{tag}"
    dest = "dst_t"
    try:
        _seed_mongo_from_sqlite(src_db, "src_t", src, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute('CREATE TABLE "dst_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
        conn.execute('INSERT INTO "dst_t" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_mongo_to_sqlite(
            source_cfg=_mongo_cfg(src),
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
        _drop_mongo(src)
        _drop_table(dest_db, dest)


def test_live_mongo_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_MONGO_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    src = f"mongo_sqlite_str_{tag}"
    dest = "dst_t"
    try:
        _seed_mongo_from_sqlite(src_db, "src_t", src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(src), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_cfg(dest_db, dest), "format": "sqlite"}
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
        assert summary.get("load_method") == "snapshot_find_mongo_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest_db, dest) == 800
    finally:
        _drop_mongo(src)
        _drop_table(dest_db, dest)
