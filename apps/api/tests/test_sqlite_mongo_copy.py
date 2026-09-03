"""SQLite SELECT fetchmany → Mongo insert_many — dest count_documents."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlite_mongo import (  # noqa: E402
    copy_sqlite_to_mongo,
    sqlite_mongo_copy_enabled,
    sqlite_mongo_type_is_copy_safe,
    sqlite_value_to_bson,
)
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


def _dest_count(collection: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(collection), schema="", table_name=collection
    )
    assert n is not None
    return int(n)


def _drop_mongo(name: str) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
    finally:
        client.close()


def test_sqlite_mongo_copy_safe_types():
    assert sqlite_mongo_type_is_copy_safe("INTEGER") is True
    assert sqlite_mongo_type_is_copy_safe("TEXT") is True
    assert sqlite_mongo_type_is_copy_safe("DATE") is True
    assert sqlite_mongo_type_is_copy_safe("REAL") is True
    assert sqlite_mongo_type_is_copy_safe("BOOLEAN") is True
    assert sqlite_mongo_type_is_copy_safe("DATETIME") is False
    assert sqlite_mongo_type_is_copy_safe("TIMESTAMP") is False
    assert sqlite_mongo_type_is_copy_safe("TIMESTAMPTZ") is False
    assert sqlite_mongo_type_is_copy_safe("BLOB") is False


def test_sqlite_mongo_date_iso_to_bson_midnight():
    assert sqlite_value_to_bson("2020-01-02", "DATE") == datetime(
        2020, 1, 2, tzinfo=timezone.utc
    )
    assert sqlite_value_to_bson(date(2020, 1, 2), "DATE") == datetime(
        2020, 1, 2, tzinfo=timezone.utc
    )
    assert sqlite_value_to_bson("2020-01-02", "TEXT") == "2020-01-02"
    assert sqlite_value_to_bson(None, "DATE") is None
    with pytest.raises(FastPathUnavailable, match="not ISO"):
        sqlite_value_to_bson("not-a-date", "DATE")
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_value_to_bson("2020-01-02 12:00:00", "DATETIME")
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        sqlite_value_to_bson(b"x", "TEXT")


def test_sqlite_mongo_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_MONGO_COPY", "0")
    assert sqlite_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_mongo(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_mongo_cfg("missing"),
            dest_table="missing",
            pairs=[("id", "id")],
            mongo_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlite_mongo_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_mongo(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_mongo_cfg("missing"),
            dest_table="missing",
            pairs=[("id", "id")],
            mongo_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_sqlite_mongo_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_MONGO_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_dst_{tag}"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_mongo(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(dest)


def test_live_sqlite_mongo_empty_string_and_null_preserved(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_null_{tag}"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    mongo, coll = _mongo_coll(dest)
    try:
        result = copy_sqlite_to_mongo(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        docs = {int(d["id"]): d.get("label") for d in coll.find({}, {"_id": 0})}
        assert docs[1] is None
        assert docs[2] == ""
        assert docs[3] == "x"
    finally:
        coll.drop()
        mongo.close()


def test_live_sqlite_mongo_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_skip_{tag}"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_mongo(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_mongo(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(dest)


def test_live_sqlite_mongo_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_occ_{tag}"
    _seed(src, "src_t", 800)
    mongo, coll = _mongo_coll(dest)
    try:
        coll.drop()
        coll.insert_many([{"id": 1, "label": "g"}, {"id": 2, "label": "g"}])
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Mongo dest"):
            copy_sqlite_to_mongo(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        coll.drop()
        mongo.close()


def test_live_sqlite_mongo_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_ow_{tag}"
    _seed(src, "src_t", 800)
    mongo, coll = _mongo_coll(dest)
    try:
        coll.drop()
        coll.insert_one({"id": 1, "label": "ghost"})
        result = copy_sqlite_to_mongo(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        coll.drop()
        mongo.close()


def test_live_sqlite_mongo_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_MONGO_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_mongo_str_{tag}"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
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
        assert summary.get("load_method") == "select_sqlite_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(dest)
