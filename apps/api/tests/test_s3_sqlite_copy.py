"""S3 CSV GET → SQLite executemany — dest COUNT(*)."""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_s3_sqlite import copy_s3_to_sqlite, s3_sqlite_copy_enabled  # noqa: E402
from services.copy_sqlite_s3 import copy_sqlite_to_s3  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _s3_cfg(bucket: str, key: str) -> dict:
    return {
        "type": "s3",
        "format": "s3",
        "host": "127.0.0.1",
        "port": 9000,
        "database": bucket,
        "table": key,
        "username": "dataflow",
        "password": "dataflowsecret",
        "ssl": False,
        "path_style": True,
    }


def _s3_client():
    _minio_or_skip()
    boto3 = pytest.importorskip("boto3")
    return boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )


def _ensure_bucket(client, bucket: str) -> None:
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


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


def _dest_count(path: Path | str, table: str) -> int:
    n = destination_row_count("sqlite", _cfg(path, table), schema="", table_name=table)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _seed_s3_csv_from_sqlite(src_db: Path, table: str, bucket: str, key: str, rows: int) -> None:
    _seed_sqlite(src_db, table, rows)
    result = copy_sqlite_to_s3(
        source_cfg=_cfg(src_db, table),
        source_table=table,
        dest_cfg=_s3_cfg(bucket, key),
        dest_table=key,
        pairs=[("id", "id"), ("label", "label")],
        s3_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_s3_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_S3_SQLITE_COPY", "0")
    assert s3_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_sqlite(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_cfg(tmp_path / "dst.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_s3_sqlite_jsonl_declines(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_S3_SQLITE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    key = f"src_{tag}.jsonl"
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=key, Body=b'{"id":1}\n')
        with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
            copy_s3_to_sqlite(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_cfg(tmp_path / "dst.db", "nope"),
                dest_table="nope",
                pairs=[("id", "id")],
                sqlite_ddls=["INTEGER"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, key)


def test_s3_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_s3_to_sqlite(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_cfg(":memory:", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_s3_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_S3_SQLITE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = "dst_t"
    try:
        _seed_s3_csv_from_sqlite(src_db, "src_t", bucket, key, 800)
        result = copy_s3_to_sqlite(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert _dest_count(dest_db, dest) == 800
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_sqlite_empty_string_and_null_preserved(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
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
        copy_sqlite_to_s3(
            source_cfg=_cfg(src_db, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, key),
            dest_table=key,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        result = copy_s3_to_sqlite(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
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
        _delete_key(client, bucket, key)


def test_live_s3_sqlite_skip_when_dest_count_matches(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = "dst_t"
    try:
        _seed_s3_csv_from_sqlite(src_db, "src_t", bucket, key, 800)
        first = copy_s3_to_sqlite(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_sqlite(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_sqlite_occupied_mismatch_declines(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = "dst_t"
    try:
        _seed_s3_csv_from_sqlite(src_db, "src_t", bucket, key, 800)
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
            copy_s3_to_sqlite(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest_db, dest) == 2
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_sqlite_overwrite_replaces_dest(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = "dst_t"
    try:
        _seed_s3_csv_from_sqlite(src_db, "src_t", bucket, key, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute('CREATE TABLE "dst_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
        conn.execute('INSERT INTO "dst_t" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_s3_to_sqlite(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _dest_count(dest_db, dest) == 800
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_S3_SQLITE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    dest_db = tmp_path / "dst.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = "dst_t"
    try:
        _seed_s3_csv_from_sqlite(src_db, "src_t", bucket, key, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, key), "format": "s3"}
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
        assert summary.get("load_method") == "get_csv_s3_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest_db, dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_table(dest_db, dest)
