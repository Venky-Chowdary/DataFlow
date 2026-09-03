"""SQLite SELECT CSV → S3 upload — dest artifact COUNT."""

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
from services.copy_s3_common import (  # noqa: E402
    s3_csv_cell,
    s3_format_delimited_row,
    s3_parse_delimited_cell,
)
from services.copy_sqlite_s3 import (  # noqa: E402
    copy_sqlite_to_s3,
    sqlite_s3_copy_enabled,
    sqlite_s3_type_is_copy_safe,
)
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


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("s3", _s3_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def test_sqlite_s3_csv_wire():
    assert s3_csv_cell(None) == "\\N"
    assert s3_csv_cell("") == ""
    assert s3_parse_delimited_cell("\\N") is None
    assert s3_parse_delimited_cell("") == ""
    assert s3_format_delimited_row(["id", "\\N", ""], ",") == 'id,\\N,""'


def test_sqlite_s3_copy_safe_types():
    assert sqlite_s3_type_is_copy_safe("INTEGER") is True
    assert sqlite_s3_type_is_copy_safe("TEXT") is True
    assert sqlite_s3_type_is_copy_safe("DATE") is True
    assert sqlite_s3_type_is_copy_safe("REAL") is True
    assert sqlite_s3_type_is_copy_safe("BLOB") is False


def test_sqlite_s3_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_S3_COPY", "0")
    assert sqlite_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_s3(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.csv"),
            dest_table="nope.csv",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlite_s3_json_key_declines(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_S3_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_sqlite_to_s3(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.json"),
            dest_table="nope.json",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlite_s3_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_s3(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_s3_cfg("missing", "nope.csv"),
            dest_table="nope.csv",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_sqlite_s3_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_s3(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("s3_write") == "insert"
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, dest)


def test_live_sqlite_s3_empty_string_and_null_preserved(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        result = copy_sqlite_to_s3(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(bucket, dest) == 3
        body = client.get_object(Bucket=bucket, Key=dest)["Body"].read().decode("utf-8")
        assert "\\N" in body
        assert '""' in body
    finally:
        _delete_key(client, bucket, dest)


def test_live_sqlite_s3_skip_when_dest_count_matches(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_s3(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_s3(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, dest)


def test_live_sqlite_s3_occupied_mismatch_declines(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    _seed(src, "src_t", 800)
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,g\n2,g\n")
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_sqlite_to_s3(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        _delete_key(client, bucket, dest)


def test_live_sqlite_s3_overwrite_replaces_dest(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    _seed(src, "src_t", 800)
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,ghost\n")
        result = copy_sqlite_to_s3(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("s3_write") == "overwrite"
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, dest)


def test_live_sqlite_s3_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, dest), "format": "s3"}
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
        assert summary.get("load_method") == "select_sqlite_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, dest)
