"""MongoDB snapshot find CSV → S3 upload — dest artifact COUNT."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_mongo_s3 import (  # noqa: E402
    copy_mongo_to_s3,
    mongo_s3_copy_enabled,
    mongo_s3_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
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


def _seed_mongo(name: str, rows: int) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
        coll.insert_many(
            [{"id": i, "label": f"r{i}"} for i in range(1, int(rows) + 1)],
            ordered=False,
        )
    finally:
        client.close()


def _drop_mongo(name: str) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
    finally:
        client.close()


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("s3", _s3_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def test_mongo_s3_copy_safe_types():
    assert mongo_s3_type_is_copy_safe("string") is True
    assert mongo_s3_type_is_copy_safe("long") is True
    assert mongo_s3_type_is_copy_safe("BIGINT") is True
    assert mongo_s3_type_is_copy_safe("DATE") is True
    assert mongo_s3_type_is_copy_safe("object") is False
    assert mongo_s3_type_is_copy_safe("array") is False
    assert mongo_s3_type_is_copy_safe("bindata") is False
    assert mongo_s3_type_is_copy_safe("timestamptz") is False


def test_mongo_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_S3_COPY", "0")
    assert mongo_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_s3(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.csv"),
            dest_table="nope.csv",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_mongo_s3_json_key_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MONGO_S3_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_mongo_to_s3(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.json"),
            dest_table="nope.json",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mongo_s3_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MONGO_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_src_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        _seed_mongo(src, 800)
        result = copy_mongo_to_s3(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("s3_write") == "insert"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert _dest_count(bucket, dest) == 800
    finally:
        _drop_mongo(src)
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_empty_string_and_null_preserved():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_null_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    mongo, coll = _mongo_coll(src)
    try:
        coll.drop()
        coll.insert_many(
            [
                {"id": 1, "label": None},
                {"id": 2, "label": ""},
                {"id": 3, "label": "x"},
            ]
        )
        result = copy_mongo_to_s3(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(bucket, dest) == 3
        body = client.get_object(Bucket=bucket, Key=dest)["Body"].read().decode("utf-8")
        assert "\\N" in body
        assert '""' in body
    finally:
        coll.drop()
        mongo.close()
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_nested_declines():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_nest_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    mongo, coll = _mongo_coll(src)
    try:
        coll.drop()
        coll.insert_one({"id": 1, "label": {"nested": True}})
        with pytest.raises(FastPathUnavailable, match="nested"):
            copy_mongo_to_s3(
                source_cfg=_mongo_cfg(src),
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["BIGINT", "TEXT"],
                replace_destination=True,
            )
    finally:
        coll.drop()
        mongo.close()
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_skip_when_dest_count_matches():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_skip_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        _seed_mongo(src, 800)
        first = copy_mongo_to_s3(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_s3(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(bucket, dest) == 800
    finally:
        _drop_mongo(src)
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_occupied_mismatch_declines():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_occ_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        _seed_mongo(src, 800)
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,g\n2,g\n")
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_mongo_to_s3(
                source_cfg=_mongo_cfg(src),
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        _drop_mongo(src)
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_overwrite_replaces_dest():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_ow_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        _seed_mongo(src, 800)
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,ghost\n")
        result = copy_mongo_to_s3(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("s3_write") == "overwrite"
        assert _dest_count(bucket, dest) == 800
    finally:
        _drop_mongo(src)
        _delete_key(client, bucket, dest)


def test_live_mongo_s3_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MONGO_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_s3_str_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        _seed_mongo(src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(src), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, dest), "format": "s3"}
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
        assert summary.get("load_method") == "snapshot_find_mongo_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(bucket, dest) == 800
    finally:
        _drop_mongo(src)
        _delete_key(client, bucket, dest)
