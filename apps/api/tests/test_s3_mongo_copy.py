"""S3 CSV GET → MongoDB insert_many — dest count_documents."""

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
from services.copy_mongo_s3 import copy_mongo_to_s3  # noqa: E402
from services.copy_s3_mongo import copy_s3_to_mongo, s3_mongo_copy_enabled  # noqa: E402
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


def _dest_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _seed_s3_csv_from_mongo(src: str, bucket: str, key: str, rows: int) -> None:
    _seed_mongo(src, rows)
    result = copy_mongo_to_s3(
        source_cfg=_mongo_cfg(src),
        source_table=src,
        dest_cfg=_s3_cfg(bucket, key),
        dest_table=key,
        pairs=[("id", "id"), ("label", "label")],
        s3_ddls=["BIGINT", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_s3_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_S3_MONGO_COPY", "0")
    assert s3_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_mongo(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_s3_mongo_jsonl_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MONGO_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    key = f"src_{tag}.jsonl"
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=key, Body=b'{"id":1}\n')
        with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
            copy_s3_to_mongo(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_mongo_cfg("nope"),
                dest_table="nope",
                pairs=[("id", "id")],
                mongo_ddls=["BIGINT"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_mongo_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MONGO_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_src_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_dst_{tag}"
    try:
        _seed_s3_csv_from_mongo(mid, bucket, key, 800)
        _drop_mongo(dest)
        result = copy_s3_to_mongo(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(mid)
        _drop_mongo(dest)
        _delete_key(client, bucket, key)


def test_live_s3_mongo_empty_string_and_null_preserved():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_null_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_null_dst_{tag}"
    mongo, coll = _mongo_coll(mid)
    try:
        coll.drop()
        coll.insert_many(
            [
                {"id": 1, "label": None},
                {"id": 2, "label": ""},
                {"id": 3, "label": "x"},
            ]
        )
        copy_mongo_to_s3(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_s3_cfg(bucket, key),
            dest_table=key,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        _drop_mongo(dest)
        result = copy_s3_to_mongo(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        dest_client, dest_coll = _mongo_coll(dest)
        try:
            rows = list(dest_coll.find({}, {"_id": 0}).sort("id", 1))
        finally:
            dest_client.close()
        assert rows[0] == {"id": 1, "label": None}
        assert rows[1] == {"id": 2, "label": ""}
        assert rows[2] == {"id": 3, "label": "x"}
    finally:
        coll.drop()
        mongo.close()
        _drop_mongo(dest)
        _delete_key(client, bucket, key)


def test_live_s3_mongo_skip_when_dest_count_matches():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_skip_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_skip_dst_{tag}"
    try:
        _seed_s3_csv_from_mongo(mid, bucket, key, 800)
        _drop_mongo(dest)
        first = copy_s3_to_mongo(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_mongo(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(mid)
        _drop_mongo(dest)
        _delete_key(client, bucket, key)


def test_live_s3_mongo_occupied_mismatch_declines():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_occ_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_occ_dst_{tag}"
    try:
        _seed_s3_csv_from_mongo(mid, bucket, key, 800)
        dest_client, dest_coll = _mongo_coll(dest)
        try:
            dest_coll.drop()
            dest_coll.insert_many([{"id": 1, "label": "g"}, {"id": 2, "label": "g"}])
        finally:
            dest_client.close()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Mongo dest"):
            copy_s3_to_mongo(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop_mongo(mid)
        _drop_mongo(dest)
        _delete_key(client, bucket, key)


def test_live_s3_mongo_overwrite_replaces_dest():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_ow_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_ow_dst_{tag}"
    try:
        _seed_s3_csv_from_mongo(mid, bucket, key, 800)
        dest_client, dest_coll = _mongo_coll(dest)
        try:
            dest_coll.drop()
            dest_coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            dest_client.close()
        result = copy_s3_to_mongo(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(mid)
        _drop_mongo(dest)
        _delete_key(client, bucket, key)


def test_live_s3_mongo_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MONGO_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mongo_str_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mongo_str_dst_{tag}"
    try:
        _seed_s3_csv_from_mongo(mid, bucket, key, 800)
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, key), "format": "s3"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
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
        assert summary.get("load_method") == "get_csv_s3_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(mid)
        _drop_mongo(dest)
        _delete_key(client, bucket, key)
