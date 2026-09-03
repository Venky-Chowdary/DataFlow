"""S3 → S3 CopyObject — dest artifact COUNT."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_s3_common import s3_family_name, s3_type_is_copy_safe  # noqa: E402
from services.copy_s3_s3 import copy_s3_to_s3, s3_s3_copy_enabled  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


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


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("s3", _s3_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _seed_jsonl(client, bucket: str, key: str, rows: int) -> None:
    _ensure_bucket(client, bucket)
    body = "\n".join(
        json.dumps({"id": i, "label": f"r{i}"}, separators=(",", ":"))
        for i in range(1, rows + 1)
    ) + "\n"
    client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def test_s3_family_and_copy_safe_exts():
    assert s3_family_name("minio") == "s3"
    assert s3_family_name("amazon_s3") == "s3"
    assert s3_family_name("s3") == "s3"
    assert s3_type_is_copy_safe("clone.jsonl") is True
    assert s3_type_is_copy_safe("export.csv") is True
    assert s3_type_is_copy_safe("data.parquet") is True
    assert s3_type_is_copy_safe("blob.bin") is False


def test_s3_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_S3_S3_COPY", "0")
    assert s3_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_s3(
            source_cfg=_s3_cfg("missing", "a.jsonl"),
            source_table="a.jsonl",
            dest_cfg=_s3_cfg("missing", "b.jsonl"),
            dest_table="b.jsonl",
            pairs=[("id", "id")],
            s3_ddls=["long"],
            replace_destination=True,
        )


def test_s3_s3_same_object_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_S3_COPY", raising=False)
    cfg = _s3_cfg("same-bucket", "same.jsonl")
    with pytest.raises(FastPathUnavailable, match="same object"):
        copy_s3_to_s3(
            source_cfg=cfg,
            source_table="same.jsonl",
            dest_cfg=cfg,
            dest_table="same.jsonl",
            pairs=[("id", "id")],
            s3_ddls=["long"],
            replace_destination=True,
        )


def test_live_s3_s3_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _delete_key(client, bucket, dest)
        result = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("s3_read") == "copy_object"
        assert result.source_snapshot.get("s3_write") == "insert"
        assert _dest_count(bucket, dest) == 800
        assert _dest_count(bucket, src) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_copy_is_not_put(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    _seed_jsonl(client, bucket, src, 80)
    _delete_key(client, bucket, dest)
    import services.copy_s3_common as copy_s3_common

    orig = copy_s3_common.boto3_client

    def _wrapped(service, cfg):
        inner = orig(service, cfg)
        if service == "s3":
            def _no_put(*_a, **_k):
                raise AssertionError("S3→S3 COPY must not PUT dest bytes")
            inner.put_object = _no_put
            inner.upload_file = _no_put
            inner.upload_fileobj = _no_put
        return inner

    monkeypatch.setattr(copy_s3_common, "boto3_client", _wrapped)
    try:
        result = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("s3_read") == "copy_object"
        assert _dest_count(bucket, dest) == 80
    finally:
        monkeypatch.setattr(copy_s3_common, "boto3_client", orig)
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_empty_string_and_null_preserved():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _ensure_bucket(client, bucket)
        body = (
            json.dumps({"id": 1, "label": None}) + "\n"
            + json.dumps({"id": 2, "label": ""}) + "\n"
            + json.dumps({"id": 3, "label": "x"}) + "\n"
        )
        client.put_object(Bucket=bucket, Key=src, Body=body.encode("utf-8"))
        result = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(bucket, dest) == 3
        dest_body = client.get_object(Bucket=bucket, Key=dest)["Body"].read().decode("utf-8")
        assert dest_body == body
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_skip_when_dest_count_matches():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _delete_key(client, bucket, dest)
        first = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_occupied_mismatch_declines():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _seed_jsonl(client, bucket, dest, 2)
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_s3_to_s3(
                source_cfg=_s3_cfg(bucket, src),
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_overwrite_replaces_dest():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _seed_jsonl(client, bucket, dest, 1)
        result = copy_s3_to_s3(
            source_cfg=_s3_cfg(bucket, src),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("s3_write") == "overwrite"
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_s3_s3_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _delete_key(client, bucket, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, src), "format": "s3"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, dest), "format": "s3"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_object_s3_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("CopyObject" in line or "S3" in line for line in ddl_log)
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)
