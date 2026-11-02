"""GCS → GCS copy_blob / rewrite — dest artifact COUNT."""

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
from services.copy_gcs_common import gcs_family_name, gcs_type_is_copy_safe  # noqa: E402
from services.copy_gcs_gcs import copy_gcs_to_gcs, gcs_gcs_copy_enabled  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _gcs_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 4443), timeout=1):
            pass
    except OSError:
        pytest.skip("fake-gcs 4443 not reachable")


def _gcs_cfg(bucket: str, key: str) -> dict:
    return {
        "type": "gcs",
        "format": "gcs",
        "host": "127.0.0.1",
        "port": 4443,
        "database": bucket,
        "table": key,
        "connection_string": "http://127.0.0.1:4443",
        "ssl": False,
    }


def _gcs_client():
    _gcs_or_skip()
    pytest.importorskip("google.cloud.storage")
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    return storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://127.0.0.1:4443"),
    )


def _ensure_bucket(client, bucket: str) -> None:
    handle = client.bucket(bucket)
    try:
        if handle.exists():
            return
    except Exception:
        pass
    client.create_bucket(bucket)


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("gcs", _gcs_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _seed_jsonl(client, bucket: str, key: str, rows: int) -> None:
    _ensure_bucket(client, bucket)
    body = "\n".join(
        json.dumps({"id": i, "label": f"r{i}"}, separators=(",", ":"))
        for i in range(1, rows + 1)
    ) + "\n"
    client.bucket(bucket).blob(key).upload_from_string(body.encode("utf-8"))


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.bucket(bucket).blob(key).delete()
    except Exception:
        return


def test_gcs_family_and_copy_safe_exts():
    assert gcs_family_name("google_cloud_storage") == "gcs"
    assert gcs_family_name("gcs") == "gcs"
    assert gcs_type_is_copy_safe("clone.jsonl") is True
    assert gcs_type_is_copy_safe("export.csv") is True
    assert gcs_type_is_copy_safe("data.parquet") is True
    assert gcs_type_is_copy_safe("blob.bin") is False


def test_gcs_gcs_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_GCS_GCS_COPY", "0")
    assert gcs_gcs_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_gcs_to_gcs(
            source_cfg=_gcs_cfg("missing", "a.jsonl"),
            source_table="a.jsonl",
            dest_cfg=_gcs_cfg("missing", "b.jsonl"),
            dest_table="b.jsonl",
            pairs=[("id", "id")],
            gcs_ddls=["long"],
            replace_destination=True,
        )


def test_gcs_gcs_same_object_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_GCS_GCS_COPY", raising=False)
    cfg = _gcs_cfg("same-bucket", "same.jsonl")
    with pytest.raises(FastPathUnavailable, match="same object"):
        copy_gcs_to_gcs(
            source_cfg=cfg,
            source_table="same.jsonl",
            dest_cfg=cfg,
            dest_table="same.jsonl",
            pairs=[("id", "id")],
            gcs_ddls=["long"],
            replace_destination=True,
        )


def test_gcs_gcs_public_proxy_declines():
    dest = {
        **_gcs_cfg("missing", "b.jsonl"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:4443",
        "connection_string": "https://caboose.proxy.rlwy.net:4443",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_gcs_to_gcs(
            source_cfg=_gcs_cfg("missing", "a.jsonl"),
            source_table="a.jsonl",
            dest_cfg=dest,
            dest_table="b.jsonl",
            pairs=[("id", "id")],
            gcs_ddls=["long"],
            replace_destination=True,
        )


def test_gcs_gcs_bin_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_GCS_GCS_COPY", raising=False)
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.bin"
    dest = f"dst_{tag}.bin"
    try:
        _ensure_bucket(client, bucket)
        client.bucket(bucket).blob(src).upload_from_string(b"\x00\x01")
        with pytest.raises(FastPathUnavailable, match="COPY-safe"):
            copy_gcs_to_gcs(
                source_cfg=_gcs_cfg(bucket, src),
                source_table=src,
                dest_cfg=_gcs_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id")],
                gcs_ddls=["bytes"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_GCS_GCS_COPY", raising=False)
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _delete_key(client, bucket, dest)
        result = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("gcs_read") == "copy_blob"
        assert result.source_snapshot.get("gcs_write") == "insert"
        assert _dest_count(bucket, dest) == 800
        assert _dest_count(bucket, src) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_copy_is_not_put(monkeypatch):
    monkeypatch.delenv("DATAFLOW_GCS_GCS_COPY", raising=False)
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    _seed_jsonl(client, bucket, src, 80)
    _delete_key(client, bucket, dest)
    from google.cloud.storage.blob import Blob

    orig_upload_string = Blob.upload_from_string
    orig_upload_file = Blob.upload_from_file
    orig_upload_filename = Blob.upload_from_filename

    def _no_put(self, *a, **k):
        raise AssertionError("GCS→GCS COPY must not PUT dest bytes")

    monkeypatch.setattr(Blob, "upload_from_string", _no_put)
    monkeypatch.setattr(Blob, "upload_from_file", _no_put)
    monkeypatch.setattr(Blob, "upload_from_filename", _no_put)
    try:
        result = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("gcs_read") == "copy_blob"
        assert _dest_count(bucket, dest) == 80
    finally:
        monkeypatch.setattr(Blob, "upload_from_string", orig_upload_string)
        monkeypatch.setattr(Blob, "upload_from_file", orig_upload_file)
        monkeypatch.setattr(Blob, "upload_from_filename", orig_upload_filename)
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_empty_string_and_null_preserved():
    client = _gcs_client()
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
        client.bucket(bucket).blob(src).upload_from_string(body.encode("utf-8"))
        result = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(bucket, dest) == 3
        dest_body = client.bucket(bucket).blob(dest).download_as_bytes().decode("utf-8")
        assert dest_body == body
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_skip_when_dest_count_matches():
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _delete_key(client, bucket, dest)
        first = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_occupied_mismatch_declines():
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _seed_jsonl(client, bucket, dest, 2)
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied GCS dest"):
            copy_gcs_to_gcs(
                source_cfg=_gcs_cfg(bucket, src),
                source_table=src,
                dest_cfg=_gcs_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                gcs_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_overwrite_replaces_dest():
    client = _gcs_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, bucket, src, 800)
        _seed_jsonl(client, bucket, dest, 1)
        result = copy_gcs_to_gcs(
            source_cfg=_gcs_cfg(bucket, src),
            source_table=src,
            dest_cfg=_gcs_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            gcs_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("gcs_write") == "overwrite"
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)


def test_live_gcs_gcs_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_GCS_GCS_COPY", raising=False)
    client = _gcs_client()
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
        job_id = f"gcs-gcs-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_gcs_cfg(bucket, src), "format": "gcs"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_gcs_cfg(bucket, dest), "format": "gcs"}
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
        assert summary.get("load_method") == "copy_blob_gcs_gcs"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("GCS" in line or "copy_blob" in line for line in ddl_log)
        assert _dest_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, src)
        _delete_key(client, bucket, dest)
