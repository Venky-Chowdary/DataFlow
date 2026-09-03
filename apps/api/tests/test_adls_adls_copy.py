"""ADLS → ADLS start_copy_from_url — dest artifact COUNT."""

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

from services.copy_adls_adls import adls_adls_copy_enabled, copy_adls_to_adls  # noqa: E402
from services.copy_adls_common import adls_family_name, adls_type_is_copy_safe  # noqa: E402
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402

_AZURITE_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw=="
)


def _adls_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 10000), timeout=1):
            pass
    except OSError:
        pytest.skip("Azurite 10000 not reachable")


def _adls_cfg(container: str, key: str) -> dict:
    return {
        "type": "adls",
        "format": "adls",
        "host": "127.0.0.1",
        "port": 10000,
        "database": container,
        "table": key,
        "username": "devstoreaccount1",
        "password": _AZURITE_KEY,
        "ssl": False,
    }


def _adls_client():
    _adls_or_skip()
    pytest.importorskip("azure.storage.blob")
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient(
        account_url="http://127.0.0.1:10000/devstoreaccount1",
        credential=_AZURITE_KEY,
    )


def _ensure_container(client, name: str) -> None:
    handle = client.get_container_client(name)
    try:
        if handle.exists():
            return
    except Exception:
        pass
    handle.create_container()


def _dest_count(container: str, key: str) -> int:
    n = destination_row_count(
        "adls", _adls_cfg(container, key), schema="", table_name=key
    )
    assert n is not None
    return int(n)


def _seed_jsonl(client, container: str, key: str, rows: int) -> None:
    _ensure_container(client, container)
    body = "\n".join(
        json.dumps({"id": i, "label": f"r{i}"}, separators=(",", ":"))
        for i in range(1, rows + 1)
    ) + "\n"
    client.get_blob_client(container, key).upload_blob(body.encode("utf-8"), overwrite=True)


def _delete_key(client, container: str, key: str) -> None:
    try:
        client.get_blob_client(container, key).delete_blob()
    except Exception:
        return


def test_adls_family_and_copy_safe_exts():
    assert adls_family_name("azure_blob_storage") == "adls"
    assert adls_family_name("azure_data_lake") == "adls"
    assert adls_family_name("adls") == "adls"
    assert adls_type_is_copy_safe("clone.jsonl") is True
    assert adls_type_is_copy_safe("export.csv") is True
    assert adls_type_is_copy_safe("data.parquet") is True
    assert adls_type_is_copy_safe("blob.bin") is False


def test_adls_adls_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ADLS_ADLS_COPY", "0")
    assert adls_adls_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_adls_to_adls(
            source_cfg=_adls_cfg("missing", "a.jsonl"),
            source_table="a.jsonl",
            dest_cfg=_adls_cfg("missing", "b.jsonl"),
            dest_table="b.jsonl",
            pairs=[("id", "id")],
            adls_ddls=["long"],
            replace_destination=True,
        )


def test_adls_adls_same_object_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADLS_ADLS_COPY", raising=False)
    cfg = _adls_cfg("samecontainer", "same.jsonl")
    with pytest.raises(FastPathUnavailable, match="same object"):
        copy_adls_to_adls(
            source_cfg=cfg,
            source_table="same.jsonl",
            dest_cfg=cfg,
            dest_table="same.jsonl",
            pairs=[("id", "id")],
            adls_ddls=["long"],
            replace_destination=True,
        )


def test_adls_adls_public_proxy_declines():
    dest = {
        **_adls_cfg("missing", "b.jsonl"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:10000",
        "connection_string": "https://caboose.proxy.rlwy.net:10000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_adls_to_adls(
            source_cfg=_adls_cfg("missing", "a.jsonl"),
            source_table="a.jsonl",
            dest_cfg=dest,
            dest_table="b.jsonl",
            pairs=[("id", "id")],
            adls_ddls=["long"],
            replace_destination=True,
        )


def test_adls_adls_bin_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADLS_ADLS_COPY", raising=False)
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.bin"
    dest = f"dst_{tag}.bin"
    try:
        _ensure_container(client, container)
        client.get_blob_client(container, src).upload_blob(b"\x00\x01", overwrite=True)
        with pytest.raises(FastPathUnavailable, match="COPY-safe"):
            copy_adls_to_adls(
                source_cfg=_adls_cfg(container, src),
                source_table=src,
                dest_cfg=_adls_cfg(container, dest),
                dest_table=dest,
                pairs=[("id", "id")],
                adls_ddls=["bytes"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADLS_ADLS_COPY", raising=False)
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, container, src, 800)
        _delete_key(client, container, dest)
        result = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("adls_read") == "start_copy_from_url"
        assert result.source_snapshot.get("adls_write") == "insert"
        assert _dest_count(container, dest) == 800
        assert _dest_count(container, src) == 800
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_copy_is_not_put(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADLS_ADLS_COPY", raising=False)
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    _seed_jsonl(client, container, src, 80)
    _delete_key(client, container, dest)
    from azure.storage.blob import BlobClient

    orig_upload = BlobClient.upload_blob

    def _no_put(self, *a, **k):
        raise AssertionError("ADLS→ADLS COPY must not PUT dest bytes")

    monkeypatch.setattr(BlobClient, "upload_blob", _no_put)
    try:
        result = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("adls_read") == "start_copy_from_url"
        assert _dest_count(container, dest) == 80
    finally:
        monkeypatch.setattr(BlobClient, "upload_blob", orig_upload)
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_empty_string_and_null_preserved():
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _ensure_container(client, container)
        body = (
            json.dumps({"id": 1, "label": None}) + "\n"
            + json.dumps({"id": 2, "label": ""}) + "\n"
            + json.dumps({"id": 3, "label": "x"}) + "\n"
        )
        client.get_blob_client(container, src).upload_blob(
            body.encode("utf-8"), overwrite=True
        )
        result = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(container, dest) == 3
        dest_body = (
            client.get_blob_client(container, dest)
            .download_blob()
            .readall()
            .decode("utf-8")
        )
        assert dest_body == body
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_skip_when_dest_count_matches():
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, container, src, 800)
        _delete_key(client, container, dest)
        first = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(container, dest) == 800
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_occupied_mismatch_declines():
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, container, src, 800)
        _seed_jsonl(client, container, dest, 2)
        assert _dest_count(container, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied ADLS dest"):
            copy_adls_to_adls(
                source_cfg=_adls_cfg(container, src),
                source_table=src,
                dest_cfg=_adls_cfg(container, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                adls_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(container, dest) == 2
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_overwrite_replaces_dest():
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, container, src, 800)
        _seed_jsonl(client, container, dest, 1)
        result = copy_adls_to_adls(
            source_cfg=_adls_cfg(container, src),
            source_table=src,
            dest_cfg=_adls_cfg(container, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            adls_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("adls_write") == "overwrite"
        assert _dest_count(container, dest) == 800
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)


def test_live_adls_adls_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADLS_ADLS_COPY", raising=False)
    client = _adls_client()
    tag = uuid.uuid4().hex[:8]
    container = f"dfc{tag}"
    src = f"src_{tag}.jsonl"
    dest = f"dst_{tag}.jsonl"
    try:
        _seed_jsonl(client, container, src, 800)
        _delete_key(client, container, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"adls-adls-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_adls_cfg(container, src), "format": "adls"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_adls_cfg(container, dest), "format": "adls"}
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
        assert summary.get("load_method") == "start_copy_from_url_adls_adls"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("ADLS" in line or "start_copy_from_url" in line for line in ddl_log)
        assert _dest_count(container, dest) == 800
    finally:
        _delete_key(client, container, src)
        _delete_key(client, container, dest)
