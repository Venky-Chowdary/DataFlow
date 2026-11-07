"""Iceberg snapshot Parquet → S3 CSV PUT — dest artifact COUNT."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
from services.copy_iceberg_s3 import (  # noqa: E402
    copy_iceberg_to_s3,
    iceberg_s3_copy_enabled,
    iceberg_value_to_s3,
)
from services.copy_s3_common import s3_dest_count  # noqa: E402
from services.copy_s3_iceberg import copy_s3_to_iceberg  # noqa: E402
from services.copy_sqlite_s3 import copy_sqlite_to_s3  # noqa: E402

REST_URI = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "http://127.0.0.1:8181").rstrip("/")
REST_WAREHOUSE = os.environ.get(
    "DATAFLOW_ICEBERG_REST_WAREHOUSE", "file:///tmp/iceberg-rest-wh"
)


def _rest_reachable() -> bool:
    try:
        host = REST_URI.split("://", 1)[-1].split("/", 1)[0]
        hostname, _, port_s = host.partition(":")
        port = int(port_s or "8181")
        with socket.create_connection((hostname, port), timeout=1.5):
            pass
        with urlopen(f"{REST_URI}/v1/config", timeout=2) as resp:
            return int(getattr(resp, "status", 0) or 0) == 200
    except (OSError, URLError, ValueError):
        return False


requires_rest = pytest.mark.skipif(
    not _rest_reachable(),
    reason=f"Iceberg REST catalog not reachable at {REST_URI}",
)


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


def _sqlite_cfg(path: Path | str, table: str) -> dict:
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


def _iceberg_cfg(table: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
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


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _drop_iceberg(table: str) -> None:
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config
    from pyiceberg.exceptions import NoSuchTableError

    cfg = _iceberg_cfg(table)
    try:
        parsed = parse_iceberg_catalog_config(cfg)
        catalog = load_catalog(cfg)
        catalog.drop_table(parsed["namespace"] + (parsed["table_name"],))
    except NoSuchTableError:
        return


def _s3_count(bucket: str, key: str) -> int:
    return int(s3_dest_count(_s3_cfg(bucket, key), key))


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


def _seed_iceberg_from_s3(src_db: Path, bucket: str, key: str, ice: str, rows: int) -> None:
    _seed_sqlite(src_db, "src_t", rows)
    copy_sqlite_to_s3(
        source_cfg=_sqlite_cfg(src_db, "src_t"),
        source_table="src_t",
        dest_cfg=_s3_cfg(bucket, key),
        dest_table=key,
        pairs=[("id", "id"), ("label", "label")],
        s3_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    _drop_iceberg(ice)
    result = copy_s3_to_iceberg(
        source_cfg=_s3_cfg(bucket, key),
        source_table=key,
        dest_cfg=_iceberg_cfg(ice),
        dest_table=ice,
        pairs=[("id", "id"), ("label", "label")],
        iceberg_ddls=["long", "string"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_iceberg_s3_copy_safe_types():
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("date") is True
    assert iceberg_type_is_copy_safe("timestamptz") is False
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("uuid") is False


def test_iceberg_s3_date_bind():
    assert iceberg_value_to_s3(date(2020, 1, 2)) == date(2020, 1, 2)
    assert iceberg_value_to_s3(None) is None
    with pytest.raises(FastPathUnavailable, match="binary"):
        iceberg_value_to_s3(b"x")


def test_iceberg_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ICEBERG_S3_COPY", "0")
    assert iceberg_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "out.csv"),
            dest_table="out.csv",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_iceberg_s3_json_dest_declines():
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "out.json"),
            dest_table="out.json",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_iceberg_s3_public_proxy_declines():
    dest = {
        **_s3_cfg("missing", "out.csv"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:9000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=dest,
            dest_table="out.csv",
            pairs=[("id", "id")],
            s3_ddls=["INTEGER"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_s3_dest_count(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    mid_key = f"mid_{tag}.csv"
    ice = f"ice_s3_mid_{tag}"
    dest = f"ice_s3_dst_{tag}.csv"
    try:
        _seed_iceberg_from_s3(src_db, bucket, mid_key, ice, 800)
        result = copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg(ice),
            source_table=ice,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        assert result.source_snapshot.get("s3_write") == "insert"
        assert _s3_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, mid_key)
        _delete_key(client, bucket, dest)
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_s3_skip_when_dest_count_matches(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    mid_key = f"mid_{tag}.csv"
    ice = f"ice_s3_skip_{tag}"
    dest = f"ice_s3_skip_{tag}.csv"
    try:
        _seed_iceberg_from_s3(src_db, bucket, mid_key, ice, 800)
        first = copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg(ice),
            source_table=ice,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg(ice),
            source_table=ice,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _s3_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, mid_key)
        _delete_key(client, bucket, dest)
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_s3_occupied_mismatch_declines(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    mid_key = f"mid_{tag}.csv"
    ice = f"ice_s3_occ_{tag}"
    dest = f"ice_s3_occ_{tag}.csv"
    try:
        _seed_iceberg_from_s3(src_db, bucket, mid_key, ice, 800)
        _seed_sqlite(ghost_db, "ghost", 2)
        copy_sqlite_to_s3(
            source_cfg=_sqlite_cfg(ghost_db, "ghost"),
            source_table="ghost",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert _s3_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_iceberg_to_s3(
                source_cfg=_iceberg_cfg(ice),
                source_table=ice,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _s3_count(bucket, dest) == 2
    finally:
        _delete_key(client, bucket, mid_key)
        _delete_key(client, bucket, dest)
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_s3_overwrite_replaces_dest(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    mid_key = f"mid_{tag}.csv"
    ice = f"ice_s3_ow_{tag}"
    dest = f"ice_s3_ow_{tag}.csv"
    try:
        _seed_iceberg_from_s3(src_db, bucket, mid_key, ice, 800)
        _seed_sqlite(ghost_db, "ghost", 1)
        copy_sqlite_to_s3(
            source_cfg=_sqlite_cfg(ghost_db, "ghost"),
            source_table="ghost",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        result = copy_iceberg_to_s3(
            source_cfg=_iceberg_cfg(ice),
            source_table=ice,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("s3_write") == "overwrite"
        assert _s3_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, mid_key)
        _delete_key(client, bucket, dest)
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_s3_stream_load_method(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    mid_key = f"mid_{tag}.csv"
    ice = f"ice_s3_str_{tag}"
    dest = f"ice_s3_str_{tag}.csv"
    try:
        _seed_iceberg_from_s3(src_db, bucket, mid_key, ice, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_iceberg_cfg(ice), "format": "iceberg"}
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
        assert summary.get("load_method") == "iceberg_parquet_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("S3" in line for line in ddl_log)
        assert _s3_count(bucket, dest) == 800
    finally:
        _delete_key(client, bucket, mid_key)
        _delete_key(client, bucket, dest)
        _drop_iceberg(ice)
