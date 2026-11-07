"""S3 CSV GET → Iceberg snapshot — dest COUNT from file footers."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
from services.copy_s3_iceberg import copy_s3_to_iceberg, s3_iceberg_copy_enabled  # noqa: E402
from services.copy_sqlite_s3 import copy_sqlite_to_s3  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402

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


def _iceberg_count(table: str) -> int:
    n = destination_row_count(
        "iceberg", _iceberg_cfg(table), schema="default", table_name=table
    )
    assert n is not None
    return int(n)


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


def _seed_s3_csv(src_db: Path, table: str, bucket: str, key: str, rows: int) -> None:
    _seed_sqlite(src_db, table, rows)
    result = copy_sqlite_to_s3(
        source_cfg=_sqlite_cfg(src_db, table),
        source_table=table,
        dest_cfg=_s3_cfg(bucket, key),
        dest_table=key,
        pairs=[("id", "id"), ("label", "label")],
        s3_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_s3_iceberg_copy_safe_types():
    assert iceberg_type_is_copy_safe("INTEGER") is True
    assert iceberg_type_is_copy_safe("TEXT") is True
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("DATE") is True
    assert iceberg_type_is_copy_safe("timestamptz") is False
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("list<int>") is False


def test_s3_iceberg_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_S3_ICEBERG_COPY", "0")
    assert s3_iceberg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_iceberg(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_iceberg_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


def test_s3_iceberg_public_proxy_declines():
    dest = _iceberg_cfg("nope")
    src = {
        **_s3_cfg("missing", "a.csv"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:9000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_s3_to_iceberg(
            source_cfg=src,
            source_table="a.csv",
            dest_cfg=dest,
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


def test_s3_iceberg_jsonl_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_ICEBERG_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    key = f"src_{tag}.jsonl"
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=key, Body=b'{"id":1}\n')
        with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
            copy_s3_to_iceberg(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_iceberg_cfg("nope"),
                dest_table="nope",
                pairs=[("id", "id")],
                iceberg_ddls=["long"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, key)


@requires_rest
def test_live_s3_iceberg_dest_count(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_S3_ICEBERG_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ice_dst_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_iceberg(dest)
        result = copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("s3_read") == "get_csv"
        assert result.source_snapshot.get("iceberg_write") == "append"
        assert _iceberg_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_iceberg(dest)


@requires_rest
def test_live_s3_iceberg_empty_string_and_null(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ice_null_{tag}"
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
            source_cfg=_sqlite_cfg(src_db, "src_t"),
            source_table="src_t",
            dest_cfg=_s3_cfg(bucket, key),
            dest_table=key,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        _drop_iceberg(dest)
        result = copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _iceberg_count(dest) == 3
    finally:
        _delete_key(client, bucket, key)
        _drop_iceberg(dest)


@requires_rest
def test_live_s3_iceberg_skip_when_dest_count_matches(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ice_skip_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_iceberg(dest)
        first = copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _iceberg_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_iceberg(dest)


@requires_rest
def test_live_s3_iceberg_occupied_mismatch_declines(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    ghost_key = f"ghost_{tag}.csv"
    dest = f"s3_ice_occ_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _seed_s3_csv(ghost_db, "ghost", bucket, ghost_key, 2)
        _drop_iceberg(dest)
        copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, ghost_key),
            source_table=ghost_key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert _iceberg_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Iceberg dest"):
            copy_s3_to_iceberg(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_iceberg_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                iceberg_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _iceberg_count(dest) == 2
    finally:
        _delete_key(client, bucket, key)
        _delete_key(client, bucket, ghost_key)
        _drop_iceberg(dest)


@requires_rest
def test_live_s3_iceberg_overwrite_replaces_dest(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    ghost_key = f"ghost_{tag}.csv"
    dest = f"s3_ice_ow_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _seed_s3_csv(ghost_db, "ghost", bucket, ghost_key, 1)
        _drop_iceberg(dest)
        copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, ghost_key),
            source_table=ghost_key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        result = copy_s3_to_iceberg(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("iceberg_write") == "overwrite"
        assert _iceberg_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _delete_key(client, bucket, ghost_key)
        _drop_iceberg(dest)


@requires_rest
def test_live_s3_iceberg_stream_load_method(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_S3_ICEBERG_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ice_str_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_iceberg(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-ice-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, key), "format": "s3"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_iceberg_cfg(dest), "format": "iceberg"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "get_csv_s3_iceberg_snapshot"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        assert _iceberg_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_iceberg(dest)
