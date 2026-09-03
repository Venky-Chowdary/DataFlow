"""PostgreSQL COPY CSV → S3 upload — dest artifact COUNT."""

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
from services.copy_pg_s3 import copy_postgres_to_s3, pg_s3_copy_enabled, pg_s3_type_is_copy_safe  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


def _pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pg_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
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


def _pg_connect():
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        return psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL auth failed: {exc}")


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


def _seed_pg(cur, table: str, rows: int) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    cur.execute(
        f'CREATE TABLE public."{table}" ('
        "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
    )
    cur.execute(
        f'INSERT INTO public."{table}" (id, label) '
        f"SELECT g, 'r' || g::text FROM generate_series(1, {int(rows)}) g"
    )


def _drop_pg(cur, table: str) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("s3", _s3_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def test_pg_s3_copy_safe_types():
    assert pg_s3_type_is_copy_safe("BIGINT") is True
    assert pg_s3_type_is_copy_safe("VARCHAR(32)") is True
    assert pg_s3_type_is_copy_safe("DATE") is True
    assert pg_s3_type_is_copy_safe("JSONB") is False
    assert pg_s3_type_is_copy_safe("BYTEA") is False
    assert pg_s3_type_is_copy_safe("TIMESTAMPTZ") is False


def test_pg_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_S3_COPY", "0")
    assert pg_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.csv"),
            dest_table="nope.csv",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_pg_s3_json_key_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PG_S3_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.json"),
            dest_table="nope.json",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_pg_s3_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PG_S3_COPY", raising=False)
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_src_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _delete_key(client, bucket, dest)
        result = copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_schema="public",
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
        assert _dest_count(bucket, dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)


def test_live_pg_s3_empty_string_and_null_preserved():
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_null_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            cur.execute(
                f'CREATE TABLE public."{src}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, label) VALUES '
                "(1, NULL), (2, ''), (3, 'x')"
            )
        pg.commit()
        result = copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_schema="public",
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
        lines = [ln for ln in body.splitlines() if ln]
        assert lines[0].startswith("id")
        assert "\\N" in body or lines[1].endswith(",")
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)


def test_live_pg_s3_skip_when_dest_count_matches():
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_skip_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        first = copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_schema="public",
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
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)


def test_live_pg_s3_occupied_mismatch_declines():
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_occ_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,g\n2,g\n")
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_postgres_to_s3(
                source_cfg=_pg_cfg(),
                source_schema="public",
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)


def test_live_pg_s3_overwrite_replaces_dest():
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_ow_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,ghost\n")
        result = copy_postgres_to_s3(
            source_cfg=_pg_cfg(),
            source_schema="public",
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
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)


def test_live_pg_s3_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PG_S3_COPY", raising=False)
    pg = _pg_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_s3_str_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _delete_key(client, bucket, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"pg-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, dest), "format": "s3"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_csv_pg_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(bucket, dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        pg.close()
        _delete_key(client, bucket, dest)
