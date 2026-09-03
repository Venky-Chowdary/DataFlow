"""MySQL SELECT CSV → S3 upload — dest artifact COUNT."""

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
from services.copy_mysql_s3 import (  # noqa: E402
    copy_mysql_to_s3,
    mysql_s3_copy_enabled,
    mysql_s3_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _minio_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9000), timeout=1):
            pass
    except OSError:
        pytest.skip("MinIO 9000 not reachable")


def _mysql_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=1):
            pass
    except OSError:
        pytest.skip("MySQL 3306 not reachable")


def _mysql_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
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


def _mysql_connect():
    _mysql_or_skip()
    pymysql = pytest.importorskip("pymysql")
    try:
        return pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="dataflow",
            password="dataflow",
            database="dataflow",
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"MySQL auth failed: {exc}")


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


def _seed_mysql(cur, table: str, rows: int) -> None:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute(
        f"CREATE TABLE `{table}` ("
        "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
    )
    cur.executemany(
        f"INSERT INTO `{table}` (id, label) VALUES (%s, %s)",
        [(i, f"r{i}") for i in range(1, int(rows) + 1)],
    )


def _drop_mysql(cur, table: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")


def _dest_count(bucket: str, key: str) -> int:
    n = destination_row_count("s3", _s3_cfg(bucket, key), schema="", table_name=key)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def test_mysql_s3_copy_safe_types():
    assert mysql_s3_type_is_copy_safe("BIGINT") is True
    assert mysql_s3_type_is_copy_safe("VARCHAR(32)") is True
    assert mysql_s3_type_is_copy_safe("DATE") is True
    assert mysql_s3_type_is_copy_safe("JSON") is False
    assert mysql_s3_type_is_copy_safe("BLOB") is False
    assert mysql_s3_type_is_copy_safe("TIMESTAMP") is False


def test_mysql_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MYSQL_S3_COPY", "0")
    assert mysql_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.csv"),
            dest_table="nope.csv",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_mysql_s3_json_key_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MYSQL_S3_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "nope.json"),
            dest_table="nope.json",
            pairs=[("id", "id")],
            s3_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mysql_s3_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MYSQL_S3_COPY", raising=False)
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_src_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        result = copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
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
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)


def test_live_mysql_s3_empty_string_and_null_preserved():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_null_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.executemany(
                f"INSERT INTO `{src}` (id, label) VALUES (%s, %s)",
                [(1, None), (2, ""), (3, "x")],
            )
        result = copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
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
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)


def test_live_mysql_s3_skip_when_dest_count_matches():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_skip_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        first = copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
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
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)


def test_live_mysql_s3_occupied_mismatch_declines():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_occ_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,g\n2,g\n")
        assert _dest_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_mysql_to_s3(
                source_cfg=_mysql_cfg(),
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(bucket, dest) == 2
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)


def test_live_mysql_s3_overwrite_replaces_dest():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_ow_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=dest, Body=b"id,label\n1,ghost\n")
        result = copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
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
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)


def test_live_mysql_s3_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MYSQL_S3_COPY", raising=False)
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"mysql_s3_str_{tag}"
    bucket = f"dfc{tag}"
    dest = f"dst_{tag}.csv"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mysql-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": src}
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
        assert summary.get("load_method") == "select_mysql_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(bucket, dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        mysql.close()
        _delete_key(client, bucket, dest)
