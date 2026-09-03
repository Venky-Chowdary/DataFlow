"""S3 CSV GET → MySQL STRICT LOAD DATA — dest COUNT(*)."""

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
from services.copy_mysql_s3 import copy_mysql_to_s3  # noqa: E402
from services.copy_s3_mysql import copy_s3_to_mysql, s3_mysql_copy_enabled  # noqa: E402
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


def _dest_count(table: str) -> int:
    n = destination_row_count("mysql", _mysql_cfg(), schema="", table_name=table)
    assert n is not None
    return int(n)


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _seed_s3_csv_from_mysql(mysql, client, mysql_table: str, bucket: str, key: str, rows: int) -> None:
    with mysql.cursor() as cur:
        _seed_mysql(cur, mysql_table, rows)
    _delete_key(client, bucket, key)
    result = copy_mysql_to_s3(
        source_cfg=_mysql_cfg(),
        source_table=mysql_table,
        dest_cfg=_s3_cfg(bucket, key),
        dest_table=key,
        pairs=[("id", "id"), ("label", "label")],
        s3_ddls=["BIGINT", "TEXT"],
        replace_destination=True,
    )
    assert result.target_rows == rows


def test_s3_mysql_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_S3_MYSQL_COPY", "0")
    assert s3_mysql_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_mysql(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_mysql_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            mysql_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_s3_mysql_jsonl_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MYSQL_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    key = f"src_{tag}.jsonl"
    try:
        _ensure_bucket(client, bucket)
        client.put_object(Bucket=bucket, Key=key, Body=b'{"id":1}\n')
        with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
            copy_s3_to_mysql(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_mysql_cfg(),
                dest_table="nope",
                pairs=[("id", "id")],
                mysql_ddls=["BIGINT"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, key)


def test_live_s3_mysql_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_src_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_dst_{tag}"
    try:
        _seed_s3_csv_from_mysql(mysql, client, mid, bucket, key, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        result = copy_s3_to_mysql(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)


def test_live_s3_mysql_empty_string_and_null_preserved():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_null_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_null_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            cur.execute(
                f"CREATE TABLE `{mid}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.executemany(
                f"INSERT INTO `{mid}` (id, label) VALUES (%s, %s)",
                [(1, None), (2, ""), (3, "x")],
            )
        copy_mysql_to_s3(
            source_cfg=_mysql_cfg(),
            source_table=mid,
            dest_cfg=_s3_cfg(bucket, key),
            dest_table=key,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        result = copy_s3_to_mysql(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        with mysql.cursor() as cur:
            cur.execute(f"SELECT id, label FROM `{dest}` ORDER BY id")
            rows = cur.fetchall()
        assert rows[0] == (1, None)
        assert rows[1] == (2, "")
        assert rows[2] == (3, "x")
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)


def test_live_s3_mysql_skip_when_dest_count_matches():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_skip_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_skip_dst_{tag}"
    try:
        _seed_s3_csv_from_mysql(mysql, client, mid, bucket, key, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        first = copy_s3_to_mysql(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_mysql(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)


def test_live_s3_mysql_occupied_mismatch_declines():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_occ_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_occ_dst_{tag}"
    try:
        _seed_s3_csv_from_mysql(mysql, client, mid, bucket, key, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'g'), (2, 'g')")
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied MySQL dest"):
            copy_s3_to_mysql(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_mysql_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mysql_ddls=["BIGINT", "TEXT"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)


def test_live_s3_mysql_overwrite_replaces_dest():
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_ow_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_ow_dst_{tag}"
    try:
        _seed_s3_csv_from_mysql(mysql, client, mid, bucket, key, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label TEXT)"
            )
            cur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'ghost')")
        result = copy_s3_to_mysql(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "TEXT"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)


def test_live_s3_mysql_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    mid = f"s3_mysql_str_{tag}"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_mysql_str_dst_{tag}"
    try:
        _seed_s3_csv_from_mysql(mysql, client, mid, bucket, key, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-mysql-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, key), "format": "s3"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": dest}
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
        assert summary.get("load_method") == "get_csv_s3_load_data_mysql"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, mid)
            _drop_mysql(cur, dest)
        mysql.close()
        _delete_key(client, bucket, key)
