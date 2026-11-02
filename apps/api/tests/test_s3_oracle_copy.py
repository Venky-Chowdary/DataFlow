"""S3 CSV GET → Oracle executemany — dest COUNT(*) before commit."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_oracle_pg import oracle_type_is_copy_safe  # noqa: E402
from services.copy_s3_oracle import copy_s3_to_oracle, s3_oracle_copy_enabled  # noqa: E402
from services.copy_sqlite_oracle import (  # noqa: E402
    sqlite_declared_to_oracle_ddl,
    sqlite_oracle_type_is_copy_safe,
    sqlite_value_to_oracle,
)
from services.copy_sqlite_s3 import copy_sqlite_to_s3  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _oracle_password() -> str:
    env = (
        os.environ.get("DATAFLOW_ORACLE_PASSWORD")
        or os.environ.get("ORA_PASSWORD")
        or ""
    ).strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _ora_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("Oracle 1521 not reachable")


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


def _ora_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _ora_connect():
    _ora_or_skip()
    oracledb = pytest.importorskip("oracledb")
    try:
        return oracledb.connect(
            user="dataflow",
            password=_oracle_password(),
            dsn="127.0.0.1:1521/XEPDB1",
        )
    except Exception as exc:
        pytest.skip(f"Oracle auth failed: {exc}")


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


def _delete_key(client, bucket: str, key: str) -> None:
    try:
        client.delete_object(Bucket=bucket, Key=key)
    except Exception:
        return


def _drop_ora(table: str) -> None:
    conn = _ora_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        conn.commit()
    finally:
        conn.close()


def _dest_count(table: str) -> int:
    n = destination_row_count(
        "oracle", _ora_cfg(), schema="DATAFLOW", table_name=table
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


def test_s3_oracle_copy_safe_types():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("CLOB") is False
    assert sqlite_oracle_type_is_copy_safe("INTEGER") is True
    assert sqlite_oracle_type_is_copy_safe("TEXT") is True
    assert sqlite_oracle_type_is_copy_safe("DATETIME") is False
    assert sqlite_declared_to_oracle_ddl("TEXT") == "VARCHAR2(4000)"


def test_s3_oracle_date_iso_bind():
    coerced = [0]
    assert sqlite_value_to_oracle("2020-01-02", "DATE", coerced) == date(2020, 1, 2)
    assert sqlite_value_to_oracle(None, "DATE", coerced) is None
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_value_to_oracle("2020-01-02 12:00:00", "DATETIME", coerced)


def test_s3_oracle_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_S3_ORACLE_COPY", "0")
    assert s3_oracle_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_s3_to_oracle(
            source_cfg=_s3_cfg("missing", "a.csv"),
            source_table="a.csv",
            dest_cfg=_ora_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_s3_oracle_jsonl_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_S3_ORACLE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    bucket = f"dfc{tag}"
    key = f"src_{tag}.jsonl"
    try:
        try:
            client.head_bucket(Bucket=bucket)
        except Exception:
            client.create_bucket(Bucket=bucket)
        client.put_object(Bucket=bucket, Key=key, Body=b'{"id":1}\n')
        with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
            copy_s3_to_oracle(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_ora_cfg(),
                dest_table="nope",
                pairs=[("id", "id")],
                oracle_ddls=["NUMBER"],
                replace_destination=True,
            )
    finally:
        _delete_key(client, bucket, key)


def test_s3_oracle_public_proxy_declines():
    src = {
        **_s3_cfg("missing", "a.csv"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:9000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_s3_to_oracle(
            source_cfg=src,
            source_table="a.csv",
            dest_cfg=_ora_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_live_s3_oracle_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_S3_ORACLE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ora_dst_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_ora(dest)
        result = copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("s3_read") == "get_csv"
        assert result.source_snapshot.get("oracle_write") == "insert"
        assert _dest_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_ora(dest)


def test_live_s3_oracle_empty_string_and_null(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ora_null_{tag}"
    conn = sqlite3.connect(src_db)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    ora = _ora_connect()
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
        _drop_ora(dest)
        result = copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert result.source_snapshot.get("empty_string_as_null_cells") == 1
        assert _dest_count(dest) == 3
        cur = ora.cursor()
        cur.execute(f"SELECT id, label FROM {dest} ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        ora.close()
        _delete_key(client, bucket, key)
        _drop_ora(dest)


def test_live_s3_oracle_skip_when_dest_count_matches(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ora_skip_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_ora(dest)
        first = copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_ora(dest)


def test_live_s3_oracle_occupied_mismatch_declines(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    ghost_key = f"ghost_{tag}.csv"
    dest = f"s3_ora_occ_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _seed_s3_csv(ghost_db, "ghost", bucket, ghost_key, 2)
        _drop_ora(dest)
        copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, ghost_key),
            source_table=ghost_key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=True,
        )
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Oracle dest"):
            copy_s3_to_oracle(
                source_cfg=_s3_cfg(bucket, key),
                source_table=key,
                dest_cfg=_ora_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _delete_key(client, bucket, key)
        _delete_key(client, bucket, ghost_key)
        _drop_ora(dest)


def test_live_s3_oracle_overwrite_replaces_dest(tmp_path):
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    ghost_db = tmp_path / "ghost.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    ghost_key = f"ghost_{tag}.csv"
    dest = f"s3_ora_ow_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _seed_s3_csv(ghost_db, "ghost", bucket, ghost_key, 1)
        _drop_ora(dest)
        copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, ghost_key),
            source_table=ghost_key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=True,
        )
        result = copy_s3_to_oracle(
            source_cfg=_s3_cfg(bucket, key),
            source_table=key,
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(4000)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("oracle_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _delete_key(client, bucket, ghost_key)
        _drop_ora(dest)


def test_live_s3_oracle_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_S3_ORACLE_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src_db = tmp_path / "src.db"
    bucket = f"dfc{tag}"
    key = f"src_{tag}.csv"
    dest = f"s3_ora_str_{tag}"
    try:
        _seed_s3_csv(src_db, "src_t", bucket, key, 800)
        _drop_ora(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"s3-ora-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, key), "format": "s3"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": dest}
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
        assert summary.get("load_method") == "get_csv_s3_executemany_oracle"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Oracle" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _delete_key(client, bucket, key)
        _drop_ora(dest)
