"""Oracle SHARE-lock SELECT CSV → S3 upload — dest artifact COUNT."""

from __future__ import annotations

import os
import socket
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
from services.copy_oracle_s3 import (  # noqa: E402
    copy_oracle_to_s3,
    oracle_s3_copy_enabled,
    oracle_value_to_s3,
)
from services.copy_s3_common import s3_dest_count  # noqa: E402


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


def _seed_ora(table: str, rows: int) -> None:
    conn = _ora_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        cur.execute(
            f"CREATE TABLE {table} ("
            "id NUMBER NOT NULL PRIMARY KEY, label VARCHAR2(32))"
        )
        cur.executemany(
            f"INSERT INTO {table} (id, label) VALUES (:1, :2)",
            [(i, f"r{i}") for i in range(1, int(rows) + 1)],
        )
        conn.commit()
    finally:
        conn.close()


def _s3_count(bucket: str, key: str) -> int:
    return int(s3_dest_count(_s3_cfg(bucket, key), key))


def test_oracle_s3_copy_safe_types():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("CLOB") is False
    assert oracle_type_is_copy_safe("JSON") is False


def test_oracle_s3_date_bind():
    assert oracle_value_to_s3(date(2020, 1, 2)) == date(2020, 1, 2)
    assert oracle_value_to_s3(None) is None
    with pytest.raises(FastPathUnavailable, match="binary"):
        oracle_value_to_s3(b"x")


def test_oracle_s3_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ORACLE_S3_COPY", "0")
    assert oracle_s3_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "out.csv"),
            dest_table="out.csv",
            pairs=[("id", "id")],
            s3_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_oracle_s3_json_dest_declines():
    with pytest.raises(FastPathUnavailable, match="CSV/TSV"):
        copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_s3_cfg("missing", "out.json"),
            dest_table="out.json",
            pairs=[("id", "id")],
            s3_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_oracle_s3_public_proxy_declines():
    dest = {
        **_s3_cfg("missing", "out.csv"),
        "host": "",
        "endpoint_url": "https://caboose.proxy.rlwy.net:9000",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=dest,
            dest_table="out.csv",
            pairs=[("id", "id")],
            s3_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_live_oracle_s3_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ORACLE_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_src_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_dst_{tag}.csv"
    try:
        _seed_ora(src, 800)
        result = copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("oracle_read") == "share_select"
        assert result.source_snapshot.get("s3_write") == "insert"
        assert _s3_count(bucket, dest) == 800
    finally:
        _drop_ora(src)
        _delete_key(client, bucket, dest)


def test_live_oracle_s3_empty_string_and_null():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_null_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_null_{tag}.csv"
    conn = _ora_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{src} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        cur.execute(
            f"CREATE TABLE {src} ("
            "id NUMBER NOT NULL PRIMARY KEY, label VARCHAR2(32))"
        )
        cur.executemany(
            f"INSERT INTO {src} (id, label) VALUES (:1, :2)",
            [(1, None), (2, ""), (3, "x")],
        )
        conn.commit()
        result = copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _s3_count(bucket, dest) == 3
    finally:
        conn.close()
        _drop_ora(src)
        _delete_key(client, bucket, dest)


def test_live_oracle_s3_skip_when_dest_count_matches():
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_skip_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_skip_{tag}.csv"
    try:
        _seed_ora(src, 800)
        first = copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _s3_count(bucket, dest) == 800
    finally:
        _drop_ora(src)
        _delete_key(client, bucket, dest)


def test_live_oracle_s3_occupied_mismatch_declines(tmp_path):
    import sqlite3

    from services.copy_sqlite_s3 import copy_sqlite_to_s3

    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_occ_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_occ_{tag}.csv"
    ghost_db = tmp_path / "ghost.db"
    conn = sqlite3.connect(ghost_db)
    conn.execute('CREATE TABLE "ghost" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany('INSERT INTO "ghost" (id, label) VALUES (?, ?)', [(1, "a"), (2, "b")])
    conn.commit()
    conn.close()
    try:
        _seed_ora(src, 800)
        copy_sqlite_to_s3(
            source_cfg={
                "type": "sqlite",
                "format": "sqlite",
                "database": str(ghost_db),
                "table": "ghost",
            },
            source_table="ghost",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert _s3_count(bucket, dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied S3 dest"):
            copy_oracle_to_s3(
                source_cfg=_ora_cfg(),
                source_table=src,
                dest_cfg=_s3_cfg(bucket, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                s3_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
        assert _s3_count(bucket, dest) == 2
    finally:
        _drop_ora(src)
        _delete_key(client, bucket, dest)


def test_live_oracle_s3_overwrite_replaces_dest(tmp_path):
    import sqlite3

    from services.copy_sqlite_s3 import copy_sqlite_to_s3

    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_ow_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_ow_{tag}.csv"
    ghost_db = tmp_path / "ghost.db"
    conn = sqlite3.connect(ghost_db)
    conn.execute('CREATE TABLE "ghost" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.execute('INSERT INTO "ghost" (id, label) VALUES (1, "g")')
    conn.commit()
    conn.close()
    try:
        _seed_ora(src, 800)
        copy_sqlite_to_s3(
            source_cfg={
                "type": "sqlite",
                "format": "sqlite",
                "database": str(ghost_db),
                "table": "ghost",
            },
            source_table="ghost",
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        result = copy_oracle_to_s3(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_s3_cfg(bucket, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            s3_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("s3_write") == "overwrite"
        assert _s3_count(bucket, dest) == 800
    finally:
        _drop_ora(src)
        _delete_key(client, bucket, dest)


def test_live_oracle_s3_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ORACLE_S3_COPY", raising=False)
    client = _s3_client()
    tag = uuid.uuid4().hex[:8]
    src = f"ora_s3_str_{tag}"
    bucket = f"dfc{tag}"
    dest = f"ora_s3_str_{tag}.csv"
    try:
        _seed_ora(src, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-s3-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_s3_cfg(bucket, dest), "format": "s3"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "NUMBER", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR2(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "NUMBER", "label": "VARCHAR2(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_oracle_upload_s3"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("S3" in line for line in ddl_log)
        assert _s3_count(bucket, dest) == 800
    finally:
        _drop_ora(src)
        _delete_key(client, bucket, dest)
