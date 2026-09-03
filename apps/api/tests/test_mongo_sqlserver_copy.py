"""MongoDB → SQL Server snapshot find + fast_executemany — dest COUNT(*)."""

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
from services.copy_mongo_pg import mongo_type_is_copy_safe  # noqa: E402
from services.copy_mongo_sqlserver import (  # noqa: E402
    copy_mongo_to_sqlserver,
    mongo_sqlserver_copy_enabled,
)
from services.copy_sqlserver_mongo import copy_sqlserver_to_mongo  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _ss_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1433), timeout=1):
            pass
    except OSError:
        pytest.skip("SQL Server 1433 not reachable")


def _ss_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1433,
        "database": "dataflow",
        "username": "sa",
        "password": "DataFlow_CDC_2022!",
        "schema": "dbo",
        "trust_server_certificate": True,
        "encrypt": "yes",
    }


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
    }


def _ss_connect():
    _ss_or_skip()
    pymssql = pytest.importorskip("pymssql")
    try:
        return pymssql.connect(
            server="127.0.0.1",
            port=1433,
            user="sa",
            password="DataFlow_CDC_2022!",
            database="dataflow",
            login_timeout=3,
            autocommit=True,
        )
    except Exception as exc:
        pytest.skip(f"SQL Server auth failed: {exc}")


def _mongo_coll(name: str):
    _mongo_or_skip()
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as exc:
        client.close()
        pytest.skip(f"MongoDB ping failed: {exc}")
    return client, client["dataflow"][name]


def _seed_ss(cur, table: str, rows: int) -> None:
    cur.execute(f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]")
    cur.execute(
        f"CREATE TABLE dbo.[{table}] ("
        "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
    )
    cur.execute(
        f"""
        WITH n AS (
          SELECT TOP ({int(rows)}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS seq
          FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO dbo.[{table}] (id, label)
        SELECT seq, CONCAT(N'r', seq) FROM n
        """
    )


def _drop_ss(cur, table: str) -> None:
    cur.execute(
        f"IF OBJECT_ID(N'dbo.{table}', 'U') IS NOT NULL DROP TABLE dbo.[{table}]"
    )


def _drop_mongo(name: str) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
    finally:
        client.close()


def _mongo_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _ss_count(table: str) -> int:
    n = destination_row_count("sqlserver", _ss_cfg(), schema="dbo", table_name=table)
    assert n is not None
    return int(n)


def _seed_mongo_from_ss(ss, src: str, mongo: str, rows: int) -> None:
    cur = ss.cursor()
    _seed_ss(cur, src, rows)
    _drop_mongo(mongo)
    result = copy_sqlserver_to_mongo(
        source_cfg=_ss_cfg(),
        source_schema="dbo",
        source_table=src,
        dest_cfg=_mongo_cfg(mongo),
        dest_table=mongo,
        pairs=[("id", "id"), ("label", "label")],
        mongo_ddls=["BIGINT", "NVARCHAR(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _mongo_count(mongo) == rows


def test_mongo_sqlserver_copy_safe_types():
    assert mongo_type_is_copy_safe("string") is True
    assert mongo_type_is_copy_safe("long") is True
    assert mongo_type_is_copy_safe("NVARCHAR(32)") is True
    assert mongo_type_is_copy_safe("BIGINT") is True
    assert mongo_type_is_copy_safe("object") is False
    assert mongo_type_is_copy_safe("array") is False
    assert mongo_type_is_copy_safe("bindata") is False
    assert mongo_type_is_copy_safe("timestamptz") is False


def test_mongo_sqlserver_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_SQLSERVER_COPY", "0")
    assert mongo_sqlserver_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table="nope",
            pairs=[("id", "id")],
            sqlserver_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mongo_sqlserver_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_src_{tag}"
    mid = f"mongo_ss_mid_{tag}"
    dest = f"mongo_ss_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        result = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert _ss_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_null_{tag}"
    mid = f"mongo_ss_null_mid_{tag}"
    dest = f"mongo_ss_null_dst_{tag}"
    try:
        cur = ss.cursor()
        _drop_ss(cur, src)
        cur.execute(
            f"CREATE TABLE dbo.[{src}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{src}] (id, label) VALUES "
            "(1, NULL), (2, N''), (3, N'x')"
        )
        _drop_mongo(mid)
        copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(mid),
            dest_table=mid,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        cur = ss.cursor()
        _drop_ss(cur, dest)
        result = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        cur = ss.cursor()
        cur.execute(f"SELECT id, label FROM dbo.[{dest}] ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_skip_{tag}"
    mid = f"mongo_ss_skip_mid_{tag}"
    dest = f"mongo_ss_skip_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        first = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _ss_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_occ_{tag}"
    mid = f"mongo_ss_occ_mid_{tag}"
    dest = f"mongo_ss_occ_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{dest}] (id, label) VALUES "
            "(1, N'ghost'), (2, N'ghost')"
        )
        with pytest.raises(FastPathUnavailable, match="occupied SQL Server dest"):
            copy_mongo_to_sqlserver(
                source_cfg=_mongo_cfg(mid),
                source_table=mid,
                dest_cfg=_ss_cfg(),
                dest_schema="dbo",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
        assert _ss_count(dest) == 2
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_ow_{tag}"
    mid = f"mongo_ss_ow_mid_{tag}"
    dest = f"mongo_ss_ow_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'ghost')")
        result = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        assert _ss_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_source_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_est_{tag}"
    mid = f"mongo_ss_est_mid_{tag}"
    dest = f"mongo_ss_est_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 80)
        cur = ss.cursor()
        _drop_ss(cur, dest)

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo source COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_sqlserver(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ss_cfg(),
            dest_schema="dbo",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _ss_count(dest) == 80
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()


def test_live_mongo_sqlserver_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ss_stream_{tag}"
    mid = f"mongo_ss_stream_mid_{tag}"
    dest = f"mongo_ss_stream_dst_{tag}"
    try:
        _seed_mongo_from_ss(ss, src, mid, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-ss-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(mid), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": dest}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "NVARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "NVARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "mongo_snapshot_find_fast_executemany_sqlserver"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _ss_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_ss(cur, dest)
        _drop_mongo(mid)
        ss.close()
