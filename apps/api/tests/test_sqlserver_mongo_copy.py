"""SQL Server → MongoDB HOLDLOCK SELECT + insert_many — dest count_documents."""

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
from services.copy_sqlserver_mongo import (  # noqa: E402
    copy_sqlserver_to_mongo,
    sqlserver_mongo_copy_enabled,
    sqlserver_mongo_type_is_copy_safe,
)
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


def _dest_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def test_sqlserver_mongo_copy_safe_types():
    assert sqlserver_mongo_type_is_copy_safe("BIGINT") is True
    assert sqlserver_mongo_type_is_copy_safe("NVARCHAR(32)") is True
    assert sqlserver_mongo_type_is_copy_safe("DATE") is True
    assert sqlserver_mongo_type_is_copy_safe("VARBINARY") is False
    assert sqlserver_mongo_type_is_copy_safe("XML") is False
    assert sqlserver_mongo_type_is_copy_safe("DATETIME") is False
    assert sqlserver_mongo_type_is_copy_safe("DATETIME2") is False
    assert sqlserver_mongo_type_is_copy_safe("TIME") is False


def test_sqlserver_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLSERVER_MONGO_COPY", "0")
    assert sqlserver_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_sqlserver_mongo_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_MONGO_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_src_{tag}"
    dest = f"ss_mongo_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 800)
        _drop_mongo(dest)
        result = copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert _dest_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_null_{tag}"
    dest = f"ss_mongo_null_dst_{tag}"
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
        _drop_mongo(dest)
        result = copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        client, coll = _mongo_coll(dest)
        try:
            docs = list(coll.find({}, {"_id": 0}).sort("id", 1))
        finally:
            client.close()
        assert docs[0]["label"] is None
        assert docs[1]["label"] == ""
        assert docs[2]["label"] == "x"
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_skip_{tag}"
    dest = f"ss_mongo_skip_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 800)
        _drop_mongo(dest)
        first = copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_occ_{tag}"
    dest = f"ss_mongo_occ_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 800)
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_many(
                [{"id": 1, "label": "ghost"}, {"id": 2, "label": "ghost"}],
                ordered=False,
            )
        finally:
            client.close()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Mongo dest"):
            copy_sqlserver_to_mongo(
                source_cfg=_ss_cfg(),
                source_schema="dbo",
                source_table=src,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_ow_{tag}"
    dest = f"ss_mongo_ow_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 800)
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_est_{tag}"
    dest = f"ss_mongo_est_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 80)
        _drop_mongo(dest)
        copy_sqlserver_to_mongo(
            source_cfg=_ss_cfg(),
            source_schema="dbo",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        assert _dest_count(dest) == 80
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()


def test_live_sqlserver_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_MONGO_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_mongo_stream_{tag}"
    dest = f"ss_mongo_stream_dst_{tag}"
    try:
        cur = ss.cursor()
        _seed_ss(cur, src, 800)
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
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
        assert summary.get("load_method") == "select_sqlserver_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        cur = ss.cursor()
        _drop_ss(cur, src)
        _drop_mongo(dest)
        ss.close()
