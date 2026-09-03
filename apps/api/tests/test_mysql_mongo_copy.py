"""MySQL → MongoDB consistent-snapshot SELECT + insert_many — dest count_documents."""

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
from services.copy_mysql_mongo import (  # noqa: E402
    copy_mysql_to_mongo,
    mysql_mongo_copy_enabled,
    mysql_mongo_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


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


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
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


def _seed_mysql(cur, table: str, rows: int) -> None:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute(
        f"CREATE TABLE `{table}` ("
        "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
    )
    cur.execute("SET SESSION cte_max_recursion_depth = 10000")
    cur.execute(
        f"""
        INSERT INTO `{table}` (id, label)
        WITH RECURSIVE n AS (
          SELECT 1 AS seq
          UNION ALL SELECT seq + 1 FROM n WHERE seq < {int(rows)}
        )
        SELECT seq, CONCAT('r', seq) FROM n
        """
    )


def _drop_mysql(cur, table: str) -> None:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")


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


def test_mysql_mongo_copy_safe_types():
    assert mysql_mongo_type_is_copy_safe("BIGINT") is True
    assert mysql_mongo_type_is_copy_safe("VARCHAR(32)") is True
    assert mysql_mongo_type_is_copy_safe("DATE") is True
    assert mysql_mongo_type_is_copy_safe("JSON") is False
    assert mysql_mongo_type_is_copy_safe("BLOB") is False
    assert mysql_mongo_type_is_copy_safe("TIMESTAMP") is False
    assert mysql_mongo_type_is_copy_safe("DATETIME") is False
    assert mysql_mongo_type_is_copy_safe("TIME") is False


def test_mysql_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MYSQL_MONGO_COPY", "0")
    assert mysql_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mysql_mongo_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MYSQL_MONGO_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_src_{tag}"
    dest = f"my_mongo_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _drop_mongo(dest)
        result = copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_null_{tag}"
    dest = f"my_mongo_null_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, label) VALUES "
                "(1, NULL), (2, ''), (3, 'x')"
            )
        _drop_mongo(dest)
        result = copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
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
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_skip_{tag}"
    dest = f"my_mongo_skip_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _drop_mongo(dest)
        first = copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_occ_{tag}"
    dest = f"my_mongo_occ_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
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
            copy_mysql_to_mongo(
                source_cfg=_mysql_cfg(),
                source_table=src,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_ow_{tag}"
    dest = f"my_mongo_ow_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_est_{tag}"
    dest = f"my_mongo_est_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 80)
        _drop_mongo(dest)
        copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        assert _dest_count(dest) == 80
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()


def test_live_mysql_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MYSQL_MONGO_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"my_mongo_stream_{tag}"
    dest = f"my_mongo_stream_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            _seed_mysql(cur, src, 800)
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mysql-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "VARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_mysql_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
        _drop_mongo(dest)
        mysql.close()
