"""MongoDB → MySQL snapshot find + STRICT LOAD DATA — dest COUNT(*)."""

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
from services.copy_mongo_mysql import (  # noqa: E402
    copy_mongo_to_mysql,
    mongo_mysql_copy_enabled,
)
from services.copy_mongo_pg import mongo_type_is_copy_safe  # noqa: E402
from services.copy_mysql_mongo import copy_mysql_to_mongo  # noqa: E402
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


def _mongo_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _mysql_count(table: str) -> int:
    n = destination_row_count("mysql", _mysql_cfg(), schema="", table_name=table)
    assert n is not None
    return int(n)


def _seed_mongo_from_mysql(mysql, src: str, mongo: str, rows: int) -> None:
    with mysql.cursor() as cur:
        _seed_mysql(cur, src, rows)
    _drop_mongo(mongo)
    result = copy_mysql_to_mongo(
        source_cfg=_mysql_cfg(),
        source_table=src,
        dest_cfg=_mongo_cfg(mongo),
        dest_table=mongo,
        pairs=[("id", "id"), ("label", "label")],
        mongo_ddls=["BIGINT", "VARCHAR(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _mongo_count(mongo) == rows


def test_mongo_mysql_copy_safe_types():
    assert mongo_type_is_copy_safe("string") is True
    assert mongo_type_is_copy_safe("long") is True
    assert mongo_type_is_copy_safe("VARCHAR(32)") is True
    assert mongo_type_is_copy_safe("BIGINT") is True
    assert mongo_type_is_copy_safe("object") is False
    assert mongo_type_is_copy_safe("array") is False
    assert mongo_type_is_copy_safe("bindata") is False
    assert mongo_type_is_copy_safe("timestamptz") is False


def test_mongo_mysql_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_MYSQL_COPY", "0")
    assert mongo_mysql_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_mysql(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_mysql_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            mysql_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mongo_mysql_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_src_{tag}"
    mid = f"mongo_my_mid_{tag}"
    dest = f"mongo_my_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        result = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert _mysql_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_null_{tag}"
    mid = f"mongo_my_null_mid_{tag}"
    dest = f"mongo_my_null_dst_{tag}"
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
        _drop_mongo(mid)
        copy_mysql_to_mongo(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(mid),
            dest_table=mid,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        result = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        with mysql.cursor() as cur:
            cur.execute(f"SELECT id, label FROM `{dest}` ORDER BY id")
            rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_skip_{tag}"
    mid = f"mongo_my_skip_mid_{tag}"
    dest = f"mongo_my_skip_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        first = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _mysql_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_occ_{tag}"
    mid = f"mongo_my_occ_mid_{tag}"
    dest = f"mongo_my_occ_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f"INSERT INTO `{dest}` (id, label) VALUES "
                "(1, 'ghost'), (2, 'ghost')"
            )
        with pytest.raises(FastPathUnavailable, match="occupied MySQL dest"):
            copy_mongo_to_mysql(
                source_cfg=_mongo_cfg(mid),
                source_table=mid,
                dest_cfg=_mysql_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mysql_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
        assert _mysql_count(dest) == 2
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_ow_{tag}"
    mid = f"mongo_my_ow_mid_{tag}"
    dest = f"mongo_my_ow_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'ghost')")
        result = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        assert _mysql_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_source_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_est_{tag}"
    mid = f"mongo_my_est_mid_{tag}"
    dest = f"mongo_my_est_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 80)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo source COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_mysql(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _mysql_count(dest) == 80
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()


def test_live_mongo_mysql_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_my_stream_{tag}"
    mid = f"mongo_my_stream_mid_{tag}"
    dest = f"mongo_my_stream_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        with mysql.cursor() as cur:
            _drop_mysql(cur, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-mysql-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(mid), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mysql_cfg(), "format": "mysql", "table": dest}
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
        assert summary.get("load_method") == "mongo_snapshot_find_load_data_mysql"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _mysql_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            _drop_mysql(cur, src)
            _drop_mysql(cur, dest)
        _drop_mongo(mid)
        mysql.close()
