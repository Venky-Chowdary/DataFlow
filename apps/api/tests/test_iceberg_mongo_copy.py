"""Iceberg → MongoDB snapshot Parquet + insert_many — dest count_documents."""

from __future__ import annotations

import os
import socket
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
from services.copy_iceberg_mongo import (  # noqa: E402
    copy_iceberg_to_mongo,
    iceberg_mongo_copy_enabled,
    iceberg_mongo_type_is_copy_safe,
)
from services.copy_mysql_iceberg import copy_mysql_to_iceberg  # noqa: E402
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


def _iceberg_cfg(table: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
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


def _iceberg_count(table: str) -> int:
    n = destination_row_count(
        "iceberg", _iceberg_cfg(table), schema="default", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_iceberg_from_mysql(mysql, src: str, ice: str, rows: int) -> None:
    with mysql.cursor() as cur:
        _seed_mysql(cur, src, rows)
    _drop_iceberg(ice)
    result = copy_mysql_to_iceberg(
        source_cfg=_mysql_cfg(),
        source_table=src,
        dest_cfg=_iceberg_cfg(ice),
        dest_table=ice,
        pairs=[("id", "id"), ("label", "label")],
        iceberg_ddls=["long", "string"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _iceberg_count(ice) == rows


def test_iceberg_mongo_copy_safe_types():
    assert iceberg_mongo_type_is_copy_safe("string") is True
    assert iceberg_mongo_type_is_copy_safe("long") is True
    assert iceberg_mongo_type_is_copy_safe("date") is True
    assert iceberg_mongo_type_is_copy_safe("VARCHAR(32)") is True
    assert iceberg_mongo_type_is_copy_safe("binary") is False
    assert iceberg_mongo_type_is_copy_safe("uuid") is False
    assert iceberg_mongo_type_is_copy_safe("timestamptz") is False
    assert iceberg_mongo_type_is_copy_safe("timestamp") is False
    assert iceberg_mongo_type_is_copy_safe("list<string>") is False


def test_iceberg_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ICEBERG_MONGO_COPY", "0")
    assert iceberg_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["long"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_mongo_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_ICEBERG_MONGO_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_src_{tag}"
    ice = f"ice_mongo_mid_{tag}"
    dest = f"ice_mongo_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        _drop_mongo(dest)
        result = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_empty_string_and_null_preserved():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_null_{tag}"
    ice = f"ice_mongo_null_mid_{tag}"
    dest = f"ice_mongo_null_dst_{tag}"
    try:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(
                f"CREATE TABLE `{src}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f"INSERT INTO `{src}` (id, label) VALUES "
                "(1, NULL), (2, ''), (3, 'x')"
            )
        _drop_iceberg(ice)
        copy_mysql_to_iceberg(
            source_cfg=_mysql_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(ice),
            dest_table=ice,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        _drop_mongo(dest)
        result = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
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
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_skip_{tag}"
    ice = f"ice_mongo_skip_mid_{tag}"
    dest = f"ice_mongo_skip_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        _drop_mongo(dest)
        first = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert second.source_snapshot.get("iceberg_read") == "skip"
        assert second.source_snapshot.get("mongo_write") == "skip"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_occ_{tag}"
    ice = f"ice_mongo_occ_mid_{tag}"
    dest = f"ice_mongo_occ_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
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
            copy_iceberg_to_mongo(
                source_cfg=_iceberg_cfg(ice),
                source_schema="default",
                source_table=ice,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_overwrite_replaces_dest():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_ow_{tag}"
    ice = f"ice_mongo_ow_mid_{tag}"
    dest = f"ice_mongo_ow_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_source_count_is_not_scan(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    from pyiceberg.table import DataScan

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_scan_{tag}"
    ice = f"ice_mongo_scan_mid_{tag}"
    dest = f"ice_mongo_scan_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 80)
        _drop_mongo(dest)

        def _no_count(self):
            raise AssertionError("Iceberg→Mongo source COUNT must not scan().count()")

        def _no_arrow(self):
            raise AssertionError("Iceberg→Mongo COPY must not scan().to_arrow()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)
        result = copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_est_{tag}"
    ice = f"ice_mongo_est_mid_{tag}"
    dest = f"ice_mongo_est_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 80)
        _drop_mongo(dest)
        copy_iceberg_to_mongo(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        assert _dest_count(dest) == 80
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()


@requires_rest
def test_live_iceberg_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_ICEBERG_MONGO_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_mongo_str_{tag}"
    ice = f"ice_mongo_str_mid_{tag}"
    dest = f"ice_mongo_str_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_iceberg_cfg(ice), "format": "iceberg"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "iceberg_parquet_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_iceberg(ice)
        _drop_mongo(dest)
        mysql.close()
