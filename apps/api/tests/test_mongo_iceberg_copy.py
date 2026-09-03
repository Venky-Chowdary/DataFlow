"""MongoDB → Iceberg snapshot find + CSV + catalog snapshot — dest footer COUNT."""

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
from services.copy_mongo_iceberg import (  # noqa: E402
    copy_mongo_to_iceberg,
    mongo_iceberg_copy_enabled,
)
from services.copy_mongo_pg import mongo_type_is_copy_safe  # noqa: E402
from services.copy_mysql_iceberg import copy_mysql_to_iceberg  # noqa: E402
from services.copy_mysql_mongo import copy_mysql_to_mongo  # noqa: E402
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


def _mongo_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _dest_count(table: str) -> int:
    n = destination_row_count(
        "iceberg", _iceberg_cfg(table), schema="default", table_name=table
    )
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
    assert _dest_count(ice) == rows


def test_mongo_iceberg_copy_safe_types():
    assert mongo_type_is_copy_safe("string") is True
    assert mongo_type_is_copy_safe("long") is True
    assert mongo_type_is_copy_safe("date") is True
    assert mongo_type_is_copy_safe("BIGINT") is True
    assert mongo_type_is_copy_safe("object") is False
    assert mongo_type_is_copy_safe("array") is False
    assert mongo_type_is_copy_safe("bindata") is False
    assert mongo_type_is_copy_safe("timestamptz") is False
    from services.copy_iceberg_mongo import iceberg_mongo_type_is_copy_safe

    assert iceberg_mongo_type_is_copy_safe("string") is True
    assert iceberg_mongo_type_is_copy_safe("timestamp") is False


def test_mongo_iceberg_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_ICEBERG_COPY", "0")
    assert mongo_iceberg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_iceberg_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


@requires_rest
def test_live_mongo_iceberg_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_ICEBERG_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_src_{tag}"
    mid = f"mongo_ice_mid_{tag}"
    dest = f"mongo_ice_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        _drop_iceberg(dest)
        result = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert result.source_snapshot.get("iceberg_write") == "append"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_empty_string_and_null_preserved():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    tag = uuid.uuid4().hex[:8]
    mid = f"mongo_ice_null_{tag}"
    dest = f"mongo_ice_null_dst_{tag}"
    try:
        _drop_mongo(mid)
        client, coll = _mongo_coll(mid)
        try:
            coll.insert_many(
                [
                    {"id": 1, "label": None},
                    {"id": 2, "label": ""},
                    {"id": 3, "label": "x"},
                ],
                ordered=False,
            )
        finally:
            client.close()
        _drop_iceberg(dest)
        result = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        cfg = _iceberg_cfg(dest)
        parsed = parse_iceberg_catalog_config(cfg)
        tbl = load_catalog(cfg).load_table(parsed["namespace"] + (parsed["table_name"],))
        arrow = tbl.scan().to_arrow().sort_by("id")
        labels = [arrow.column("label")[i].as_py() for i in range(arrow.num_rows)]
        assert labels == [None, "", "x"]
    finally:
        _drop_mongo(mid)
        _drop_iceberg(dest)


@requires_rest
def test_live_mongo_iceberg_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_skip_{tag}"
    mid = f"mongo_ice_skip_mid_{tag}"
    dest = f"mongo_ice_skip_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        _drop_iceberg(dest)
        first = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert second.source_snapshot.get("iceberg_write") == "skip"
        assert second.source_snapshot.get("mongo_read") == "skip"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_occ_{tag}"
    mid = f"mongo_ice_occ_mid_{tag}"
    ice_src = f"mongo_ice_occ_ice_{tag}"
    dest = f"mongo_ice_occ_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        _seed_iceberg_from_mysql(mysql, ice_src, dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Iceberg dest"):
            copy_mongo_to_iceberg(
                source_cfg=_mongo_cfg(mid),
                source_table=mid,
                dest_cfg=_iceberg_cfg(dest),
                dest_table=dest,
                dest_schema="default",
                pairs=[("id", "id"), ("label", "label")],
                iceberg_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{ice_src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_overwrite_replaces_snapshot():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_ow_{tag}"
    mid = f"mongo_ice_ow_mid_{tag}"
    ice_src = f"mongo_ice_ow_ice_{tag}"
    dest = f"mongo_ice_ow_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        _seed_iceberg_from_mysql(mysql, ice_src, dest, 1)
        result = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("iceberg_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{ice_src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_source_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_est_{tag}"
    mid = f"mongo_ice_est_mid_{tag}"
    dest = f"mongo_ice_est_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 80)
        _drop_iceberg(dest)

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo source COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
        assert _mongo_count(mid) == 80
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_dest_count_is_not_scan_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    from pyiceberg.table import DataScan

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_scan_{tag}"
    mid = f"mongo_ice_scan_mid_{tag}"
    dest = f"mongo_ice_scan_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 80)
        _drop_iceberg(dest)
        copy_mongo_to_iceberg(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            dest_schema="default",
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )

        def _no_count(self):
            raise AssertionError("Mongo→Iceberg dest COUNT must not scan().count()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        assert _dest_count(dest) == 80
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()


@requires_rest
def test_live_mongo_iceberg_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_ICEBERG_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_ice_str_{tag}"
    mid = f"mongo_ice_str_mid_{tag}"
    dest = f"mongo_ice_str_dst_{tag}"
    try:
        _seed_mongo_from_mysql(mysql, src, mid, 800)
        _drop_iceberg(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-ice-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(mid), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_iceberg_cfg(dest), "format": "iceberg"}
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
        assert summary.get("load_method") == "mongo_snapshot_find_csv_iceberg_snapshot"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
        _drop_mongo(mid)
        _drop_iceberg(dest)
        mysql.close()
