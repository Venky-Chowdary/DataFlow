"""Iceberg → MySQL snapshot Parquet + STRICT LOAD DATA — dest COUNT(*)."""

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
from services.copy_iceberg_mysql import (  # noqa: E402
    copy_iceberg_to_mysql,
    iceberg_mysql_copy_enabled,
)
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
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


def _iceberg_count(table: str) -> int:
    cfg = _iceberg_cfg(table)
    n = destination_row_count("iceberg", cfg, schema="default", table_name=table)
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


def test_iceberg_mysql_copy_safe_types():
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("date") is True
    assert iceberg_type_is_copy_safe("VARCHAR(32)") is True
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("uuid") is False
    assert iceberg_type_is_copy_safe("timestamptz") is False
    assert iceberg_type_is_copy_safe("list<string>") is False


def test_iceberg_mysql_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ICEBERG_MYSQL_COPY", "0")
    assert iceberg_mysql_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_mysql_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            mysql_ddls=["BIGINT"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_mysql_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_src_{tag}"
    ice = f"ice_my_mid_{tag}"
    dest = f"ice_my_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        result = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        assert result.source_snapshot.get("load_data") == "tempfile"
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_empty_string_and_null_preserved():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_null_{tag}"
    ice = f"ice_my_null_mid_{tag}"
    dest = f"ice_my_null_dst_{tag}"
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
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        result = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
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
        assert rows == [(1, None), (2, ""), (3, "x")]
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_skip_{tag}"
    ice = f"ice_my_skip_mid_{tag}"
    dest = f"ice_my_skip_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        first = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_occ_{tag}"
    ice = f"ice_my_occ_mid_{tag}"
    dest = f"ice_my_occ_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f"INSERT INTO `{dest}` (id, label) VALUES (1, 'ghost'), (2, 'ghost')"
            )
        with pytest.raises(FastPathUnavailable, match="occupied MySQL dest"):
            copy_iceberg_to_mysql(
                source_cfg=_iceberg_cfg(ice),
                source_schema="default",
                source_table=ice,
                dest_cfg=_mysql_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mysql_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 2
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_overwrite_replaces_dest():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_ow_{tag}"
    ice = f"ice_my_ow_mid_{tag}"
    dest = f"ice_my_ow_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
            cur.execute(
                f"CREATE TABLE `{dest}` ("
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(f"INSERT INTO `{dest}` (id, label) VALUES (1, 'ghost')")
        result = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_source_count_is_not_scan(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_scan_{tag}"
    ice = f"ice_my_scan_mid_{tag}"
    dest = f"ice_my_scan_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 80)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")

        def _no_count(self):
            raise AssertionError("Iceberg→MySQL source COUNT must not scan().count()")

        def _no_arrow(self):
            raise AssertionError("Iceberg→MySQL COPY must not scan().to_arrow()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)
        result = copy_iceberg_to_mysql(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_mysql_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mysql_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 80
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()


@requires_rest
def test_live_iceberg_mysql_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_MYSQL_COPY", raising=False)
    mysql = _mysql_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_my_stream_{tag}"
    ice = f"ice_my_stream_mid_{tag}"
    dest = f"ice_my_stream_dst_{tag}"
    try:
        _seed_iceberg_from_mysql(mysql, src, ice, 800)
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-my-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database",
            {
                "format": "iceberg",
                "connection_string": REST_URI,
                "warehouse": REST_WAREHOUSE,
                "table": ice,
                "schema": "default",
                "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
            },
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
        assert summary.get("load_method") == "iceberg_parquet_load_data_mysql"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        with mysql.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM `{dest}`")
            assert int(cur.fetchone()[0]) == 800
    finally:
        with mysql.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src}`")
            cur.execute(f"DROP TABLE IF EXISTS `{dest}`")
        _drop_iceberg(ice)
        mysql.close()
