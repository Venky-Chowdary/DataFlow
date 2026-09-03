"""Iceberg → SQL Server snapshot Parquet + fast_executemany — dest COUNT(*)."""

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
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
from services.copy_iceberg_sqlserver import (  # noqa: E402
    copy_iceberg_to_sqlserver,
    iceberg_sqlserver_copy_enabled,
)
from services.copy_sqlserver_iceberg import copy_sqlserver_to_iceberg  # noqa: E402
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


def _iceberg_cfg(table: str) -> dict:
    return {
        "type": "iceberg",
        "connection_string": REST_URI,
        "warehouse": REST_WAREHOUSE,
        "table": table,
        "schema": "default",
        "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
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


def _seed_iceberg_from_ss(ss, src: str, ice: str, rows: int) -> None:
    _seed_ss(ss.cursor(), src, rows)
    _drop_iceberg(ice)
    result = copy_sqlserver_to_iceberg(
        source_cfg=_ss_cfg(),
        source_table=src,
        dest_cfg=_iceberg_cfg(ice),
        dest_table=ice,
        pairs=[("id", "id"), ("label", "label")],
        iceberg_ddls=["long", "string"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _iceberg_count(ice) == rows


def test_iceberg_sqlserver_copy_safe_types():
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("VARCHAR(32)") is True
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("uuid") is False
    assert iceberg_type_is_copy_safe("timestamptz") is False


def test_iceberg_sqlserver_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ICEBERG_SQLSERVER_COPY", "0")
    assert iceberg_sqlserver_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_ss_cfg(),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlserver_ddls=["BIGINT"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_sqlserver_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_src_{tag}"
    ice = f"ice_ss_mid_{tag}"
    dest = f"ice_ss_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 800)
        _drop_ss(ss.cursor(), dest)
        result = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_empty_string_and_null_preserved():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_null_{tag}"
    ice = f"ice_ss_null_mid_{tag}"
    dest = f"ice_ss_null_dst_{tag}"
    try:
        cur = ss.cursor()
        _drop_ss(cur, src)
        cur.execute(
            f"CREATE TABLE dbo.[{src}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{src}] (id, label) VALUES (1, NULL), (2, N''), (3, N'x')"
        )
        _drop_iceberg(ice)
        copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(ice),
            dest_table=ice,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        _drop_ss(ss.cursor(), dest)
        result = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
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
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_skip_{tag}"
    ice = f"ice_ss_skip_mid_{tag}"
    dest = f"ice_ss_skip_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 800)
        _drop_ss(ss.cursor(), dest)
        first = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_occ_{tag}"
    ice = f"ice_ss_occ_mid_{tag}"
    dest = f"ice_ss_occ_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(
            f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'ghost'), (2, N'ghost')"
        )
        with pytest.raises(FastPathUnavailable, match="occupied SQL Server dest"):
            copy_iceberg_to_sqlserver(
                source_cfg=_iceberg_cfg(ice),
                source_schema="default",
                source_table=ice,
                dest_cfg=_ss_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
                replace_destination=False,
            )
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 2
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_overwrite_replaces_dest():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_ow_{tag}"
    ice = f"ice_ss_ow_mid_{tag}"
    dest = f"ice_ss_ow_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 800)
        cur = ss.cursor()
        _drop_ss(cur, dest)
        cur.execute(
            f"CREATE TABLE dbo.[{dest}] ("
            "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(32) NULL)"
        )
        cur.execute(f"INSERT INTO dbo.[{dest}] (id, label) VALUES (1, N'ghost')")
        result = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_source_count_is_not_scan(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_scan_{tag}"
    ice = f"ice_ss_scan_mid_{tag}"
    dest = f"ice_ss_scan_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 80)
        _drop_ss(ss.cursor(), dest)

        def _no_count(self):
            raise AssertionError("Iceberg→SQL Server source COUNT must not scan().count()")

        def _no_arrow(self):
            raise AssertionError("Iceberg→SQL Server COPY must not scan().to_arrow()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)
        result = copy_iceberg_to_sqlserver(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_ss_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlserver_ddls=["BIGINT", "NVARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 80
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()


@requires_rest
def test_live_iceberg_sqlserver_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_SQLSERVER_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ice_ss_stream_{tag}"
    ice = f"ice_ss_stream_mid_{tag}"
    dest = f"ice_ss_stream_dst_{tag}"
    try:
        _seed_iceberg_from_ss(ss, src, ice, 800)
        _drop_ss(ss.cursor(), dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-ss-copy-{tag}"
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
        assert summary.get("load_method") == "iceberg_parquet_fast_executemany_sqlserver"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        cur = ss.cursor()
        cur.execute(f"SELECT COUNT(*) FROM dbo.[{dest}]")
        assert int(cur.fetchone()[0]) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_ss(ss.cursor(), dest)
        _drop_iceberg(ice)
        ss.close()
