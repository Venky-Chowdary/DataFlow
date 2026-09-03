"""SQL Server → Iceberg SELECT + CSV + snapshot — dest COUNT from file footers."""

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
from services.copy_sqlserver_iceberg import (  # noqa: E402
    copy_sqlserver_to_iceberg,
    sqlserver_iceberg_copy_enabled,
)
from services.copy_sqlserver_pg import sqlserver_type_is_copy_safe  # noqa: E402
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


def _dest_count(table: str) -> int:
    cfg = _iceberg_cfg(table)
    n = destination_row_count("iceberg", cfg, schema="default", table_name=table)
    assert n is not None
    return int(n)


def test_sqlserver_iceberg_copy_safe_types():
    assert sqlserver_type_is_copy_safe("NVARCHAR(32)") is True
    assert sqlserver_type_is_copy_safe("BIGINT") is True
    assert sqlserver_type_is_copy_safe("DATE") is True
    assert sqlserver_type_is_copy_safe("VARBINARY(16)") is False
    assert sqlserver_type_is_copy_safe("XML") is False
    assert sqlserver_type_is_copy_safe("UNIQUEIDENTIFIER") is False


def test_sqlserver_iceberg_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SQLSERVER_ICEBERG_COPY", "0")
    assert sqlserver_iceberg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table="missing",
            dest_cfg=_iceberg_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


@requires_rest
def test_live_sqlserver_iceberg_dest_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_ICEBERG_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_src_{tag}"
    dest = f"ss_ice_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_iceberg(dest)
        result = copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("iceberg_write") == "append"
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_empty_string_and_null_preserved():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_null_{tag}"
    dest = f"ss_ice_null_dst_{tag}"
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
        _drop_iceberg(dest)
        result = copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
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
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_skip_when_dest_count_matches():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_skip_{tag}"
    dest = f"ss_ice_skip_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_iceberg(dest)
        first = copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_occupied_mismatch_declines():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import write_mapped_rows

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_occ_{tag}"
    dest = f"ss_ice_occ_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_iceberg(dest)
        written = write_mapped_rows(
            connection_string=REST_URI,
            warehouse=REST_WAREHOUSE,
            table_name=f"default.{dest}",
            headers=["id", "label"],
            data_rows=[["1", "ghost"], ["2", "ghost"]],
            mappings=[
                {"source": "id", "target": "id", "transform": "direct"},
                {"source": "label", "target": "label", "transform": "direct"},
            ],
            column_types={"id": "BIGINT", "label": "VARCHAR(32)"},
            write_mode="append",
            create_table=True,
            extra={"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
        )
        assert written.ok, written.error
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Iceberg dest"):
            copy_sqlserver_to_iceberg(
                source_cfg=_ss_cfg(),
                source_table=src,
                dest_cfg=_iceberg_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                iceberg_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_overwrite_replaces_snapshot():
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import write_mapped_rows

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_ow_{tag}"
    dest = f"ss_ice_ow_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_iceberg(dest)
        written = write_mapped_rows(
            connection_string=REST_URI,
            warehouse=REST_WAREHOUSE,
            table_name=f"default.{dest}",
            headers=["id", "label"],
            data_rows=[["1", "ghost"]],
            mappings=[
                {"source": "id", "target": "id", "transform": "direct"},
                {"source": "label", "target": "label", "transform": "direct"},
            ],
            column_types={"id": "BIGINT", "label": "VARCHAR(32)"},
            write_mode="append",
            create_table=True,
            extra={"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
        )
        assert written.ok, written.error
        result = copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("iceberg_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_dest_count_is_not_scan_count(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_scan_{tag}"
    dest = f"ss_ice_scan_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 80)
        _drop_iceberg(dest)
        copy_sqlserver_to_iceberg(
            source_cfg=_ss_cfg(),
            source_table=src,
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )

        def _no_count(self):
            raise AssertionError("SQL Server→Iceberg dest COUNT must not scan().count()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        assert _dest_count(dest) == 80
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()


@requires_rest
def test_live_sqlserver_iceberg_stream_load_method(monkeypatch):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_SQLSERVER_ICEBERG_COPY", raising=False)
    ss = _ss_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"ss_ice_stream_{tag}"
    dest = f"ss_ice_stream_dst_{tag}"
    try:
        _seed_ss(ss.cursor(), src, 800)
        _drop_iceberg(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ss-ice-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ss_cfg(), "format": "sqlserver", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database",
            {
                "format": "iceberg",
                "connection_string": REST_URI,
                "warehouse": REST_WAREHOUSE,
                "table": dest,
                "schema": "default",
                "extra": {"catalog_type": "rest", "warehouse": REST_WAREHOUSE},
            },
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
        assert summary.get("load_method") == "select_sqlserver_csv_iceberg_snapshot"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_ss(ss.cursor(), src)
        _drop_iceberg(dest)
        ss.close()
