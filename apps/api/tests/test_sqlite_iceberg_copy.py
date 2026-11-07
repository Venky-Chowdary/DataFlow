"""SQLite SELECT CSV → Iceberg snapshot — dest COUNT from file footers."""

from __future__ import annotations

import os
import socket
import sqlite3
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
from services.copy_sqlite_iceberg import (  # noqa: E402
    copy_sqlite_to_iceberg,
    sqlite_iceberg_copy_enabled,
    sqlite_iceberg_type_is_copy_safe,
)
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


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
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


def _seed(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(
        f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
    )
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


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


def test_sqlite_iceberg_copy_safe_types():
    assert sqlite_iceberg_type_is_copy_safe("INTEGER") is True
    assert sqlite_iceberg_type_is_copy_safe("TEXT") is True
    assert sqlite_iceberg_type_is_copy_safe("DATE") is True
    assert sqlite_iceberg_type_is_copy_safe("REAL") is True
    assert sqlite_iceberg_type_is_copy_safe("BOOLEAN") is True
    assert sqlite_iceberg_type_is_copy_safe("DATETIME") is False
    assert sqlite_iceberg_type_is_copy_safe("TIMESTAMP") is False
    assert sqlite_iceberg_type_is_copy_safe("JSON") is False
    assert sqlite_iceberg_type_is_copy_safe("BLOB") is False


def test_sqlite_iceberg_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_ICEBERG_COPY", "0")
    assert sqlite_iceberg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_iceberg(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_iceberg_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


def test_sqlite_iceberg_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_iceberg(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_iceberg_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            iceberg_ddls=["long"],
            replace_destination=True,
        )


@requires_rest
def test_live_sqlite_iceberg_dest_count(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_SQLITE_ICEBERG_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_dst_{tag}"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
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
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_empty_string_and_null_preserved(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_catalog import load_catalog, parse_iceberg_catalog_config

    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_null_{tag}"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        result = copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
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
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_skip_when_dest_count_matches(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_skip_{tag}"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
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
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_occupied_mismatch_declines(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import write_mapped_rows

    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_occ_{tag}"
    _seed(src, "src_t", 800)
    try:
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
            copy_sqlite_to_iceberg(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_iceberg_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                iceberg_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_overwrite_replaces_snapshot(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from connectors.iceberg_writer import write_mapped_rows

    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_ow_{tag}"
    _seed(src, "src_t", 800)
    try:
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
        result = copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("iceberg_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_dest_count_is_not_scan_count(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_scan_{tag}"
    _seed(src, "src_t", 80)
    try:
        _drop_iceberg(dest)
        copy_sqlite_to_iceberg(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_iceberg_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )

        def _no_count(self):
            raise AssertionError("SQLite→Iceberg dest COUNT must not scan().count()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        assert _dest_count(dest) == 80
    finally:
        _drop_iceberg(dest)


@requires_rest
def test_live_sqlite_iceberg_stream_load_method(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_SQLITE_ICEBERG_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ice_stream_{tag}"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-ice-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
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
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "TEXT"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_sqlite_csv_iceberg_snapshot"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Iceberg" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_iceberg(dest)
