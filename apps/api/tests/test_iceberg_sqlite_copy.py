"""Iceberg snapshot Parquet → SQLite executemany — dest COUNT(*)."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_iceberg_pg import iceberg_type_is_copy_safe  # noqa: E402
from services.copy_iceberg_sqlite import (  # noqa: E402
    copy_iceberg_to_sqlite,
    iceberg_sqlite_copy_enabled,
    iceberg_value_to_sqlite,
)
from services.copy_sqlite_iceberg import copy_sqlite_to_iceberg  # noqa: E402
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


def _sqlite_cfg(path: Path | str, table: str) -> dict:
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


def _seed_sqlite(path: Path, table: str, rows: int) -> None:
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


def _iceberg_count(table: str) -> int:
    cfg = _iceberg_cfg(table)
    n = destination_row_count("iceberg", cfg, schema="default", table_name=table)
    assert n is not None
    return int(n)


def _sqlite_count(path: Path, table: str) -> int:
    n = destination_row_count(
        "sqlite", _sqlite_cfg(path, table), schema="", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_iceberg_from_sqlite(src: Path, src_table: str, ice: str, rows: int) -> None:
    _seed_sqlite(src, src_table, rows)
    _drop_iceberg(ice)
    result = copy_sqlite_to_iceberg(
        source_cfg=_sqlite_cfg(src, src_table),
        source_table=src_table,
        dest_cfg=_iceberg_cfg(ice),
        dest_table=ice,
        pairs=[("id", "id"), ("label", "label")],
        iceberg_ddls=["long", "string"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _iceberg_count(ice) == rows


def test_iceberg_sqlite_copy_safe_types():
    assert iceberg_type_is_copy_safe("string") is True
    assert iceberg_type_is_copy_safe("long") is True
    assert iceberg_type_is_copy_safe("date") is True
    assert iceberg_type_is_copy_safe("INTEGER") is True
    assert iceberg_type_is_copy_safe("binary") is False
    assert iceberg_type_is_copy_safe("uuid") is False
    assert iceberg_type_is_copy_safe("timestamptz") is False
    assert iceberg_type_is_copy_safe("list<string>") is False
    assert iceberg_type_is_copy_safe("map<string,string>") is False
    assert iceberg_type_is_copy_safe("struct<a:int>") is False
    assert iceberg_type_is_copy_safe("object") is False


def test_iceberg_sqlite_date_binds_as_text():
    assert iceberg_value_to_sqlite(date(2020, 1, 2)) == "2020-01-02"
    assert iceberg_value_to_sqlite(None) is None
    assert iceberg_value_to_sqlite("") == ""
    with pytest.raises(FastPathUnavailable, match="binary"):
        iceberg_value_to_sqlite(b"x")
    with pytest.raises(FastPathUnavailable, match="nested"):
        iceberg_value_to_sqlite({"a": 1})


def test_iceberg_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_ICEBERG_SQLITE_COPY", "0")
    assert iceberg_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_iceberg_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_sqlite_cfg(":memory:", "orders"),
            dest_table="orders",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_iceberg_sqlite_blob_dest_declines(tmp_path):
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg("missing"),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("payload", "payload")],
            sqlite_ddls=["BLOB"],
            replace_destination=True,
        )


@requires_rest
def test_live_iceberg_sqlite_dest_count(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_mid_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 800)
        result = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("iceberg_read") == "snapshot_parquet"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_empty_string_and_null_preserved(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_null_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_null_dst"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        _drop_iceberg(ice)
        copy_sqlite_to_iceberg(
            source_cfg=_sqlite_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_iceberg_cfg(ice),
            dest_table=ice,
            pairs=[("id", "id"), ("label", "label")],
            iceberg_ddls=["long", "string"],
            replace_destination=True,
        )
        result = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _sqlite_count(dest_db, dest) == 3
        out = sqlite3.connect(dest_db)
        try:
            rows = out.execute(f'SELECT id, label FROM "{dest}" ORDER BY id').fetchall()
        finally:
            out.close()
        assert rows == [(1, None), (2, ""), (3, "x")]
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_skip_when_dest_count_matches(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_skip_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_skip_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 800)
        first = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_occupied_mismatch_declines(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_occ_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_occ_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.executemany(
            f'INSERT INTO "{dest}" (id, label) VALUES (?, ?)',
            [(1, "ghost"), (2, "ghost")],
        )
        conn.commit()
        conn.close()
        with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
            copy_iceberg_to_sqlite(
                source_cfg=_iceberg_cfg(ice),
                source_schema="default",
                source_table=ice,
                dest_cfg=_sqlite_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _sqlite_count(dest_db, dest) == 2
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_overwrite_replaces_dest(tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_ow_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_ow_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.execute(f'INSERT INTO "{dest}" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_source_count_is_not_scan(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    from pyiceberg.table import DataScan

    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_scan_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_scan_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 80)

        def _no_count(self):
            raise AssertionError("Iceberg→SQLite source COUNT must not scan().count()")

        def _no_arrow(self):
            raise AssertionError("Iceberg→SQLite COPY must not scan().to_arrow()")

        monkeypatch.setattr(DataScan, "count", _no_count)
        monkeypatch.setattr(DataScan, "to_arrow", _no_arrow)
        result = copy_iceberg_to_sqlite(
            source_cfg=_iceberg_cfg(ice),
            source_schema="default",
            source_table=ice,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _sqlite_count(dest_db, dest) == 80
    finally:
        _drop_iceberg(ice)


@requires_rest
def test_live_iceberg_sqlite_stream_load_method(monkeypatch, tmp_path):
    pytest.importorskip("pyiceberg")
    pytest.importorskip("pyarrow")
    monkeypatch.delenv("DATAFLOW_ICEBERG_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ice = f"ice_sqlite_stream_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ice_sqlite_stream_dst"
    try:
        _seed_iceberg_from_sqlite(src, "src_t", ice, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ice-sqlite-copy-{tag}"
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
            "database", {**_sqlite_cfg(dest_db, dest), "format": "sqlite"}
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
        assert summary.get("load_method") == "iceberg_parquet_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("SQLite" in line for line in ddl_log)
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_iceberg(ice)
