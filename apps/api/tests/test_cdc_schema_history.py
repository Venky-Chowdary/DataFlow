"""Tests for native CDC schema history store."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import cdc_schema_history as hist


def test_record_rebuild_list_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(hist, "STORE_DIR", tmp_path / "cdc_schema_history")
    monkeypatch.setattr(hist, "_mongo", lambda: None)

    source_key = "postgresql:localhost:5432/app"
    table = "public.orders"

    e1 = hist.record_ddl(
        source_key,
        table,
        ddl="CREATE TABLE orders (id int, name text)",
        offset="0/16B374D0",
        schema_snapshot={
            "columns": {"id": "integer", "name": "text"},
            "nullable": {"id": False, "name": True},
            "primary_key": ["id"],
        },
    )
    assert e1["version"] == 1

    e2 = hist.record_ddl(
        source_key,
        table,
        ddl="ALTER TABLE orders ADD COLUMN email text",
        offset="0/16B38000",
        schema_snapshot={
            "columns": {"id": "integer", "name": "text", "email": "text"},
            "nullable": {"id": False, "name": True, "email": True},
            "primary_key": ["id"],
        },
    )
    assert e2["version"] == 2

    history = hist.list_history(source_key, table)
    assert len(history) == 2
    assert history[0]["version"] == 1
    assert history[1]["ddl"].startswith("ALTER")

    latest = hist.rebuild_schema(source_key, table)
    assert "email" in latest["columns"]

    mid = hist.rebuild_schema(source_key, table, up_to_offset="0/16B374D0")
    assert "email" not in mid["columns"]
    assert mid["columns"]["name"] == "text"

    by_version = hist.rebuild_schema(source_key, table, up_to_offset=1)
    assert set(by_version["columns"]) == {"id", "name"}

    assert hist.last_ddl_at(source_key, table)


def test_binlog_offset_ordering(tmp_path, monkeypatch):
    monkeypatch.setattr(hist, "STORE_DIR", tmp_path / "cdc_schema_history")
    monkeypatch.setattr(hist, "_mongo", lambda: None)

    source_key = "mysql:localhost:3306/shop"
    table = "orders"
    hist.record_ddl(
        source_key,
        table,
        ddl="SNAPSHOT",
        offset={"file": "mysql-bin.000001", "pos": 100},
        schema_snapshot={"columns": {"id": "int"}},
    )
    hist.record_ddl(
        source_key,
        table,
        ddl="ALTER TABLE orders ADD col2 int",
        offset={"file": "mysql-bin.000001", "pos": 500},
        schema_snapshot={"columns": {"id": "int", "col2": "int"}},
    )

    mid = hist.rebuild_schema(
        source_key,
        table,
        up_to_offset={"file": "mysql-bin.000001", "pos": 100},
    )
    assert set(mid["columns"]) == {"id"}

    latest = hist.rebuild_schema(
        source_key,
        table,
        up_to_offset={"file": "mysql-bin.000001", "pos": 500},
    )
    assert "col2" in latest["columns"]


def test_connection_fingerprint_prefers_connector_id():
    fp = hist.connection_fingerprint({"host": "h", "port": 1, "database": "d"}, connector_id="c-9")
    assert fp == "connector:c-9"
    fp2 = hist.connection_fingerprint({"host": "h", "port": 5432, "database": "db", "type": "postgresql"})
    assert "postgresql" in fp2 and "db" in fp2
