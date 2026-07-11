"""Sync cursor watermark tests."""

import json

from services.sync_cursor import (
    build_cursor_key,
    map_source_to_target,
    max_cursor_value,
    requires_incremental,
    requires_upsert,
    resolve_sync_contract,
    set_watermark,
    get_watermark,
)


def test_resolve_sync_contract():
    contract = resolve_sync_contract([
        {"name": "orders", "sync_mode": "incremental_append", "cursor_field": "updated_at", "selected": True},
    ])
    assert contract is not None
    assert contract.sync_mode == "incremental_append"
    assert contract.cursor_field == "updated_at"


def test_requires_incremental_and_upsert():
    assert requires_incremental("incremental_append")
    assert requires_incremental("cdc")
    assert not requires_incremental("full_refresh_overwrite")
    assert requires_upsert("incremental_deduped")
    assert not requires_upsert("incremental_append")


def test_watermark_roundtrip(tmp_path, monkeypatch):
    store = tmp_path / "sync_cursors.json"
    monkeypatch.setattr("services.sync_cursor.STORE_PATH", store)

    key = build_cursor_key(
        source_type="postgresql",
        source_database="src",
        source_object="orders",
        dest_type="postgresql",
        dest_database="dst",
        dest_object="orders",
        stream_name="orders",
    )
    assert get_watermark(key) is None
    set_watermark(key, "2024-01-01T00:00:00Z", metadata={"job_id": "j1"})
    assert get_watermark(key) == "2024-01-01T00:00:00Z"
    data = json.loads(store.read_text())
    assert data["cursors"][0]["metadata"]["job_id"] == "j1"


def test_max_cursor_value():
    headers = ["id", "updated_at"]
    rows = [["1", "2024-01-01"], ["2", "2024-06-01"]]
    assert max_cursor_value(rows, headers, "updated_at") == "2024-06-01"


def test_map_source_to_target():
    mappings = [{"source": "order_id", "target": "id"}]
    assert map_source_to_target("order_id", mappings) == "id"
    assert map_source_to_target("missing", mappings) == "missing"
