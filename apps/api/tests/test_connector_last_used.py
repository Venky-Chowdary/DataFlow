"""Saved connectors must remember last_used_at when a transfer actually runs."""

from __future__ import annotations


def test_from_dict_preserves_last_used_at():
    from services.connector_store import SavedConnector

    conn = SavedConnector.from_dict(
        {
            "id": "c1",
            "name": "MySQL venky2001",
            "type": "mysql",
            "role": "both",
            "host": "db.example",
            "last_used_at": "2026-08-15T12:00:00Z",
        }
    )
    assert conn.last_used_at == "2026-08-15T12:00:00Z"
    assert conn.to_dict()["last_used_at"] == "2026-08-15T12:00:00Z"


def test_mark_used_stamps_source_and_dest(tmp_path, monkeypatch):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")

    src = cs.create_connector(
        {"name": "MySQL venky2001", "type": "mysql", "host": "db.example", "port": 3306}
    )
    dst = cs.create_connector(
        {"name": "SnowFlake Dest", "type": "snowflake", "host": "acct.snowflakecomputing.com"}
    )
    unused = cs.create_connector(
        {"name": "Redis", "type": "redis", "host": "localhost", "port": 6379}
    )

    stamped = cs.mark_used(src.id, dst.id, src.id, None, "")
    assert stamped == 2

    assert cs.get_connector(src.id).last_used_at
    assert cs.get_connector(dst.id).last_used_at
    assert cs.get_connector(unused.id).last_used_at is None
    assert cs.get_connector(src.id).last_used_at == cs.get_connector(dst.id).last_used_at
