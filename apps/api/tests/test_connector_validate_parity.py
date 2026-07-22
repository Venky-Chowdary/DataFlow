"""Connectors Test and Validate G2 must share one probe path for every driver."""

from __future__ import annotations

from services.connector_probe import probe_cfg_from_saved, probe_saved_connector
from services.connector_store import SavedConnector, create_connector
from services.preflight_service import inspect_destination_for_preflight


def test_probe_cfg_from_saved_includes_connection_string_secrets():
    conn = SavedConnector(
        id="c1",
        name="Redis",
        type="redis",
        role="both",
        host="",
        port=0,
        database="0",
        username="",
        password="",
        connection_string="redis://:s3cret@redis.example:6379/0",
        ssl=False,
    )
    cfg = probe_cfg_from_saved(conn)
    assert cfg["connection_string"].startswith("redis://")
    assert "s3cret" in cfg["connection_string"]
    assert cfg["host"] == ""


def test_inspect_destination_uses_saved_probe_not_empty_form(
    monkeypatch, tmp_path,
):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")

    conn = create_connector({
        "name": "Redis Dest",
        "type": "redis",
        "host": "",
        "port": 0,
        "database": "0",
        "username": "",
        "password": "",
        "connection_string": "redis://:correct@127.0.0.1:6379/0",
        "ssl": False,
    })

    calls: list[dict] = []

    def _fake_probe(db_type: str, cfg: dict):
        calls.append({"db_type": db_type, **{k: cfg.get(k) for k in (
            "host", "password", "connection_string", "username",
        )}})
        assert cfg.get("connection_string"), "Validate must pass connection_string"
        assert "correct" in (cfg.get("connection_string") or "")
        # Must NOT invent localhost auth when URI is present.
        return True, "Redis connected"

    monkeypatch.setattr("src.transfer.connector_registry.run_probe", _fake_probe)
    monkeypatch.setattr(
        "src.transfer.endpoint_intelligence.introspect_endpoint",
        lambda endpoint: {
            "connected": True,
            "message": "ok",
            "schema": {},
            "columns": [],
            "objects": [{"name": "db0"}],
        },
    )

    # Studio sends connector_id only — empty password / connection_string / host
    # (same shape as TransferPage when a saved connector is selected).
    meta = inspect_destination_for_preflight(
        connector_id=conn.id,
        dest_type="",
        dest_host="",
        dest_port=0,
        dest_username="",
        dest_password="",
        dest_connection_string="",
        dest_table="csvtestfile",
    )
    assert meta["connected"] is True, meta
    assert calls, "probe must run"
    assert calls[0]["connection_string"].startswith("redis://")

    ok, msg, cfg = probe_saved_connector(conn.id)
    assert ok is True
    assert cfg["connection_string"] == calls[0]["connection_string"]


def test_inspect_destination_surfaces_auth_failure_from_same_probe(monkeypatch, tmp_path):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")

    conn = create_connector({
        "name": "Bad Redis",
        "type": "redis",
        "host": "127.0.0.1",
        "port": 6379,
        "database": "0",
        "username": "default",
        "password": "wrong",
        "connection_string": "",
        "ssl": False,
    })

    monkeypatch.setattr(
        "src.transfer.connector_registry.run_probe",
        lambda *_a, **_k: (False, "invalid username-password pair or user is disabled."),
    )

    meta = inspect_destination_for_preflight(connector_id=conn.id)
    assert meta["connected"] is False
    assert "username-password" in (meta["message"] or "").lower()
