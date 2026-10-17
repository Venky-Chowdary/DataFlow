"""The save DTO must keep an SFTP host-key pin the probe told the operator to save."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_host_key_pin_round_trips_through_the_save_dto(monkeypatch, tmp_path: Path):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    monkeypatch.setattr(cs, "_backend_choice", "file")

    from src.routers.saved_connectors_router import ConnectorSaveDTO

    body = ConnectorSaveDTO(
        name="sftp-pin",
        type="sftp",
        role="source",
        host="files.example",
        port=22,
        username="loader",
        host_key="SHA256:abc123",
        host_key_policy="reject",
    )
    data = body.model_dump(by_alias=True)
    assert data["host_key"] == "SHA256:abc123"
    conn = cs.create_connector(data)
    loaded = cs.get_connector(conn.id)
    assert loaded is not None
    assert loaded.extra.get("host_key") == "SHA256:abc123"
    assert loaded.extra.get("host_key_policy") == "reject"

    again = ConnectorSaveDTO(
        name="sftp-pin",
        type="sftp",
        role="source",
        host="files.example",
        port=22,
        username="loader",
    )
    updated = cs.update_connector(conn.id, again.model_dump(by_alias=True))
    assert updated is not None
    assert updated.extra.get("host_key") == "SHA256:abc123"
