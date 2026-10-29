"""QA minor: update_connector setting host, port and host_key in one call drops host_key.

Route: MCP ``tools/call update_connector`` -> ``/api/v1/copilot/confirm`` ->
the saved connector store. Every field the call names is applied, or the call
is refused; none is silently dropped.
"""

from __future__ import annotations

import base64
import hashlib

import pytest

from tests import r4_route_harness as h


def _pin(seed: bytes) -> str:
    return "SHA256:" + base64.b64encode(hashlib.sha256(seed).digest()).decode().rstrip("=")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.mark.parametrize("backend", ["file", "mongo"])
@pytest.mark.parametrize("had_pin", [False, True])
def test_host_port_and_host_key_in_one_update_all_land(client, monkeypatch, had_pin, backend):
    import services.connector_store as store
    from services.connector_store import create_connector, get_connector

    if backend == "mongo":
        # Production's store. The in-memory job store does not hold connectors,
        # so this uses the live local MongoDB (skips when absent).
        pymongo = pytest.importorskip("pymongo")
        if not h.port_up(27017):
            pytest.skip("MongoDB not reachable on :27017")
        db = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)[
            "dataflow_r4_connectors"
        ]
        db["connectors"].delete_many({})
        monkeypatch.setattr(store, "_backend_choice", "mongo")
        monkeypatch.setattr(store, "_mongo_collection", lambda: db["connectors"])

    old_pin, new_pin = _pin(b"old-host"), _pin(b"new-host")
    saved = create_connector({
        "name": f"sftp-pin-{int(had_pin)}", "type": "sftp", "role": "source",
        "workspace_id": "", "host": "127.0.0.1", "port": 2222,
        "username": "dataflow", "password": "dataflow", "database": "uploads",
        **({"host_key": old_pin} if had_pin else {}),
    })
    ok, staged = h.mcp_call(client, "update_connector", {
        "connector_id": saved.id, "host": "127.0.0.2", "port": 2200, "host_key": new_pin,
    })
    assert ok, staged
    assert {"host", "port", "host_key"} <= set(staged["preview"]["changes"]), staged
    resp = client.post(
        "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "re-point sftp"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body.get("ok") is True, body

    after = get_connector(saved.id)
    assert after.host == "127.0.0.2"
    assert int(after.port) == 2200
    assert (after.extra or {}).get("host_key") == new_pin, after.extra
