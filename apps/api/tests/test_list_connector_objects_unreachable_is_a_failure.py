"""An unreachable connector is a failed listing, never "connected: false, 0 objects".

CI on PR 397 (test_d314, BigQuery black hole): when the driver's own 2 s
timeout answered just before the tool deadline did, introspect_endpoint
returned ``connected: False`` with the timeout as its message and
``list_connector_objects`` reported success with an empty object list, which
an assistant reads as "this connector has no tables". Whichever timeout wins,
the answer must be a failure naming the reason.
"""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")


@pytest.fixture
def connector():
    from services.connector_store import create_connector, delete_connector

    conn = create_connector({
        "name": "unreachable-pg-lco", "type": "postgresql", "host": "127.0.0.1",
        "port": 1, "database": "db", "username": "u", "password": "p", "schema": "public",
    })
    try:
        yield conn.id
    finally:
        delete_connector(conn.id)


def _list(monkeypatch, connector_id: str, info: dict):
    import src.transfer.endpoint_intelligence as ei
    from src.ai.copilot.schema_tools import list_connector_objects

    monkeypatch.setattr(ei, "introspect_endpoint", lambda *_a, **_k: dict(info))
    return list_connector_objects(connector_id=connector_id)


def test_a_driver_timeout_is_reported_as_a_failure(monkeypatch, connector):
    result = _list(monkeypatch, connector, {
        "connected": False, "objects": [],
        "message": "Timeout of 2.0s exceeded, last exception: Read timed out.",
    })
    assert not result.success, result
    assert "unreachable-pg-lco" in result.error and "Timeout of 2.0s exceeded" in result.error
    assert result.output == {"connector_name": "unreachable-pg-lco", "connected": False}


def test_an_empty_reachable_connector_still_lists_nothing_successfully(monkeypatch, connector):
    result = _list(monkeypatch, connector, {"connected": True, "objects": [], "message": ""})
    assert result.success, result
    assert result.output["objects"] == [] and result.output["connected"] is True


def test_a_reachable_connector_lists_its_objects(monkeypatch, connector):
    result = _list(monkeypatch, connector, {
        "connected": True, "objects": [{"name": "orders", "type": "table"}], "message": "",
    })
    assert result.success, result
    assert result.output["objects"] == ["orders"]
