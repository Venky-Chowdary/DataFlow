"""D238: a connector created with test_first keeps the probe's verdict.

create_connector(test_first=True) ran the connectivity probe before the
Confirm, but the saved connector never recorded it: health read "untested"
until a separate test_connector ran the same probe again.
"""

from __future__ import annotations

import uuid

import pytest

from conftest import spend_pilot_ack


@pytest.fixture
def probe_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.transfer.connector_registry as registry
    import src.transfer.endpoint_intelligence as intel

    monkeypatch.setattr(registry, "run_probe", lambda *_a, **_k: (True, "Connected to Redis 7.2"))
    monkeypatch.setattr(intel, "introspect_endpoint", lambda *_a, **_k: {"connected": True, "objects": ["k"]})


def _create(test_first: bool) -> str:
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute(
        "create_connector",
        {
            "name": f"d238-{uuid.uuid4().hex[:8]}",
            "type": "redis",
            "host": "redis.d238.test",
            "port": 6379,
            "test_first": test_first,
        },
    )
    assert staged.success, staged.error
    return spend_pilot_ack(staged.output["ack_id"], "d238-test")["connector_id"]


def _saved(connector_id: str):
    from services.connector_store import get_connector

    conn = get_connector(connector_id)
    assert conn is not None
    return conn


def test_probed_create_saves_passed_health(probe_passes: None) -> None:
    from services.connector_store import connector_health, delete_connector

    cid = _create(test_first=True)
    try:
        conn = _saved(cid)
        assert conn.last_test_ok is True
        assert conn.last_tested_at
        assert connector_health(conn.to_dict()) == "passed"
    finally:
        delete_connector(cid)


def test_unprobed_create_stays_untested(probe_passes: None) -> None:
    from services.connector_store import connector_health, delete_connector

    cid = _create(test_first=False)
    try:
        conn = _saved(cid)
        assert conn.last_test_ok is None
        assert connector_health(conn.to_dict()) == "untested"
    finally:
        delete_connector(cid)
