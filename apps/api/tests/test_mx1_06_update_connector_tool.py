"""QA MX1-06 — Pilot had no update_connector: changing host/port/password meant
delete + recreate (which also breaks every schedule bound to the connector id).

The tool stages the change behind Confirm like every other connector mutation
and applies it through the same ``PUT /saved-connectors/{id}`` handler the
Connectors screen uses. Secrets never appear in the preview.
"""

from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace

import pytest

import src.ai.copilot.lifecycle_tools as lt
from src.ai.copilot.tool_permissions import can_confirm_kind

_EXISTING = {
    "id": "c-wh", "name": "Warehouse", "type": "postgresql", "host": "old-host",
    "port": 5432, "database": "dw", "role": "both",
}


@pytest.fixture
def staged(monkeypatch):
    acks: list[dict] = []

    class Ledger:
        def put(self, *, kind, payload, preview):
            acks.append({"kind": kind, "payload": payload, "preview": preview})
            return f"ack-{len(acks)}"

    import src.ai.copilot.ack_ledger as ledger_mod

    monkeypatch.setattr(ledger_mod, "get_ack_ledger", lambda: Ledger())
    monkeypatch.setattr(lt, "_connector_dict", lambda cid="", name="": dict(_EXISTING) if (name or cid) in {"Warehouse", "c-wh"} else None)
    return acks


def test_update_connector_stages_change_without_leaking_secrets(staged):
    tr = lt.update_connector(name="Warehouse", host="new-host", port=6543, password="N3wSecret")
    assert tr.success, tr.error
    out = tr.output
    assert out["action"] == "update_connector" and out["requires_confirm"] is True
    assert "N3wSecret" not in repr(out)
    assert out["preview"]["changes"] == {"host": "new-host", "port": 6543, "password": "(changed — hidden)"}
    assert staged[0]["kind"] == "update_connector"
    assert staged[0]["payload"] == {
        "connector_id": "c-wh", "name": "Warehouse",
        "changes": {"host": "new-host", "port": 6543, "password": "N3wSecret"},
    }


def test_update_connector_requires_a_change_and_a_known_connector(staged):
    assert "What should change" in lt.update_connector(name="Warehouse").error
    assert "No connector matched" in lt.update_connector(name="Nope", host="x").error
    assert lt.update_connector(host="x").error.startswith("Which connector")


def test_update_connector_is_a_registered_confirmable_tool():
    from src.ai.copilot.tools import DataPilotTools

    assert "update_connector" in lt.LIFECYCLE_TOOL_NAMES
    assert lt.ACK_KIND_BY_TOOL["update_connector"] == "update_connector"
    assert can_confirm_kind("viewer", "update_connector") is False
    assert can_confirm_kind("editor", "update_connector") is True
    assert hasattr(DataPilotTools, "_update_connector")


def test_confirm_applies_change_through_the_saved_connector_put(monkeypatch):
    from fastapi import BackgroundTasks

    from src.routers import copilot_router

    scr = importlib.import_module("src.routers.saved_connectors_router")
    existing = SimpleNamespace(**{**_EXISTING, "username": "etl", "password": "OldPw",
                                  "schema": "public", "ssl": True, "connection_string": ""})
    monkeypatch.setattr(scr, "get_connector", lambda cid, workspace_id=None: existing if cid == "c-wh" else None)
    calls: list = []
    monkeypatch.setattr(scr, "update_saved_connector",
                        lambda cid, body, req, ws: calls.append((cid, body)) or {"id": cid, "name": body.name})
    out = asyncio.run(copilot_router._run_lifecycle_confirm(
        "update_connector",
        {"connector_id": "c-wh", "name": "Warehouse", "changes": {"host": "new-host", "port": 6543}},
        SimpleNamespace(headers={}), BackgroundTasks(),
    ))
    (cid, body), = calls
    assert cid == "c-wh"
    assert (body.host, body.port, body.database, body.username, body.password) == ("new-host", 6543, "dw", "etl", "OldPw")
    assert out["connector_id"] == "c-wh" and "test_connector" in out["next"]
