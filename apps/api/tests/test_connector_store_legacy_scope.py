"""connector_store includes legacy (workspace_id "") rows only when the bound
caller's scope says so (D162): a fresh signup account's never does."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import connector_store  # noqa: E402
from services.workspace_filter import WorkspaceFilter, bound_workspace_filter  # noqa: E402


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    mine = connector_store.create_connector(
        {"name": "mine", "type": "postgresql", "host": "h", "port": 5432, "workspace_id": "ws-b"}
    )
    other = connector_store.create_connector(
        {"name": "other", "type": "postgresql", "host": "h", "port": 5432, "workspace_id": "ws-a"}
    )
    legacy = connector_store.create_connector(
        {"name": "legacy", "type": "postgresql", "host": "h", "port": 5432, "workspace_id": ""}
    )
    return mine, other, legacy


def _names(**kwargs) -> set[str]:
    return {c.name for c in connector_store.list_connectors(**kwargs)}


def test_system_scope_keeps_the_legacy_inclusion(store):
    assert _names(workspace_id="ws-b") == {"mine", "legacy"}


def test_a_caller_without_the_legacy_flag_never_sees_legacy_rows(store):
    mine, other, legacy = store
    no_legacy = WorkspaceFilter(frozenset({"ws-b"}), False)
    with bound_workspace_filter(no_legacy, "ws-b", "b"):
        assert _names() == {"mine"}
        assert _names(workspace_id="ws-b") == {"mine"}  # explicit workspace does not re-admit them
        assert connector_store.get_connector(legacy.id) is None
        assert connector_store.get_connector(legacy.id, workspace_id="ws-b") is None
        assert connector_store.get_connector(other.id) is None
        assert connector_store.update_connector(legacy.id, {"name": "x"}) is None
        assert connector_store.delete_connector(legacy.id) is False
    assert connector_store.get_connector(legacy.id) is not None


def test_a_caller_with_the_legacy_flag_still_sees_them(store):
    with bound_workspace_filter(WorkspaceFilter(frozenset({"ws-b"}), True), "ws-b", "b"):
        assert _names() == {"mine", "legacy"}
