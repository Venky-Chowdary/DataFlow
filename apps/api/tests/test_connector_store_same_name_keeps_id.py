"""A same-named connector create must not orphan schedules bound to the old id."""

from __future__ import annotations

import importlib
import json
from pathlib import Path


def _file_store(monkeypatch, tmp_path: Path):
    store = tmp_path / "connectors.json"
    store.write_text('{"connectors": []}', encoding="utf-8")
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    import services.connector_store as cs

    importlib.reload(cs)
    monkeypatch.setattr(cs, "_backend_choice", "file", raising=False)
    return cs, store


def test_same_name_create_keeps_id_and_takes_new_config(monkeypatch, tmp_path: Path) -> None:
    cs, store = _file_store(monkeypatch, tmp_path)
    first = cs.create_connector(
        {"name": "Ops PG", "type": "postgresql", "role": "both", "host": "old-host", "password": "s1"}
    )
    second = cs.create_connector(
        {"name": "Ops PG", "type": "postgresql", "role": "both", "host": "new-host", "password": "s2"}
    )
    assert second.id == first.id
    assert second.host == "new-host"
    assert second.password == "s2"
    live = [c for c in cs.list_connectors() if c.name == "Ops PG"]
    assert [c.id for c in live] == [first.id]
    raw = json.loads(store.read_text(encoding="utf-8"))
    assert [c["id"] for c in raw["connectors"] if c["name"] == "Ops PG"] == [first.id]
    assert cs.get_connector(first.id) is not None


def test_same_name_create_collapses_stale_duplicates_to_the_first(monkeypatch, tmp_path: Path) -> None:
    cs, _ = _file_store(monkeypatch, tmp_path)
    a = cs.create_connector({"name": "Dup", "type": "sqlite", "role": "both", "database": "/a.db"})
    b = cs.create_connector({"name": "Dup", "type": "sqlite", "role": "both", "database": "/b.db"})
    assert b.id == a.id
    other = cs.create_connector({"name": "Other", "type": "sqlite", "role": "both"})
    assert other.id != a.id
    assert sorted(c.name for c in cs.list_connectors()) == ["Dup", "Other"]
