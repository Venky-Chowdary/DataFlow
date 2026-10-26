"""Shape drafts restore a recipe without becoming an approval."""

from __future__ import annotations

import json

import pytest

from services.shape_draft_store import (
    delete_shape_draft,
    load_shape_draft,
    route_key,
    save_shape_draft,
)
from services.shape_models import ShapeError


def test_round_trip_keeps_steps_and_ignores_layout_in_the_hash(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    key = route_key("sample-orders.csv", "orders")
    saved = save_shape_draft(
        route_key=key,
        recipe={"steps": [{"op": "trim", "column": "email"}]},
        source_columns=["email"],
        canvas_layout={"zoom": 1, "x": 12},
    )
    assert saved["recipe_hash"]
    assert saved["canvas_layout"]["zoom"] == 1
    again = save_shape_draft(
        route_key=key,
        recipe={"steps": [{"op": "trim", "column": "email"}]},
        source_columns=["email"],
        canvas_layout={"zoom": 0.5},
    )
    assert again["recipe_hash"] == saved["recipe_hash"]
    loaded = load_shape_draft(key)
    assert loaded is not None
    assert loaded["steps"][0]["op"] == "trim"
    delete_shape_draft(key)
    assert load_shape_draft(key) is None
    raw = json.loads((tmp_path / "drafts.json").read_text(encoding="utf-8"))
    assert raw == {}


def test_a_draft_refuses_an_unknown_operation(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    with pytest.raises(ShapeError):
        save_shape_draft(
            route_key="a→b",
            recipe={"steps": [{"op": "not-a-real-op", "column": "email"}]},
            source_columns=["email"],
        )


def test_drafts_are_namespaced_by_workspace(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    key = route_key("orders.csv", "orders")
    save_shape_draft(
        route_key=key,
        workspace_id="ws-a",
        recipe={"steps": [{"op": "trim", "column": "email"}]},
        source_columns=["email"],
    )
    assert load_shape_draft(key, workspace_id="ws-a")["workspace_id"] == "ws-a"
    assert load_shape_draft(key, workspace_id="ws-b") is None
    assert load_shape_draft(key) is None

    delete_shape_draft(key, workspace_id="ws-b")
    delete_shape_draft(key)
    assert load_shape_draft(key, workspace_id="ws-a") is not None


def test_a_crafted_route_key_cannot_reach_another_workspace_slot(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    save_shape_draft(
        route_key="a→b",
        workspace_id="ws-a",
        recipe={"steps": [{"op": "trim", "column": "email"}]},
        source_columns=["email"],
    )
    crafted = json.dumps(["ws-a", "a→b"], ensure_ascii=False)
    assert load_shape_draft(crafted) is None
    assert load_shape_draft(crafted, workspace_id="ws-b") is None
    save_shape_draft(
        route_key=crafted,
        recipe={"steps": [{"op": "trim", "column": "name"}]},
        source_columns=["name"],
    )
    assert load_shape_draft("a→b", workspace_id="ws-a")["steps"][0]["column"] == "email"


def test_a_legacy_unscoped_draft_is_only_visible_unscoped(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    path = tmp_path / "drafts.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    path.write_text(
        json.dumps({"a→b": {"route_key": "a→b", "steps": [], "saved_at": "2026-01-01"}}),
        encoding="utf-8",
    )
    assert load_shape_draft("a→b") is not None
    assert load_shape_draft("a→b", workspace_id="ws-a") is None
    delete_shape_draft("a→b", workspace_id="ws-a")
    assert load_shape_draft("a→b") is not None
    delete_shape_draft("a→b")
    assert load_shape_draft("a→b") is None


def test_one_workspace_cannot_evict_another_workspace_drafts(tmp_path, monkeypatch):
    import services.shape_draft_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    monkeypatch.setattr(store, "MAX_DRAFTS", 3)
    recipe = {"steps": [{"op": "trim", "column": "email"}]}
    save_shape_draft(
        route_key="victim", workspace_id="ws-a", recipe=recipe, source_columns=["email"]
    )
    for i in range(10):
        save_shape_draft(
            route_key=f"spam-{i}", workspace_id="ws-b", recipe=recipe, source_columns=["email"]
        )
    assert load_shape_draft("victim", workspace_id="ws-a") is not None
    assert load_shape_draft("spam-9", workspace_id="ws-b") is not None
    assert load_shape_draft("spam-0", workspace_id="ws-b") is None
