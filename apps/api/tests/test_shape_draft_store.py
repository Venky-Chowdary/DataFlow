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


def test_one_workspace_cannot_evict_another_workspaces_drafts(tmp_path, monkeypatch):
    """The draft cap is per workspace (D154): a busy tenant keeps only its own newest."""
    import services.shape_draft_store as store
    from services.workspace_filter import bound_workspace_filter

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "drafts.json")
    monkeypatch.setattr(store, "MAX_DRAFTS", 3)
    recipe = {"steps": [{"op": "trim", "column": "email"}]}

    with bound_workspace_filter(None, stamp="ws-quiet"):
        save_shape_draft(route_key="quiet-route", recipe=recipe, source_columns=["email"])
    with bound_workspace_filter(None, stamp="ws-busy"):
        for n in range(6):
            save_shape_draft(route_key=f"busy-{n}", recipe=recipe, source_columns=["email"])
        assert [load_shape_draft(f"busy-{n}") is not None for n in range(6)] == [False] * 3 + [True] * 3
    with bound_workspace_filter(None, stamp="ws-quiet"):
        assert load_shape_draft("quiet-route") is not None
