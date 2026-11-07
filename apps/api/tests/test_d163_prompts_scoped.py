"""D163: suggested prompts and status text name only the caller's connectors."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import World, build_world, owner_marker  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch) -> World:
    return build_world(tmp_path, monkeypatch, signup=True)


def test_prompts_and_status_exclude_foreign_and_legacy_names(world):
    a, legacy = owner_marker("a", world.tag), owner_marker("legacy", world.tag)
    for path in ("/api/v1/copilot/prompts", "/api/v1/copilot/status", "/api/v1/mcp/status"):
        text = world.b.get(path).text
        assert a not in text and legacy not in text, path


def test_owner_prompts_name_their_own_connector(world):
    a, legacy = owner_marker("a", world.tag), owner_marker("legacy", world.tag)
    prompts = world.a.get("/api/v1/copilot/prompts").text
    assert a in prompts and legacy not in prompts


def test_example_phrase_helpers_are_scoped(world):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.example_phrases import example_connector_name

    a, legacy = owner_marker("a", world.tag), owner_marker("legacy", world.tag)
    with bound_workspace_filter(WorkspaceFilter(frozenset({world.ws_b}), False), world.ws_b, "b"):
        assert example_connector_name() == "your connector"
    with bound_workspace_filter(WorkspaceFilter(frozenset({world.ws_a}), False), world.ws_a, "a"):
        name = example_connector_name()
        assert a in name and legacy not in name
