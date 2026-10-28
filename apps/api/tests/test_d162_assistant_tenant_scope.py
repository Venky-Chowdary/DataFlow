"""D162/D163: the assistant never shows another account's connections.

Pilot tools, the chat agent and the MCP/copilot status routes read tenant-owned
stores outside the routers that authorize them. Every one is driven here as B
against A's seeded workspace (``tests/d154_world.py``); A's connection,
pipeline, contract and job names must not appear in anything B is told.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import World, build_world, owner_marker  # noqa: E402
from services.workspace_filter import WorkspaceFilter, bound_workspace_filter  # noqa: E402

_NAME_SUFFIXES = ("-conn", "-sched", "-contract", "-project", "-recipe", "-plan", "-src", "-dst",
                  ".example", "_orders", "_copy", "-pipeline")


def _needles(world: World, *, legacy: bool = True) -> list[str]:
    markers = [owner_marker("a", world.tag)] + ([owner_marker("legacy", world.tag)] if legacy else [])
    return [f"{m}{s}" for m in markers for s in _NAME_SUFFIXES] + [world.ws_a]


@pytest.fixture(params=["multi_tenant", "legacy_single_tenant"])
def world(request, tmp_path, monkeypatch) -> World:
    return build_world(tmp_path, monkeypatch, signup=request.param == "multi_tenant")


def _legacy_allowed(world: World) -> bool:
    """Unscoped rows are visible to tenants only in a legacy deployment."""
    return world.m is not None


def test_every_read_tool_hides_other_workspaces_names(world):
    from src.ai.copilot.tool_permissions import READ, TOOL_PERMISSIONS, caller_role
    from src.ai.copilot.tools import TOOL_DEFINITIONS, DataPilotTools

    marker = owner_marker("a", world.tag)
    tools = DataPilotTools()
    needles = _needles(world, legacy=not _legacy_allowed(world))
    hits: dict[str, str] = {}
    for definition in TOOL_DEFINITIONS:
        name = definition["name"]
        if TOOL_PERMISSIONS.get(name, ("", ""))[1] != READ:
            continue
        props = (definition.get("input_schema") or definition.get("parameters") or {}).get("properties", {})
        by_id = {}
        for prop, spec in props.items():
            if spec.get("type") != "string":
                continue
            by_id[prop] = (
                world.ids["a_job"] if "job" in prop else
                world.ids["a_schedule"] if "schedule" in prop else
                world.ids["a_connector"] if "connector" in prop else
                world.ids["a_preflight_run"] if "run" in prop else
                world.ids["a_plan"] if "plan" in prop else marker
            )
        by_name = {p: marker for p, spec in props.items() if spec.get("type") == "string"}
        for args in ({}, by_id, by_name):
            # B's tenant scope, bound the way the middleware binds it.
            with bound_workspace_filter(WorkspaceFilter(frozenset({world.ws_b}), _legacy_allowed(world)),
                                        world.ws_b, "b"), caller_role("admin"):
                result = tools.execute(name, dict(args))
            text = json.dumps(getattr(result, "output", None), default=str) + str(getattr(result, "error", ""))
            leaked = [n for n in needles if n in text]
            if leaked:
                hits[name] = leaked[0]
    assert hits == {}, hits


CHAT_PROMPTS = (
    "what connectors do I have", "show my connectors", "list my jobs", "list my pipelines",
    "list schedules", "list contracts", "give me a workspace briefing", "show my recent jobs",
    "how many jobs do I have", "show my saved connections", "which connections exist",
    "do I have any postgres connection", "what is in quarantine", "show preflight runs",
    "list transfer plans", "show failed jobs", "what can you do",
)


@pytest.mark.parametrize("header", ["none", "own"])
def test_chat_as_b_never_names_a_connection(world, header):
    headers = {"X-Workspace-Id": world.ws_b} if header == "own" else {}
    needles = _needles(world, legacy=not _legacy_allowed(world))
    for prompt in CHAT_PROMPTS:
        response = world.b.post("/api/v1/copilot/chat", json={"message": prompt}, headers=headers)
        assert response.status_code == 200, (prompt, response.status_code, response.text[:200])
        leaked = [n for n in needles if n in response.text]
        assert leaked == [], (prompt, leaked)
    for path in ("/api/v1/copilot/prompts", "/api/v1/copilot/status", "/api/v1/mcp/status"):
        text = world.b.get(path, headers=headers).text
        assert [n for n in needles if n in text] == [], path


def test_chat_with_a_workspace_that_is_not_yours_is_refused(world):
    forced = {"X-Workspace-Id": world.ws_a}
    response = world.b.post("/api/v1/copilot/chat", json={"message": "list contracts"}, headers=forced)
    assert response.status_code == 403, response.text


def test_the_owner_still_gets_their_own_names_from_the_assistant(world):
    contracts = world.a.post("/api/v1/copilot/chat", json={"message": "list contracts"})
    assert contracts.status_code == 200
    assert owner_marker("a", world.tag) in contracts.text


def test_pilot_working_memory_is_not_shared_between_accounts(world):
    from src.ai.copilot.pilot_agent import get_pilot_agent

    agent = get_pilot_agent()
    ids = {}
    for actor in ("alice@example.com", "bob@example.com"):
        with bound_workspace_filter(None, "", actor):
            ids[actor, "fallback"] = agent._ensure_data_context(None)["pilot_session_id"]
            ids[actor, "client"] = agent._ensure_data_context({"pilot_session_id": "shared-sid"})["pilot_session_id"]
    assert ids["alice@example.com", "fallback"] != ids["bob@example.com", "fallback"]
    assert ids["alice@example.com", "client"] != ids["bob@example.com", "client"]
    with bound_workspace_filter(None, "", "alice@example.com"):
        assert agent._ensure_data_context(None)["pilot_session_id"] == ids["alice@example.com", "fallback"]
