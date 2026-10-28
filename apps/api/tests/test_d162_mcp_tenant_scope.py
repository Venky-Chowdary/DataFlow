"""D162: every MCP tool that takes a connector / job / schedule reference.

Iterates the live MCP registry (``GET /mcp/tools``) rather than a hand-written
list, so a tool added later is covered the day it appears. Each read tool is
called through ``POST /mcp/tools/call`` as B with A's, and the legacy,
objects named by id, by name and by host; the answer must be not-found or
empty and nothing of A's — id, name, host, table name or row value — may
appear anywhere in the body, error texts included. Positive controls: A
still reads A's objects, and the platform admin (the legacy owner) reads the
legacy ones.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import World, build_world, owner_marker  # noqa: E402

_REF = re.compile(r"connector|job|schedule|pipeline|left|right|source|destination|dest|name|table|query|dataset", re.I)


@pytest.fixture(autouse=True)
def _no_mcp_rate_limit(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")


@pytest.fixture
def world(tmp_path, monkeypatch) -> World:
    return build_world(tmp_path, monkeypatch, signup=True)


def _registry(client) -> list[dict]:
    response = client.get("/api/v1/mcp/tools")
    assert response.status_code == 200, response.text
    body = response.json()
    return body["tools"] if isinstance(body, dict) else body


def _read_tools(client) -> list[dict]:
    from src.ai.copilot.tool_permissions import READ, TOOL_PERMISSIONS

    out = []
    for tool in _registry(client):
        name = tool.get("name")
        schema = tool.get("input_schema") or tool.get("inputSchema") or tool.get("parameters") or {}
        props = schema.get("properties", {}) or {}
        if TOOL_PERMISSIONS.get(name, ("", ""))[1] != READ:
            continue
        if any(_REF.search(p) for p in props):
            out.append({"name": name, "props": props})
    return out


def _calls(tool: dict, refs: dict[str, str]) -> list[dict]:
    """Argument sets naming the foreign objects every way the tool could take them."""
    calls = []
    for key in ("id", "name", "host"):
        args = {}
        for prop, spec in tool["props"].items():
            if spec.get("type") != "string":
                continue
            p = prop.lower()
            kind = "job" if "job" in p else "schedule" if ("schedule" in p or "pipeline" in p) else "connector"
            args[prop] = refs[f"{kind}_{key}"] if f"{kind}_{key}" in refs else refs[f"connector_{key}"]
            if "table" in p:
                args[prop] = refs["table"]
            if p in ("query", "sql"):
                args[prop] = f"select * from {refs['table']}"
            if "dataset" in p:
                args[prop] = refs["connector_name"]
        calls.append(args)
    return calls


def _refs(world: World, label: str) -> dict[str, str]:
    marker = owner_marker(label, world.tag)
    return {
        "connector_id": world.ids[f"{label}_db"],
        "connector_name": f"{marker}-db",
        "connector_host": f"{marker}.sqlite",
        "job_id": world.ids[f"{label}_job"],
        "job_name": f"job-{marker}",
        "job_host": marker,
        "schedule_id": world.ids[f"{label}_schedule"],
        "schedule_name": f"{marker}-sched",
        "schedule_host": marker,
        "table": f"{marker}_tbl",
    }


def _forbidden(world: World, label: str) -> list[str]:
    marker = owner_marker(label, world.tag)
    return [
        marker, world.ids[f"{label}_db"], world.ids[f"{label}_connector"],
        world.ids[f"{label}_schedule"], f"job-{marker}",
    ]


def _call(client, name: str, args: dict):
    response = client.post("/api/v1/mcp/tools/call", json={"name": name, "arguments": args})
    return response, response.text


@pytest.mark.parametrize("label", ["a", "legacy"])
def test_b_gets_nothing_of_a_or_legacy_from_any_mcp_read_tool(world, label):
    tools = _read_tools(world.b)
    assert len(tools) >= 20, [t["name"] for t in tools]  # the registry really was iterated
    refs = _refs(world, label)
    forbidden = _forbidden(world, label)
    leaks = []
    for tool in tools:
        for args in _calls(tool, refs):
            _response, text = _call(world.b, tool["name"], args)
            sent = {str(v) for v in args.values()}
            # The caller's own words are not a leak; anything else of A's is.
            body = text
            for token in sent:
                body = body.replace(token, "")
            hit = [f for f in forbidden if f in body]
            if hit:
                leaks.append((tool["name"], args, hit[0]))
    assert leaks == [], leaks[:5]


def test_positive_controls_owner_and_legacy_owner_still_read_their_objects(world):
    refs_a = _refs(world, "a")
    refs_l = _refs(world, "legacy")
    # A reads A's: connection, objects, rows, job, schedule.
    ok = _call(world.a, "list_connectors", {})[1]
    assert refs_a["connector_name"] in ok and refs_l["connector_name"] not in ok
    objs = _call(world.a, "list_connector_objects", {"connector_name": refs_a["connector_name"]})[1]
    assert refs_a["table"] in objs
    sample = _call(
        world.a, "sample_connector_object",
        {"connector_name": refs_a["connector_name"], "table": refs_a["table"]},
    )[1]
    assert f"{owner_marker('a', world.tag)}-secret-row" in sample
    assert refs_a["job_id"] in _call(world.a, "get_job", {"job_id": refs_a["job_id"]})[1]
    assert refs_a["schedule_id"] in _call(world.a, "get_schedule", {"schedule_id": refs_a["schedule_id"]})[1]
    # The legacy owner (platform admin) reads legacy.
    admin_list = _call(world.admin, "list_connectors", {})[1]
    assert refs_l["connector_name"] in admin_list and refs_a["connector_name"] in admin_list
    legacy_sample = _call(
        world.admin, "sample_connector_object",
        {"connector_name": refs_l["connector_name"], "table": refs_l["table"]},
    )[1]
    assert f"{owner_marker('legacy', world.tag)}-secret-row" in legacy_sample
    assert refs_l["job_id"] in _call(world.admin, "get_job", {"job_id": refs_l["job_id"]})[1]


def test_b_is_told_not_found_for_a_foreign_connector_and_job(world):
    refs = _refs(world, "a")
    for tool, args in (
        ("test_connector", {"connector_name": refs["connector_name"]}),
        ("list_connector_objects", {"connector_id": refs["connector_id"]}),
        ("get_job", {"job_id": refs["job_id"]}),
        ("get_schedule", {"schedule_id": refs["schedule_id"]}),
    ):
        response, text = _call(world.b, tool, args)
        body = json.loads(text) if text.startswith(("{", "[")) else {}
        assert response.status_code in {200, 404, 422}, (tool, response.status_code)
        assert "not found" in text.lower() or "no connector matched" in text.lower() or response.status_code == 404, (tool, text[:200])
        assert refs["connector_host"] not in text, tool
        assert not (isinstance(body, dict) and body.get("success") is True and body.get("output")), (tool, text[:200])
