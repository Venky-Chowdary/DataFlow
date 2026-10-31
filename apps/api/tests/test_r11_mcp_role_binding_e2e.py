"""RB6 (QA handover, build 9c75ada4): MCP ran tools REST refuses.

QA: a viewer key ran ``test_connector`` through MCP while
``POST /connectors/saved/{id}/test`` refused it (403).

Root causes on 9c75ada4:

* src/ai/copilot/tool_permissions.py mapped ``test_connector`` to
  ``connector.read``; the REST route needs ``connector.write``.
* ``lifecycle_tools.test_connector`` never called
  ``require_admin_connector_access``, so a workspace admin who is not the
  platform admin could make the API host run a ``singer_tap`` probe
  (an executable), which REST refuses (saved_connectors_router.py).
* Both MCP doors (``POST /mcp/tools/call`` and the Streamable ``POST /mcp``)
  bound the raw ``request.state.user["role"]``: a workspace editor whose
  platform label is ``member`` was treated as a viewer, and an API key acted
  with its stored role even after its creator was demoted (the creator cap in
  ``resolve_effective_role`` was skipped). RBACMiddleware skips
  ``POST /mcp/tools/call``, so nothing else caught it.

Checked through the real routes on the live MongoDB (``r9_workspace_world``).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import PASSWORD, r9  # noqa: E402,F401


@pytest.fixture(autouse=True)
def _no_mcp_rate_limit(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")


def _connector(r9, *, ctype: str = "postgresql", name: str = "rb6 pg") -> str:
    from services.connector_store import create_connector

    conn = create_connector({
        "name": name, "type": ctype, "host": "127.0.0.1", "port": 1,
        "database": "rb6", "username": "rb6", "password": "rb6-secret",
        "workspace_id": r9.acme,
    })
    return conn.id


def _sqlite_args(tmp_path, name: str) -> dict:
    import sqlite3

    (tmp_path / "sqlite").mkdir(exist_ok=True)
    path = tmp_path / "sqlite" / f"{name.replace(' ', '_')}.db"
    with sqlite3.connect(path) as db:
        db.execute("create table if not exists t (id integer primary key, v text)")
    return {"name": name, "type": "sqlite", "database": str(path)}


def _rest_call(client, tool: str, arguments: dict):
    return client.post("/api/v1/mcp/tools/call", json={"name": tool, "arguments": arguments})


def _rpc_call(client, tool: str, arguments: dict) -> dict:
    res = client.post(
        "/api/v1/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": tool, "arguments": arguments}},
        headers={"Accept": "application/json, text/event-stream"},
    )
    assert res.status_code == 200, res.text
    return res.json()["result"]


def _refused(res) -> bool:
    if res.status_code == 403:
        return True
    detail = res.json().get("detail") if res.headers.get("content-type", "").startswith("application/json") else None
    return res.status_code == 422 and str((detail or {}).get("error", "")).startswith("Your role")


def test_viewer_cannot_test_a_connector_through_either_mcp_door(r9):
    cid = _connector(r9)
    viewer = "viewer-rb6@example.test"
    r9.world.member(viewer, "viewer")
    v = r9.login(viewer, PASSWORD, r9.acme)
    # The REST twin refuses the viewer.
    assert v.post(f"/api/v1/connectors/saved/{cid}/test").status_code == 403

    res = _rest_call(v, "test_connector", {"connector_id": cid})
    assert _refused(res), (res.status_code, res.text)
    rpc = _rpc_call(v, "test_connector", {"connector_id": cid})
    assert rpc["isError"] is True
    assert rpc["content"][0]["text"].startswith("Your role"), rpc

    # The same holds for a viewer API key.
    key = r9.world.mint_key("viewer")
    assert _refused(_rest_call(r9.world.as_key(key), "test_connector", {"connector_id": cid}))


def test_workspace_editor_with_member_platform_label_acts_as_editor_over_mcp(r9, tmp_path):
    _connector(r9)
    editor = "editor-rb6@example.test"
    r9.world.member(editor, "editor")
    e = r9.login(editor, PASSWORD, r9.acme)
    args = _sqlite_args(tmp_path, "rb6 staged")
    # REST lets the workspace editor create a connector; MCP must agree (it
    # read the platform label "member" -> viewer on 9c75ada4).
    res = _rest_call(e, "create_connector", args)
    assert res.status_code == 200, res.text
    assert res.json()["output"].get("ack_id")
    rpc = _rpc_call(e, "create_connector", {**args, "name": "rb6 staged rpc"})
    assert rpc["isError"] is False, rpc


def test_api_key_is_capped_at_its_creator_current_role_over_mcp(r9, tmp_path):
    from services import team_store

    lead = "lead-rb6@example.test"
    r9.world.member(lead, "admin")
    lead_client = r9.login(lead, PASSWORD, r9.acme)
    key = r9.world.mint_key("admin", by=lead_client, name="rb6-lead-key")
    args = _sqlite_args(tmp_path, "rb6 capped")
    first = _rest_call(r9.world.as_key(key), "create_connector", args)
    assert first.status_code == 200, first.text

    # The creator is demoted to viewer: the key stops acting as admin at once.
    team_store._write_membership(team_store.Membership(
        workspace_id=r9.acme, email=lead, role="viewer", added_by="owner@acme.test",
    ))
    k = r9.world.as_key(key)
    assert k.post("/api/v1/connectors/saved", json={**args, "name": "rest"}).status_code == 403
    res = _rest_call(k, "create_connector", {**args, "name": "rb6 capped 2"})
    assert _refused(res), (res.status_code, res.text)
    rpc = _rpc_call(k, "create_connector", {**args, "name": "rb6 capped 3"})
    assert rpc["isError"] is True and rpc["content"][0]["text"].startswith("Your role"), rpc


def test_workspace_admin_cannot_make_the_host_run_a_singer_probe_over_mcp(r9, monkeypatch, tmp_path):
    taps = tmp_path / "taps"
    taps.mkdir()
    monkeypatch.setenv("DATAWRAP_SINGER_TAPS_DIR", str(taps))
    cid = _connector(r9, ctype="singer_tap", name="rb6 singer")
    owner = r9.owner  # workspace admin, not the platform admin
    rest = owner.post(f"/api/v1/connectors/saved/{cid}/test")
    assert rest.status_code == 403, rest.text

    ran: list[str] = []
    import services.connector_probe as probe

    real = probe.probe_saved_connector

    def spy(*a, **k):
        ran.append("probe")
        return real(*a, **k)

    monkeypatch.setattr(probe, "probe_saved_connector", spy)
    res = _rest_call(owner, "test_connector", {"connector_id": cid})
    assert _refused(res), (res.status_code, res.text)
    rpc = _rpc_call(owner, "test_connector", {"connector_id": cid})
    assert rpc["isError"] is True, rpc
    assert ran == [], "the singer probe ran for a non-platform admin"
