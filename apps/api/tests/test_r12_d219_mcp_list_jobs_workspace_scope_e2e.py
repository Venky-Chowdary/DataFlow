"""QA D219 (prod 38eae820): MCP ``list_jobs`` with ``scope=workspace`` returned
every workspace's jobs — ``total`` 5164 for both ``workspace`` and ``all``.

Root cause: ``job_list_scope`` turns ``workspace`` into ``workspace_id=""``,
and ``job_workspace_filter`` lets the caller's tenant filter win over it. For a
platform admin (or a platform key) that filter is "every workspace", so the
``workspace`` scope narrowed nothing. ``workspace`` now means the workspace
this turn acts in (the bound stamp), intersected with what the caller may read.

Through the real MCP route on the live MongoDB (``r9_workspace_world``).
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import PASSWORD, r9  # noqa: E402,F401


@pytest.fixture(autouse=True)
def _no_mcp_rate_limit(monkeypatch, tmp_path):
    import src.ai.copilot.ack_ledger as ack_mod

    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr(ack_mod, "_ledger", None)


def _seed(r9) -> dict[str, int]:
    rows = []
    plan = {r9.acme: 3, r9.bravo_workspace_id: 5, "": 2}
    for ws, n in plan.items():
        for _ in range(n):
            doc = {"_id": uuid.uuid4().hex[:24], "status": "completed",
                   "source": "s", "destination": "d"}
            if ws:
                doc["workspace_id"] = ws
            rows.append(doc)
    r9.db["transfer_jobs"].insert_many(rows)
    return plan


def _totals(client) -> tuple[int, int, dict]:
    out = {}
    for scope in ("workspace", "all"):
        res = client.post("/api/v1/mcp/tools/call",
                          json={"name": "list_jobs", "arguments": {"limit": 100, "scope": scope}})
        assert res.status_code == 200, res.text
        body = res.json()
        payload = body.get("output") or (body.get("result") or {}).get("output") or body
        out[scope] = payload
    return int(out["workspace"]["total"]), int(out["all"]["total"]), out


def test_platform_admin_without_a_workspace_is_not_told_every_workspace_is_its_own(r9):
    plan = _seed(r9)
    # The QA call: platform admin, no X-Workspace-Id. It acts in no workspace,
    # so a ``workspace`` census must not come back as every workspace's jobs.
    root = r9.login("root@example.test", "Bootstrap-Admin-2026!", None)
    res = root.post("/api/v1/mcp/tools/call",
                    json={"name": "list_jobs", "arguments": {"limit": 100, "scope": "workspace"}})
    body = res.json()
    text = str(body)
    assert "X-Workspace-Id" in text, body
    payload = body.get("output") or {}
    assert int((payload or {}).get("total") or 0) != sum(plan.values()), body
    res = root.post("/api/v1/mcp/tools/call",
                    json={"name": "list_jobs", "arguments": {"limit": 100, "scope": "all"}})
    assert res.status_code == 200, res.text
    out = res.json().get("output") or res.json()
    assert int(out["total"]) == sum(plan.values()), out
    assert out["scope"] == "all"


def test_platform_admin_with_a_header_keeps_that_workspace(r9):
    plan = _seed(r9)
    ws_total, all_total, out = _totals(r9.root)  # header: Acme
    assert ws_total == plan[r9.acme], out
    assert all_total == plan[r9.acme], out


def test_workspace_owner_scope_all_is_still_only_what_they_may_read(r9):
    plan = _seed(r9)
    ws_total, all_total, out = _totals(r9.owner)
    assert ws_total == plan[r9.acme], out
    # ``all`` never widens past the caller's tenant filter.
    assert all_total < sum(plan.values()), out
    assert all_total >= plan[r9.acme], out
