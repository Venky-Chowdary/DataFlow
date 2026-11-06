"""D218 / D219 — a platform admin's All-workspaces and Unassigned views.

D218: a platform admin landed in the oldest workspace ("Test · 2") and the
legacy rows written before workspaces (workspace_id missing / null / "")
had no switcher entry at all: once the UI names a workspace on every request,
isolation hides them from every screen. Devin's partial fix landed the
switcher on the server's default; there was still no way to *see* legacy
data short of the off-by-default DATAFLOW_LEGACY_UNSCOPED_ADMIN_VIEW flag.

D219: MCP ``list_jobs`` scope=workspace == scope=all for a platform key.
134dec2a9 made ``workspace`` the bound workspace (or a refusal when there is
none); the QA's expected "explicit unassigned bucket for admins" was missing.

The API now names two reserved ``X-Workspace-Id`` views, platform admins
only: ``__all__`` (every workspace, the same as no header) and
``__unassigned__`` (the rows of no workspace, read-only for creates, audited).
``GET /team/workspaces`` lists them; ``list_jobs`` takes ``scope=unassigned``.

Real REST/MCP routes on the live MongoDB (``r9_workspace_world``).
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

ROOT_PASSWORD = "Bootstrap-Admin-2026!"
ALL = "__all__"
UNASSIGNED = "__unassigned__"


@pytest.fixture(autouse=True)
def _no_mcp_rate_limit(monkeypatch, tmp_path):
    import src.ai.copilot.ack_ledger as ack_mod
    import services.tenant_scope as ts

    monkeypatch.setenv("DATAFLOW_MCP_RATE_LIMIT", "0")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr(ack_mod, "_ledger", None)
    ts._LEGACY_VIEW_AUDITED.clear()


def _seed(r9) -> dict[str, int]:
    """3 Acme jobs, 5 Bravo jobs, and 4 legacy jobs in every unscoped spelling."""
    rows = []
    for ws, n in ((r9.acme, 3), (r9.bravo_workspace_id, 5)):
        rows += [{"_id": uuid.uuid4().hex[:24], "status": "completed", "workspace_id": ws}
                 for _ in range(n)]
    for spelling in ("missing", None, "", "   "):
        doc = {"_id": uuid.uuid4().hex[:24], "status": "failed"}
        if spelling != "missing":
            doc["workspace_id"] = spelling
        rows.append(doc)
    r9.db["transfer_jobs"].insert_many(rows)
    return {"acme": 3, "bravo": 5, "legacy": 4, "all": 12}


def _list_jobs(client, scope: str) -> dict:
    res = client.post("/api/v1/mcp/tools/call",
                      json={"name": "list_jobs", "arguments": {"limit": 100, "scope": scope}})
    body = res.json()
    return {"status": res.status_code, **(body.get("output") or {}),
            "error": body.get("detail") or body.get("error")}


def _root(r9, header: str | None):
    return r9.login("root@example.test", ROOT_PASSWORD, header)


def test_directory_lists_the_views_for_a_platform_admin_only(r9):
    n = _seed(r9)
    listed = _root(r9, None).get("/api/v1/team/workspaces").json()
    views = {v["id"]: v for v in listed["views"]}
    assert set(views) == {ALL, UNASSIGNED}
    assert views[UNASSIGNED]["legacy_jobs"] == n["legacy"]
    assert views[ALL]["name"] == "All workspaces"
    assert r9.owner.get("/api/v1/team/workspaces").json()["views"] == []


def test_unassigned_view_shows_exactly_the_legacy_rows_and_is_audited(r9):
    n = _seed(r9)
    root = _root(r9, UNASSIGNED)
    # MCP: in the Unassigned view "this workspace" is the legacy bucket.
    for scope in ("workspace", "unassigned"):
        out = _list_jobs(root, scope)
        assert out["status"] == 200, out
        assert out["total"] == n["legacy"], (scope, out)
        assert out["status_counts"] == {"failed": n["legacy"]}
    # ``all`` never widens past the view the request names.
    assert _list_jobs(root, "all")["total"] == n["legacy"]
    # The UI's job list reads the same population.
    jobs = root.get("/api/v1/jobs")
    assert jobs.status_code == 200, jobs.text
    assert jobs.json()["total"] == n["legacy"], jobs.json().get("total")
    events = r9.world.audit("tenant_scope.legacy_admin_view.used")
    assert any((e.get("details") or {}).get("view") == "unassigned" for e in events), events


def test_all_view_is_every_workspace_and_workspace_scope_stays_honest(r9):
    n = _seed(r9)
    root = _root(r9, ALL)
    assert _list_jobs(root, "all")["total"] == n["all"]
    assert _list_jobs(root, "unassigned")["total"] == n["legacy"]
    # Acting in no single workspace: "workspace" is refused, never every row.
    out = _list_jobs(root, "workspace")
    assert out.get("total") != n["all"], out
    assert "X-Workspace-Id" in str(out["error"]), out
    jobs = root.get("/api/v1/jobs").json()
    assert jobs["total"] == n["all"]


def test_a_named_workspace_hides_legacy_and_the_unassigned_scope_is_admin_only(r9):
    n = _seed(r9)
    root = _root(r9, r9.acme)
    assert _list_jobs(root, "workspace")["total"] == n["acme"]
    # A workspace owner can ask for the bucket, and gets nothing: isolation holds.
    assert _list_jobs(r9.owner, "unassigned")["total"] == 0
    assert _list_jobs(r9.owner, "all")["total"] == n["acme"]
    # ...and cannot name either view.
    for view in (ALL, UNASSIGNED):
        r9.owner.headers["X-Workspace-Id"] = view
        try:
            denied = r9.owner.get("/api/v1/ops/dlq")
            assert denied.status_code == 403, (view, denied.text)
            assert _list_jobs(r9.owner, "all")["total"] == 0
        finally:
            r9.owner.headers["X-Workspace-Id"] = r9.acme


def test_nothing_new_lands_in_unassigned(r9):
    """Not only at the route: every store that stamps rows from the bound
    scope (preflight runs, transfer plans, drafts) is refused in a view."""
    from fastapi import HTTPException

    from services import preflight_run_store, transfer_plan_store
    from services.tenant_scope import TenantScope, bind_tenant_scope

    root = _root(r9, UNASSIGNED)
    res = root.post("/api/v1/schedules/", json={
        "name": "d218", "source_connector_id": "x", "source_table": "t",
        "dest_connector_id": "y", "dest_table": "t2", "interval": "daily",
    })
    assert res.status_code == 400, res.text
    assert "read-only view" in res.text

    # A plan saved through the real route in the Unassigned view is refused,
    # and nothing landed in the legacy bucket.
    before = len([p for p in transfer_plan_store.list_plans(limit=500) if not (getattr(p, "workspace_id", "") or "")])
    res = root.post("/api/v1/transfer/plans", json={"name": "d218 plan"})
    assert res.status_code == 400, res.text
    assert "read-only view" in res.text
    after = len([p for p in transfer_plan_store.list_plans(limit=500) if not (getattr(p, "workspace_id", "") or "")])
    assert after == before

    # The seam itself, for every view and for an admin with no workspace of
    # its own in the All view: current_workspace_stamp() raises the route's 400.
    for view in (UNASSIGNED, ALL):
        scope = TenantScope(actor="root@example.test", unrestricted=True, enforced=True,
                            memberships=(), requested_workspace_id=view)
        with bind_tenant_scope(scope):
            with pytest.raises(HTTPException) as exc:
                preflight_run_store.save_preflight_run(
                    {"passed": False, "blockers": ["x"], "warnings": []},
                    source_label="s", dest_label="d", validation_mode="strict",
                )
            assert exc.value.status_code == 400, view
            with pytest.raises(HTTPException):
                transfer_plan_store.create_plan({"name": "seam"})
    assert len([p for p in transfer_plan_store.list_plans(limit=500) if not (getattr(p, "workspace_id", "") or "")]) == before


def test_a_recipe_and_a_tenant_cannot_be_created_under_a_view(r9):
    import src.ai.copilot.ack_ledger as ack_mod
    from services.transform_library import get_transform_library

    library = get_transform_library()
    ids_before = {r.id for r in library.list("")}
    for view in (ALL, UNASSIGNED):
        root = _root(r9, view)
        ack_id = ack_mod.get_ack_ledger().put(
            kind="create_transform_recipe",
            payload={"template_key": "standardise-email-hash-pii"},
            preview={"name": "Standardise Email + Hash PII"},
        )
        res = root.post("/api/v1/copilot/confirm", json={"ack_id": ack_id, "reason": "d218"})
        assert res.status_code == 400, (view, res.text)
        # A view id is never a recipe's workspace, and nothing landed unscoped.
        for ws in (view, ""):
            try:
                rows = library.list(ws)
            except TypeError:
                rows = []
            assert not [r for r in rows if getattr(r, "id", "") not in ids_before], (view, ws)
        # A tenant: the header names no workspace, and a view id does not exist.
        res = root.post("/api/v1/workspace/tenant", json={"name": "Acme Corp"})
        assert res.status_code == 400, (view, res.text)
        assert "workspace_id is required" in res.text
        res = root.post("/api/v1/workspace/tenant", json={"name": "Acme Corp", "workspace_id": view})
        assert res.status_code == 400, (view, res.text)
        assert "does not exist" in res.text


def test_views_are_offered_only_while_isolation_is_enforced(r9, monkeypatch):
    """The single-operator deployment (auth off, isolation off) renders the
    switcher exactly as before the views existed: the studio-ui pixel
    baselines are captured there."""
    import services.tenant_scope as ts

    monkeypatch.setattr(ts, "require_workspace_isolation", lambda: False)
    listed = _root(r9, None).get("/api/v1/team/workspaces").json()
    assert listed["views"] == []
    assert listed["default_workspace_id"] != ALL


def test_an_admin_with_no_workspace_lands_on_all_and_keeps_the_view_it_used(r9):
    root = _root(r9, None)
    listed = root.get("/api/v1/team/workspaces").json()
    assert listed["workspaces"], listed  # there are workspaces; none is the admin's
    assert listed["default_workspace_id"] == ALL
    me = root.get("/api/v1/auth/me", headers={"X-Workspace-Id": UNASSIGNED})
    assert me.status_code == 200, me.text
    # A view is never echoed as the workspace a row is stamped with...
    assert me.json()["workspace_id"] == ""
    # ...but it is the landing choice from now on.
    assert me.json()["default_workspace_id"] == UNASSIGNED
    again = _root(r9, None)
    assert again.get("/api/v1/team/workspaces").json()["default_workspace_id"] == UNASSIGNED
    assert again.get("/api/v1/auth/me").json()["default_workspace_id"] == UNASSIGNED
