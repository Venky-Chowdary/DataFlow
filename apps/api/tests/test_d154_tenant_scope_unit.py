"""D154 tenant-scope resolver: isolation default, filters, header narrowing."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import tenant_scope as ts  # noqa: E402
from services.team_store import require_workspace_isolation  # noqa: E402
from services.workspace_filter import WorkspaceFilter  # noqa: E402


def _clear_flags(monkeypatch):
    for name in ("REQUIRE_WORKSPACE", "SIGNUP_ENABLED"):
        for prefix in ("DATAFLOW_", "DATAWRAP_"):
            monkeypatch.delenv(prefix + name, raising=False)


def test_signup_enforces_isolation_without_the_flag(monkeypatch):
    _clear_flags(monkeypatch)
    assert require_workspace_isolation() is False
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "1")
    assert require_workspace_isolation() is True
    monkeypatch.setenv("DATAWRAP_SIGNUP_ENABLED", "0")
    monkeypatch.setenv("DATAWRAP_REQUIRE_WORKSPACE", "1")
    assert require_workspace_isolation() is True


def _request(email: str, role: str = "member", header: str = ""):
    return SimpleNamespace(
        state=SimpleNamespace(user_email=email, user={"email": email, "role": role}),
        headers={"X-Workspace-Id": header} if header else {},
    )


@pytest.fixture
def members(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    table = {"a@x.test": (("ws-a", "admin"), ("ws-shared", "viewer"))}
    monkeypatch.setattr(ts, "ordered_memberships", lambda email: table.get(email, ()))
    return table


@pytest.mark.parametrize("enforced", [True, False])
def test_restricted_actor_sees_memberships_and_header_only_narrows(members, monkeypatch, enforced):
    monkeypatch.setattr(ts, "require_workspace_isolation", lambda: enforced)
    scope = ts.resolve_tenant_scope(_request("a@x.test"))
    assert scope.allows("ws-a") and scope.allows("ws-shared")
    assert not scope.allows("ws-b")
    assert scope.allows("") is (not enforced)
    assert scope.workspace_id == "ws-a"  # personal first
    assert scope.can_write("ws-a") and not scope.can_write("ws-shared")

    narrowed = ts.resolve_tenant_scope(_request("a@x.test", header="ws-shared"))
    assert narrowed.allows("ws-shared") and not narrowed.allows("ws-a")
    with pytest.raises(HTTPException) as exc:
        ts.resolve_tenant_scope(_request("a@x.test", header="ws-b"))
    assert exc.value.status_code == 403


def test_memberless_actor_gets_nothing_not_global(members, monkeypatch):
    monkeypatch.setattr(ts, "require_workspace_isolation", lambda: True)
    scope = ts.resolve_tenant_scope(_request("stranger@x.test"))
    assert scope.filter == WorkspaceFilter(frozenset(), False)
    assert scope.single_workspace_filter() == WorkspaceFilter.nothing()
    with pytest.raises(HTTPException) as exc:
        scope.target_workspace_id(write=True)
    assert exc.value.status_code == 403


def test_platform_admin_is_unrestricted_and_header_narrows(members, monkeypatch):
    monkeypatch.setattr(ts, "require_workspace_isolation", lambda: True)
    scope = ts.resolve_tenant_scope(_request("root@x.test", role="admin"))
    assert scope.filter.is_everything and scope.allows("") and scope.allows("ws-b")
    narrowed = ts.resolve_tenant_scope(_request("root@x.test", role="admin", header="ws-b"))
    assert narrowed.allows("ws-b") and not narrowed.allows("ws-a") and not narrowed.allows("")


def test_api_key_admin_role_is_not_platform_admin_unless_creator_is(members, monkeypatch):
    from src.services import auth_service

    key_user = {"email": "a@x.test", "role": "admin", "auth_kind": "api_key"}
    monkeypatch.setattr(auth_service, "lookup_user", lambda email: {"email": email, "role": "member"})
    assert ts.actor_is_unrestricted(key_user) is False
    monkeypatch.setattr(auth_service, "lookup_user", lambda email: {"email": email, "role": "admin"})
    assert ts.actor_is_unrestricted(key_user) is True


def test_filter_matches_and_mongo_query_agree_on_a_live_store():
    pymongo = pytest.importorskip("pymongo")
    uri = os.environ.get("DATAFLOW_TEST_MONGO_URI") or "mongodb://127.0.0.1:27017"
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=1500)
    try:
        client.admin.command("ping")
    except Exception:
        pytest.skip("MongoDB not reachable")
    coll = client["dataflow_test"][f"d154_filter_{uuid4().hex}"]
    rows = [
        {"_id": "missing"},
        {"_id": "null", "workspace_id": None},
        {"_id": "empty", "workspace_id": ""},
        {"_id": "a", "workspace_id": "ws-a"},
        {"_id": "b", "workspace_id": "ws-b"},
    ]
    coll.insert_many(rows)
    try:
        filters = [
            WorkspaceFilter.everything(),
            WorkspaceFilter.nothing(),
            WorkspaceFilter.only(""),
            WorkspaceFilter.only("ws-a"),
            WorkspaceFilter(None, False),
            WorkspaceFilter(frozenset({"ws-a"}), True),
            WorkspaceFilter(frozenset({"ws-a", "ws-b"}), False),
        ]
        for wf in filters:
            in_mongo = {d["_id"] for d in coll.find(wf.mongo_query())}
            in_memory = {r["_id"] for r in rows if wf.matches(r.get("workspace_id"))}
            assert in_mongo == in_memory, wf
    finally:
        coll.drop()
        client.close()
