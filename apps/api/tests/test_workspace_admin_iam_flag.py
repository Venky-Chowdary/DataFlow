"""DATAFLOW_WORKSPACE_ADMIN_IAM: workspace admins keep IAM/SCIM by default (audited);
/workspace/api-keys stays scoped to the caller's own workspace either way."""

from __future__ import annotations

import pytest

from services import audit_coverage, integrations_store
from services.rbac import PLATFORM_ADMIN_PERMISSIONS, principal_permissions
from tests.test_workspace_admin_is_not_platform_admin import (  # noqa: F401
    _workspace_admin,
    root,
)


@pytest.fixture
def flag_default(monkeypatch):
    monkeypatch.delenv("DATAFLOW_WORKSPACE_ADMIN_IAM", raising=False)
    monkeypatch.delenv("DATAWRAP_WORKSPACE_ADMIN_IAM", raising=False)


@pytest.fixture
def events(monkeypatch):
    seen = []
    real = audit_coverage.record_security_event

    def spy(event, **kw):
        seen.append((event, kw))
        return real(event, **kw)

    monkeypatch.setattr(audit_coverage, "record_security_event", spy)
    return seen


def test_default_keeps_workspace_admin_iam_and_scim(root, flag_default, events):
    member = {"email": "m@example.com", "role": "member"}
    assert PLATFORM_ADMIN_PERMISSIONS <= principal_permissions(member, "admin")

    peer, _ws = _workspace_admin(root)
    assert peer.get("/api/v1/iam/service-accounts").status_code == 200
    assert peer.get("/api/v1/scim/v2/Users").status_code == 200

    would = [kw for name, kw in events if name == "authz.workspace_admin_iam.would_deny"]
    assert {kw["details"]["required_permission"] for kw in would} == {
        "iam.manage",
        "scim.provision",
    }


def test_platform_admin_is_not_audited_as_would_deny(root, flag_default, events):
    assert root.get("/api/v1/iam/service-accounts").status_code == 200
    assert not [n for n, _ in events if n == "authz.workspace_admin_iam.would_deny"]


def test_flag_off_is_the_strict_rule(root, monkeypatch):
    monkeypatch.setenv("DATAFLOW_WORKSPACE_ADMIN_IAM", "0")
    member = {"email": "m@example.com", "role": "member"}
    assert not principal_permissions(member, "admin") & PLATFORM_ADMIN_PERMISSIONS
    peer, _ws = _workspace_admin(root)
    assert peer.get("/api/v1/iam/service-accounts").status_code == 403


@pytest.mark.parametrize("flag", ["1", "0"])
def test_workspace_api_keys_are_scoped_to_the_callers_workspace(root, monkeypatch, flag):
    monkeypatch.setenv("DATAFLOW_WORKSPACE_ADMIN_IAM", flag)
    legacy = integrations_store.create_api_key("Legacy", "root@example.com", expires_in="30d")
    peer, ws = _workspace_admin(root)
    other = integrations_store.create_api_key(
        "Other ws", "x@example.com", expires_in="30d", workspace_id="ws-someone-else"
    )
    if flag == "0":
        assert peer.get("/api/v1/workspace/api-keys").status_code == 403
        return

    mine = peer.post("/api/v1/workspace/api-keys", json={"name": "Mine", "role": "editor"})
    assert mine.status_code == 200, mine.text
    assert mine.json()["workspace_id"] == ws

    listed = {k["id"] for k in peer.get("/api/v1/workspace/api-keys").json()["keys"]}
    assert listed == {mine.json()["id"]}

    for foreign in (legacy, other):
        assert peer.delete(f"/api/v1/workspace/api-keys/{foreign['id']}").status_code == 404
    assert {k["id"] for k in integrations_store.list_api_keys()} >= {legacy["id"], other["id"]}

    assert peer.delete(f"/api/v1/workspace/api-keys/{mine.json()['id']}").status_code == 200

    everything = {k["id"] for k in root.get("/api/v1/workspace/api-keys").json()["keys"]}
    assert {legacy["id"], other["id"]} <= everything
