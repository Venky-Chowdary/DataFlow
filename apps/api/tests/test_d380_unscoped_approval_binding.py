"""D380 AC-3 / AC-4: an approval staged with no workspace is bound to the
confirming caller's workspace only when that hands them nothing of another
tenant's: they staged it, or every connector it uses is their workspace's.
Refusals say "transfer", not "schedule".
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException


class _Scope(SimpleNamespace):
    def can_write(self, workspace_id: str) -> bool:
        return workspace_id in self.writable

    def target_workspace_id(self, *, write: bool = False) -> str:  # noqa: ARG002
        return self.target


@pytest.fixture
def world(monkeypatch):
    import services.connector_store as connector_store
    import services.tenant_scope as tenant_scope

    owners = {"conn-a": "ws-a", "conn-b": "ws-b", "conn-legacy": ""}
    state = {"scope": _Scope(actor="member@example.com", target="ws-a", writable={"ws-a"})}
    monkeypatch.setattr(tenant_scope, "resolve_tenant_scope", lambda _req: state["scope"])
    monkeypatch.setattr(
        connector_store, "connector_owner_workspace",
        lambda cid: (cid in owners, owners.get(cid, "")),
    )
    monkeypatch.setattr("services.team_store.require_workspace_isolation", lambda: True)
    return state


def _payload(src: str, dst: str, workspace: str = "") -> dict:
    return {
        "workspace_id": workspace,
        "source": {"connector_id": src, "table": "t"},
        "destination": {"connector_id": dst, "table": "t"},
    }


def _confirm(payload, staged_by=""):
    from services.confirm_transfer_workspace import confirm_transfer_workspace

    return confirm_transfer_workspace(payload, staged_by=staged_by, http_request=None)


def test_unscoped_approval_over_the_callers_own_connectors_is_stamped_there(world):
    out = _confirm(_payload("conn-a", "conn-a"))
    assert out["workspace_id"] == "ws-a"


def test_unscoped_approval_over_another_workspaces_connector_is_refused(world):
    for src, dst in (("conn-b", "conn-a"), ("conn-a", "conn-b"), ("conn-legacy", "conn-a"),
                     ("conn-gone", "conn-a")):
        with pytest.raises(HTTPException) as refused:
            _confirm(_payload(src, dst), staged_by="someone-else@example.com")
        assert refused.value.status_code == 403
        assert "transfer" in refused.value.detail and "schedule" not in refused.value.detail


def test_the_stager_may_confirm_their_own_unscoped_approval(world):
    out = _confirm(_payload("conn-legacy", "conn-a"), staged_by="member@example.com")
    assert out["workspace_id"] == "ws-a"


def test_an_inline_endpoint_on_an_unscoped_approval_needs_the_stager(world):
    payload = _payload("conn-a", "conn-a")
    payload["source"] = {"host": "db.example", "table": "t"}
    with pytest.raises(HTTPException) as refused:
        _confirm(payload, staged_by="")
    assert refused.value.status_code == 403


def test_a_scoped_approval_needs_write_access_there_and_says_transfer(world):
    assert _confirm(_payload("conn-a", "conn-a", "ws-a"))["workspace_id"] == "ws-a"
    with pytest.raises(HTTPException) as refused:
        _confirm(_payload("conn-b", "conn-b", "ws-b"))
    assert refused.value.status_code == 403
    assert "transfer" in refused.value.detail and "schedule" not in refused.value.detail


def test_no_workspace_at_all_is_refused_under_isolation(world):
    world["scope"] = _Scope(actor="admin@example.com", target="", writable=set())
    with pytest.raises(HTTPException) as refused:
        _confirm(_payload("conn-a", "conn-a"))
    assert refused.value.status_code == 400
    assert "transfer" in refused.value.detail
