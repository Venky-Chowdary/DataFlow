"""The MCP approval bypass: an MCP client could spend the approval it staged.

Staging tools return an ``ack_id``; ``confirm_action`` (MCP) or
``POST /api/v1/copilot/confirm`` performs the mutation. Any caller who could
read the workspace and do the staged kind of change could confirm on any
channel, so an MCP client that auto-approves its own tool calls removed the
human approval with the same workspace API key it staged with.

The workspace confirm policy (services.mcp_confirm_policy) now decides, with
``interactive`` as the default: confirm only from an interactive sign-in
session, never through MCP, never with an API key or MCP OAuth token, and
only by the stager or a workspace admin. ``four_eyes`` adds a second person;
``agent`` is the explicit opt-out.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import mcp_confirm_policy as policy  # noqa: E402
from services.mcp_confirm_policy import ConfirmRefused, check_confirm_channel  # noqa: E402
from tests.test_mcp_policy import policy_client  # noqa: E402,F401 - shared harness


def _request(*, path: str = "/api/v1/copilot/confirm", user: dict | None = None,
             api_key: bool = False, cookie: bool = False) -> Any:
    state = SimpleNamespace(user=user, api_key_auth=api_key)
    if cookie:
        state.cookie_session_jti = "jti-1"
    return SimpleNamespace(state=state, url=SimpleNamespace(path=path))


def _check(request: Any, *, mode: str, role: str = "editor", actor: str = "alice@x",
           staged_by: str = "alice@x", auth: bool = True, monkeypatch) -> str:
    monkeypatch.setattr(policy, "confirm_mode_for", lambda ws: mode)
    return check_confirm_channel(
        request, staged_by=staged_by, staged_in="ws1", role=role, actor=actor, auth_enforced=auth,
    )


_SESSION = {"email": "alice@x", "role": "editor"}
_KEY = {"email": "alice@x", "role": "admin", "auth_kind": "api_key"}
_OAUTH = {"email": "alice@x", "role": "admin", "auth_kind": "oauth"}


def test_default_mode_is_interactive_when_nothing_is_stored(monkeypatch):
    import services.integrations_store as store

    monkeypatch.setattr(store, "get_mcp_policy", lambda: {"enabled": True, "allowed_tools": None})
    assert policy.confirm_mode_for("ws1") == "interactive"


def test_unreadable_policy_store_never_falls_open(monkeypatch):
    import services.integrations_store as store

    def _boom() -> dict:
        raise RuntimeError("control plane down")

    monkeypatch.setattr(store, "get_mcp_policy", _boom)
    assert policy.confirm_mode_for("ws1") == "interactive"


def test_per_workspace_mode_overrides_the_default(monkeypatch):
    import services.integrations_store as store

    monkeypatch.setattr(store, "get_mcp_policy", lambda: {"confirm": {
        "default": "interactive", "workspaces": {"ops": "agent", "fin": "four_eyes", "bad": "yolo"}}})
    assert policy.confirm_mode_for("ops") == "agent"
    assert policy.confirm_mode_for("fin") == "four_eyes"
    assert policy.confirm_mode_for("bad") == "interactive"
    assert policy.confirm_mode_for("other") == "interactive"


@pytest.mark.parametrize("mode", ["interactive", "four_eyes"])
def test_mcp_confirm_action_is_refused_even_for_an_admin(mode, monkeypatch):
    req = _request(path="/api/v1/mcp", user=_KEY, api_key=True)
    with pytest.raises(ConfirmRefused, match="MCP client cannot spend the approval"):
        _check(req, mode=mode, role="admin", actor="bob@x", monkeypatch=monkeypatch)


@pytest.mark.parametrize("user,api_key", [(_KEY, True), (_OAUTH, False)])
def test_rest_confirm_with_an_agent_credential_is_refused(user, api_key, monkeypatch):
    req = _request(user=user, api_key=api_key)
    with pytest.raises(ConfirmRefused, match="interactive sign-in session"):
        _check(req, mode="interactive", role="admin", monkeypatch=monkeypatch)


@pytest.mark.parametrize("cookie", [True, False], ids=["cookie-session", "session-token"])
def test_stager_confirms_from_an_interactive_session(cookie, monkeypatch):
    req = _request(user=_SESSION, cookie=cookie)
    assert _check(req, mode="interactive", monkeypatch=monkeypatch) == "interactive"


def test_another_member_cannot_confirm_but_an_admin_can(monkeypatch):
    req = _request(user={"email": "carol@x", "role": "editor"})
    with pytest.raises(ConfirmRefused, match="Only the person who staged"):
        _check(req, mode="interactive", actor="carol@x", monkeypatch=monkeypatch)
    assert _check(req, mode="interactive", actor="carol@x", role="admin", monkeypatch=monkeypatch)


def test_four_eyes_needs_a_different_admin(monkeypatch):
    own = _request(user={"email": "alice@x", "role": "admin"})
    with pytest.raises(ConfirmRefused, match="second person"):
        _check(own, mode="four_eyes", role="admin", monkeypatch=monkeypatch)
    editor = _request(user={"email": "bob@x", "role": "editor"})
    with pytest.raises(ConfirmRefused, match="workspace admin"):
        _check(editor, mode="four_eyes", role="editor", actor="bob@x", monkeypatch=monkeypatch)
    admin = _request(user={"email": "bob@x", "role": "admin"})
    assert _check(admin, mode="four_eyes", role="admin", actor="bob@x", monkeypatch=monkeypatch) == "four_eyes"


def test_agent_mode_is_the_previous_behaviour(monkeypatch):
    req = _request(path="/api/v1/mcp", user=_KEY, api_key=True)
    assert _check(req, mode="agent", role="admin", monkeypatch=monkeypatch) == "agent"


def test_open_deployment_still_refuses_the_mcp_channel(monkeypatch):
    with pytest.raises(ConfirmRefused):
        _check(_request(path="/api/v1/mcp"), mode="interactive", role="", actor="", auth=False,
               monkeypatch=monkeypatch)
    assert _check(_request(), mode="interactive", role="", actor="", auth=False, monkeypatch=monkeypatch)


# ------------------------------------------------------------ through the app


@pytest.fixture
def app_client(policy_client):
    """The MCP policy suite's app: auth enforced, viewer / scoped / admin API keys."""
    return policy_client


def _stage(staged_by: str) -> str:
    from src.ai.copilot.ack_ledger import get_ack_ledger

    ledger = get_ack_ledger()
    aid = ledger.put(kind="cancel_job", payload={"job_id": "job-does-not-exist"})
    entries = getattr(ledger, "_entries", None)
    assert entries is not None, "test expects the file ledger"
    entries[aid]["staged_by"] = staged_by
    return aid


def _pending(aid: str) -> bool:
    from src.ai.copilot.ack_ledger import get_ack_ledger

    peek = get_ack_ledger().peek(aid)
    return bool(peek) and not peek.get("consumed")


def test_an_mcp_client_cannot_confirm_its_own_staged_approval(app_client):
    client, admin = app_client["client"], app_client["tokens"]["admin"]
    aid = _stage("admin@example.test")

    via_mcp = client.post(
        "/api/v1/mcp",
        headers={"Authorization": f"Bearer {admin}", "Accept": "application/json"},
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/call",
              "params": {"name": "confirm_action", "arguments": {"ack_id": aid}}},
    )
    assert via_mcp.status_code == 200
    assert "cannot spend the approval" in via_mcp.text, via_mcp.text

    via_rest = client.post(
        "/api/v1/copilot/confirm",
        headers={"Authorization": f"Bearer {admin}"},
        json={"ack_id": aid, "reason": "auto"},
    )
    assert via_rest.status_code == 403, via_rest.text
    assert "interactive sign-in session" in via_rest.text

    assert _pending(aid), "a refused confirm must leave the approval pending"
    refused = [e for e in app_client["audit_events"] if e.get("action") == "pilot.confirm.refused_channel"]
    assert len(refused) == 2


def test_an_api_key_cannot_switch_the_confirm_policy_off(app_client):
    client, admin = app_client["client"], app_client["tokens"]["admin"]
    resp = client.put(
        "/api/v1/mcp/policy",
        headers={"Authorization": f"Bearer {admin}"},
        json={"enabled": True, "allowed_tools": None, "confirm": {"default": "agent"}},
    )
    assert resp.status_code == 403, resp.text
    assert app_client["integrations_store"].get_mcp_policy()["confirm"]["default"] == "interactive"
    # The rest of the policy stays editable by the same key, and keeps confirm.
    resp = client.put(
        "/api/v1/mcp/policy",
        headers={"Authorization": f"Bearer {admin}"},
        json={"enabled": True, "allowed_tools": None},
    )
    assert resp.status_code == 200
    assert resp.json()["confirm"]["default"] == "interactive"


def test_policy_store_rejects_an_unknown_mode(app_client):
    store = app_client["integrations_store"]
    with pytest.raises(ValueError, match="confirm policy must be one of"):
        store.set_mcp_policy({"enabled": True, "confirm": {"default": "auto"}})
    saved = store.set_mcp_policy({"enabled": True, "confirm": {"default": "four_eyes",
                                                               "workspaces": {"ops": "agent"}}})
    assert saved["confirm"] == {"default": "four_eyes", "workspaces": {"ops": "agent"}}


# ------------------------------------------- who may change the confirm policy


def _merge(requested: dict, *, admin: bool = False, ws: str = "A", stored: dict | None = None) -> dict:
    stored = stored or {"default": "interactive", "workspaces": {"B": "four_eyes"}}
    return policy.merge_confirm_change(stored, requested, platform_admin=admin, caller_workspace=ws)


def test_workspace_admin_cannot_change_the_default_or_another_workspace():
    """Review: PUT /mcp/policy needs only workspace.manage, the store is deployment-wide."""
    with pytest.raises(PermissionError, match="platform admin"):
        _merge({"default": "agent"})
    with pytest.raises(PermissionError, match="'B'"):
        _merge({"workspaces": {"B": "agent"}})
    with pytest.raises(PermissionError, match="'B'"):
        _merge({"workspaces": {"B": None}})


def test_workspace_admin_changes_only_their_own_entry_and_b_survives():
    merged = _merge({"default": "interactive", "workspaces": {"A": "agent"}})
    assert merged == {"default": "interactive", "workspaces": {"A": "agent", "B": "four_eyes"}}
    cleared = _merge({"workspaces": {"A": None}}, stored=merged)
    assert cleared == {"default": "interactive", "workspaces": {"B": "four_eyes"}}


def test_platform_admin_may_change_the_default_and_any_workspace():
    merged = _merge({"default": "four_eyes", "workspaces": {"B": "interactive"}}, admin=True)
    assert merged == {"default": "four_eyes", "workspaces": {"B": "interactive"}}


def test_workspace_admin_put_merges_and_cannot_touch_b(app_client, monkeypatch):
    import services.effective_role as effective_role
    import services.rbac as rbac

    client, admin = app_client["client"], app_client["tokens"]["admin"]
    store = app_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "confirm": {"default": "interactive", "workspaces": {"B": "four_eyes"}}})
    # A workspace admin of A, signed in interactively (not a platform admin).
    monkeypatch.setattr(policy, "credential_kind", lambda request: "session_token")
    monkeypatch.setattr(rbac, "holds_platform_admin_role", lambda user: False)
    monkeypatch.setattr(effective_role, "workspace_id_from_request_headers", lambda headers: "A")
    headers = {"Authorization": f"Bearer {admin}"}

    refused = client.put("/api/v1/mcp/policy", headers=headers,
                         json={"enabled": True, "confirm": {"workspaces": {"B": "agent"}}})
    assert refused.status_code == 403, refused.text
    refused = client.put("/api/v1/mcp/policy", headers=headers,
                         json={"enabled": True, "confirm": {"default": "agent"}})
    assert refused.status_code == 403, refused.text

    ok = client.put("/api/v1/mcp/policy", headers=headers,
                    json={"enabled": True, "confirm": {"workspaces": {"A": "agent"}}})
    assert ok.status_code == 200, ok.text
    assert ok.json()["confirm"] == {"default": "interactive", "workspaces": {"A": "agent", "B": "four_eyes"}}
    audited = [e for e in app_client["audit_events"] if e.get("action") == "mcp.policy.updated"]
    assert audited[-1]["details"]["old"]["confirm"]["workspaces"] == {"B": "four_eyes"}
    assert audited[-1]["details"]["new"]["confirm"]["workspaces"] == {"A": "agent", "B": "four_eyes"}
