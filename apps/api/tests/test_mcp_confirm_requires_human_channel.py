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
        "default": "interactive", "workspaces": {"ops": "agent", "fin": "four_eyes", "bad": "yolo"},
        "set_by_platform_admin": ["ops"]}})
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
    merged = _merge({"default": "interactive", "workspaces": {"A": "four_eyes"}})
    assert merged == {"default": "interactive", "workspaces": {"A": "four_eyes", "B": "four_eyes"}}
    cleared = _merge({"workspaces": {"A": None}}, stored=merged)
    assert cleared == {"default": "interactive", "workspaces": {"B": "four_eyes"}}


def test_platform_admin_may_change_the_default_and_any_workspace():
    merged = _merge({"default": "four_eyes", "workspaces": {"B": "interactive"}}, admin=True)
    assert merged == {"default": "four_eyes", "workspaces": {"B": "interactive"},
                      "set_by_platform_admin": ["B"]}


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
                    json={"enabled": True, "confirm": {"workspaces": {"A": "four_eyes"}}})
    assert ok.status_code == 200, ok.text
    assert ok.json()["confirm"] == {"default": "interactive", "workspaces": {"A": "four_eyes", "B": "four_eyes"}}
    audited = [e for e in app_client["audit_events"] if e.get("action") == "mcp.policy.updated"]
    assert audited[-1]["details"]["old"]["confirm"]["workspaces"] == {"B": "four_eyes"}
    assert audited[-1]["details"]["new"]["confirm"]["workspaces"] == {"A": "four_eyes", "B": "four_eyes"}



# ------------------------------------------ review 2: ceiling and atomicity


def test_workspace_admin_cannot_loosen_below_the_default():
    """N6: agent < interactive < four_eyes; the default is a floor for workspace admins."""
    with pytest.raises(PermissionError, match="looser than the deployment default"):
        _merge({"workspaces": {"A": "agent"}})
    with pytest.raises(PermissionError, match="looser"):
        _merge({"workspaces": {"A": "interactive"}}, stored={"default": "four_eyes", "workspaces": {}})
    assert _merge({"workspaces": {"A": "agent"}}, admin=True)["workspaces"]["A"] == "agent"
    assert _merge({"workspaces": {"A": "four_eyes"}})["workspaces"]["A"] == "four_eyes"


def test_non_mapping_workspaces_is_a_validation_error():
    with pytest.raises(ValueError, match="workspaces must be an object"):
        _merge({"workspaces": ["A"]})
    with pytest.raises(ValueError, match="workspaces must be an object"):
        _merge({"workspaces": 5})


def test_concurrent_policy_updates_do_not_lose_each_other(app_client, monkeypatch):
    """N7: read-merge-write raced; two admins of different workspaces lost an update."""
    import threading
    import time

    store = app_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "confirm": {"default": "interactive", "workspaces": {}}})

    def slow_merge(ws: str):
        def _m(stored):
            current = policy.normalize_confirm_policy(stored)
            time.sleep(0.2)  # both readers see the same stored policy without the lock
            return policy.merge_confirm_change(current, {"workspaces": {ws: "four_eyes"}},
                                               platform_admin=False, caller_workspace=ws)
        return _m

    threads = [threading.Thread(target=store.set_mcp_policy, args=({"enabled": True},),
                                kwargs={"confirm_merge": slow_merge(ws)}) for ws in ("A", "B", "C")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert store.get_mcp_policy()["confirm"]["workspaces"] == {"A": "four_eyes", "B": "four_eyes", "C": "four_eyes"}


def test_compare_and_swap_conflict_merges_onto_the_newer_version(app_client, monkeypatch):
    """A second replica wrote between read and write: the change is re-merged, not lost."""
    import json

    import services.integrations_persistence as persistence

    store = app_client["integrations_store"]
    calls = {"n": 0}

    def cas(new_policy, expected_rev, updated_at):
        calls["n"] += 1
        if calls["n"] == 1:
            # The other replica's write lands now (B -> four_eyes).
            raw = store._load_raw()
            raw["mcp_policy"]["confirm"]["workspaces"]["B"] = "four_eyes"
            raw["mcp_policy"]["rev"] = expected_rev + 1
            store.STORE_PATH.write_text(json.dumps(raw), encoding="utf-8")
            return "conflict"
        return "nodb"

    monkeypatch.setattr(persistence, "save_mcp_policy_if_revision", cas)
    store.set_mcp_policy({"enabled": True}, confirm_merge=lambda stored: policy.merge_confirm_change(
        stored, {"workspaces": {"A": "four_eyes"}}, platform_admin=False, caller_workspace="A"))

    assert calls["n"] == 2
    assert store.get_mcp_policy()["confirm"]["workspaces"] == {"A": "four_eyes", "B": "four_eyes"}


def test_put_with_non_mapping_workspaces_is_422(app_client, monkeypatch):
    client, admin = app_client["client"], app_client["tokens"]["admin"]
    monkeypatch.setattr(policy, "credential_kind", lambda request: "session_token")
    resp = client.put("/api/v1/mcp/policy", headers={"Authorization": f"Bearer {admin}"},
                      json={"enabled": True, "confirm": {"workspaces": ["A"]}})
    assert resp.status_code == 422, resp.text


# ------------------------------------------ review 3: per-section writes, outages


class _FakeSettingsCollection:
    """Enough of a Mongo collection for the integrations document (no server)."""

    def __init__(self, *, down: bool = False):
        self.doc: dict | None = None
        self.down = down
        self.writes: list[str] = []

    def _check(self):
        if self.down:
            raise ConnectionError("server selection timed out")

    @staticmethod
    def _get(doc, path):
        for key in path.split("."):
            if not isinstance(doc, dict) or key not in doc:
                return None
            doc = doc[key]
        return doc

    def _matches(self, match):
        if self.doc is None:
            return False
        for key, want in match.items():
            if key == "_id":
                continue
            if key == "$or":
                if not any(self._matches(alt) for alt in want):
                    return False
            elif isinstance(want, dict) and "$exists" in want:
                if (self._get(self.doc, key) is not None) != want["$exists"]:
                    return False
            elif self._get(self.doc, key) != want:
                return False
        return True

    def find_one(self, match):
        self._check()
        import copy

        return copy.deepcopy(self.doc)

    def insert_one(self, doc):
        self._check()
        self.writes.append("insert")
        if self.doc is not None:
            raise type("DuplicateKeyError", (Exception,), {})("dup")
        import copy

        self.doc = copy.deepcopy(doc)

    def replace_one(self, match, doc, upsert=False):
        self._check()
        self.writes.append("replace")
        import copy

        hit = self._matches(match) or (upsert and self.doc is None)
        if hit:
            self.doc = copy.deepcopy(doc)
        return SimpleNamespace(matched_count=int(hit))

    def update_one(self, match, update, upsert=False):
        self._check()
        self.writes.append("update")
        import copy

        inserted = False
        if self.doc is None and upsert:
            self.doc, inserted = {"_id": match["_id"]}, True
        if not self._matches(match):
            return SimpleNamespace(matched_count=0)
        sets = dict(update.get("$set") or {})
        if inserted:
            sets.update(update.get("$setOnInsert") or {})
        for path, value in sets.items():
            node = self.doc
            *parents, leaf = path.split(".")
            for key in parents:
                node = node.setdefault(key, {})
            node[leaf] = copy.deepcopy(value)
        return SimpleNamespace(matched_count=1)


def _policy_change(store, ws: str):
    store.set_mcp_policy({"enabled": True}, confirm_merge=lambda stored: policy.merge_confirm_change(
        stored, {"workspaces": {ws: "four_eyes"}}, platform_admin=False, caller_workspace=ws))


@pytest.mark.parametrize("database", [True, False], ids=["database", "file"])
def test_another_section_writer_does_not_revert_a_concurrent_policy_change(app_client, monkeypatch, database):
    """Review 3: set_pilot_engine_preference read the whole document, a policy
    change landed, and the Pilot write put the stale policy (and rev) back."""
    import services.integrations_persistence as persistence

    store = app_client["integrations_store"]
    coll = _FakeSettingsCollection()
    monkeypatch.setattr(persistence, "_collection", lambda: coll if database else None)
    real_now = store._now
    fired = {"n": 0}

    def race_then_now():
        # The Pilot writer stamps updated_at after any read, just before it
        # writes: the policy change lands exactly there.
        if not fired["n"]:
            fired["n"] = 1
            _policy_change(store, "A")
        return real_now()

    monkeypatch.setattr(store, "_now", race_then_now)
    store.set_pilot_engine_preference("cloud")
    monkeypatch.setattr(store, "_now", real_now)

    assert fired["n"] == 1
    assert store.get_mcp_policy()["confirm"]["workspaces"] == {"A": "four_eyes"}
    assert store.get_pilot_engine_preference() == "cloud"

    # And the other way round: a policy write carries no stale Pilot section.
    def merge_with_race(stored):
        store.set_pilot_engine_preference("local")
        return policy.merge_confirm_change(stored, {"workspaces": {"B": "four_eyes"}},
                                           platform_admin=False, caller_workspace="B")

    store.set_mcp_policy({"enabled": True}, confirm_merge=merge_with_race)
    assert store.get_pilot_engine_preference() == "local"
    assert store.get_mcp_policy()["confirm"]["workspaces"] == {"A": "four_eyes", "B": "four_eyes"}
    if database:
        assert coll.writes.count("replace") == 0, "no writer replaces the whole document"


def test_a_failed_read_never_heals_the_local_file_over_the_database(app_client, monkeypatch):
    import json

    import services.integrations_persistence as persistence

    store = app_client["integrations_store"]
    store.STORE_PATH.write_text(json.dumps({"pilot": {"engine": "ollama"}}), encoding="utf-8")
    coll = _FakeSettingsCollection(down=True)
    coll.doc = {"_id": persistence.DOC_ID, "pilot": {"engine": "anthropic"}}
    monkeypatch.setattr(persistence, "_collection", lambda: coll)
    store._load_raw()
    assert coll.writes == [], "a transient read error is not an absent document"

    coll.down, coll.doc = False, None
    store._load_raw()
    assert coll.writes and set(coll.writes) == {"update"} and coll.doc["pilot"] == {"engine": "ollama"}


def test_policy_write_while_the_database_is_down_is_503_not_409(app_client, monkeypatch):
    import services.integrations_persistence as persistence

    client, admin = app_client["client"], app_client["tokens"]["admin"]
    coll = _FakeSettingsCollection(down=True)
    monkeypatch.setattr(persistence, "_collection", lambda: coll)
    resp = client.put("/api/v1/mcp/policy", headers={"Authorization": f"Bearer {admin}"},
                      json={"enabled": False, "allowed_tools": None})
    assert resp.status_code == 503, resp.text
    assert "unavailable" in resp.text

    import services.control_plane_store as cps

    monkeypatch.setattr(persistence, "_collection", lambda: None)
    monkeypatch.setattr(cps, "control_plane_configured", lambda: True)
    resp = client.put("/api/v1/mcp/policy", headers={"Authorization": f"Bearer {admin}"},
                      json={"enabled": False, "allowed_tools": None})
    assert resp.status_code == 503, resp.text


def test_a_stored_entry_looser_than_the_default_is_clamped_when_read(app_client):
    """Review 3: the floor was enforced only when a workspace admin wrote."""
    store = app_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "confirm": {"default": "four_eyes",
                                                       "workspaces": {"A": "agent", "C": "four_eyes"}}})
    assert policy.confirm_mode_for("A") == "four_eyes"
    assert policy.confirm_mode_for("C") == "four_eyes"
    # A platform admin's own choice stays in force, and is recorded as theirs.
    store.set_mcp_policy({"enabled": True}, confirm_merge=lambda stored: policy.merge_confirm_change(
        stored, {"workspaces": {"A": "interactive"}}, platform_admin=True, caller_workspace=""))
    store.set_mcp_policy({"enabled": True}, confirm_merge=lambda stored: policy.merge_confirm_change(
        stored, {"workspaces": {"A": "agent"}}, platform_admin=True, caller_workspace=""))
    assert policy.confirm_mode_for("A") == "agent"
    assert store.get_mcp_policy()["confirm"]["set_by_platform_admin"] == ["A"]
    # A workspace admin rewriting the entry takes the platform admin's mark off.
    store.set_mcp_policy({"enabled": True}, confirm_merge=lambda stored: policy.merge_confirm_change(
        stored, {"workspaces": {"A": "four_eyes"}}, platform_admin=False, caller_workspace="A"))
    assert "set_by_platform_admin" not in store.get_mcp_policy()["confirm"]


def test_headerless_policy_change_uses_the_workspace_the_rbac_gate_used(app_client, monkeypatch):
    """Review 3: the gate resolves a headerless request only for exactly one
    membership; the policy route picked the one *admin* membership among several."""
    import services.rbac as rbac
    import services.team_store as team_store

    client, admin = app_client["client"], app_client["tokens"]["admin"]
    store = app_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "confirm": {"default": "interactive", "workspaces": {}}})
    monkeypatch.setattr(policy, "credential_kind", lambda request: "session_token")
    monkeypatch.setattr(rbac, "holds_platform_admin_role", lambda user: False)
    body = {"enabled": True, "confirm": {"workspaces": {"ws-a": "four_eyes"}}}
    headers = {"Authorization": f"Bearer {admin}"}

    monkeypatch.setattr(team_store, "list_memberships_for_user", lambda email: [
        {"workspace_id": "ws-a", "role": "admin"}, {"workspace_id": "ws-b", "role": "viewer"}])
    refused = client.put("/api/v1/mcp/policy", headers=headers, json=body)
    assert refused.status_code == 403, refused.text
    assert store.get_mcp_policy()["confirm"]["workspaces"] == {}

    monkeypatch.setattr(team_store, "list_memberships_for_user", lambda email: [
        {"workspace_id": "ws-a", "role": "admin"}])
    ok = client.put("/api/v1/mcp/policy", headers=headers, json=body)
    assert ok.status_code == 200, ok.text
    assert ok.json()["confirm"]["workspaces"] == {"ws-a": "four_eyes"}


# --------------------------------------------- review 4: re-affirming marks nothing


def test_platform_admin_round_trip_does_not_exempt_pre_floor_entries(app_client, monkeypatch):
    """A platform admin's GET + PUT of the whole policy marked every entry as
    theirs, exempting a pre-floor workspace 'agent' entry from the clamp forever."""
    client, admin = app_client["client"], app_client["tokens"]["admin"]
    store = app_client["integrations_store"]
    store.set_mcp_policy({"enabled": True, "confirm": {"default": "interactive", "workspaces": {"A": "agent"}}})
    assert policy.confirm_mode_for("A") == "interactive"  # clamped

    monkeypatch.setattr(policy, "credential_kind", lambda request: "session_token")
    headers = {"Authorization": f"Bearer {admin}"}
    current = client.get("/api/v1/mcp/policy", headers=headers).json()
    put = client.put("/api/v1/mcp/policy", headers=headers, json=current)
    assert put.status_code == 200, put.text
    assert "set_by_platform_admin" not in put.json()["confirm"]
    assert policy.confirm_mode_for("A") == "interactive"

    # The exemption is an explicit, platform-admin-only choice.
    body = {**current, "confirm": {**current["confirm"], "set_by_platform_admin": ["A"]}}
    assert client.put("/api/v1/mcp/policy", headers=headers, json=body).status_code == 200
    assert policy.confirm_mode_for("A") == "agent"
    with pytest.raises(PermissionError, match="platform admin"):
        policy.merge_confirm_change({"default": "interactive", "workspaces": {"B": "agent"}},
                                    {"set_by_platform_admin": ["B"]}, platform_admin=False, caller_workspace="B")


# ------------------------------- review 4: heal and within-section races


def test_heal_of_an_absent_document_loses_nothing_to_a_concurrent_writer(app_client, monkeypatch):
    """The heal wrote the whole file as one insert: a writer's upsert that landed
    first made it fail, and every other section of the file was never copied up."""
    import json

    import services.integrations_persistence as persistence

    store = app_client["integrations_store"]
    store.STORE_PATH.write_text(json.dumps({
        "sso": {"saml": {"entity_id": "https://idp.example/saml"}},
        "pilot": {"engine": "local"},
        "ai_providers": {"openai": {"model": "file-model"}},
    }), encoding="utf-8")
    coll = _FakeSettingsCollection()
    monkeypatch.setattr(persistence, "_collection", lambda: coll)
    real_read = persistence.read_document
    raced = {"n": 0}

    def read_then_race():
        state = real_read()
        if state[0] == "absent" and not raced["n"]:
            raced["n"] = 1  # another replica's Pilot write upserts the document now
            coll.update_one({"_id": persistence.DOC_ID},
                            {"$set": {"pilot.engine": "cloud", "updated_at": "t"}}, upsert=True)
        return state

    monkeypatch.setattr(persistence, "read_document", read_then_race)
    store._load_raw()

    assert raced["n"] == 1
    assert coll.doc["sso"]["saml"] == {"entity_id": "https://idp.example/saml"}
    assert coll.doc["ai_providers"]["openai"] == {"model": "file-model"}
    assert coll.doc["pilot"] == {"engine": "cloud"}, "the writer's value is not healed over"


def _race_in_thread(monkeypatch, module, name, other):
    """On the first call of ``module.name``, start ``other`` in a thread and give it
    time to run before continuing: without a lock it lands between read and write."""
    import threading
    import time

    real = getattr(module, name)
    state: dict = {"thread": None, "real": real}

    def wrapped(*args, **kwargs):
        if state["thread"] is None:
            state["thread"] = threading.Thread(target=other)
            state["thread"].start()
            time.sleep(0.4)
        return real(*args, **kwargs)

    monkeypatch.setattr(module, name, wrapped)
    return state


def test_a_create_does_not_resurrect_a_concurrent_revoke(app_client, monkeypatch):
    store = app_client["integrations_store"]
    first = store.create_api_key("first", "admin@example.test", expires_in="30d")
    state = _race_in_thread(monkeypatch, store, "_save_file_keys",
                            lambda: store.revoke_api_key(first["id"]))
    second = store.create_api_key("second", "admin@example.test", expires_in="30d")
    state["thread"].join(10)
    monkeypatch.setattr(store, "_save_file_keys", state["real"])

    records = {r["id"]: r for r in store._file_key_records()}
    assert second["id"] in records
    assert records[first["id"]].get("revoked_at"), "the revoke was put back by the create"


@pytest.mark.parametrize("database", [True, False], ids=["database", "file"])
def test_two_fields_of_one_provider_updated_at_once_both_stick(app_client, monkeypatch, database):
    import services.integrations_persistence as persistence

    store = app_client["integrations_store"]
    coll = _FakeSettingsCollection()
    monkeypatch.setattr(persistence, "_collection", lambda: coll if database else None)
    store.update_ai_provider("openai", {"model": "m1"})
    before = store._load_raw()["ai_providers"]["openai"]["api_key"]
    state = _race_in_thread(monkeypatch, store, "_now",
                            lambda: store.update_ai_provider("openai", {"api_key": "sk-concurrent-1234"}))
    store.update_ai_provider("openai", {"model": "m2"})
    state["thread"].join(10)

    cfg = store._load_raw()["ai_providers"]["openai"]
    assert cfg["model"] == "m2"
    assert cfg["api_key"] and cfg["api_key"] != before, "the concurrent key change was reverted"
