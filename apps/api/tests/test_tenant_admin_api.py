"""Tenant lifecycle over HTTP: created, amended, removed — never a 500.

Creating an enterprise tenant answered ``500 Internal Server Error`` for every
caller: the route asked ``can_admin_workspace`` for the authority to proceed and
that name was never imported, so the first authorization check raised
``NameError`` inside the handler. These tests drive the real routes so the
authority question is asked of the resolver the API gate uses — the platform
label plus the membership row — and pin the refusals that must stay refusals.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest
from fastapi.testclient import TestClient

from services import team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits
from src.main import app


@pytest.fixture
def stores(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    import services.tenant_store as tenant_store
    from services import mfa_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(mfa_store, "mongo_database", lambda: None)
    return tmp_path


@pytest.fixture
def admin(stores, monkeypatch):
    """A deployment with authentication on, signed in as the platform admin."""
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "root@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "Bootstrap-Admin-2026")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    reset_auth_rate_limits()
    c = TestClient(app)
    login = c.post(
        "/api/v1/auth/login",
        json={"email": "root@example.com", "password": "Bootstrap-Admin-2026"},
    )
    assert login.status_code == 200, login.text
    c.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return c


def _workspace(client, name: str) -> str:
    response = client.post("/api/v1/team/workspaces", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()["workspace"]["id"]


def _signed_in(client, *, email: str, workspace_id: str, workspace_role: str) -> TestClient:
    """A second identity, created by the admin and signed in for itself."""
    created = client.post(
        "/api/v1/team/users",
        json={
            "email": email,
            "platform_role": "member",
            "workspace_id": workspace_id,
            "workspace_role": workspace_role,
        },
    )
    assert created.status_code == 200, created.text
    issued = created.json()["temporary_password"]
    peer = TestClient(app)
    login = peer.post("/api/v1/auth/login", json={"email": email, "password": issued})
    assert login.status_code == 200, login.text
    peer.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return peer


def _enroll_two_factor(client) -> None:
    """Requiring MFA is refused from a session the policy would hold (round 11)."""
    from services import totp

    secret = client.post("/api/v1/auth/mfa/totp/enroll", json={}).json()["secret"]
    code = totp.totp(totp.secret_bytes(secret))
    assert client.post("/api/v1/auth/mfa/totp/confirm", json={"code": code}).status_code == 200


def test_platform_admin_creates_a_tenant_for_a_workspace(admin):
    ws_id = _workspace(admin, "Acme")
    _enroll_two_factor(admin)

    created = admin.post(
        "/api/v1/workspace/tenant",
        json={
            "workspace_id": ws_id,
            "name": "Acme Enterprise",
            "custom_domain": "data.acme.com",
            "data_region": "us-east-1",
            "security_contact_email": "security@acme.com",
            "mfa_required": True,
            "session_timeout_hours": 4,
            "ip_allowlist": ["203.0.113.0/24"],
        },
    )
    assert created.status_code == 200, created.text
    tenant = created.json()
    assert tenant["workspace_id"] == ws_id
    assert tenant["name"] == "Acme Enterprise"
    assert tenant["mfa_required"] is True
    assert tenant["session_timeout_hours"] == 4
    assert tenant["ip_allowlist"] == ["203.0.113.0/24"]

    listed = admin.get("/api/v1/workspace/tenants")
    assert listed.status_code == 200, listed.text
    assert [t["id"] for t in listed.json()["tenants"]] == [tenant["id"]]

    events = admin.get("/api/v1/audit/events", params={"limit": 100})
    assert events.status_code == 200, events.text
    assert "workspace.tenant.create" in {
        e.get("action") for e in events.json().get("events", [])
    }


def test_tenant_without_a_workspace_is_refused_and_a_named_one_reads_back(admin):
    """A tenant is resolved by workspace, so it must be created against one.

    An unnamed ``workspace_id`` wrote a tenant that ``GET /workspace/tenant``
    could never resolve — saved, invisible on reload, and never authorized
    against any workspace.
    """
    unscoped = admin.post("/api/v1/workspace/tenant", json={"name": "Nowhere"})
    assert unscoped.status_code == 400, unscoped.text
    assert "workspace_id" in unscoped.json()["detail"]
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []

    ws_id = _workspace(admin, "Scoped")
    # The header alone names the workspace, as the browser sends it.
    created = admin.post(
        "/api/v1/workspace/tenant",
        json={"name": "Scoped Tenant"},
        headers={"X-Workspace-Id": ws_id},
    )
    assert created.status_code == 200, created.text
    assert created.json()["workspace_id"] == ws_id

    read = admin.get("/api/v1/workspace/tenant", headers={"X-Workspace-Id": ws_id})
    assert read.status_code == 200, read.text
    assert read.json()["name"] == "Scoped Tenant"


def test_saved_egress_allowlist_turns_security_posture_on_for_that_workspace(admin):
    """The Security card reads this flag. It must follow the tenant just saved.

    An unscoped read stays off, and a second workspace does not inherit the list.
    Clearing the list turns the flag off again.
    """
    ws_id = _workspace(admin, "Egress On")
    other = _workspace(admin, "Egress Off")
    headers = {"X-Workspace-Id": ws_id}

    before = admin.get("/api/v1/workspace/security/posture", headers=headers)
    assert before.status_code == 200, before.text
    assert before.json()["egress_allowlist_enabled"] is False
    assert before.json()["scope_workspace_id"] == ws_id
    assert "no-store" in (before.headers.get("cache-control") or "")

    created = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Egress On", "egress_allowlist": ["10.0.0.0/8"]},
        headers=headers,
    )
    assert created.status_code == 200, created.text
    assert created.json()["egress_allowlist"] == ["10.0.0.0/8"]

    on = admin.get("/api/v1/workspace/security/posture", headers=headers)
    assert on.status_code == 200, on.text
    assert on.json()["egress_allowlist_enabled"] is True
    assert on.json()["scope_workspace_id"] == ws_id

    sibling = admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": other})
    assert sibling.json()["egress_allowlist_enabled"] is False
    assert sibling.json()["scope_workspace_id"] == other

    unscoped = admin.get("/api/v1/workspace/security/posture")
    assert unscoped.json()["egress_allowlist_enabled"] is False
    assert unscoped.json()["scope_workspace_id"] in (None, "")

    cleared = admin.patch(
        f"/api/v1/workspace/tenant/{created.json()['id']}",
        json={"egress_allowlist": []},
        headers=headers,
    )
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["egress_allowlist"] == []
    off = admin.get("/api/v1/workspace/security/posture", headers=headers)
    assert off.json()["egress_allowlist_enabled"] is False


def test_tenant_created_for_a_workspace_other_than_the_active_one_is_refused(admin):
    """Saving into a workspace the session is not scoped to is a contradiction."""
    active = _workspace(admin, "Active")
    other = _workspace(admin, "Other")
    conflict = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": other, "name": "Mismatched"},
        headers={"X-Workspace-Id": active},
    )
    assert conflict.status_code == 403, conflict.text
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []


def test_tenant_is_amended_and_removed_by_its_workspace_admin(admin):
    ws_id = _workspace(admin, "Beta")
    lead = _signed_in(admin, email="lead@example.com", workspace_id=ws_id, workspace_role="admin")

    created = lead.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Beta", "data_region": "eu-west-1"},
    )
    assert created.status_code == 200, created.text
    tenant_id = created.json()["id"]

    amended = lead.patch(
        f"/api/v1/workspace/tenant/{tenant_id}",
        json={"name": "Beta Renamed", "session_timeout_hours": 12},
    )
    assert amended.status_code == 200, amended.text
    assert amended.json()["name"] == "Beta Renamed"
    assert amended.json()["session_timeout_hours"] == 12

    removed = lead.delete(f"/api/v1/workspace/tenant/{tenant_id}")
    assert removed.status_code == 200, removed.text
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []


def test_tenant_removal_is_filed_at_the_level_the_warnings_view_asks_for(admin):
    """Removing a tenant is the most destructive act here; it must not read as info.

    The audit reader filters on the stored value, so ``level="warning"`` — the
    English word rather than this system's ``warn`` — hid the deletion from the
    Warnings view while every other warning-level event appeared there.
    """
    ws_id = _workspace(admin, "Zeta")
    created = admin.post(
        "/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Zeta"}
    )
    assert created.status_code == 200, created.text
    assert admin.delete(f"/api/v1/workspace/tenant/{created.json()['id']}").status_code == 200

    warned = admin.get("/api/v1/audit/events", params={"limit": 200, "level": "warn"})
    assert warned.status_code == 200, warned.text
    assert "workspace.tenant.delete" in {
        e.get("action") for e in warned.json().get("events", [])
    }


def test_workspace_editor_is_refused_in_words_not_by_a_fault(admin):
    ws_id = _workspace(admin, "Gamma")
    editor = _signed_in(
        admin, email="editor@example.com", workspace_id=ws_id, workspace_role="editor"
    )

    refused = editor.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Gamma"},
    )
    assert refused.status_code == 403, refused.text
    body = refused.json()
    assert body["required_permission"] == "workspace.manage"
    assert body["effective_role"] == "editor"


def test_admin_of_one_workspace_cannot_create_a_tenant_for_another(admin):
    """The body names the workspace, so the header cannot borrow its authority."""
    mine = _workspace(admin, "Mine")
    theirs = _workspace(admin, "Theirs")
    lead = _signed_in(admin, email="mine@example.com", workspace_id=mine, workspace_role="admin")

    refused = lead.post(
        "/api/v1/workspace/tenant",
        headers={"X-Workspace-Id": mine},
        json={"workspace_id": theirs, "name": "Not mine"},
    )
    assert refused.status_code == 403, refused.text
    assert "Workspace admin required" in refused.text
    assert admin.get("/api/v1/workspace/tenants").json()["tenants"] == []


def test_second_tenant_for_one_workspace_is_a_stated_conflict(admin):
    ws_id = _workspace(admin, "Delta")
    first = admin.post(
        "/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Delta"}
    )
    assert first.status_code == 200, first.text

    again = admin.post(
        "/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Delta again"}
    )
    assert again.status_code == 400, again.text
    assert "already has a tenant" in again.text


def test_domain_already_in_use_is_stated_not_a_fault(admin):
    first_ws = _workspace(admin, "Eps1")
    second_ws = _workspace(admin, "Eps2")
    assert (
        admin.post(
            "/api/v1/workspace/tenant",
            json={"workspace_id": first_ws, "name": "Eps1", "custom_domain": "data.eps.com"},
        ).status_code
        == 200
    )
    clash = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": second_ws, "name": "Eps2", "custom_domain": "www.data.eps.com"},
    )
    assert clash.status_code == 400, clash.text
    assert "already in use" in clash.text


def test_workspace_admin_cannot_list_another_tenant(admin):
    mine = _workspace(admin, "Mine-list")
    theirs = _workspace(admin, "Theirs-list")
    created = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": theirs, "name": "Secret tenant", "custom_domain": "secret.example.com"},
    )
    assert created.status_code == 200, created.text
    lead = _signed_in(admin, email="mine-list@example.com", workspace_id=mine, workspace_role="admin")
    listed = lead.get("/api/v1/workspace/tenants")
    assert listed.status_code == 200, listed.text
    assert listed.json()["tenants"] == []


def test_missing_tenant_is_a_404_on_every_verb(admin):
    assert admin.patch("/api/v1/workspace/tenant/nope", json={"name": "x"}).status_code == 404
    assert admin.delete("/api/v1/workspace/tenant/nope").status_code == 404


def _unbind(tenant_id: str) -> None:
    """Rewrite a stored tenant as a pre-scoping record (``workspace_id: ""``)."""
    import json

    import services.tenant_store as tenant_store

    raw = json.loads(tenant_store.STORE_PATH.read_text(encoding="utf-8"))
    for t in raw["tenants"]:
        if t["id"] == tenant_id:
            t["workspace_id"] = ""
    tenant_store.STORE_PATH.write_text(json.dumps(raw), encoding="utf-8")


def test_unbound_tenant_is_refused_to_a_workspace_admin_on_every_verb(admin):
    """An empty ``workspace_id`` must fail closed, not skip the authority check."""
    import services.tenant_store as tenant_store

    ws_id = _workspace(admin, "Orphaned")
    created = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws_id, "name": "Legacy", "custom_domain": "legacy.example.com"},
    )
    assert created.status_code == 200, created.text
    tenant_id = created.json()["id"]
    _unbind(tenant_id)
    lead = _signed_in(admin, email="orphan-lead@example.com", workspace_id=ws_id, workspace_role="admin")

    patched = lead.patch(
        f"/api/v1/workspace/tenant/{tenant_id}", json={"custom_domain": "evil.example.com"}
    )
    assert patched.status_code == 403, patched.text
    removed = lead.delete(f"/api/v1/workspace/tenant/{tenant_id}")
    assert removed.status_code == 403, removed.text

    kept = tenant_store.get_tenant(tenant_id)
    assert kept is not None and kept.custom_domain == "legacy.example.com"


def test_unbound_tenant_is_amended_and_removed_by_a_platform_admin(admin):
    import services.tenant_store as tenant_store

    ws_id = _workspace(admin, "Orphaned-admin")
    created = admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws_id, "name": "Legacy"})
    assert created.status_code == 200, created.text
    tenant_id = created.json()["id"]
    _unbind(tenant_id)

    amended = admin.patch(f"/api/v1/workspace/tenant/{tenant_id}", json={"name": "Legacy Renamed"})
    assert amended.status_code == 200, amended.text
    assert admin.delete(f"/api/v1/workspace/tenant/{tenant_id}").status_code == 200
    assert tenant_store.get_tenant(tenant_id) is None


def test_store_missing_its_tenants_list_still_accepts_a_write(stores, monkeypatch):
    """A store file written in another shape must not fault the next create."""
    import services.tenant_store as tenant_store

    tenant_store.STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tenant_store.STORE_PATH.write_text('{"version": 2}', encoding="utf-8")

    tenant = tenant_store.create_tenant(workspace_id="ws-legacy", name="Legacy")
    assert tenant.id
    assert [t.id for t in tenant_store.list_tenants()] == [tenant.id]


def test_www_prefix_is_the_same_domain(stores):
    import services.tenant_store as tenant_store

    tenant_store.create_tenant(workspace_id="ws-1", name="One", custom_domain="www.acme.com")
    found = tenant_store.get_tenant_by_domain("acme.com")
    assert found is not None and found.workspace_id == "ws-1"


def test_a_record_belonging_to_no_workspace_never_serves_a_domain(stores):
    """A pre-scoping record must not answer host routing.

    Tenants written before they were workspace-scoped hold ``workspace_id: ""``.
    Resolving one for a client domain would serve that domain out of an empty
    scope — no workspace to read data, members or audit from.
    """
    import json

    import services.tenant_store as tenant_store

    scoped = tenant_store.create_tenant(workspace_id="ws-1", name="Scoped", custom_domain="scoped.example.com")
    unscoped = tenant_store.create_tenant(workspace_id="ws-2", name="Legacy", custom_domain="legacy.example.com")
    raw = json.loads(tenant_store.STORE_PATH.read_text(encoding="utf-8"))
    for t in raw["tenants"]:
        if t["id"] == unscoped.id:
            t["workspace_id"] = ""
    tenant_store.STORE_PATH.write_text(json.dumps(raw), encoding="utf-8")

    assert tenant_store.get_tenant_by_domain("legacy.example.com") is None
    found = tenant_store.get_tenant_by_domain("scoped.example.com")
    assert found is not None and found.id == scoped.id


def test_an_unscoped_record_is_not_a_trusted_browser_origin(stores, monkeypatch):
    """CORS may not trust a domain that resolution itself refuses.

    Domain resolution stopped answering for a record with no workspace, but
    ``cors_origins`` still appended every ``custom_domain`` — so a browser at
    that host was handed ``access-control-allow-origin`` for credentialed calls.
    """
    import json

    import services.platform_config as platform_config
    import services.tenant_store as tenant_store

    tenant_store.create_tenant(workspace_id="ws-1", name="Scoped", custom_domain="scoped.example.com")
    legacy = tenant_store.create_tenant(workspace_id="ws-2", name="Legacy", custom_domain="legacy.example.com")
    raw = json.loads(tenant_store.STORE_PATH.read_text(encoding="utf-8"))
    for t in raw["tenants"]:
        if t["id"] == legacy.id:
            t["workspace_id"] = ""
    tenant_store.STORE_PATH.write_text(json.dumps(raw), encoding="utf-8")
    monkeypatch.delenv("CORS_ORIGINS", raising=False)

    origins = platform_config.cors_origins()
    assert "https://scoped.example.com" in origins
    assert "https://legacy.example.com" not in origins


def _foreign_tenant(admin) -> tuple[str, str, TestClient]:
    """A tenant in workspace "theirs" and the admin of a different workspace."""
    mine = _workspace(admin, "Mine-read")
    theirs = _workspace(admin, "Theirs-read")
    created = admin.post(
        "/api/v1/workspace/tenant",
        json={
            "workspace_id": theirs,
            "name": "Victim tenant",
            "custom_domain": "victim.example.com",
            "ip_allowlist": ["203.0.113.0/24"],
        },
        headers={"X-Workspace-Id": theirs},
    )
    assert created.status_code == 200, created.text
    lead = _signed_in(admin, email="mine-read@example.com", workspace_id=mine, workspace_role="admin")
    return mine, theirs, lead


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/workspace/tenant",
        "/api/v1/workspace/tenant/byok-keys",
        "/api/v1/workspace/security/posture",
        "/api/v1/workspace/security/report",
    ],
)
def test_workspace_admin_cannot_read_another_workspaces_tenant_data(admin, path):
    mine, theirs, lead = _foreign_tenant(admin)

    by_header = lead.get(path, headers={"X-Workspace-Id": theirs})
    assert by_header.status_code == 403, by_header.text
    assert "victim.example.com" not in by_header.text
    assert "203.0.113.0/24" not in by_header.text

    # The custom domain names the tenant even when the header names the caller's own workspace.
    by_host = lead.get(path, headers={"X-Workspace-Id": mine, "Host": "victim.example.com"})
    assert by_host.status_code == 403, by_host.text
    assert "203.0.113.0/24" not in by_host.text

    assert admin.get(path, headers={"X-Workspace-Id": theirs}).status_code == 200


def test_workspace_admin_still_reads_their_own_tenant_data(admin):
    mine = _workspace(admin, "Own-read")
    created = admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": mine, "name": "Own tenant"},
        headers={"X-Workspace-Id": mine},
    )
    assert created.status_code == 200, created.text
    lead = _signed_in(admin, email="own-read@example.com", workspace_id=mine, workspace_role="admin")
    headers = {"X-Workspace-Id": mine}
    for path in (
        "/api/v1/workspace/tenant",
        "/api/v1/workspace/tenant/byok-keys",
        "/api/v1/workspace/security/posture",
        "/api/v1/workspace/security/report",
    ):
        response = lead.get(path, headers=headers)
        assert response.status_code == 200, (path, response.text)
    assert lead.get("/api/v1/workspace/security/posture", headers=headers).json()["tenant_id"] == created.json()["id"]


def test_workspace_admin_cannot_rotate_or_add_keys_through_another_domain(admin):
    mine, _theirs, lead = _foreign_tenant(admin)
    headers = {"X-Workspace-Id": mine, "Host": "victim.example.com"}
    added = lead.post("/api/v1/workspace/tenant/byok-keys", json={"label": "x"}, headers=headers)
    assert added.status_code == 403, added.text
    rotated = lead.post("/api/v1/workspace/tenant/byok-keys/any/rotate", headers=headers)
    assert rotated.status_code == 403, rotated.text


def test_workspace_admin_cannot_read_another_workspace_record(admin):
    mine = _workspace(admin, "Mine-record")
    theirs = _workspace(admin, "Theirs-record")
    lead = _signed_in(admin, email="mine-record@example.com", workspace_id=mine, workspace_role="admin")

    own = lead.get(f"/api/v1/workspace/workspaces/{mine}")
    assert own.status_code == 200, own.text
    refused = lead.get(f"/api/v1/workspace/workspaces/{theirs}")
    assert refused.status_code == 403, refused.text
    assert "Theirs-record" not in refused.text
    assert admin.get(f"/api/v1/workspace/workspaces/{theirs}").status_code == 200


def test_notification_listing_is_scoped_to_the_callers_workspace(admin, stores, monkeypatch):
    monkeypatch.setenv("DATAFLOW_NOTIFICATION_STORE", str(stores / "notifications.json"))
    from services.notification_store import create_channel

    mine = _workspace(admin, "Mine-notify")
    theirs = _workspace(admin, "Theirs-notify")
    for ws_id, label in ((mine, "mine-channel"), (theirs, "theirs-channel"), ("", "global-channel")):
        create_channel(workspace_id=ws_id, kind="slack", label=label, config={}, created_by="root@example.com")
    lead = _signed_in(admin, email="mine-notify@example.com", workspace_id=mine, workspace_role="admin")

    scoped = lead.get("/api/v1/workspace/notifications", headers={"X-Workspace-Id": mine})
    assert scoped.status_code == 200, scoped.text
    assert [c["label"] for c in scoped.json()["channels"]] == ["mine-channel"]

    foreign = lead.get(
        f"/api/v1/workspace/notifications?workspace_id={theirs}", headers={"X-Workspace-Id": mine}
    )
    assert foreign.status_code == 403, foreign.text
    assert "theirs-channel" not in foreign.text

    # No header: the caller's own workspace (D154), never another tenant's or the global list.
    unscoped = lead.get("/api/v1/workspace/notifications")
    assert unscoped.status_code == 200, unscoped.text
    assert [c["label"] for c in unscoped.json()["channels"]] == ["mine-channel"]
    assert "theirs-channel" not in unscoped.text and "global-channel" not in unscoped.text

    platform = admin.get("/api/v1/workspace/notifications")
    assert platform.status_code == 200, platform.text
    assert [c["label"] for c in platform.json()["channels"]] == ["global-channel"]
