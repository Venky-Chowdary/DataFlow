"""Security review round 2 (M1): only a platform administrator controls the data-region fence.

A workspace admin could ``POST /workspace/tenant`` with ``data_region: ""``:
the workspace then had a record of its own, ``enforced_region`` returned
``""`` and the deployment-wide (unscoped) fence no longer applied. The same
admin could ``PATCH`` the workspace's own region to ``""``, and the audit
event did not record the region change. Yet verifying one connector's region
needs a platform admin.

Now setting, changing or clearing ``data_region`` (create, patch, delete) is
platform-admin only, a workspace record with an empty region inherits the
unscoped or deployment region, and the audit trail records the old and the
new value.
"""

from __future__ import annotations

import pytest
from tenancy_world import tenancy_world  # noqa: F401

S3_US = {"kind": "database", "format": "s3", "host": "us-east-1", "database": "us-bucket", "table": "k"}


@pytest.fixture
def world(tenancy_world, monkeypatch):
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    import src.transfer.background as bg

    monkeypatch.setattr(bg, "run_transfer_async", lambda *a, **k: None)
    w = tenancy_world
    w.admin_b = w.member("wsadmin-b@tenancy.example.com", w.workspace_b, "admin")
    return w


def _execute(client):
    src = {"kind": "database", "format": "sqlite", "database": ":memory:", "table": "t"}
    return client.post("/api/v1/transfer/execute", json={
        "source": src, "destination": S3_US, "mappings": [{"source": "id", "target": "id"}], "async_mode": True,
    }).status_code


def _audit(action):
    from services import audit_log

    return [e for e in audit_log.list_audit_events(limit=200) if e.get("action") == action]


def test_a_workspace_admin_cannot_escape_the_deployment_fence(world):
    from services import data_residency as dr
    from services import tenant_store

    tenant_store.create_tenant(workspace_id="", name="deployment", data_region="eu-west-1")
    assert _execute(world.b) == 403
    r = world.admin_b.post("/api/v1/workspace/tenant", json={"name": "B's own", "data_region": ""})
    assert r.status_code in (200, 403), r.text
    assert dr.enforced_region(world.workspace_b) == "eu-west-1"
    assert _execute(world.b) == 403


def test_an_empty_workspace_record_inherits_the_unscoped_region(world):
    """Function level: the record exists, its region is empty, the fence still holds."""
    from services import data_residency as dr
    from services import tenant_store

    tenant_store.create_tenant(workspace_id="", name="deployment", data_region="eu-west-1")
    tenant_store.create_tenant(workspace_id=world.workspace_b, name="B", data_region="")
    assert dr.enforced_region(world.workspace_b) == "eu-west-1"


def test_a_workspace_admin_cannot_set_a_region_on_create(world):
    r = world.admin_b.post("/api/v1/workspace/tenant", json={"name": "B", "data_region": "us-east-1"})
    assert r.status_code == 403, r.text


def test_a_workspace_admin_cannot_change_or_clear_or_delete_a_fence(world):
    from services import data_residency as dr

    r = world.admin.post("/api/v1/workspace/tenant", json={
        "name": "B", "data_region": "eu-west-1", "workspace_id": world.workspace_b})
    assert r.status_code == 200, r.text
    tid = r.json()["id"]
    for region in ("", "us-east-1"):
        r = world.admin_b.patch(f"/api/v1/workspace/tenant/{tid}", json={"data_region": region})
        assert r.status_code == 403, (region, r.text)
    assert dr.enforced_region(world.workspace_b) == "eu-west-1"
    # Other tenant settings stay the workspace admin's; restating the same region is no change.
    r = world.admin_b.patch(f"/api/v1/workspace/tenant/{tid}", json={"name": "B renamed", "data_region": "eu-west-1"})
    assert r.status_code == 200, r.text
    r = world.admin_b.delete(f"/api/v1/workspace/tenant/{tid}")
    assert r.status_code == 403, r.text
    assert dr.enforced_region(world.workspace_b) == "eu-west-1"
    assert _execute(world.b) == 403


def test_platform_admin_changes_are_audited_with_old_and_new_values(world):
    r = world.admin.post("/api/v1/workspace/tenant", json={
        "name": "B", "data_region": "eu-west-1", "workspace_id": world.workspace_b})
    assert r.status_code == 200, r.text
    tid = r.json()["id"]
    created = _audit("workspace.tenant.create")[0]["details"]
    assert created.get("data_region") == "eu-west-1" and created.get("data_region_before") == ""
    r = world.admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"data_region": "eu-central-1"},
                          headers={"X-Workspace-Id": world.workspace_b})
    assert r.status_code == 200, r.text
    updated = _audit("workspace.tenant.update")[0]["details"]
    assert updated.get("data_region_before") == "eu-west-1"
    assert updated.get("data_region") == "eu-central-1"
    r = world.admin.delete(f"/api/v1/workspace/tenant/{tid}", headers={"X-Workspace-Id": world.workspace_b})
    assert r.status_code == 200, r.text
    deleted = _audit("workspace.tenant.delete")[0]["details"]
    assert deleted.get("data_region_before") == "eu-central-1"


def test_a_workspace_admin_keeps_other_tenant_settings(world):
    """Control: an empty-region record (MFA, allowlists, domain) is still the workspace admin's."""
    r = world.admin_b.post("/api/v1/workspace/tenant", json={"name": "B", "security_contact_email": "sec@b.example.com"})
    assert r.status_code == 200, r.text
    tid = r.json()["id"]
    r = world.admin_b.patch(f"/api/v1/workspace/tenant/{tid}", json={"name": "B2"})
    assert r.status_code == 200, r.text
    r = world.admin_b.delete(f"/api/v1/workspace/tenant/{tid}")
    assert r.status_code == 200, r.text
