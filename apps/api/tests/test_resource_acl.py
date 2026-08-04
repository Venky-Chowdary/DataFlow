"""Resource ACL — deny-by-default when grants exist."""

from __future__ import annotations

import pytest

from services import resource_acl as acl


@pytest.fixture(autouse=True)
def _isolated_acl_store(tmp_path, monkeypatch):
    path = tmp_path / "resource_acls.jsonl"
    monkeypatch.setattr(acl, "STORE_PATH", path)
    monkeypatch.setenv("RESOURCE_ACL_STORE", str(path))
    yield


def test_open_when_no_grants():
    acl.assert_resource_acl(
        tenant_id="ws1",
        resource_type="connector",
        resource_id="c1",
        principal="alice@example.com",
        min_role="viewer",
    )


def test_deny_non_grantee_when_grants_exist():
    acl.upsert_grant(
        tenant_id="ws1",
        resource_type="connector",
        resource_id="c1",
        principal="owner@example.com",
        role="owner",
    )
    with pytest.raises(PermissionError):
        acl.assert_resource_acl(
            tenant_id="ws1",
            resource_type="connector",
            resource_id="c1",
            principal="alice@example.com",
            min_role="viewer",
        )


def test_grantee_editor_can_read():
    acl.upsert_grant(
        tenant_id="ws1",
        resource_type="job",
        resource_id="j1",
        principal="bob@example.com",
        role="editor",
    )
    acl.assert_resource_acl(
        tenant_id="ws1",
        resource_type="job",
        resource_id="j1",
        principal="bob@example.com",
        min_role="viewer",
    )


def test_admin_bypasses_acl():
    acl.upsert_grant(
        tenant_id="ws1",
        resource_type="job",
        resource_id="j1",
        principal="owner@example.com",
        role="owner",
    )
    acl.assert_resource_acl(
        tenant_id="ws1",
        resource_type="job",
        resource_id="j1",
        principal="outsider@example.com",
        min_role="owner",
        is_admin=True,
    )


def test_corrupt_acl_store_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "resource_acls.jsonl"
    path.write_text("{not-json\n", encoding="utf-8")
    monkeypatch.setattr(acl, "STORE_PATH", path)
    monkeypatch.setenv("RESOURCE_ACL_STORE", str(path))
    with pytest.raises(RuntimeError):
        acl.list_grants(tenant_id="ws1")


def test_revoke_removes_deny_default():
    g = acl.upsert_grant(
        tenant_id="ws1",
        resource_type="contract",
        resource_id="ct1",
        principal="a@example.com",
        role="viewer",
    )
    assert acl.revoke_grant(g.id, tenant_id="ws1") is True
    acl.assert_resource_acl(
        tenant_id="ws1",
        resource_type="contract",
        resource_id="ct1",
        principal="b@example.com",
        min_role="viewer",
    )
