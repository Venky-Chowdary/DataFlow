"""SEC-16: only a platform administrator widens the connector egress allowlist.

Every connector dial refuses private, loopback and CGNAT targets unless the
workspace's egress allowlist lists the range. A workspace admin could save any
range for their own workspace (``POST`` or ``PATCH /workspace/tenant``). On a
hosted, multi-tenant deployment a private range is the deployment's own
network, not the customer's: ``10.0.0.0/8`` reaches this server's neighbours
(databases, caches, internal services). So a customer workspace admin could
open a server-side path into the operator's network.

Adding a range is now platform-admin only, like the data-region fence (security
review round 2, M1). A workspace admin may still read the list, narrow it or
clear it: that only ever refuses more. Cloud metadata and link-local stay
unsavable for everyone.
"""

from __future__ import annotations

import pytest
from tenancy_world import tenancy_world  # noqa: F401


@pytest.fixture
def world(tenancy_world):
    w = tenancy_world
    w.admin_b = w.member("wsadmin-b@tenancy.example.com", w.workspace_b, "admin")
    return w


def _tenant_id(client) -> str:
    r = client.post("/api/v1/workspace/tenant", json={"name": "B"})
    assert r.status_code == 200, r.text
    return r.json()["id"]


def _message(r) -> str:
    body = r.json()
    return str(body.get("detail") or (body.get("error") or {}).get("message") or "")


def _allowlist(workspace_id: str) -> list[str]:
    from services.tenant_store import get_tenant_for_workspace

    tenant = get_tenant_for_workspace(workspace_id)
    return list(tenant.egress_allowlist) if tenant else []


def test_a_workspace_admin_cannot_add_a_private_range_on_create(world):
    r = world.admin_b.post(
        "/api/v1/workspace/tenant", json={"name": "B", "egress_allowlist": ["10.0.0.0/8"]}
    )
    assert r.status_code == 403, r.text
    assert "platform administrator" in _message(r)
    assert _allowlist(world.workspace_b) == []


def test_a_workspace_admin_cannot_widen_the_list_on_patch(world):
    tid = _tenant_id(world.admin_b)
    r = world.admin_b.patch(
        f"/api/v1/workspace/tenant/{tid}", json={"egress_allowlist": ["192.168.0.0/16"]}
    )
    assert r.status_code == 403, r.text
    assert "192.168.0.0/16" in _message(r)
    assert _allowlist(world.workspace_b) == []


def test_a_platform_admin_adds_a_range_and_the_workspace_admin_may_narrow_or_clear_it(world):
    tid = _tenant_id(world.admin_b)
    r = world.admin.patch(
        f"/api/v1/workspace/tenant/{tid}",
        json={"egress_allowlist": ["10.20.0.0/16", "192.168.5.0/24"]},
    )
    assert r.status_code == 200, r.text
    assert _allowlist(world.workspace_b) == ["10.20.0.0/16", "192.168.5.0/24"]

    # Narrowing only refuses more: the workspace admin may do it.
    r = world.admin_b.patch(
        f"/api/v1/workspace/tenant/{tid}", json={"egress_allowlist": ["10.20.0.0/16"]}
    )
    assert r.status_code == 200, r.text
    assert _allowlist(world.workspace_b) == ["10.20.0.0/16"]

    # Saving the same list again (other settings changed) is not a widening.
    r = world.admin_b.patch(
        f"/api/v1/workspace/tenant/{tid}",
        json={"name": "B renamed", "egress_allowlist": ["10.20.0.0/16"]},
    )
    assert r.status_code == 200, r.text

    # Swapping one range for another is a widening.
    r = world.admin_b.patch(
        f"/api/v1/workspace/tenant/{tid}", json={"egress_allowlist": ["10.30.0.0/16"]}
    )
    assert r.status_code == 403, r.text

    r = world.admin_b.patch(f"/api/v1/workspace/tenant/{tid}", json={"egress_allowlist": []})
    assert r.status_code == 200, r.text
    assert _allowlist(world.workspace_b) == []


def test_metadata_stays_unsavable_even_for_a_platform_admin(world):
    tid = _tenant_id(world.admin_b)
    r = world.admin.patch(
        f"/api/v1/workspace/tenant/{tid}", json={"egress_allowlist": ["169.254.169.254"]}
    )
    assert r.status_code in (400, 422), r.text
    assert _allowlist(world.workspace_b) == []
