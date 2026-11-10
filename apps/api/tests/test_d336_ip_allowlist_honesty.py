"""D336: an IP allowlist the server cannot enforce is not saved as if it worked.

Production QA: with the trusted proxy hop count unset every caller resolves to
the platform proxy's private address, so a saved allowlist blocks nothing, yet
``PATCH /workspace/tenant`` answered 200 and an invalid CIDR was stored as
``[]``. These tests drive the real routes on a live MongoDB
(tests/r8_ent_harness.py):

* hop count unset + private peer: saving a new list is refused (409) and
  nothing is stored, whatever ``X-Forwarded-For`` the caller sends;
* hop count N: the address is ``X-Forwarded-For`` ``hops[-N]``; a list that
  excludes it is refused by the lock-out guard, one that includes it is
  enforced (403 for a caller outside it, even with a spoofed left-most hop);
* an invalid CIDR is refused, not dropped.
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401

LIST = ["203.0.113.0/24"]
INSIDE = "203.0.113.50"
OUTSIDE = "198.51.100.9"
PROXY_PEER = "100.64.0.9"  # carrier-grade NAT: what the API sees behind the edge
SETTINGS = "/api/v1/workspace/settings"
_HOP_VARS = ("DATAWRAP_TRUSTED_PROXY_COUNT", "DATAFLOW_TRUSTED_PROXY_COUNT", "TRUSTED_PROXY_COUNT")


@pytest.fixture
def hops_unset(monkeypatch):
    for name in _HOP_VARS + ("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE"):
        monkeypatch.delenv(name, raising=False)


def _peer(ent, client, ip: str):
    from fastapi.testclient import TestClient

    c = TestClient(ent.app, client=(ip, 5000))
    c.headers.update({k: v for k, v in client.headers.items() if k.lower() in ("authorization", "x-workspace-id")})
    return c


def _stored_list(ent, ws: str) -> list[str] | None:
    from services.tenant_store import get_tenant_for_workspace

    t = get_tenant_for_workspace(ws)
    return None if t is None else list(t.ip_allowlist)


def test_hops_unset_refuses_to_save_a_list_it_cannot_enforce(ent, hops_unset):
    ws = ent.workspace("D336 unset")
    admin = _peer(ent, ent.admin, PROXY_PEER)
    # Create: refused, no tenant written.
    res = admin.post("/api/v1/workspace/tenant",
                     json={"workspace_id": ws, "name": "D336", "ip_allowlist": LIST},
                     headers={"X-Workspace-Id": ws})
    assert res.status_code == 409, res.text
    detail = res.json()["detail"]
    assert "would not be enforced" in detail and "proxy hop count" in detail
    assert "DATAWRAP_" not in detail and "DATAFLOW_" not in detail
    assert _stored_list(ent, ws) is None

    # Patch an existing tenant: refused, the stored list stays empty.
    created = admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "D336"},
                         headers={"X-Workspace-Id": ws})
    assert created.status_code == 200, created.text
    tid = created.json()["id"]
    res = admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": LIST},
                      headers={"X-Workspace-Id": ws})
    assert res.status_code == 409, res.text
    assert _stored_list(ent, ws) == []

    # A member is still admitted: nothing claims to block.
    member = ent.member(email="d336-unset@example.com", workspace_id=ws, role="editor")
    assert _peer(ent, member, "10.0.0.7").get(SETTINGS).status_code == 200


def test_hops_unset_ignores_a_spoofed_forwarded_for(ent, hops_unset):
    ws = ent.workspace("D336 spoof")
    admin = _peer(ent, ent.admin, PROXY_PEER)
    created = admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Spoof"},
                         headers={"X-Workspace-Id": ws})
    tid = created.json()["id"]
    # The caller claims an address inside the list: without a hop count the
    # header is client-supplied and must not make the list look enforceable.
    res = admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": LIST},
                      headers={"X-Workspace-Id": ws, "X-Forwarded-For": INSIDE})
    assert res.status_code == 409, res.text
    posture = admin.get("/api/v1/workspace/security/posture",
                        headers={"X-Workspace-Id": ws, "X-Forwarded-For": INSIDE}).json()
    assert posture["enforcement"]["caller_ip"] == PROXY_PEER


def test_hops_unset_still_allows_clearing_and_resaving_a_stored_list(ent, hops_unset):
    from services.tenant_store import update_tenant

    ws = ent.workspace("D336 staged")
    admin = _peer(ent, ent.admin, PROXY_PEER)
    tid = admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Staged"},
                     headers={"X-Workspace-Id": ws}).json()["id"]
    update_tenant(tid, ip_allowlist=LIST)  # saved before this deployment refused it
    # The settings form re-sends the stored list with every save: other fields
    # stay editable, and the answer still says the list is not enforced.
    res = admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"name": "Renamed", "ip_allowlist": LIST},
                      headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    ip = res.json()["enforcement"]["ip_allowlist"]
    assert ip["enforced"] is False and ip["status"] == "untrusted_client_ip"
    # Removing the list is always allowed.
    res = admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": []},
                      headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    assert _stored_list(ent, ws) == []


@pytest.mark.parametrize("bad", ["999.1.1.1/99", "not-an-ip", "10.0.0.0/33"])
def test_invalid_cidr_is_refused_not_dropped(ent, monkeypatch, bad):
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")
    ws = ent.workspace("D336 cidr")
    hdr = {"X-Workspace-Id": ws, "X-Forwarded-For": INSIDE}
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Cidr", "ip_allowlist": [INSIDE + "/32", bad]},
                         headers=hdr)
    assert res.status_code in (400, 422), res.text
    assert bad in res.text
    assert _stored_list(ent, ws) is None
    tid = ent.admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Cidr"},
                         headers=hdr).json()["id"]
    res = ent.admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": [bad]}, headers=hdr)
    assert res.status_code in (400, 422), res.text
    assert _stored_list(ent, ws) == []


def test_hop_count_n_resolves_the_real_client_and_enforces(ent, monkeypatch):
    """Two trusted proxies (CDN -> edge): the client is ``hops[-2]``."""
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "2")
    for name in ("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE"):
        monkeypatch.delenv(name, raising=False)

    def chain(client_ip: str, spoofed: str = "") -> dict[str, str]:
        hops = ([spoofed] if spoofed else []) + [client_ip, "10.1.2.3"]
        return {"X-Forwarded-For": ", ".join(hops)}

    ws = ent.workspace("D336 hops 2")
    hdr = {"X-Workspace-Id": ws}
    # A list that excludes the resolved caller: the lock-out guard refuses it.
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Hops2", "ip_allowlist": LIST},
                         headers={**hdr, **chain(OUTSIDE, spoofed=INSIDE)})
    assert res.status_code == 400, res.text
    assert OUTSIDE in res.json()["detail"] and "lock you out" in res.json()["detail"]
    assert _stored_list(ent, ws) is None

    # A list that includes the caller: saved and enforced.
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Hops2", "ip_allowlist": LIST},
                         headers={**hdr, **chain(INSIDE)})
    assert res.status_code == 200, res.text
    assert res.json()["enforcement"]["ip_allowlist"]["status"] == "enforced"
    assert res.json()["enforcement"]["caller_ip"] == INSIDE

    member = ent.member(email="d336-hops2@example.com", workspace_id=ws, role="editor")
    assert member.get(SETTINGS, headers=chain(INSIDE)).status_code == 200
    out = member.get(SETTINGS, headers=chain(OUTSIDE))
    assert out.status_code == 403 and out.json()["reason"] == "ip_not_in_workspace_allowlist"
    # A spoofed left-most hop inside the list does not get past the trusted hop.
    spoofed = member.get(SETTINGS, headers=chain(OUTSIDE, spoofed=INSIDE))
    assert spoofed.status_code == 403 and spoofed.json()["ip"] == OUTSIDE
