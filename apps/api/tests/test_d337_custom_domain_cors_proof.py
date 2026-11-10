"""D337: a saved custom domain is not a credentialed CORS origin until DNS proves it.

Production QA: a workspace admin PATCHed ``custom_domain="evil.com"`` and the
API at once answered ``Access-Control-Allow-Origin: https://evil.com`` with
``Access-Control-Allow-Credentials: true`` for every route, because
``cors_policy.tenant_custom_origin_allowed`` trusted any saved domain
(``tenant_store.get_tenant_by_domain`` is global) and
``platform_config.cors_origins`` listed every domain at startup.

The DNS lookup is replaced by a test double (``lookup_txt``); everything else
goes through the real app on a live MongoDB (tests/r8_ent_harness.py).
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401

SAVED = "/api/v1/connectors/saved"
ACAO = "access-control-allow-origin"
ACAC = "access-control-allow-credentials"


@pytest.fixture
def dns(monkeypatch):
    """Fake TXT zone: {record name: [values]}."""
    from services import custom_domain_verification

    zone: dict[str, list[str]] = {}
    monkeypatch.setattr(custom_domain_verification, "lookup_txt", lambda name: list(zone.get(name, [])))
    for name in ("DATAWRAP_CORS_INCLUDE_TENANT_DOMAINS", "DATAFLOW_CORS_INCLUDE_TENANT_DOMAINS"):
        monkeypatch.delenv(name, raising=False)
    return zone


def _tenant(ent, ws: str, domain: str) -> dict:
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "D337", "custom_domain": domain},
                         headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    return res.json()


def _preflight(client, origin: str):
    return client.options(SAVED, headers={"Origin": origin, "Access-Control-Request-Method": "GET",
                                          "Access-Control-Request-Headers": "authorization,x-workspace-id"})


def _no_cors(res) -> None:
    assert ACAO not in res.headers, dict(res.headers)
    assert ACAC not in res.headers, dict(res.headers)


def test_unverified_domain_gets_no_cors_headers(ent, dns):
    ws = ent.workspace("D337 evil")
    body = _tenant(ent, ws, "evil.com")
    assert body["custom_domain"] == "evil.com"
    assert body["custom_domain_verified"] is False
    assert body["custom_domain_verification"]["status"] == "pending_dns_verification"

    origin = "https://evil.com"
    _no_cors(_preflight(ent.admin, origin))
    _no_cors(ent.admin.get(SAVED, headers={"Origin": origin, "X-Workspace-Id": ws}))

    posture = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws}).json()
    cors = posture["custom_domain_cors"]
    assert cors["enabled"] is False and cors["origin"] is None
    assert "as soon as it is saved" not in cors["hint"]


def test_admin_cannot_mark_a_domain_verified_by_saving_the_flag(ent, dns):
    ws = ent.workspace("D337 self-verify")
    tid = _tenant(ent, ws, "self-claimed.example.com")["id"]
    res = ent.admin.patch(f"/api/v1/workspace/tenant/{tid}",
                          json={"custom_domain_verified": True}, headers={"X-Workspace-Id": ws})
    assert res.status_code in (200, 422)
    from services.tenant_store import get_tenant

    assert get_tenant(tid).custom_domain_verified is False
    _no_cors(_preflight(ent.admin, "https://self-claimed.example.com"))


def test_wrong_or_missing_txt_record_does_not_verify(ent, dns):
    ws = ent.workspace("D337 wrong txt")
    body = _tenant(ent, ws, "data.acme.example")
    tid = body["id"]
    res = ent.admin.post(f"/api/v1/workspace/tenant/{tid}/custom-domain/verify", headers={"X-Workspace-Id": ws})
    assert res.status_code == 409, res.text
    assert "_datawrap-challenge.data.acme.example" in res.json()["detail"]
    dns["_datawrap-challenge.data.acme.example"] = ["datawrap-domain-verification=not-the-token"]
    res = ent.admin.post(f"/api/v1/workspace/tenant/{tid}/custom-domain/verify", headers={"X-Workspace-Id": ws})
    assert res.status_code == 409, res.text
    _no_cors(_preflight(ent.admin, "https://data.acme.example"))


def test_verified_domain_is_allowed_only_for_its_own_workspace(ent, dns):
    ws = ent.workspace("D337 verified")
    other = ent.workspace("D337 other")
    body = _tenant(ent, ws, "data.acme.example")
    ch = body["custom_domain_verification"]
    assert ch["record_type"] == "TXT" and ch["record_name"] == "_datawrap-challenge.data.acme.example"
    dns[ch["record_name"]] = ["unrelated=1", ch["record_value"]]

    res = ent.admin.post(f"/api/v1/workspace/tenant/{body['id']}/custom-domain/verify",
                         headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    assert res.json()["custom_domain_verified"] is True
    assert res.json()["custom_domain_verification"]["status"] == "verified"

    origin = "https://data.acme.example"
    pre = _preflight(ent.admin, origin)
    assert pre.status_code == 200 and pre.headers[ACAO] == origin and pre.headers[ACAC] == "true"
    own = ent.admin.get(SAVED, headers={"Origin": origin, "X-Workspace-Id": ws})
    assert own.headers.get(ACAO) == origin and own.headers.get(ACAC) == "true"
    # Not scoped to the tenant's workspace: no CORS headers, so the browser
    # will not hand the response to the tenant's page.
    _no_cors(ent.admin.get(SAVED, headers={"Origin": origin, "X-Workspace-Id": other}))
    _no_cors(ent.admin.get(SAVED, headers={"Origin": origin}))
    # Only the exact https origin.
    _no_cors(_preflight(ent.admin, "http://data.acme.example"))
    _no_cors(_preflight(ent.admin, "https://data.acme.example:8443"))
    # A stranger origin is still refused, and gets no credentials header.
    _no_cors(_preflight(ent.admin, "https://evil.example"))
    _no_cors(ent.admin.get(SAVED, headers={"Origin": "https://evil.example", "X-Workspace-Id": ws}))

    posture = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws}).json()
    assert posture["custom_domain_cors"]["enabled"] is True
    assert posture["custom_domain_cors"]["origin"] == origin


def test_changing_the_domain_drops_the_proof_and_resending_it_keeps_it(ent, dns):
    ws = ent.workspace("D337 change")
    body = _tenant(ent, ws, "data.acme.example")
    ch = body["custom_domain_verification"]
    dns[ch["record_name"]] = [ch["record_value"]]
    assert ent.admin.post(f"/api/v1/workspace/tenant/{body['id']}/custom-domain/verify",
                          headers={"X-Workspace-Id": ws}).status_code == 200
    # The settings form re-sends the same domain with every save.
    same = ent.admin.patch(f"/api/v1/workspace/tenant/{body['id']}",
                           json={"custom_domain": "https://data.acme.example/", "name": "Renamed"},
                           headers={"X-Workspace-Id": ws})
    assert same.status_code == 200 and same.json()["custom_domain_verified"] is True
    moved = ent.admin.patch(f"/api/v1/workspace/tenant/{body['id']}",
                            json={"custom_domain": "other.acme.example"}, headers={"X-Workspace-Id": ws})
    assert moved.status_code == 200
    assert moved.json()["custom_domain_verified"] is False
    assert moved.json()["custom_domain_verification"]["record_value"] != ch["record_value"]
    _no_cors(_preflight(ent.admin, "https://other.acme.example"))
    _no_cors(_preflight(ent.admin, "https://data.acme.example"))


@pytest.mark.parametrize("bad", ["not a domain!!", "evil.com/path?x=1", "203.0.113.5", "localhost", "-bad-.example.com"])
def test_invalid_host_name_is_refused(ent, dns, bad):
    ws = ent.workspace("D337 invalid")
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Bad", "custom_domain": bad},
                         headers={"X-Workspace-Id": ws})
    assert res.status_code in (400, 422), res.text
    from services.tenant_store import get_tenant_for_workspace

    assert get_tenant_for_workspace(ws) is None


def test_startup_origins_never_list_tenant_domains(ent, dns, monkeypatch):
    from services import platform_config

    ws = ent.workspace("D337 startup")
    body = _tenant(ent, ws, "startup.acme.example")
    monkeypatch.delenv("CORS_ORIGINS", raising=False)
    assert "https://startup.acme.example" not in platform_config.cors_origins()
    ch = body["custom_domain_verification"]
    dns[ch["record_name"]] = [ch["record_value"]]
    ent.admin.post(f"/api/v1/workspace/tenant/{body['id']}/custom-domain/verify", headers={"X-Workspace-Id": ws})
    # Verified origins are admitted live (and workspace-scoped), never from the static list.
    assert "https://startup.acme.example" not in platform_config.cors_origins()


def test_mcp_origin_check_refuses_an_unverified_domain(ent, dns):
    ws = ent.workspace("D337 mcp")
    _tenant(ent, ws, "mcp.acme.example")
    res = ent.anonymous().get("/api/v1/mcp", headers={"Origin": "https://mcp.acme.example"})
    assert res.status_code == 403, res.text
