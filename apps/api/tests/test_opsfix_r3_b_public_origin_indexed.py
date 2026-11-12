"""Ops review round 3, Medium B: the public origin must not read every tenant.

``public_origin`` (MCP manifest and status, the SAML start and callback
routes, which are unauthenticated) decided whether the request's host was
allowed by building the set of every tenant's custom domain:
``list_tenants()`` read and parsed the whole tenant store on every call, on
the event loop. With 3,000 tenants eight manifest calls stalled the loop by
0.57 s. The host is now matched with one indexed lookup of that domain
(``get_tenant_by_domain``); which hosts are allowed or refused is unchanged.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from services import public_origin, tenant_store
from services.shared_record_store import SharedRecordSet
from tests._control_plane_fakes import install_shared_store

ITEMS = "control_plane_items"
TENANTS = 3000


def _request(headers: dict[str, str], host: str = "api-backend.internal:8000") -> SimpleNamespace:
    lowered = {k.lower(): v for k, v in headers.items()}
    lowered.setdefault("host", host)
    return SimpleNamespace(headers=lowered, url=SimpleNamespace(scheme="http", netloc=host),
                           base_url=f"http://{host}/")


@pytest.fixture
def many_tenants(tmp_path, monkeypatch):
    """3,000 tenants on custom domains in the shared store, and a spy on every tenant-store read."""
    db = install_shared_store(monkeypatch)
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    for flag in ("_settled", "_split_checked"):
        monkeypatch.setattr(tenant_store._DOC, flag, False)
    monkeypatch.setattr(tenant_store._DOC, "_cache", {})
    rows = [{"id": f"t{i}", "workspace_id": f"ws-{i}", "name": f"T{i}", "custom_domain": f"d{i}.example.com"}
            for i in range(TENANTS)]
    rows.append({"id": "t-orphan", "workspace_id": "", "name": "orphan", "custom_domain": "orphan.example.com"})
    for seq, row in enumerate(rows):
        db[ITEMS].insert_one({
            "_id": f"tenant_settings:{row['id']}", "store": "tenant_settings", "key": row["id"],
            "part": row["workspace_id"], "rev": 1, "seq": seq, "body": json.dumps(row, sort_keys=True),
            "tags": [f"domain:{row['custom_domain']}"],
        })
    db["control_plane_docs"].insert_one({"_id": "tenant_settings", "kind": "store", "state": "done",
                                         "import_sources": []})
    db["control_plane_docs"].insert_one({"_id": "index_fields:v2:tenant_settings", "kind": "index_fields"})

    scopes: list[dict] = []
    real_list = tenant_store.list_tenants

    def whole_store(*args, **kwargs):
        scopes.append({"list_tenants": True})
        return real_list(*args, **kwargs)

    monkeypatch.setattr(tenant_store, "list_tenants", whole_store)
    real_load = SharedRecordSet.load

    def spy(self, **kwargs):
        if self.name == "tenant_settings":
            scopes.append(kwargs)
        return real_load(self, **kwargs)

    monkeypatch.setattr(SharedRecordSet, "load", spy)
    for key in ("DATAWRAP_PUBLIC_API_ORIGIN", "DATAFLOW_PUBLIC_API_ORIGIN", "DATAWRAP_WEB_URL", "DATAFLOW_WEB_URL",
                "DATAWRAP_WEB_DOMAIN", "DATAFLOW_WEB_DOMAIN", "DATAWRAP_PUBLIC_ORIGIN_ALLOWED_HOSTS",
                "DATAFLOW_PUBLIC_ORIGIN_ALLOWED_HOSTS", "DATAWRAP_SAML_SP_ENTITY_ID", "DATAFLOW_SAML_SP_ENTITY_ID"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DATAWRAP_PUBLIC_API_ORIGIN", "https://api.example.net")
    monkeypatch.setenv("DATAWRAP_PUBLIC_ORIGIN_ALLOWED_HOSTS", "extra.example.org")
    return scopes


def _no_whole_store_read(scopes: list[dict]) -> None:
    assert scopes, "the tenant store was consulted"
    assert {"list_tenants": True} not in scopes, "list_tenants() was called"
    unscoped = [s for s in scopes if "list_tenants" not in s and not any(s.get(k) is not None for k in ("part", "parts", "keys", "tag"))]
    assert unscoped == [], f"whole-store reads: {unscoped}"


def test_allowed_and_refused_hosts_are_decided_by_one_indexed_lookup(many_tenants):
    origin = public_origin.public_origin
    assert origin(_request({}, host="d17.example.com")) == "http://d17.example.com"
    assert origin(_request({"X-Forwarded-Host": "D2999.example.com", "X-Forwarded-Proto": "https"})) == (
        "https://d2999.example.com"
    )
    # Refused, exactly as before: unknown host, a tenant without a workspace, a host that only
    # normalises to a tenant's domain (port, www.), and a forwarded host that is not listed.
    for host in ("evil.example.com", "orphan.example.com", "www.d17.example.com", "d17.example.com:8443"):
        assert origin(_request({"X-Forwarded-Host": host})) == "https://api.example.net", host
    # The configured hosts are still allowed without touching the store.
    assert origin(_request({"X-Forwarded-Host": "extra.example.org"})) == "http://extra.example.org"
    assert origin(_request({}, host="api.example.net")) == "http://api.example.net"
    _no_whole_store_read(many_tenants)


def test_mcp_manifest_and_saml_start_read_no_whole_tenant_store(many_tenants, monkeypatch):
    from fastapi.testclient import TestClient

    from src.main import app

    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    with TestClient(app) as client:
        many_tenants.clear()  # start-up work (lifespan) runs once, before the API serves; not the finding
        resp = client.get("/api/v1/mcp/manifest", headers={"X-Forwarded-Host": "d42.example.com"})
        assert resp.status_code == 200, resp.text
        assert "d42.example.com/api/v1/mcp" in json.dumps(resp.json())
        resp = client.get("/api/v1/mcp/manifest", headers={"X-Forwarded-Host": "evil.example.com"})
        assert "evil.example.com" not in resp.text
        client.get("/api/v1/auth/sso/saml/start", headers={"X-Forwarded-Host": "d42.example.com"},
                   follow_redirects=False)
    # What the SAML start and callback routes build their URLs with (three times per request).
    from starlette.requests import Request

    from src.routers import auth_router

    request = Request({"type": "http", "method": "GET", "path": "/", "scheme": "http", "query_string": b"",
                       "server": ("api-backend.internal", 8000),
                       "headers": [(b"host", b"api-backend.internal:8000"), (b"x-forwarded-host", b"d42.example.com")]})
    assert auth_router._saml_acs_url(request) == "http://d42.example.com/api/v1/auth/sso/saml/callback"
    assert auth_router._saml_sp_entity_id(request).startswith("http://d42.example.com/")
    _no_whole_store_read(many_tenants)
