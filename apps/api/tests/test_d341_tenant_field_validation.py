"""D341: tenant create/patch refuses junk with field-level errors.

Production QA created tenants with ``security_contact_email="not-an-email"``,
``custom_domain="not a domain!!"``, ``data_region="moon-1"`` and the name
``<img src=x onerror=alert(1)>``; ``ip_allowlist=["999.1.1.1/99"]`` was
silently stored as ``[]``. Each must be a 422 naming the field, and nothing
may be written. Real routes on a live MongoDB (tests/r8_ent_harness.py).
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401

BAD = [
    ("security_contact_email", "not-an-email"),
    ("security_contact_email", "a@b"),
    ("custom_domain", "not a domain!!"),
    ("custom_domain", "<script>.example.com"),
    ("data_region", "moon-1"),
    ("name", "<img src=x onerror=alert(1)>"),
    ("name", "Acme\x00Corp"),
    ("ip_allowlist", ["999.1.1.1/99"]),
    ("egress_allowlist", ["<script>"]),
]


@pytest.fixture
def one_proxy(monkeypatch):
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")


def _field_error(res, field: str) -> None:
    assert res.status_code == 422, res.text
    detail = res.json()["detail"]
    assert isinstance(detail, list), detail
    assert any(field in [str(p) for p in (d.get("loc") or [])] for d in detail), detail


@pytest.mark.parametrize(("field", "value"), BAD)
def test_create_refuses_each_bad_field(ent, one_proxy, field, value):
    from services.tenant_store import get_tenant_for_workspace

    ws = ent.workspace("D341 create")
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Acme", field: value},
                         headers={"X-Workspace-Id": ws, "X-Forwarded-For": "203.0.113.5"})
    _field_error(res, field)
    assert get_tenant_for_workspace(ws) is None


@pytest.mark.parametrize(("field", "value"), BAD)
def test_patch_refuses_each_bad_field_and_keeps_the_stored_value(ent, one_proxy, field, value):
    from services.tenant_store import get_tenant

    ws = ent.workspace("D341 patch")
    hdr = {"X-Workspace-Id": ws, "X-Forwarded-For": "203.0.113.5"}
    created = ent.admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Acme"}, headers=hdr)
    assert created.status_code == 200, created.text
    before = get_tenant(created.json()["id"]).to_dict()
    res = ent.admin.patch(f"/api/v1/workspace/tenant/{created.json()['id']}", json={field: value}, headers=hdr)
    _field_error(res, field)
    after = get_tenant(created.json()["id"]).to_dict()
    assert after[field] == before[field]


def test_good_values_are_accepted_and_normalized(ent, one_proxy):
    ws = ent.workspace("D341 good")
    res = ent.admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws, "name": "Acme & Sons, Inc. (EU)", "custom_domain": "https://Data.Acme.example/",
              "security_contact_email": "Security@Acme.example", "data_region": "eu-west-1",
              "ip_allowlist": ["203.0.113.5"]},
        headers={"X-Workspace-Id": ws, "X-Forwarded-For": "203.0.113.5"},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["name"] == "Acme & Sons, Inc. (EU)"
    assert body["custom_domain"] == "data.acme.example"
    assert body["security_contact_email"] == "security@acme.example"
    assert body["data_region"] == "eu-west-1"
    assert body["ip_allowlist"] == ["203.0.113.5/32"]
    # Clearing optional fields stays possible.
    res = ent.admin.patch(f"/api/v1/workspace/tenant/{body['id']}",
                          json={"custom_domain": "", "security_contact_email": "", "data_region": ""},
                          headers={"X-Workspace-Id": ws, "X-Forwarded-For": "203.0.113.5"})
    assert res.status_code == 200, res.text


def test_store_refuses_junk_for_callers_that_bypass_the_route(tmp_path, monkeypatch):
    from services import tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    for kwargs in ({"security_contact_email": "nope"}, {"data_region": "moon-1"},
                   {"name": "<b>x</b>"}, {"custom_domain": "not a domain!!"}):
        with pytest.raises(ValueError):
            tenant_store.create_tenant(workspace_id="ws-d341", **{"name": "Acme", **kwargs})
    assert tenant_store.list_tenants() == []
