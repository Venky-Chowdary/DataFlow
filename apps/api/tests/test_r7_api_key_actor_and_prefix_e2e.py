"""Settings → Audit Logs and API Keys: key identity on screen, and the key brand.

QA (production 557f556c): the audit actor column showed full API-key ids
(``api_key:<uuid>``) and the API Keys page showed a ``dfk_`` prefix.

* ``/audit/events`` now carries ``actor_display`` — the key's name and a short
  masked id — so the UI never has to print a raw key id.
* New keys take the Datawrap prefix ``dwk_`` behind the brand flag
  (``DATAWRAP_BRAND_OBJECT_NAMES=1``, audited per key), because customers hold
  keys in their own systems (secret scanners, CI masks keyed on ``dfk_``).
  Both prefixes authenticate, with the flag on or off.
"""

from __future__ import annotations

import re

import pytest
from fastapi.testclient import TestClient

from services import audit_log, integrations_store, team_store, user_store
from services.auth_rate_limit import reset_auth_rate_limits

ADMIN_EMAIL = "root@example.com"
ADMIN_PASSWORD = "Bootstrap-Admin-2026"
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@pytest.fixture
def admin(tmp_path, monkeypatch):
    from src.main import app

    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", ADMIN_EMAIL)
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", ADMIN_PASSWORD)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-not-production")
    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.delenv("DATAFLOW_BRAND_OBJECT_NAMES", raising=False)
    monkeypatch.setattr(team_store, "mongo_database", lambda: None)
    monkeypatch.setattr(user_store, "mongo_database", lambda: None)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    import services.tenant_store as tenant_store

    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    try:  # services.brand arrived with round 5; absent on older builds
        from services import brand

        brand.reset_resolution_cache()
    except ImportError:
        pass
    reset_auth_rate_limits()
    client = TestClient(app)
    login = client.post("/api/v1/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert login.status_code == 200, login.text
    client.headers["Authorization"] = f"Bearer {login.json()['token']}"
    return client


def _mint(client: TestClient, name: str) -> dict:
    res = client.post("/api/v1/workspace/api-keys", json={"name": name, "role": "admin"})
    assert res.status_code == 200, res.text
    return res.json()


def _as_key(raw: str) -> TestClient:
    from src.main import app

    c = TestClient(app)
    c.headers["Authorization"] = f"Bearer {raw}"
    return c


def test_audit_actor_shows_key_name_and_masked_id_not_the_uuid(admin):
    key = _mint(admin, "CI deploy")
    res = _as_key(key["key"]).patch("/api/v1/workspace/pilot-engine", json={"engine": "local"})
    assert res.status_code == 200, res.text

    events = admin.get("/api/v1/audit/events?limit=200").json()["events"]
    by_key = [e for e in events if str(e.get("actor_id") or "") == key["id"]]
    assert by_key, "no audit event attributed to the API key"
    for ev in by_key:
        display = ev["actor_display"]
        assert display == f"CI deploy (key …{key['id'][-4:]})"
        assert not UUID.search(display)


def test_unknown_or_revoked_key_actor_is_still_masked(admin):
    key = _mint(admin, "Old key")
    assert _as_key(key["key"]).patch(
        "/api/v1/workspace/pilot-engine", json={"engine": "local"}
    ).status_code == 200
    assert admin.delete(f"/api/v1/workspace/api-keys/{key['id']}").status_code == 200
    events = admin.get("/api/v1/audit/events?limit=200").json()["events"]
    ev = next(e for e in events if str(e.get("actor_id") or "") == key["id"])
    # A revoked key keeps its name; nothing on screen is the raw id.
    assert ev["actor_display"] == f"Old key (key …{key['id'][-4:]})"


def test_new_keys_take_the_brand_prefix_behind_the_flag_and_both_authenticate(admin, monkeypatch):
    legacy = _mint(admin, "legacy")
    assert legacy["key"].startswith("dfk_")  # flag off: unchanged for customers

    monkeypatch.setenv("DATAWRAP_BRAND_OBJECT_NAMES", "1")
    branded = _mint(admin, "branded")
    assert branded["key"].startswith("dwk_")
    assert branded["prefix"].startswith("dwk_")

    for raw in (legacy["key"], branded["key"]):
        res = _as_key(raw).get("/api/v1/workspace/pilot-engine")
        assert res.status_code == 200, (raw[:4], res.text)

    rotated = admin.post(f"/api/v1/workspace/api-keys/{legacy['id']}/rotate", json={})
    if rotated.status_code == 200:
        assert rotated.json()["key"].startswith("dwk_")

    # Flag back off: existing dwk_ keys keep working.
    monkeypatch.delenv("DATAWRAP_BRAND_OBJECT_NAMES")
    assert _as_key(branded["key"]).get("/api/v1/workspace/pilot-engine").status_code == 200

    # A wrong key under either prefix is refused.
    for bogus in ("dwk_" + "x" * 43, "dfk_" + "x" * 43):
        assert _as_key(bogus).get("/api/v1/workspace/pilot-engine").status_code == 401

    events = admin.get("/api/v1/audit/events?limit=300").json()["events"]
    audited = [e for e in events if e.get("action") == "brand.object_name.created"
               and (e.get("details") or {}).get("kind") == "api_key_prefix"]
    assert audited, "issuing a dwk_ key under the flag must be audited"


def test_both_prefixes_are_redacted_from_logs():
    from services.log_redaction import redact_secret_shapes

    for raw in ("dwk_" + "A1b2C3d4E5f6G7h8I9j0", "dfk_" + "A1b2C3d4E5f6G7h8I9j0"):
        out = redact_secret_shapes(f"Authorization failed for {raw} at 10:00")
        assert "A1b2C3d4E5f6G7h8I9j0" not in out
