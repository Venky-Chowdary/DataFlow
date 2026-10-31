"""Deployment-wide settings are a platform admin's to change (owner decision, round 10).

General (the organization profile), SSO and AI providers (keys, Ollama base
URL, Pilot engine) are each one document for the whole deployment. On dbf206cc
any workspace admin could rewrite them for every tenant. Now:

* a workspace admin gets 403 on every write and reads ``editable: false`` with
  the reason; secrets stay masked;
* a platform admin (the environment bootstrap admin here) writes them;
* a key a platform admin minted *inside a workspace* is that workspace's key,
  not a platform admin, so it gets 403 too;
* every refusal is the named audit event ``platform_settings.write.denied``;
* ``POST /team/users/{email}/platform-role`` is how a deployment gets its next
  platform admin: platform-admin session only, reason required, audited.

Real REST routes, real MongoDB (teams, accounts, API keys) via
``r9_workspace_world``.
"""

from __future__ import annotations

from typing import Any

import pytest

from r9_workspace_world import OWNER, ROOT, r9  # noqa: F401

REASON = "Only a platform admin can change deployment-wide settings"
DENIED = "platform_settings.write.denied"
OIDC_SECRET = "oidc-client-secret-r10-value"

# (method, path, body, group) — every write route of the three groups.
WRITES: list[tuple[str, str, dict[str, Any] | None, str]] = [
    ("PATCH", "/api/v1/workspace/settings", {"org_name": "Hijacked Org"}, "general"),
    # Invalid body: the refusal still comes first (no 422 before the 403).
    ("PATCH", "/api/v1/workspace/settings", {"retention_days": 0}, "general"),
    (
        "PATCH",
        "/api/v1/workspace/sso/oidc",
        {"enabled": False, "issuer": "https://evil.example.test", "client_id": "evil"},
        "sso",
    ),
    ("PATCH", "/api/v1/workspace/sso/saml", {"enabled": False, "entity_id": "evil"}, "sso"),
    ("PATCH", "/api/v1/workspace/sso/oidc", {"clear_client_secret": True}, "sso"),
    (
        "PATCH",
        "/api/v1/workspace/ai-providers/ollama",
        {"base_url": "http://evil.example.test:11434"},
        "ai_providers",
    ),
    # Test-and-save: refused before the live key check is attempted.
    ("PATCH", "/api/v1/workspace/ai-providers/openai", {"api_key": "sk-evil-r10"}, "ai_providers"),
    ("DELETE", "/api/v1/workspace/ai-providers/openai/key", None, "ai_providers"),
    ("PATCH", "/api/v1/workspace/pilot-engine", {"engine": "cloud"}, "ai_providers"),
]

READS = (
    "/api/v1/workspace/settings",
    "/api/v1/workspace/sso",
    "/api/v1/workspace/ai-providers",
    "/api/v1/workspace/pilot-engine",
)


@pytest.fixture
def world(r9, tmp_path, monkeypatch):
    from services import integrations_store, workspace_store

    monkeypatch.setattr(workspace_store, "STORE_PATH", tmp_path / "workspace.json")
    # The AI routes hydrate these from the store; restore them afterwards.
    for name in ("OLLAMA_BASE_URL", "OLLAMA_MODEL"):
        monkeypatch.setenv(name, "")
    yield r9
    for name in ("OLLAMA_BASE_URL", "OLLAMA_MODEL"):
        integrations_store._HYDRATED_ENV.discard(name)


def _send(client, method: str, path: str, body: dict[str, Any] | None):
    if method == "DELETE":
        return client.delete(path)
    return client.request(method, path, json=body)


def _seed_as_platform_admin(world) -> None:
    root = world.world.root
    assert root.patch("/api/v1/workspace/settings", json={"org_name": "Datawrap Prod"}).status_code == 200
    seeded = root.patch(
        "/api/v1/workspace/sso/oidc",
        json={
            "enabled": False,
            "issuer": "https://idp.example.test",
            "client_id": "datawrap",
            "client_secret": OIDC_SECRET,
        },
    )
    assert seeded.status_code == 200, seeded.text
    assert root.patch("/api/v1/workspace/pilot-engine", json={"engine": "local"}).status_code == 200


def _state() -> dict[str, Any]:
    from services import integrations_store, workspace_store

    raw = integrations_store._load_raw()
    return {
        "settings": {k: v for k, v in workspace_store.get_workspace_settings().items()},
        "sso": raw["sso"],
        "ai": raw["ai_providers"],
        "pilot": raw["pilot"],
    }


def test_workspace_admin_is_refused_every_write_and_reads_editable_false(world):
    _seed_as_platform_admin(world)
    before = _state()
    owner = world.owner  # Acme's admin by membership, platform role "member"
    assert world.world.role_of(OWNER) == "admin"

    for method, path, body, group in WRITES:
        res = _send(owner, method, path, body)
        assert res.status_code == 403, (method, path, res.status_code, res.text)
        assert res.json()["detail"] == REASON, (method, path, res.text)

    assert _state() == before, "a refused write changed a deployment-wide document"

    for path in READS:
        res = owner.get(path)
        assert res.status_code == 200, (path, res.text)
        payload = res.json()
        assert payload["editable"] is False, (path, payload)
        assert payload["editable_reason"] == REASON, (path, payload)

    sso = owner.get("/api/v1/workspace/sso").json()["providers"]["oidc"]
    assert sso["client_id"] == "datawrap"
    assert OIDC_SECRET not in str(sso), "workspace admin read the SSO secret in clear"

    denials = world.world.audit(DENIED)
    assert len(denials) == len(WRITES), denials
    groups = sorted(e["details"]["group"] for e in denials)
    assert groups == sorted(g for *_x, g in WRITES)
    for event in denials:
        assert event["outcome"] == "failed"
        assert event["reason_code"] == "platform_admin_required"
        assert event["details"]["principal"]["email"] == OWNER
        assert event["details"]["principal"]["platform_admin"] is False
    operations = {e["details"]["operation"] for e in denials}
    assert {"update", "test_and_save", "clear_secret", "set_pilot_engine"} <= operations


def test_platform_admin_writes_every_group_and_reads_editable_true(world):
    root = world.world.root
    for path in READS:
        payload = root.get(path).json()
        assert payload["editable"] is True, (path, payload)
        assert payload["editable_reason"] is None

    res = root.patch("/api/v1/workspace/settings", json={"org_name": "Platform Org", "timezone": "Europe/Berlin"})
    assert res.status_code == 200, res.text
    assert res.json()["org_name"] == "Platform Org"

    res = root.patch(
        "/api/v1/workspace/sso/oidc",
        json={"enabled": False, "issuer": "https://idp.example.test", "client_id": "dw", "client_secret": OIDC_SECRET},
    )
    assert res.status_code == 200, res.text
    res = root.patch("/api/v1/workspace/sso/oidc", json={"clear_client_secret": True})
    assert res.status_code == 200, res.text

    res = root.patch("/api/v1/workspace/ai-providers/ollama", json={"base_url": "http://127.0.0.1:9"})
    assert res.status_code == 200, res.text
    assert res.json()["base_url"] == "http://127.0.0.1:9"
    res = root.delete("/api/v1/workspace/ai-providers/openai/key")
    assert res.status_code == 200, res.text
    res = root.patch("/api/v1/workspace/pilot-engine", json={"engine": "local"})
    assert res.status_code == 200, res.text
    assert res.json()["preference"] == "local"

    assert world.world.audit(DENIED) == []


def test_workspace_bound_key_of_a_platform_admin_is_refused(world):
    root = world.world.root  # X-Workspace-Id = Acme
    key = world.world.mint_key("admin", by=root, name="root-in-acme")
    assert key["workspace_id"] == world.acme
    as_key = world.world.as_key(key)

    for method, path, body, _group in WRITES:
        res = _send(as_key, method, path, body)
        assert res.status_code == 403, (method, path, res.status_code, res.text)
        assert res.json()["detail"] == REASON

    assert as_key.get("/api/v1/workspace/sso").json()["editable"] is False
    denials = world.world.audit(DENIED)
    assert len(denials) == len(WRITES)
    assert {e["details"]["principal"]["kind"] for e in denials} == {"api_key"}
    assert {e["details"]["principal"]["email"] for e in denials} == {ROOT}


def test_promotion_is_platform_admin_session_only_reasoned_and_audited(world):
    from services import user_store

    owner = world.owner
    root = world.world.root
    url = f"/api/v1/team/users/{OWNER}/platform-role"

    # A workspace admin cannot make anyone (themselves included) a platform admin.
    res = owner.post(url, json={"role": "admin", "reason": "I want it"})
    assert res.status_code == 403, res.text
    # Nor can any API key — even an unbound admin key the platform admin minted.
    unbound = world.login(ROOT, "Bootstrap-Admin-2026!", None)
    key = world.world.mint_key("admin", by=unbound, name="root-unbound")
    assert not key["workspace_id"]
    res = world.world.as_key(key).post(url, json={"role": "admin", "reason": "automation"})
    assert res.status_code == 403, res.text
    # A reason is required.
    res = root.post(url, json={"role": "admin", "reason": "   "})
    assert res.status_code == 422, res.text
    # The environment bootstrap admin is changed in the environment, not here.
    res = root.post(f"/api/v1/team/users/{ROOT}/platform-role", json={"role": "member", "reason": "x"})
    assert res.status_code in (400, 409), res.text
    assert user_store.get_user(OWNER)["role"] == "member"

    denied = world.world.audit("team.platform_role.denied")
    codes = [e["details"]["reason_code"] for e in denied]
    assert codes.count("not_platform_admin_session") == 2, codes
    assert "reason_required" in codes

    res = root.post(url, json={"role": "admin", "reason": "Owner runs production (ticket OPS-1)"})
    assert res.status_code == 200, res.text
    assert res.json()["user"]["role"] == "admin"
    assert res.json()["from_role"] == "member"
    assert user_store.get_user(OWNER)["role"] == "admin"

    granted = world.world.audit("team.platform_role.change")
    assert len(granted) == 1
    details = granted[0]["details"]
    assert granted[0]["actor"] == ROOT
    assert details["target"] == OWNER
    assert details["to_role"] == "admin"
    assert details["reason"] == "Owner runs production (ticket OPS-1)"
    assert details["principal"]["kind"] == "session"

    # The promoted account now changes deployment-wide settings.
    promoted = world.world.login(OWNER, "Owner-Password-2026!")
    assert promoted.get("/api/v1/workspace/sso").json()["editable"] is True
    res = promoted.patch("/api/v1/workspace/pilot-engine", json={"engine": "local"})
    assert res.status_code == 200, res.text

    # And can demote again (not themselves).
    res = promoted.post(url, json={"role": "member", "reason": "self"})
    assert res.status_code == 400, res.text
    res = root.post(url, json={"role": "member", "reason": "rotation ended"})
    assert res.status_code == 200, res.text
    assert user_store.get_user(OWNER)["role"] == "member"
