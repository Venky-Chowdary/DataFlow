"""QA round 11 (handover, build 9c75ada4): S7 + MCP manifest origin, end to end.

QA: "set the IP allowlist to 10.0.0.1/32, and the API still answers from any
IP"; session timeout stored but not enforced; the save toast said "applied";
the MCP manifest named the internal Railway backend over http.

Every case below goes through the real REST routes of ``src.main.app`` on a
live MongoDB (tests/r8_ent_harness.py). The client address is set the way
production sets it: the trusted proxy count is configured and the address
arrives in ``X-Forwarded-For``.
"""

from __future__ import annotations

import json
import time

import pytest

from r8_ent_harness import ent  # noqa: F401

INSIDE = "203.0.113.50"
OUTSIDE = "198.51.100.9"
LIST = ["203.0.113.0/24"]
SETTINGS = "/api/v1/workspace/settings"


def _from(ip: str) -> dict[str, str]:
    return {"X-Forwarded-For": ip}


@pytest.fixture
def one_proxy(monkeypatch):
    """One trusted proxy in front (it appends the client address)."""
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")
    for name in ("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE"):
        monkeypatch.delenv(name, raising=False)


def _tenant(ent, ws: str, *, ip: str = INSIDE, **fields) -> dict:
    res = ent.admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws, "name": "R11 tenant", **fields},
        headers={"X-Workspace-Id": ws, **_from(ip)},
    )
    assert res.status_code == 200, res.text
    return res.json()


def _events(ent, action: str) -> list[dict]:
    return [e for e in ent.audit(action=action) if e.get("action") == action]


def _no_env_names(text: str) -> None:
    assert "DATAFLOW_" not in text and "DATAWRAP_" not in text, text


# ── IP allowlist ───────────────────────────────────────────────────────────


def test_ip_outside_the_list_gets_403_for_a_session_and_for_an_api_key(ent, one_proxy):
    ws = ent.workspace("R11 allowlist")
    member = ent.member(email="r11-ip-member@example.com", workspace_id=ws, role="editor")
    key = ent.api_key(role="viewer", workspace_id=ws)
    key_client = ent.bearer(key, ws)
    key_no_header = ent.bearer(key)
    _tenant(ent, ws, ip_allowlist=LIST)

    # Outside: refused for a JWT session and an API key, with or without the header.
    for client in (member, key_client, key_no_header):
        res = client.get(SETTINGS, headers=_from(OUTSIDE))
        assert res.status_code == 403, res.text
        body = res.json()
        assert body["reason"] == "ip_not_in_workspace_allowlist"
        assert body["ip"] == OUTSIDE
        _no_env_names(res.text)
    member_no_header = ent.bearer(member.headers["Authorization"].split(" ", 1)[1])
    assert member_no_header.get(SETTINGS, headers=_from(OUTSIDE)).status_code == 403

    # Inside: both pass.
    assert member.get(SETTINGS, headers=_from(INSIDE)).status_code == 200
    assert key_client.get(SETTINGS, headers=_from(INSIDE)).status_code == 200
    assert key_no_header.get(SETTINGS, headers=_from(INSIDE)).status_code == 200

    # Public paths stay reachable from anywhere.
    assert member.get("/api/v1/health", headers=_from(OUTSIDE)).status_code == 200

    # Every block is audited, against the workspace, with the address.
    blocked = _events(ent, "workspace.ip_allowlist.blocked")
    assert len(blocked) >= 4
    assert {e["details"]["client_ip"] for e in blocked} == {OUTSIDE}
    assert {e["details"]["workspace_id"] for e in blocked} == {ws}
    kinds = {e["details"]["auth_kind"] for e in blocked}
    assert {"session", "api_key"} <= kinds


def test_a_workspace_without_a_list_is_not_affected(ent, one_proxy):
    listed = ent.workspace("R11 listed")
    free = ent.workspace("R11 free")
    _tenant(ent, listed, ip_allowlist=LIST)
    member = ent.member(email="r11-free@example.com", workspace_id=free, role="editor")
    assert member.get(SETTINGS, headers=_from(OUTSIDE)).status_code == 200


def test_kill_switch_off_admits_and_audits_would_block(ent, one_proxy, monkeypatch):
    ws = ent.workspace("R11 kill switch")
    member = ent.member(email="r11-kill@example.com", workspace_id=ws, role="editor")
    _tenant(ent, ws, ip_allowlist=LIST)
    monkeypatch.setenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "0")
    assert member.get(SETTINGS, headers=_from(OUTSIDE)).status_code == 200
    would = _events(ent, "tenant.ip_allowlist.would_block")
    assert would and would[0]["details"]["client_ip"] == OUTSIDE
    posture = ent.admin.get("/api/v1/workspace/security/posture",
                            headers={"X-Workspace-Id": ws, **_from(INSIDE)}).json()
    assert posture["ip_allowlist_enforced"] is False
    assert posture["enforcement"]["ip_allowlist"]["status"] == "audit_only"
    _no_env_names(json.dumps(posture["enforcement"]))


def test_lock_out_guard_refuses_a_list_that_excludes_the_callers_ip(ent, one_proxy):
    ws = ent.workspace("R11 lockout")
    ws_admin = ent.member(email="r11-ws-admin@example.com", workspace_id=ws, role="admin")
    created = ws_admin.post(
        "/api/v1/workspace/tenant",
        json={"workspace_id": ws, "name": "Lockout", "ip_allowlist": LIST},
        headers=_from(OUTSIDE),
    )
    assert created.status_code == 400, created.text
    assert OUTSIDE in created.json()["detail"]
    assert "lock you out" in created.json()["detail"]

    tenant = ws_admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "Lockout"},
                           headers=_from(OUTSIDE))
    assert tenant.status_code == 200, tenant.text
    tid = tenant.json()["id"]
    refused = ws_admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": LIST},
                             headers=_from(OUTSIDE))
    assert refused.status_code == 400, refused.text
    assert "your current IP address (198.51.100.9)" in refused.json()["detail"]
    _no_env_names(refused.text)
    # Nothing was saved: the admin still gets in from where they are.
    assert ws_admin.get("/api/v1/workspace/tenant", headers=_from(OUTSIDE)).json()["ip_allowlist"] == []

    ok = ws_admin.patch(f"/api/v1/workspace/tenant/{tid}", json={"ip_allowlist": LIST},
                        headers=_from(INSIDE))
    assert ok.status_code == 200, ok.text
    assert ok.json()["ip_allowlist"] == LIST


def test_platform_admin_break_glass_is_allowed_and_audited(ent, one_proxy):
    ws = ent.workspace("R11 break glass")
    _tenant(ent, ws, ip_allowlist=LIST)
    headers = {"X-Workspace-Id": ws, **_from(OUTSIDE)}

    plain = ent.admin.get(SETTINGS, headers=headers)
    assert plain.status_code == 403, plain.text
    assert plain.json().get("break_glass_header") == "X-Datawrap-Break-Glass"

    reason = "INC-4711 customer locked out, restoring access"
    glass = ent.admin.get(SETTINGS, headers={**headers, "X-Datawrap-Break-Glass": reason})
    assert glass.status_code == 200, glass.text
    used = _events(ent, "workspace.ip_allowlist.break_glass")
    assert len(used) == 1
    assert used[0]["details"]["reason"] == reason
    assert used[0]["details"]["client_ip"] == OUTSIDE
    assert used[0]["details"]["workspace_id"] == ws

    # A workspace admin cannot use it.
    ws_admin = ent.member(email="r11-glass-ws-admin@example.com", workspace_id=ws, role="admin")
    denied = ws_admin.get(SETTINGS, headers={**_from(OUTSIDE), "X-Datawrap-Break-Glass": reason})
    assert denied.status_code == 403
    assert len(_events(ent, "workspace.ip_allowlist.break_glass")) == 1


def test_proxy_header_chain_resolves_the_real_client_ip(ent, monkeypatch):
    """Railway: browser -> edge -> nginx -> edge -> API. Three proxies each
    append the peer they saw, so the client is hops[-3]."""
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "3")
    ws = ent.workspace("R11 proxy chain")
    member = ent.member(email="r11-chain@example.com", workspace_id=ws, role="editor")
    real = f"{INSIDE}, 100.64.0.7, 34.86.1.2"
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "Chain", "ip_allowlist": LIST},
                         headers={"X-Workspace-Id": ws, "X-Forwarded-For": real})
    assert res.status_code == 200, res.text

    assert member.get(SETTINGS, headers={"X-Forwarded-For": real}).status_code == 200
    # The client prepends an allowed address: the trusted hop still decides.
    spoof = member.get(SETTINGS, headers={"X-Forwarded-For": f"203.0.113.1, {OUTSIDE}, 100.64.0.7, 34.86.1.2"})
    assert spoof.status_code == 403
    assert spoof.json()["ip"] == OUTSIDE
    # A chain shorter than the trusted proxies never yields a client value.
    short = member.get(SETTINGS, headers={"X-Forwarded-For": "203.0.113.1"})
    assert short.status_code == 403
    assert short.json()["ip"] == "testclient"


# ── Session timeout ─────────────────────────────────────────────────────────


def _age_sessions(ent, email: str, seconds: int) -> None:
    then = int(time.time()) - seconds
    ent.db["auth_sessions"].update_many({"email": email},
                                        {"$set": {"last_seen_at": then, "created_at": then}})
    path = ent.tmp / "sessions.json"
    if path.exists():
        data = json.loads(path.read_text())
        for row in data["sessions"].values():
            if row["email"] == email:
                row["last_seen_at"] = then
                row["created_at"] = then
        path.write_text(json.dumps(data))


def test_session_timeout_is_enforced_once_the_hours_are_saved(ent):
    ws = ent.workspace("R11 idle")
    email = "r11-idle@example.com"
    member = ent.member(email=email, workspace_id=ws, role="editor")
    tenant = _tenant(ent, ws)
    patched = ent.admin.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"session_timeout_hours": 1},
                              headers={"X-Workspace-Id": ws})
    assert patched.status_code == 200, patched.text
    assert patched.json()["enforce_session_timeout"] is True
    assert patched.json()["enforcement"]["session_timeout"]["enforced"] is True

    _age_sessions(ent, email, 30 * 60)
    assert member.get(SETTINGS).status_code == 200
    _age_sessions(ent, email, 2 * 3600)
    expired = member.get(SETTINGS)
    assert expired.status_code == 401
    assert expired.json()["reason"] == "session_idle_timeout"


def test_sessions_survive_a_lost_session_file_and_stay_idle_checked(ent):
    ws = ent.workspace("R11 durable")
    email = "r11-durable@example.com"
    member = ent.member(email=email, workspace_id=ws, role="editor")
    _tenant(ent, ws, session_timeout_hours=1)

    # Redeploy loses the local file: the session is still known (Mongo).
    (ent.tmp / "sessions.json").unlink(missing_ok=True)
    assert member.get(SETTINGS).status_code == 200
    # ...and its idle clock still runs.
    _age_sessions(ent, email, 2 * 3600)
    (ent.tmp / "sessions.json").unlink(missing_ok=True)
    expired = member.get(SETTINGS)
    assert expired.status_code == 401
    assert expired.json()["reason"] == "session_idle_timeout"


# ── Save feedback ───────────────────────────────────────────────────────────


def test_save_response_reports_enforcement_from_the_posture_function(ent, one_proxy, monkeypatch):
    ws = ent.workspace("R11 feedback")
    created = _tenant(ent, ws, ip_allowlist=LIST, session_timeout_hours=4)
    enf = created["enforcement"]
    assert enf["ip_allowlist"]["enforced"] is True
    assert enf["ip_allowlist"]["status"] == "enforced"
    assert enf["session_timeout"]["enforced"] is True
    assert enf["session_timeout"]["hours"] == 4
    assert enf["mfa"]["enforced"] is False

    posture = ent.admin.get("/api/v1/workspace/security/posture",
                            headers={"X-Workspace-Id": ws, **_from(INSIDE)}).json()
    assert posture["enforcement"]["ip_allowlist"] == enf["ip_allowlist"]
    assert posture["enforcement"]["session_timeout"] == enf["session_timeout"]

    monkeypatch.setenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "off")
    off = ent.admin.patch(f"/api/v1/workspace/tenant/{created['id']}",
                          json={"enforce_session_timeout": False},
                          headers={"X-Workspace-Id": ws, **_from(INSIDE)})
    assert off.status_code == 200, off.text
    enf = off.json()["enforcement"]
    assert enf["ip_allowlist"]["enforced"] is False
    assert enf["session_timeout"]["enforced"] is False
    assert len(enf["warnings"]) >= 2
    assert any("platform administrator" in w for w in enf["warnings"])
    _no_env_names(off.text)

    posture = ent.admin.get("/api/v1/workspace/security/posture",
                            headers={"X-Workspace-Id": ws, **_from(INSIDE)})
    _no_env_names(posture.text)
    report = ent.admin.get("/api/v1/workspace/security/report",
                           headers={"X-Workspace-Id": ws, **_from(INSIDE)})
    assert report.status_code == 200
    _no_env_names(report.text)


# ── MCP manifest origin ─────────────────────────────────────────────────────


INTERNAL = "dataflow-api-production-722b.up.railway.app"


def test_mcp_manifest_never_names_the_internal_host_or_http(ent, monkeypatch):
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAWRAP_WEB_DOMAIN", "app.datawrap.example")
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", INTERNAL)
    spoofed = {"Host": INTERNAL, "X-Forwarded-Proto": "http", "X-Forwarded-Host": "evil.example"}
    client = ent.anonymous()
    for path in ("/api/v1/mcp/manifest", "/api/v1/mcp/status"):
        res = client.get(path, headers=spoofed)
        if res.status_code == 401:
            res = ent.admin.get(path, headers=spoofed)
        assert res.status_code == 200, (path, res.text)
        text = res.text
        assert INTERNAL not in text and "railway" not in text, text
        assert "evil.example" not in text
        assert "http://" not in text
        assert "https://app.datawrap.example/api/v1/mcp" in text

    # A forwarded host on the allow-list (a tenant custom domain) is honoured, over https.
    ws = ent.workspace("R11 manifest")
    _tenant(ent, ws, custom_domain="data.customer.example")
    vanity = client.get("/api/v1/mcp/manifest",
                        headers={"Host": INTERNAL, "X-Forwarded-Host": "data.customer.example",
                                 "X-Forwarded-Proto": "http"}).json()
    assert vanity["endpoints"]["mcp"] == "https://data.customer.example/api/v1/mcp"
