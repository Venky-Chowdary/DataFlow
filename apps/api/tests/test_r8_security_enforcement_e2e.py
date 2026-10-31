"""Security posture reports exactly what is enforced (QA enterprise #6/#7/#8).

QA (production 557f556c): the posture honestly said ``*_enforced=false`` for
MFA, IP allowlist and session timeout. Round 8:

* **Session timeout** is enforced as an *idle* timeout on sign-in sessions
  when the workspace tenant sets ``enforce_session_timeout`` (round 11: on by
  default and whenever the hours are saved).
* **IP allowlist** (round 11): enforced on every host after auth once a list
  is saved; the platform kill switch turns it into audit-only, and the
  posture names what is missing without deployment variable names.
* **MFA** (round 11) is enforced when the tenant sets ``mfa_required``; tests/test_r11_mfa_e2e.py.
"""

from __future__ import annotations

import json
import time

from r8_ent_harness import ent  # noqa: F401


def _tenant(ent, ws: str, **fields) -> dict:
    res = ent.admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "T", **fields},
                         headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    return res.json()


def _age_sessions(ent, email: str, seconds: int) -> None:
    # Sessions are durable in Mongo with the file as a mirror (round 11).
    then = int(time.time()) - seconds
    ent.db["auth_sessions"].update_many({"email": email}, {"$set": {"last_seen_at": then, "created_at": then}})
    path = ent.tmp / "sessions.json"
    data = json.loads(path.read_text())
    for row in data["sessions"].values():
        if row.get("email") == email:
            row["last_seen_at"] = then
            row["created_at"] = then
    path.write_text(json.dumps(data))


def test_idle_sessions_expire_only_when_the_workspace_enforces_the_policy(ent):
    ws = ent.workspace("Idle policy")
    other = ent.workspace("No policy")
    email = "idle-member-r8@example.com"
    member = ent.member(email=email, workspace_id=ws, role="editor")
    bystander_email = "idle-bystander-r8@example.com"
    bystander = ent.member(email=bystander_email, workspace_id=other, role="editor")
    key_client = ent.bearer(ent.api_key(role="viewer", workspace_id=ws), ws)
    # Round 11: a saved timeout is enforced by default; record it as policy only here.
    tenant = _tenant(ent, ws, session_timeout_hours=1, enforce_session_timeout=False)

    # Policy recorded but not enforced: two idle hours change nothing.
    _age_sessions(ent, email, 2 * 3600)
    assert member.get("/api/v1/workspace/settings").status_code == 200
    posture = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws}).json()
    assert posture["session_timeout_enforced"] is False

    on = ent.admin.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"enforce_session_timeout": True},
                         headers={"X-Workspace-Id": ws})
    assert on.status_code == 200, on.text
    posture = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws}).json()
    assert posture["session_timeout_enforced"] is True
    assert posture["enforcement"]["session_timeout"]["hours"] == 1

    # Active within the window: fine.
    _age_sessions(ent, email, 30 * 60)
    assert member.get("/api/v1/workspace/settings").status_code == 200
    # Idle past it: signed out, and the session stays revoked.
    _age_sessions(ent, email, 2 * 3600)
    expired = member.get("/api/v1/workspace/settings")
    assert expired.status_code == 401
    assert expired.json()["reason"] == "session_idle_timeout"
    _age_sessions(ent, email, 0)
    assert member.get("/api/v1/workspace/settings").status_code == 401

    # Not affected: a member of a workspace without the policy, and API keys.
    _age_sessions(ent, bystander_email, 5 * 3600)
    assert bystander.get("/api/v1/workspace/settings").status_code == 200
    assert key_client.get("/api/v1/workspace/settings").status_code == 200

    report = ent.admin.get("/api/v1/workspace/security/report", headers={"X-Workspace-Id": ws}).text
    assert "Session timeout: 1h idle; enforced=yes" in report


def test_ip_allowlist_is_reported_enforced_only_when_it_actually_is(ent, monkeypatch):
    """Round 11: enforced on every host once saved; the platform kill switch
    makes it audit-only, and the posture says so in admin-facing words."""
    monkeypatch.setenv("DATAWRAP_TRUSTED_PROXY_COUNT", "1")
    inside = {"X-Forwarded-For": "203.0.113.9"}
    ws = ent.workspace("IP policy")
    res = ent.admin.post("/api/v1/workspace/tenant",
                         json={"workspace_id": ws, "name": "T", "ip_allowlist": ["203.0.113.0/24"]},
                         headers={"X-Workspace-Id": ws, **inside})
    assert res.status_code == 200, res.text
    monkeypatch.setenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", "0")
    off = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws, **inside}).json()
    assert off["ip_allowlist_enforced"] is False
    assert off["enforcement"]["ip_allowlist"]["missing"] == ["IP allowlist enforcement for this deployment"]
    # MFA is real since round 11; this tenant does not require it, so not enforced.
    assert off["mfa_enforced"] is False and off["enforcement"]["mfa"]["status"] == "available"

    monkeypatch.delenv("DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    monkeypatch.delenv("DATAFLOW_TENANT_IP_ALLOWLIST_ENFORCE", raising=False)
    on = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws, **inside}).json()
    assert on["ip_allowlist_enforced"] is True
    # And it is real, on the shared host: a request from outside the list is refused.
    refused = ent.admin.get("/api/v1/workspace/settings",
                            headers={"X-Workspace-Id": ws, "X-Forwarded-For": "198.51.100.1"})
    assert refused.status_code == 403
    assert "allowlist" in refused.json()["detail"]
