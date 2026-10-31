"""Settings -> Audit Logs, through the real routes on the Mongo audit chain.

QA enterprise report (production 557f556c):

* #37 ``GET /audit/events?action=team.member.add`` (also ``search``/``q``)
  returned mixed actions. Root cause: the route had no ``action``/``q``
  parameter at all; FastAPI ignored the unknown query string.
* #35 admin changes to settings, SSO, notifications, AI, rotation and
  revocation showed only as generic ``http.patch`` / ``http.delete`` rows,
  even for 403s. Root causes: the handlers' events carried no workspace, so the
  workspace-scoped view never showed them, and a refused request never reached
  the handler's audit call, leaving only the catch-all row.
* #39 (fixed in round 7, confirmed here): ``GET /audit/verify`` reports
  ``verified`` for an intact workspace chain.
"""

from __future__ import annotations

from r8_ent_harness import ADMIN_EMAIL, ent  # noqa: F401


def _scoped(ent, ws: str, **params) -> list[dict]:
    return ent.audit(ws, **params)


def test_action_filter_and_search_narrow_the_events(ent):
    ws = ent.workspace("Audit filter")
    for email, role in (("filter-a-r8@example.com", "viewer"), ("filter-b-r8@example.com", "editor")):
        added = ent.admin.post(
            f"/api/v1/team/workspaces/{ws}/members",
            json={"email": email, "role": role},
            headers={"X-Workspace-Id": ws},
        )
        assert added.status_code == 200, added.text
    ent.admin.patch(
        "/api/v1/workspace/settings", json={"org_name": "Filter Co"}, headers={"X-Workspace-Id": ws}
    )
    everything = _scoped(ent, ws)
    assert len({e["action"] for e in everything}) > 1

    only = _scoped(ent, ws, action="team.member.add")
    assert only, everything
    assert {e["action"] for e in only} == {"team.member.add"}

    several = _scoped(ent, ws, action="team.member.add,workspace.settings.update")
    assert {e["action"] for e in several} == {"team.member.add", "workspace.settings.update"}

    prefix = _scoped(ent, ws, action="team.member.*")
    assert prefix and all(e["action"].startswith("team.member.") for e in prefix)

    for key, needle in (("q", "WORKSPACE/SETTINGS"), ("search", "member.add")):
        found = _scoped(ent, ws, **{key: needle})
        assert found and len(found) < len(everything)
        assert all(
            needle.lower() in (e.get("resource", "") + e.get("actor", "") + e["action"]).lower()
            for e in found
        ), found


def _named(ent, ws: str, action: str) -> list[dict]:
    return _scoped(ent, ws, action=action)


def test_admin_changes_are_named_with_actor_target_and_outcome(ent):
    ws = ent.workspace("Audit names")
    admin_email = "names-admin-r8@example.com"
    admin = ent.member(email=admin_email, workspace_id=ws, role="admin")
    viewer_email = "names-viewer-r8@example.com"
    viewer = ent.member(email=viewer_email, workspace_id=ws, role="viewer")

    # Success: settings, SSO, AI provider, notification channel, key revoke.
    assert admin.patch("/api/v1/workspace/settings", json={"org_name": "Named"}).status_code == 200
    assert admin.patch("/api/v1/workspace/sso/oidc", json={"client_id": "dw"}).status_code == 200
    assert admin.patch("/api/v1/workspace/ai-providers/ollama", json={"model": "llama3.2"}).status_code == 200
    channel = admin.post(
        "/api/v1/workspace/notifications",
        json={"workspace_id": ws, "kind": "webhook", "label": "r8",
              "config": {"url": "https://hooks.example.com/r8"}},
    )
    assert channel.status_code == 200, channel.text
    key_id = ent.admin.post(
        "/api/v1/workspace/api-keys", json={"name": "to revoke", "role": "viewer", "expires_in": "7d"},
        headers={"X-Workspace-Id": ws},
    ).json()["id"]
    assert ent.admin.delete(f"/api/v1/workspace/api-keys/{key_id}", headers={"X-Workspace-Id": ws}).status_code == 200

    settings = _named(ent, ws, "workspace.settings.update")
    assert settings[0]["actor"] == admin_email
    assert settings[0]["outcome"] == "succeeded"
    assert settings[0]["details"]["target"] == "workspace_settings"
    sso = _named(ent, ws, "workspace.sso.update")[0]
    assert (sso["details"]["target"], sso["outcome"], sso["actor"]) == ("oidc", "succeeded", admin_email)
    ai = _named(ent, ws, "workspace.ai_provider.update")[0]
    assert ai["details"]["target"] == "ollama"
    created = _named(ent, ws, "workspace.notification.create")[0]
    assert created["outcome"] == "succeeded" and created["actor"] == admin_email
    revoked = _named(ent, ws, "workspace.api_key.revoke")[0]
    assert revoked["details"]["target"] == key_id and revoked["actor"] == ADMIN_EMAIL

    # Refusals are named too, with the refused actor and a failed outcome.
    assert viewer.patch("/api/v1/workspace/settings", json={"org_name": "x"}).status_code == 403
    assert viewer.patch("/api/v1/workspace/sso/saml", json={"enabled": True}).status_code == 403
    assert viewer.delete(f"/api/v1/workspace/notifications/{channel.json()['id']}").status_code == 403
    viewer_key = ent.api_key(role="viewer", workspace_id=ws, name="viewer key")
    rotate = ent.bearer(viewer_key, ws).post(f"/api/v1/iam/api-keys/{key_id}/rotate", json={})
    assert rotate.status_code == 403
    # An invalid admin change (400) is a failed outcome as well.
    assert admin.patch("/api/v1/workspace/sso/saml", json={"enabled": True}).status_code == 400

    def refused(action: str, actor: str) -> dict:
        rows = [e for e in _named(ent, ws, action) if e.get("outcome") == "failed" and e["actor"] == actor]
        assert rows, (action, _named(ent, ws, action))
        return rows[0]

    assert refused("workspace.settings.update", viewer_email)["reason_code"] == "forbidden"
    assert refused("workspace.sso.update", viewer_email)["details"]["target"] == "saml"
    assert refused("workspace.notification.delete", viewer_email)["details"]["status"] == 403
    assert refused("workspace.sso.update", admin_email)["reason_code"] == "invalid_request"
    rot = [e for e in _named(ent, ws, "iam.api_key.rotate") if e.get("outcome") == "failed"]
    assert rot and rot[0]["details"]["target"] == key_id

    generic = [e for e in _scoped(ent, ws) if e["action"] in {"http.patch", "http.delete", "http.post"}
               and "/workspace/" in e.get("resource", "")]
    assert generic == [], generic


def test_verify_reports_an_intact_workspace_chain_as_verified(ent):
    """Round 7's per-workspace chain, confirmed through this harness: QA's case
    is an intact workspace while *another* workspace's chain has a finding."""
    ws = ent.workspace("Audit verify")
    other = ent.workspace("Audit other")
    admin = ent.member(email="verify-admin-r8@example.com", workspace_id=ws, role="admin")
    other_admin = ent.member(email="verify-other-r8@example.com", workspace_id=other, role="admin")
    admin.patch("/api/v1/workspace/settings", json={"org_name": "Verify"})
    other_admin.patch("/api/v1/workspace/settings", json={"org_name": "Other"})
    tampered = ent.db["audit_events"].update_one(
        {"workspace_id": other, "action": "workspace.settings.update"},
        {"$set": {"details.org_name": "rewritten"}},
    )
    if tampered.matched_count == 0:  # 557f556c wrote these rows without a workspace
        ent.db["audit_events"].update_one(
            {"workspace_id": other}, {"$set": {"resource": "rewritten"}}
        )
    res = admin.get("/api/v1/audit/verify?limit=1000")
    assert res.status_code == 200, res.text
    assert res.json()["verified"] is True, res.json()
