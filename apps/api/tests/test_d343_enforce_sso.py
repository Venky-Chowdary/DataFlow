"""D343: a workspace can require single sign-on for its members.

Production QA: there was no "require SSO" control; a member whose company
uses SSO could still sign in with a password, and the SSO provider config is
deployment-wide and platform-admin only. ``tenant.sso_enforced`` now refuses
the password sign-in of that workspace's members (403 ``sso_required`` with
the SSO start URL), keeps an audited break-glass for platform admins, and can
only be turned on when SSO works. Real routes on a live MongoDB
(tests/r8_ent_harness.py); the IdP is never contacted.
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ADMIN_EMAIL, ADMIN_PASSWORD, ent  # noqa: F401

OIDC_COMPLETE = {
    "issuer": "https://idp.d343.invalid",
    "client_id": "datawrap",
    "client_secret": "d343-not-a-real-secret",
    "redirect_uri": "https://app.d343.invalid/api/v1/auth/sso/oidc/callback",
}
MEMBER = "d343-member@example.com"


def _sso_on(ent) -> None:
    res = ent.admin.patch("/api/v1/workspace/sso/oidc", json={**OIDC_COMPLETE, "enabled": True})
    assert res.status_code == 200, res.text


def _member(ent, ws: str, email: str = MEMBER, role: str = "editor") -> str:
    res = ent.admin.post("/api/v1/team/users", json={
        "email": email, "platform_role": "member", "workspace_id": ws, "workspace_role": role})
    assert res.status_code == 200, res.text
    return res.json()["temporary_password"]


def _login(ent, email: str, password: str):
    return ent.anonymous().post("/api/v1/auth/login", json={"email": email, "password": password})


def _tenant(ent, ws: str, **fields) -> dict:
    res = ent.admin.post("/api/v1/workspace/tenant", json={"workspace_id": ws, "name": "D343", **fields},
                         headers={"X-Workspace-Id": ws})
    assert res.status_code == 200, res.text
    return res.json()


def test_password_sign_in_is_refused_for_members_and_break_glass_for_platform_admins(ent):
    _sso_on(ent)
    ws = ent.workspace("D343 enforced")
    other_ws = ent.workspace("D343 free")
    password = _member(ent, ws)
    outsider_pw = _member(ent, other_ws, email="d343-outsider@example.com")
    tenant = _tenant(ent, ws)
    assert _login(ent, MEMBER, password).status_code == 200  # off: password works

    on = ent.admin.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"sso_enforced": True},
                         headers={"X-Workspace-Id": ws})
    assert on.status_code == 200, on.text
    assert on.json()["sso_enforced"] is True
    assert on.json()["enforcement"]["sso"]["status"] == "enforced"

    refused = _login(ent, MEMBER, password)
    assert refused.status_code == 403, refused.text
    err = refused.json()["error"]
    assert err["code"] == "sso_required"
    assert "single sign-on" in err["message"]
    assert err["sso_start_url"] == "/api/v1/auth/sso/oidc/start"
    assert "token" not in refused.text
    # A wrong password learns nothing about the policy.
    assert _login(ent, MEMBER, "wrong-password-d343").status_code == 401
    # Members of other workspaces are not affected.
    assert _login(ent, "d343-outsider@example.com", outsider_pw).status_code == 200
    # Platform admin (a member of ws as its creator): audited break-glass.
    assert _login(ent, ADMIN_EMAIL, ADMIN_PASSWORD).status_code == 200
    events = [e for e in ent.audit() if e.get("action") == "auth.sso_enforced.break_glass"]
    assert events and events[-1]["actor"] == ADMIN_EMAIL
    assert [e for e in ent.audit() if e.get("action") == "auth.login.sso_required"]

    posture = ent.admin.get("/api/v1/workspace/security/posture", headers={"X-Workspace-Id": ws}).json()
    assert posture["sso_enforced"] is True

    off = ent.admin.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"sso_enforced": False},
                          headers={"X-Workspace-Id": ws})
    assert off.status_code == 200
    assert _login(ent, MEMBER, password).status_code == 200


def test_enforce_is_refused_while_no_sso_provider_works(ent):
    ws = ent.workspace("D343 no sso")
    tenant = _tenant(ent, ws)
    res = ent.admin.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"sso_enforced": True},
                          headers={"X-Workspace-Id": ws})
    assert res.status_code == 409, res.text
    assert "not set up" in res.json()["detail"]
    created = ent.admin.post("/api/v1/workspace/tenant",
                             json={"workspace_id": ent.workspace("D343 no sso 2"), "name": "x", "sso_enforced": True})
    assert created.status_code in (400, 409), created.text
    from services.tenant_store import get_tenant

    assert get_tenant(tenant["id"]).sso_enforced is False


def test_a_workspace_admin_turns_it_on_only_from_an_sso_session(ent):
    from src.services.auth_service import create_token

    _sso_on(ent)
    ws = ent.workspace("D343 ws admin")
    admin_email = "d343-wsadmin@example.com"
    password = _member(ent, ws, email=admin_email, role="admin")
    tenant = _tenant(ent, ws)
    pw_session = _login(ent, admin_email, password)
    assert pw_session.status_code == 200
    by_password = ent.bearer(pw_session.json()["token"], ws)
    res = by_password.patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"sso_enforced": True})
    assert res.status_code == 409, res.text
    assert "Sign in with single sign-on first" in res.json()["detail"]

    sso_token, _ = create_token(admin_email, amr=["sso"])
    res = ent.bearer(sso_token, ws).patch(f"/api/v1/workspace/tenant/{tenant['id']}", json={"sso_enforced": True})
    assert res.status_code == 200, res.text
    # Now that admin's own password sign-in is refused too; SSO sessions keep working.
    assert _login(ent, admin_email, password).status_code == 403
    assert ent.bearer(sso_token, ws).get("/api/v1/workspace/settings").status_code == 200


def test_policy_lookup_failure_refuses_password_sign_in(monkeypatch):
    from fastapi import HTTPException

    from services import sso_enforcement

    def broken(_email):
        raise RuntimeError("membership store down")

    monkeypatch.setattr(sso_enforcement, "enforcing_workspaces", broken)
    with pytest.raises(HTTPException) as exc:
        sso_enforcement.refuse_password_sign_in("someone@example.com")
    assert exc.value.status_code == 503


def test_mfa_step_cannot_finish_a_password_sign_in_under_enforcement():
    """The second step of a password sign-in is checked again (a challenge
    opened before the switch was flipped must not mint a session)."""
    import inspect

    from src.routers import mfa_router

    src = inspect.getsource(mfa_router.login_mfa)
    assert src.index("refuse_password_sign_in(") < src.index("create_token(")


# ── an accepted invitation does not open a password session ────────────────


def test_invitation_into_an_sso_workspace_opens_no_password_session(tmp_path, monkeypatch):
    import tests.test_workspace_invites as inv

    gen = inv._world(tmp_path, monkeypatch, db=None)
    client, admin, ws, _ = next(gen)
    try:
        from services import sso_enforcement, tenant_store

        monkeypatch.setattr(sso_enforcement, "ready_providers", lambda: ["oidc"])
        tenant_store.create_tenant(workspace_id=ws, name="SSO only", sso_enforced=True)
        inv._invite(client, admin, ws, "d343-invitee@example.com", role="viewer")
        token = inv._token_for("d343-invitee@example.com")
        res = inv._accept(client, token, name="Invitee", password=inv.PASSWORD)
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["account_created"] is True
        assert "token" not in body and "__Host-dw_session" not in res.headers.get("set-cookie", "")
        assert "single sign-on" in body["sso_required"]["message"]
        # And the new password cannot be used to sign in either.
        assert client.post("/api/v1/auth/login",
                           json={"email": "d343-invitee@example.com", "password": inv.PASSWORD}).status_code == 403
    finally:
        gen.close()
