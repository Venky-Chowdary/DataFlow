"""Review 4 B1/M1: only a *deleted* account's sessions are refused — never "no stored record".

0cbd047ab refused every session token whose email ``lookup_user`` could not
find. ``lookup_user`` knows environment accounts and stored password accounts
only, so every SSO identity admitted by policy (domain allowlist,
auto-provision) and every SCIM-provisioned user got 401 ``user_not_found`` on
every call after signing in. Now deleting a stored account writes an explicit
marker (``user_store.mark_account_deleted``) and the middleware refuses only a
session issued at or before it. Through the real app on live MongoDB.
"""

from __future__ import annotations

import uuid

import pytest

from r8_ent_harness import ADMIN_EMAIL, ent  # noqa: F401


def _session(ent, email: str, *, amr=("sso",)):
    from src.services.auth_service import create_token

    token, _ = create_token(email, amr=list(amr))
    return ent.bearer(token)


def _me(client) -> int:
    return client.get("/api/v1/auth/me").status_code


def test_sso_domain_user_with_no_stored_record_stays_signed_in(ent, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SSO_ALLOWED_DOMAINS", "ssoprobe.example")
    monkeypatch.setenv("DATAFLOW_SSO_ALLOWED_DOMAINS", "ssoprobe.example")
    from services import user_store
    from services.sso_authorization import require_sso_authorization

    email = f"u{uuid.uuid4().hex[:8]}@ssoprobe.example"
    require_sso_authorization(email)
    assert user_store.get_user(email) is None
    client = _session(ent, email)
    for _ in range(3):
        assert _me(client) == 200


def test_auto_provisioned_sso_user_stays_signed_in(ent, monkeypatch):
    monkeypatch.setenv("DATAWRAP_SSO_AUTO_PROVISION", "1")
    monkeypatch.setenv("DATAFLOW_SSO_AUTO_PROVISION", "1")
    from services.sso_authorization import require_sso_authorization

    email = f"auto-{uuid.uuid4().hex[:8]}@example.test"
    require_sso_authorization(email)
    assert _me(_session(ent, email)) == 200


def test_scim_provisioned_sso_user_stays_signed_in(ent):
    from services import scim_store, user_store

    email = f"scim-{uuid.uuid4().hex[:8]}@example.test"
    scim_store.create_user({"id": uuid.uuid4().hex, "userName": email, "active": True,
                            "emails": [{"value": email, "primary": True}]})
    assert user_store.get_user(email) is None
    assert _me(_session(ent, email)) == 200


def test_a_deleted_local_account_is_refused_and_a_recreated_one_is_not(ent):
    from services import user_store

    email = f"local-{uuid.uuid4().hex[:8]}@example.test"
    user_store.create_user(email=email, password="Local-Account-Pass-2026!", role="member")
    old = ent.login(email, "Local-Account-Pass-2026!")
    assert _me(old) == 200
    assert user_store.delete_user(email=email)
    assert _me(old) == 401
    assert user_store.account_deleted_since(email) is not None
    # The same email signs up again later: its new sessions work, the old one never does.
    import time

    time.sleep(1.1)
    user_store.create_user(email=email, password="Local-Account-Pass-2027!", role="member")
    fresh = ent.login(email, "Local-Account-Pass-2027!")
    assert _me(fresh) == 200
    assert _me(old) == 401


def test_a_deleted_sandbox_tester_is_refused(ent):
    from r14_sandbox_world import provision

    box = provision(ent)
    assert _me(box.tester) == 200
    gone = ent.admin.delete(f"/api/v1/team/sandbox-workspaces/{box.workspace_id}",
                            params={"confirm": box.created["workspace"]["name"], "reason": "marker test"})
    assert gone.status_code == 200, gone.text
    assert _me(box.tester) == 401


def test_keys_service_accounts_and_the_bootstrap_admin_are_unaffected(ent):
    ws = ent.workspace("Marker keys")
    key = ent.api_key(role="viewer", workspace_id=ws)
    assert ent.bearer(key, ws).get("/api/v1/connectors/saved").status_code == 200
    sa = ent.admin.post("/api/v1/iam/service-accounts", headers={"X-Workspace-Id": ws},
                        json={"name": "marker-sa", "role": "viewer", "scopes": ["connector.read"],
                              "expires_in": "7d"})
    assert sa.status_code == 200, sa.text
    assert ent.bearer(sa.json()["key"], ws).get("/api/v1/connectors/saved").status_code == 200
    assert _me(ent.admin) == 200
    assert _me(ent.login(ADMIN_EMAIL, "Bootstrap-Admin-2026")) == 200


@pytest.mark.parametrize("stored", [True, False])
def test_cookie_path_never_leaves_the_user_unset(stored, monkeypatch):
    """M1: a stored account unknown to lookup_user went through with user=None."""
    import types

    from src.middleware import auth_middleware as mw

    monkeypatch.setattr(mw, "lookup_user", lambda e: None)
    from services import user_store

    monkeypatch.setattr(user_store, "get_user_record",
                        lambda e: {"email": e, "status": "active"} if stored else None)
    monkeypatch.setattr(user_store, "account_deleted_since", lambda e: None)
    import services.auth_state as auth_state
    import services.session_policy as session_policy

    monkeypatch.setattr(session_policy, "enforce_idle", lambda **k: True)
    monkeypatch.setattr(auth_state, "get_auth_state",
                        lambda: types.SimpleNamespace(touch_session=lambda j: None, revoke_session=lambda j: None))
    req = types.SimpleNamespace(method="GET", headers={}, state=types.SimpleNamespace())
    reason = mw._attach_cookie_user(req, {"user_id": "c@example.test", "jti": "j", "created_at": 1})
    assert reason is None
    assert req.state.user == {"email": "c@example.test", "role": "viewer"}
