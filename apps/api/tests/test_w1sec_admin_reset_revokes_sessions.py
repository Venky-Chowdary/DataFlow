"""SEC-6: an administrator's password reset ends the user's existing sessions.

``POST /team/users/{email}/reset-password`` left the user's bearer token
working (``GET /auth/me`` 200) while self-service change-password and the
e-mailed reset link revoke them.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.tenancy_world import EDITOR_A, tenancy_world  # noqa: F401


def test_admin_reset_revokes_the_users_sessions(tenancy_world):  # noqa: F811
    user = tenancy_world.a
    assert user.get("/api/v1/auth/me").status_code == 200
    r = tenancy_world.admin.post(f"/api/v1/team/users/{EDITOR_A}/reset-password")
    assert r.status_code == 200, r.text
    assert r.json().get("sessions_revoked", 0) >= 1, r.text
    assert user.get("/api/v1/auth/me").status_code == 401
    # The temporary password still signs in (and must then be changed).
    fresh = TestClient(tenancy_world.app).post(
        "/api/v1/auth/login", json={"email": EDITOR_A, "password": r.json()["temporary_password"]})
    assert fresh.status_code == 200, fresh.text


def test_admin_reset_is_audited_with_the_revocation(tenancy_world):  # noqa: F811
    from services import audit_log

    tenancy_world.admin.post(f"/api/v1/team/users/{EDITOR_A}/reset-password")
    events = [e for e in audit_log.list_audit_events(limit=200) if e.get("action") == "team.user.password_reset"]
    assert events and "sessions_revoked" in (events[0].get("details") or {}), events[:1]
