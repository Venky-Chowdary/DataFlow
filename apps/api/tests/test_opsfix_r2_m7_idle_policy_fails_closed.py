"""Ops review round 2, Medium 7: the workspace idle timeout must not fail open.

``session_policy.idle_timeout_seconds`` swallowed every error and answered
"no policy", so while the tenant store was unreadable a session idle for
hours past a 1 h workspace policy was allowed, and the auth middleware then
touched the session, restarting its idle clock. A control-plane outage now
propagates: the request is answered 503 and the session is not touched.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from services import auth_sessions, session_policy, team_store, tenant_store
from services.control_plane_store import ControlPlaneUnavailable
from services.shared_record_store import SharedRecordSet

IDLE_3H = int(time.time()) - 3 * 3600


@pytest.fixture
def strict_workspace(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONTROL_STORE_BACKEND", "file")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(team_store, "list_memberships_for_user", lambda _email: [{"workspace_id": "ws-strict"}])
    tenant_store.create_tenant("ws-strict", "Strict", session_timeout_hours=1, enforce_session_timeout=True)


def _store_down(monkeypatch) -> None:
    def down(*_args, **_kwargs):
        raise ControlPlaneUnavailable("the control-plane database is unavailable")

    monkeypatch.setattr(SharedRecordSet, "load", down)


def test_the_policy_holds_while_the_store_is_up(strict_workspace):
    assert session_policy.enforce_idle(email="a@x.io", last_seen_at=IDLE_3H) is False


def test_an_unreadable_policy_is_not_read_as_no_policy(strict_workspace, monkeypatch):
    _store_down(monkeypatch)
    with pytest.raises(ControlPlaneUnavailable):
        session_policy.enforce_idle(email="a@x.io", last_seen_at=IDLE_3H)


def test_the_middleware_answers_503_and_never_touches_the_session(strict_workspace, monkeypatch):
    from src.middleware import auth_middleware
    from src.services import auth_service

    touched: list[str] = []
    monkeypatch.setattr(auth_sessions, "session_last_seen", lambda _jti: IDLE_3H)
    monkeypatch.setattr(auth_sessions, "touch_session", lambda jti, **_kw: touched.append(jti))
    monkeypatch.setattr(auth_sessions, "revoke_session", lambda jti: touched.append(f"revoked {jti}"))
    monkeypatch.setattr(auth_service, "token_jti", lambda _token: "jti-1")
    monkeypatch.setattr(auth_middleware, "verify_token", lambda _token: "a@x.io")
    monkeypatch.setattr("services.user_store.get_user_record", lambda _email: {"email": "a@x.io", "status": "active"})
    request = SimpleNamespace(state=SimpleNamespace())

    assert auth_middleware._attach_user_reason(request, "token") == "session_idle_timeout"
    assert touched == ["revoked jti-1"]
    touched.clear()

    _store_down(monkeypatch)
    assert auth_middleware._attach_user_reason(request, "token") == "auth_store_unavailable"
    assert touched == [], "a failed policy read restarted the idle clock"
