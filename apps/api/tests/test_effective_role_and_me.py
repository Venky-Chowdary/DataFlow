"""The API states the authority it will enforce, and states it per workspace.

Two authorities decide what a caller may do: the platform label on the account
and the membership role inside the workspace being addressed. The request gate
used to read only the first, and ``member`` is not one of its labels, so an
account the Team UI created as a workspace **editor** was gated as a viewer with
no way to find out why. These tests pin the resolution and the identity endpoint
the client gates its own controls on.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def team_env(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "team.json"))
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("DATAFLOW_MONGO_URI", raising=False)
    import services.team_store as team_store

    return team_store


def _workspace_with_member(team_store, *, email: str, role: str) -> str:
    ws = team_store.create_workspace(name=f"ws-{role}", created_by="admin@example.com")
    team_store.add_workspace_member(
        workspace_id=ws.id,
        email=email,
        role=role,
        added_by="admin@example.com",
        actor_is_platform_admin=True,
    )
    return ws.id


def test_platform_member_who_is_a_workspace_editor_resolves_to_editor(team_env):
    from services.effective_role import effective_permissions, resolve_effective_role
    from services.rbac import Permission

    email = "editor@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="editor")
    user = {"email": email, "role": "member"}

    assert resolve_effective_role(user, ws_id) == "editor"
    assert Permission.CONNECTOR_WRITE in effective_permissions(user, ws_id)
    # Editing inside a workspace is not administering the deployment.
    assert Permission.WORKSPACE_MANAGE not in effective_permissions(user, ws_id)


def test_workspace_viewer_keeps_read_only_authority(team_env):
    from services.effective_role import effective_permissions, resolve_effective_role
    from services.rbac import Permission

    email = "viewer@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="viewer")
    user = {"email": email, "role": "member"}

    assert resolve_effective_role(user, ws_id) == "viewer"
    granted = effective_permissions(user, ws_id)
    assert Permission.CONNECTOR_READ in granted
    assert Permission.CONNECTOR_WRITE not in granted
    assert Permission.JOB_RUN not in granted
    assert Permission.SCHEDULE_MANAGE not in granted


def test_non_member_of_the_addressed_workspace_fails_closed(team_env):
    from services.effective_role import resolve_effective_role

    ws_id = _workspace_with_member(team_env, email="editor@example.com", role="editor")
    outsider = {"email": "stranger@example.com", "role": "member"}
    assert resolve_effective_role(outsider, ws_id) == "viewer"


def test_platform_admin_is_admin_without_a_membership_row(team_env):
    from services.effective_role import resolve_effective_role

    ws_id = _workspace_with_member(team_env, email="editor@example.com", role="editor")
    admin = {"email": "admin@example.com", "role": "admin"}
    assert resolve_effective_role(admin, ws_id) == "admin"
    assert resolve_effective_role(admin, "") == "admin"


def test_membership_never_demotes_a_more_trusted_platform_role(team_env):
    """A workspace viewer row cannot strip an operator of what the platform gave."""
    from services.effective_role import resolve_effective_role

    email = "operator@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="viewer")
    assert resolve_effective_role({"email": email, "role": "operator"}, ws_id) == "operator"


def test_single_membership_answers_a_request_that_names_no_workspace(team_env):
    """The web client sends no workspace header until one is chosen."""
    from services.effective_role import resolve_effective_role, workspace_choice_is_ambiguous

    email = "editor@example.com"
    _workspace_with_member(team_env, email=email, role="editor")
    user = {"email": email, "role": "member"}

    assert resolve_effective_role(user, "") == "editor"
    assert workspace_choice_is_ambiguous(user, "") is False


def test_several_memberships_are_never_guessed_between(team_env):
    from services.effective_role import resolve_effective_role, workspace_choice_is_ambiguous

    email = "mixed@example.com"
    _workspace_with_member(team_env, email=email, role="editor")
    viewer_ws = _workspace_with_member(team_env, email=email, role="viewer")
    user = {"email": email, "role": "member"}

    # Naming no workspace must not lend the editor row's authority elsewhere.
    assert resolve_effective_role(user, "") == "viewer"
    assert workspace_choice_is_ambiguous(user, "") is True
    # Naming one answers with that workspace's row.
    assert resolve_effective_role(user, viewer_ws) == "viewer"


def test_resolved_workspace_names_the_single_membership_that_answered(team_env):
    """So the client can name that workspace on every later request."""
    from services.effective_role import resolved_workspace_id

    email = "editor@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="editor")
    user = {"email": email, "role": "member"}

    assert resolved_workspace_id(user, "") == ws_id
    assert resolved_workspace_id(user, ws_id) == ws_id
    # A second membership makes the choice ambiguous, so nothing is named.
    _workspace_with_member(team_env, email=email, role="viewer")
    assert resolved_workspace_id(user, "") == ""


def test_permission_summary_answers_the_questions_the_ui_asks(team_env):
    from services.effective_role import permission_summary

    email = "editor@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="editor")
    summary = permission_summary({"email": email, "role": "member"}, ws_id)

    assert summary["effective_role"] == "editor"
    assert summary["can_write_connectors"] is True
    assert summary["can_run_jobs"] is True
    assert summary["can_manage_schedules"] is True
    assert summary["can_manage_workspace"] is False


def test_settings_read_is_not_workspace_administration():
    """A viewer may read the workspace's own name; only writing it needs admin."""
    from services.rbac import Permission, _required_permission, role_permissions

    assert _required_permission("GET", "/api/v1/workspace/settings") == Permission.WORKSPACE_READ
    assert _required_permission("PATCH", "/api/v1/workspace/settings") == Permission.WORKSPACE_MANAGE
    assert Permission.WORKSPACE_READ in role_permissions("viewer")
    # Secret-bearing reads, and the engine choice, stay behind administration.
    assert _required_permission("GET", "/api/v1/workspace/sso") == Permission.WORKSPACE_MANAGE
    assert _required_permission("GET", "/api/v1/workspace/api-keys") == Permission.WORKSPACE_MANAGE
    assert _required_permission("GET", "/api/v1/workspace/pilot-engine") == Permission.WORKSPACE_MANAGE


def test_listing_schedules_is_a_read_but_changing_one_is_not():
    """A viewer sees the fleet; only deciding it needs schedule.manage.

    Gating the list with ``schedule.manage`` refused the viewer's own Schedules
    page, and the client drew that refusal as "No schedules yet" — schedules
    that exist were reported as absent.
    """
    from services.rbac import Permission, _required_permission, role_permissions

    assert _required_permission("GET", "/api/v1/schedules/") == Permission.SCHEDULE_READ
    assert _required_permission("POST", "/api/v1/schedules/") == Permission.SCHEDULE_MANAGE
    assert _required_permission("DELETE", "/api/v1/schedules/abc") == Permission.SCHEDULE_MANAGE
    assert Permission.SCHEDULE_READ in role_permissions("viewer")
    assert Permission.SCHEDULE_MANAGE not in role_permissions("viewer")


def test_a_parallel_run_check_is_a_run_not_a_connector_write():
    """Comparing source against destination is judged as running that pipeline.

    Without an explicit rule the POST fell through to the mutation default
    (``connector.write``), so the operator whose role is to run and reconcile
    pipelines was refused, and a viewer's drawer control stayed enabled and
    fired a doomed request.
    """
    from services.rbac import Permission, _required_permission, role_permissions

    assert _required_permission("POST", "/api/v1/fidelity/check") == Permission.JOB_RUN
    assert Permission.JOB_RUN in role_permissions("operator")
    assert Permission.JOB_RUN in role_permissions("editor")
    assert Permission.JOB_RUN not in role_permissions("viewer")


def test_refusal_names_the_permission_and_the_role_it_resolved():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.services.rbac import RBACMiddleware

    app = FastAPI()
    app.add_middleware(RBACMiddleware)

    @app.post("/api/v1/transfer/run")
    def run():  # pragma: no cover - refused before it runs
        return {"ok": True}

    client = TestClient(app)
    # No identity attached: the gate refuses and says what was needed.
    body = client.post("/api/v1/transfer/run").json()
    if "detail" in body and "Permission denied" in str(body.get("detail")):
        assert body["required_permission"] == "job.run"
        assert body["effective_role"] == "viewer"


def test_auth_me_decides_authority_in_the_workspace_it_reports(team_env, monkeypatch):
    """The reported workspace and the reported permissions are one answer.

    ``/auth/me`` names the workspace a header-less request resolved to. Reading
    the permissions in a different workspace than the one reported would let the
    client gate on authority it does not hold there.
    """
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-me-ws")
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)

    email = "editor@example.com"
    ws_id = _workspace_with_member(team_env, email=email, role="editor")

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    import src.middleware.auth_middleware as auth_mw
    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router
    from src.services.rbac import RBACMiddleware

    monkeypatch.setattr(auth_mw, "lookup_user", lambda addr: {"email": addr, "role": "member"})

    app = FastAPI()
    app.include_router(auth_router, prefix="/api/v1")
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)
    client = TestClient(app)

    token = auth_mod.create_token(email)[0]
    body = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"}).json()

    assert body["workspace_id"] == ws_id
    assert body["workspace_role"] == "editor"
    assert body["effective_role"] == "editor"
    assert body["can_write_connectors"] is True
    assert body["can_manage_workspace"] is False
    assert body["workspace_choice_ambiguous"] is False


def test_auth_me_reports_identity_and_effective_authority(team_env, monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-me")
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.auth_router import router as auth_router
    from src.services.rbac import RBACMiddleware

    app = FastAPI()
    app.include_router(auth_router, prefix="/api/v1")
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)
    client = TestClient(app)

    assert client.get("/api/v1/auth/me").status_code == 401

    token = auth_mod.create_token("admin@example.com")[0]
    body = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"}).json()
    assert body["email"] == "admin@example.com"
    assert body["effective_role"] == "admin"
    assert body["can_write_connectors"] is True
    assert body["must_change_password"] is False
    assert "job.run" in body["permissions"]

