"""PUT /schedules/{id} is a managed route. Admin may call it."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def rbac_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "password123")
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "test-secret-for-rbac")
    import src.services.auth_service as auth_mod

    monkeypatch.setattr(auth_mod, "_REQUIRE_AUTH", True)


def test_admin_put_is_ruled_and_viewer_is_forbidden(rbac_env):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.middleware.auth_middleware import AuthMiddleware
    from src.routers.schedules_router import router
    from src.services.auth_service import create_token
    from src.services.rbac import RBACMiddleware

    app = FastAPI()
    app.add_middleware(RBACMiddleware)
    app.add_middleware(AuthMiddleware)
    app.include_router(router, prefix="/api/v1")
    client = TestClient(app)
    token = create_token("admin@example.com")[0]
    admin = client.put(
        "/api/v1/schedules/missing-schedule",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "nightly"},
    )
    assert "No RBAC rule" not in admin.text
    assert admin.status_code != 403

    import services.rbac as rbac_mod

    original = rbac_mod.normalize_role
    rbac_mod.normalize_role = lambda role: "viewer"
    try:
        viewer = client.put(
            "/api/v1/schedules/missing-schedule",
            headers={"Authorization": f"Bearer {token}"},
            json={"name": "nightly"},
        )
    finally:
        rbac_mod.normalize_role = original
    assert viewer.status_code == 403
    assert "No RBAC rule" not in viewer.text
    assert "schedule.manage" in viewer.text
