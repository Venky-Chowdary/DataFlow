"""Security follow-up 2: ``ConnectorAccessMiddleware`` is registered, so HTTP requests bind their caller.

``services.connector_access.current_caller()`` was ``None`` on every HTTP
request because the middleware was never added to the app. Code that asks
"who is resolving this connector" therefore saw no caller: the platform-admin
branch of ``export_paths._unbound_may_open_flat_folder`` never fired under
auth, and Pilot tools' ``caller_workspace_id()`` was always ``""``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from tenancy_world import ADMIN_EMAIL, EDITOR_A, tenancy_world  # noqa: F401


@pytest.fixture
def world(tenancy_world, monkeypatch, tmp_path_factory):  # noqa: F811
    # Exports outside the service data directory, so only the export-area rule decides.
    exports = tmp_path_factory.mktemp("secfu2_exports")
    monkeypatch.setenv("DATAFLOW_EXPORTS_DIR", str(exports))
    tenancy_world.exports = exports
    return tenancy_world


def _capture_caller(monkeypatch) -> list:
    from services.connector_access import current_caller
    from src.routers import saved_connectors_router

    seen: list = []

    def fake_list(*_args, **_kwargs):
        seen.append(current_caller())
        return []

    monkeypatch.setattr(saved_connectors_router, "list_connectors", fake_list)
    return seen


def test_a_member_request_reaches_the_caller_layer_with_its_caller_bound(world, monkeypatch):
    seen = _capture_caller(monkeypatch)
    response = world.a.get("/api/v1/connectors/saved")
    assert response.status_code == 200, response.text
    assert len(seen) == 1
    caller = seen[0]
    assert caller is not None, "no connector caller bound on an authenticated HTTP request"
    assert caller.actor == EDITOR_A
    assert caller.requested_workspace_id == world.workspace_a
    assert caller.platform_admin is False
    assert caller.authorized_workspace_id() == world.workspace_a


def test_a_platform_admin_request_binds_a_platform_admin_caller(world, monkeypatch):
    seen = _capture_caller(monkeypatch)
    response = world.admin.get("/api/v1/connectors/saved")
    assert response.status_code == 200, response.text
    assert seen and seen[0] is not None
    assert seen[0].actor == ADMIN_EMAIL and seen[0].platform_admin is True


def test_nothing_stays_bound_after_the_request(world, monkeypatch):
    from services.connector_access import current_caller

    _capture_caller(monkeypatch)
    assert world.a.get("/api/v1/connectors/saved").status_code == 200
    assert current_caller() is None


def test_the_caller_binding_only_narrows_what_a_member_sees(world):
    """A member still lists and opens its own workspace's saved connector (no new 403)."""
    made = world.a.post(
        "/api/v1/connectors/saved",
        json={"name": "secfu2-own", "type": "postgresql", "host": "127.0.0.1", "port": 5432, "database": "d"},
    )
    assert made.status_code in (200, 201), made.text
    cid = (made.json().get("connector") or made.json()).get("id")
    listed = world.a.get("/api/v1/connectors/saved")
    assert listed.status_code == 200 and cid in listed.text
    assert world.a.get(f"/api/v1/connectors/saved/{cid}").status_code == 200
    assert world.b.get(f"/api/v1/connectors/saved/{cid}").status_code in (403, 404)


def _flat_export_db(world) -> Path:
    path = Path(world.exports) / "flat_export.sqlite"
    conn = sqlite3.connect(path)
    conn.execute("create table flat_rows(v text)")
    conn.commit()
    conn.close()
    return path


def _test_inline(client, path: Path):
    return client.post("/api/v1/connectors/test", json={"type": "sqlite", "database": str(path)})


def test_platform_admin_opens_the_flat_export_folder_over_http(world):
    """The designed platform-admin branch: nothing bound, caller is a platform admin."""
    path = _flat_export_db(world)
    response = _test_inline(world.admin, path)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body.get("success") is True, body
    assert "1 tables" in body.get("message", "")


def test_a_workspace_member_still_cannot_open_the_flat_export_folder(world):
    path = _flat_export_db(world)
    response = _test_inline(world.a, path)
    body = response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
    assert response.status_code != 200 or body.get("success") is False, response.text
    assert "flat_rows" not in response.text
