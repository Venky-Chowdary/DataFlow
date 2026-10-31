"""Singer taps stay disabled/admin-only, but say so: typed errors on test, save, run, schedule."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.api_errors import ApiError, api_error_handler
from services.request_principal import SingerUnavailable

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture(autouse=True)
def _taps_unset(monkeypatch):
    monkeypatch.delenv("DATAWRAP_SINGER_TAPS_DIR", raising=False)
    monkeypatch.delenv("DATAFLOW_SINGER_TAPS_DIR", raising=False)


@pytest.fixture
def singer_connector(monkeypatch):
    import services.connector_store as store

    saved = SimpleNamespace(type="singer_tap", workspace_id="ws")
    monkeypatch.setattr(
        store, "get_connector", lambda cid, workspace_id=None: saved if cid == "c-singer" else None
    )


def _app(*routers) -> TestClient:
    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    for router in routers:
        app.include_router(router, prefix="/api/v1")
    return TestClient(app, raise_server_exceptions=False)


def _assert_unset(err: ApiError) -> None:
    assert err.status_code == 409
    assert err.code == "singer_taps_dir_unset"
    assert "DATAWRAP_SINGER_TAPS_DIR" in err.message


def test_unset_dir_names_the_env_var():
    from services.request_principal import singer_taps_dir

    with pytest.raises(SingerUnavailable) as exc:
        singer_taps_dir()
    _assert_unset(exc.value)
    assert isinstance(exc.value, PermissionError)


def test_dir_that_is_not_a_directory_is_the_same_code(monkeypatch, tmp_path):
    from services.request_principal import singer_taps_dir

    monkeypatch.setenv("DATAWRAP_SINGER_TAPS_DIR", str(tmp_path / "missing"))
    with pytest.raises(SingerUnavailable) as exc:
        singer_taps_dir()
    assert exc.value.code == "singer_taps_dir_unset"
    assert "not a directory" in exc.value.message


def test_non_admin_gets_admin_required_before_dir_unset(monkeypatch):
    from services import rbac

    token = rbac._REQUEST_ROLE.set("editor")
    try:
        with pytest.raises(SingerUnavailable) as exc:
            rbac.require_admin_connector_access(None, "singer_tap")
    finally:
        rbac._REQUEST_ROLE.reset(token)
    assert exc.value.status_code == 403
    assert exc.value.code == "singer_admin_required"
    assert "admin role" in exc.value.message


def test_admin_with_taps_disabled_is_told_which_setting():
    from services import rbac

    with pytest.raises(SingerUnavailable) as exc:
        rbac.require_admin_connector_access(None, "singer_tap")
    _assert_unset(exc.value)
    rbac.require_admin_connector_access(None, "postgresql")


def test_test_endpoint_returns_typed_envelope():
    from src.routers.connectors_router import router

    resp = _app(router).post(
        "/api/v1/connectors/test", json={"type": "singer_tap", "tap_command": "tap-x"}
    )
    assert resp.status_code == 409
    error = resp.json()["error"]
    assert error["code"] == "singer_taps_dir_unset"
    assert "DATAWRAP_SINGER_TAPS_DIR" in error["message"]


def test_create_connector_endpoint_returns_typed_envelope():
    from src.routers.connectors_router import router

    resp = _app(router).post(
        "/api/v1/connectors/",
        json={"name": "t", "type": "singer_tap", "host": "", "port": 0, "connection_string": "tap-x"},
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "singer_taps_dir_unset"


def test_saved_connector_save_and_update_are_typed(monkeypatch):
    from src.routers import saved_connectors_router as router

    monkeypatch.setattr(router, "resolve_write_workspace", lambda request, ws: "ws")
    monkeypatch.setattr(router, "_resolve_workspace", lambda request, ws: "ws")
    existing = SimpleNamespace(type="singer_tap", workspace_id="ws")
    monkeypatch.setattr(router, "get_connector", lambda *a, **k: existing)
    monkeypatch.setattr(router, "_writable_connector_or_404", lambda request, conn: None)
    body = router.ConnectorSaveDTO(name="t", type="singer_tap")
    with pytest.raises(SingerUnavailable) as exc:
        router.save_connector(body, SimpleNamespace(state=SimpleNamespace(user=None)), "ws")
    _assert_unset(exc.value)
    with pytest.raises(SingerUnavailable) as exc:
        router.update_saved_connector("c1", body, SimpleNamespace(state=SimpleNamespace(user=None)), "ws")
    _assert_unset(exc.value)


def test_transfer_run_over_a_singer_endpoint_is_typed(singer_connector):
    from services import rbac

    with pytest.raises(SingerUnavailable) as exc:
        rbac.require_admin_for_endpoints(None, "ws", {"connector_id": "c-singer"}, {"connector_id": "dest"})
    _assert_unset(exc.value)


def test_schedule_create_enable_and_run_now_are_typed_but_pause_is_not(singer_connector):
    from services.schedule_store import _assert_schedule_connectors_allowed

    data = {"workspace_id": "ws", "source_connector_id": "c-singer", "dest_connector_id": "dest"}
    with pytest.raises(SingerUnavailable) as exc:
        _assert_schedule_connectors_allowed(data, require_taps=True)
    _assert_unset(exc.value)
    _assert_schedule_connectors_allowed(data, require_taps=False)


def test_run_now_is_refused_but_the_unattended_beat_is_not_preempted(singer_connector, monkeypatch):
    import services.schedule_store as store
    from services import schedule_runner

    sched = SimpleNamespace(
        id="s1",
        enabled=True,
        workspace_id="ws",
        source_connector_id="c-singer",
        dest_connector_id="dest",
    )
    monkeypatch.setattr(store, "get_schedule", lambda _id: sched)
    with pytest.raises(SingerUnavailable) as exc:
        schedule_runner._run_schedule("s1", manual=True)
    _assert_unset(exc.value)
    reached = []

    def _stop(**_kw):
        reached.append(True)
        raise RuntimeError("past the singer gate")

    import services.schedule_approvals as approvals

    monkeypatch.setattr(approvals, "release_create_new_dest_exists_false_refuse", _stop)
    with pytest.raises(RuntimeError, match="past the singer gate"):
        schedule_runner._run_schedule("s1", manual=False)
    assert reached


def test_schedule_router_passes_typed_error_through(monkeypatch):
    from src.routers import schedules_router

    monkeypatch.setattr(schedules_router, "resolve_write_workspace", lambda request, ws: "ws")

    def _refuse(_payload, **_kw):
        from services.request_principal import singer_taps_dir

        singer_taps_dir()

    monkeypatch.setattr(schedules_router, "create_schedule", _refuse)
    resp = _app(schedules_router.router).post(
        "/api/v1/schedules/",
        json={
            "name": "s",
            "source_connector_id": "c-singer",
            "source_table": "t",
            "dest_connector_id": "d",
            "dest_table": "t",
        },
    )
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "singer_taps_dir_unset"


def test_probe_message_names_the_setting():
    from connectors.sdk import SingerTapBridge

    ok, message = SingerTapBridge({"tap_command": "tap-x"}).check()
    assert ok is False
    assert "DATAWRAP_SINGER_TAPS_DIR" in message
    assert "execution failed" not in message
