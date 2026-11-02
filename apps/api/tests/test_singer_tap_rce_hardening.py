"""Regression tests: singer_tap must not run attacker-chosen commands."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sdk import SingerTapBridge
from connectors.sdk import test_singer_tap as singer_probe

_TAP_SCRIPT = """#!{python}
import json, sys
if "--check" in sys.argv:
    sys.exit(0)
print(json.dumps({{"type": "SCHEMA", "stream": "s", "key_properties": ["id"],
                  "schema": {{"properties": {{"id": {{"type": "integer"}}}}}}}}))
print(json.dumps({{"type": "RECORD", "stream": "s", "record": {{"id": 1}}}}))
"""


def _write_exec(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(0o755)
    return path


@pytest.fixture
def marker(tmp_path: Path) -> Path:
    return tmp_path / "pwn"


@pytest.fixture
def taps_dir(tmp_path: Path, monkeypatch) -> Path:
    d = tmp_path / "taps"
    d.mkdir()
    _write_exec(d / "tap-demo", _TAP_SCRIPT.format(python=sys.executable))
    monkeypatch.setenv("DATAWRAP_SINGER_TAPS_DIR", str(d))
    monkeypatch.delenv("DATAFLOW_SINGER_TAPS_DIR", raising=False)
    return d


def _payloads(marker: Path) -> list:
    py = f"open('{marker}','w')"
    return [
        f"python3 -c {py}",
        ["python3", "-c", py],
        [sys.executable, "-c", py],
        ["bash", "-c", f"touch {marker}"],
        ["sh", "-c", f"touch {marker}"],
        ["/usr/bin/env", "python3", "-c", py],
    ]


def test_probe_rejects_connection_string_fallback(taps_dir, marker):
    ok, msg = singer_probe(connection_string=f"python3 -c open('{marker}','w')")
    assert ok is False
    assert "connection_string" in msg
    assert not marker.exists()


def test_run_probe_rejects_connection_string_fallback(taps_dir, marker):
    from src.transfer.connector_registry import run_probe

    result = run_probe("singer_tap", {"connection_string": f"python3 -c open('{marker}','w')"})
    ok = result[0] if isinstance(result, tuple) else result.get("success")
    assert not ok
    assert not marker.exists()


@pytest.mark.parametrize("idx", range(6))
def test_interpreter_tap_command_is_not_executed(taps_dir, marker, idx):
    cmd = _payloads(marker)[idx]
    bridge = SingerTapBridge({"tap_command": cmd})
    with pytest.raises(PermissionError):
        bridge._argv()
    ok, _msg = bridge.check()
    assert ok is False
    ok, _msg = singer_probe(tap_command=cmd)
    assert ok is False
    with pytest.raises(PermissionError):
        list(bridge.read("s"))
    assert not marker.exists()


def test_tap_named_executable_outside_taps_dir_is_rejected(taps_dir, tmp_path, marker):
    outside = tmp_path / "outside"
    outside.mkdir()
    evil = _write_exec(outside / "tap-evil", f"#!/bin/sh\ntouch {marker}\n")
    for cmd in ([str(evil)], ["../outside/tap-evil"], ["bin/../../outside/tap-evil"]):
        bridge = SingerTapBridge({"tap_command": cmd})
        with pytest.raises(PermissionError):
            bridge._argv()
        assert bridge.check()[0] is False
    assert not marker.exists()


def test_non_tap_file_inside_taps_dir_is_rejected(taps_dir, marker):
    _write_exec(taps_dir / "python3", f"#!/bin/sh\ntouch {marker}\n")
    bridge = SingerTapBridge({"tap_command": ["python3"]})
    with pytest.raises(PermissionError):
        bridge._argv()
    assert not marker.exists()


def test_singer_disabled_without_taps_dir(monkeypatch):
    monkeypatch.delenv("DATAWRAP_SINGER_TAPS_DIR", raising=False)
    monkeypatch.delenv("DATAFLOW_SINGER_TAPS_DIR", raising=False)
    ok, msg = SingerTapBridge({"tap_command": "tap-demo"}).check()
    assert ok is False
    assert "disabled" in msg


def test_allowlisted_tap_runs(taps_dir):
    bridge = SingerTapBridge({"tap_command": "tap-demo --flag"})
    argv = bridge._argv("--discover")
    assert argv[0] == str((taps_dir / "tap-demo").resolve())
    assert argv[1:] == ["--flag", "--discover"]
    assert bridge.check() == (True, "Singer tap --check OK")
    batches = list(bridge.read("s"))
    assert batches and batches[0].records == [{"id": 1}]


def test_missing_tap_reports_not_found(taps_dir):
    ok, msg = SingerTapBridge({"tap_command": "tap-missing"}).check()
    assert ok is False
    assert "not found" in msg.lower()


@pytest.mark.parametrize("role,allowed", [("viewer", False), ("editor", False), ("operator", False), ("admin", True)])
def test_bound_principal_role_gates_execution(taps_dir, role, allowed):
    from services import rbac

    token = rbac._REQUEST_ROLE.set(role)
    try:
        bridge = SingerTapBridge({"tap_command": "tap-demo"})
        if allowed:
            assert bridge._argv()[0].endswith("tap-demo")
        else:
            with pytest.raises(PermissionError):
                bridge._argv()
            assert bridge.check()[0] is False
    finally:
        rbac._REQUEST_ROLE.reset(token)


@pytest.fixture
def auth_on(monkeypatch):
    from services import rbac
    from src.services import auth_service

    monkeypatch.setattr(auth_service, "auth_required", lambda: True)
    return rbac


def test_require_admin_connector_access(auth_on, taps_dir):
    from services.api_errors import ApiError

    class _Req:
        def __init__(self, role):
            self.state = type("S", (), {"user": {"role": role} if role else None})()

    for role in (None, "viewer", "editor", "operator"):
        with pytest.raises(ApiError) as exc:
            auth_on.require_admin_connector_access(_Req(role), "singer_tap")
        assert exc.value.status_code == 403
        auth_on.require_admin_connector_access(_Req(role), "postgresql")
    auth_on.require_admin_connector_access(_Req("admin"), "singer_tap")


def test_endpoint_types_cover_saved_connector_and_extras(auth_on, monkeypatch):
    import services.connector_store as store
    from src.transfer.models import EndpointConfig

    saved = type("C", (), {"type": "singer_tap"})()
    monkeypatch.setattr(store, "get_connector", lambda cid, workspace_id=None: saved if cid == "c-singer" else None)

    types = auth_on.endpoint_connector_types(
        "ws",
        EndpointConfig(kind="database", format="postgresql", connector_id="c-singer"),
        EndpointConfig(kind="database", format="postgresql", extra={"tap_command": "tap-x"}),
        {"connector_id": "c-other"},
    )
    assert types.count("singer_tap") == 2

    token = auth_on._REQUEST_ROLE.set("editor")
    try:
        with pytest.raises(PermissionError):
            auth_on.assert_bound_principal_may_use_endpoints("ws", {"connector_id": "c-singer"})
        auth_on.assert_bound_principal_may_use_endpoints("ws", {"connector_id": "c-other"})
    finally:
        auth_on._REQUEST_ROLE.reset(token)
    # Scheduler / worker threads have no bound principal and are not blocked.
    auth_on.assert_bound_principal_may_use_endpoints("ws", {"connector_id": "c-singer"})


def _client(role: str):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.middleware.base import BaseHTTPMiddleware

    from services.api_errors import ApiError, api_error_handler
    from services.rbac import RBACMiddleware
    from src.routers.connectors_router import router as connectors_router

    class _User(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            request.state.user = {"email": f"{role}@example.com", "role": role}
            request.state.user_email = f"{role}@example.com"
            return await call_next(request)

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(connectors_router, prefix="/api/v1")
    app.add_middleware(RBACMiddleware)
    app.add_middleware(_User)
    return TestClient(app)


@pytest.mark.parametrize("role", ["viewer", "editor", "operator"])
def test_connector_test_endpoint_denies_non_admin(auth_on, taps_dir, marker, role):
    body = {"type": "singer_tap", "connection_string": f"python3 -c open('{marker}','w')"}
    resp = _client(role).post("/api/v1/connectors/test", json=body)
    assert resp.status_code == 403
    assert not marker.exists()


def test_connector_test_endpoint_admin_cannot_smuggle_command(auth_on, taps_dir, marker):
    client = _client("admin")
    for body in (
        {"type": "singer_tap", "connection_string": f"python3 -c open('{marker}','w')"},
        {"type": "singer_tap", "tap_command": ["python3", "-c", f"open('{marker}','w')"]},
    ):
        resp = client.post("/api/v1/connectors/test", json=body)
        assert resp.status_code != 500 or "success" not in resp.json()
        if resp.status_code == 200:
            assert resp.json().get("success") is not True
    assert not marker.exists()


def test_connector_create_denies_non_admin_singer(auth_on):
    resp = _client("editor").post(
        "/api/v1/connectors/",
        json={"name": "t", "type": "singer_tap", "host": "", "port": 0, "connection_string": "tap-demo"},
    )
    assert resp.status_code == 403


def test_workspace_member_account_is_not_a_platform_admin(auth_on):
    """A tap runs on the host every workspace shares; a workspace admin is not enough."""
    from services.api_errors import ApiError

    class _Req:
        state = type("S", (), {"user": {"email": "m@example.com", "role": "member"}})()

    with pytest.raises(ApiError) as exc:
        auth_on.require_admin_connector_access(_Req(), "singer_tap")
    assert exc.value.status_code == 403


def test_middleware_binds_principal_for_in_request_sinks(auth_on, taps_dir):
    """The sink refuses a non-admin even where no router gate ran (probe, preflight)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from starlette.middleware.base import BaseHTTPMiddleware

    from services.rbac import RBACMiddleware

    app = FastAPI()

    seen = {}

    @app.get("/api/v1/health")
    def _list():
        try:
            SingerTapBridge({"tap_command": "tap-demo"})._argv()
            seen["ran"] = True
        except PermissionError:
            seen["ran"] = False
        return {}

    def _client(role):
        class _User(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                request.state.user = {"email": f"{role}@example.com", "role": role}
                return await call_next(request)

        a = FastAPI()
        a.router.routes.extend(app.router.routes)
        a.add_middleware(RBACMiddleware)
        a.add_middleware(_User)
        return TestClient(a)

    _client("editor").get("/api/v1/health")
    assert seen.get("ran") is False
    _client("admin").get("/api/v1/health")
    assert seen.get("ran") is True
