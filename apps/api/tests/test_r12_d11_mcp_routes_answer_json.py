"""QA r12 MongoDB D11: every MCP path answers JSON, never the SPA's HTML.

QA (production 8f7d246d): the Datawrap MCP tools (list_connector_objects,
introspect, rank) failed in the MCP client with "Unexpected token '<'
<!DOCTYPE". MCP clients are given the *web* origin (``<origin>/api/v1/mcp``,
the snippet the app shows). The web nginx proxies ``/api/`` to the API, but:

* MCP OAuth discovery (RFC 9728 ``/.well-known/oauth-protected-resource...``)
  is served by the API at its root, and every other discovery document an MCP
  client probes after a 401 (``/.well-known/oauth-authorization-server``,
  ``openid-configuration``) fell to the SPA catch-all, which answers
  ``200 index.html``. The client parses it as JSON and fails.
* ``/api`` without a trailing path also got the SPA shell, and nginx's own
  502/504 (API down) was an HTML page.
* A trailing slash on the legacy bridge (``/mcp/tools/``, ``/mcp/tools/call/``)
  was a 307 to the API's own origin, out of the same-origin proxy.

Two layers are tested: the API's routes (TestClient) and the real nginx with
``deploy/nginx.conf.template`` rendered as ``deploy/start-nginx.sh`` renders
it, in front of the real API app (uvicorn). The nginx leg needs an nginx
binary (``DATAWRAP_TEST_NGINX_BIN`` or ``nginx`` on PATH) and skips without
one.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess  # nosec B404 - runs the local nginx binary under test
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

REPO = Path(__file__).resolve().parents[3]
TEMPLATE = REPO / "deploy" / "nginx.conf.template"
COMPOSE_CONF = REPO / "deploy" / "nginx.conf"

_LIST = {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
_MCP_ACCEPT = {"Accept": "application/json, text/event-stream"}

#: (method, path, json body) — every MCP path a client may dial.
_MCP_CALLS = [
    ("POST", "/api/v1/mcp", _LIST),
    ("POST", "/api/v1/mcp/", _LIST),
    ("GET", "/api/v1/mcp/manifest", None),
    ("GET", "/api/v1/mcp/manifest/", None),
    ("GET", "/api/v1/mcp/tools", None),
    ("GET", "/api/v1/mcp/tools/", None),
    ("POST", "/api/v1/mcp/tools/call", {"name": "list_connectors", "arguments": {}}),
    ("POST", "/api/v1/mcp/tools/call/", {"name": "list_connectors", "arguments": {}}),
    ("GET", "/api/v1/mcp/status", None),
    ("GET", "/api/v1/mcp/status/", None),
    ("GET", "/api/v1/mcp/logs/", None),
    ("GET", "/api/v1/mcp/policy/", None),
]

#: Paths that must be a JSON 404 — never index.html.
_JSON_404 = [
    "/api/v1/does-not-exist",
    "/api/v1/mcp/does-not-exist",
    "/api/v1/connectors/nope/nope/nope",
]


def _is_json(content_type: str | None) -> bool:
    return (content_type or "").split(";")[0].strip() == "application/json"


@pytest.fixture
def api_client(monkeypatch):
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as client:
        yield client


@pytest.mark.parametrize("method,path,body", _MCP_CALLS)
def test_api_mcp_routes_answer_json_without_redirect(api_client, method, path, body):
    kwargs = {"headers": _MCP_ACCEPT} if body is _LIST else {}
    if body is not None:
        kwargs["json"] = body
    resp = api_client.request(method, path, follow_redirects=False, **kwargs)
    assert resp.status_code == 200, (resp.status_code, resp.headers.get("location"), resp.text[:200])
    assert _is_json(resp.headers.get("content-type")), resp.headers.get("content-type")
    resp.json()


@pytest.mark.parametrize("path", _JSON_404)
def test_api_unknown_path_is_json_404(api_client, path):
    resp = api_client.get(path, follow_redirects=False)
    assert resp.status_code == 404
    assert _is_json(resp.headers.get("content-type"))
    assert "<!doctype" not in resp.text.lower()


def test_api_serves_mcp_oauth_discovery_as_json(api_client):
    for path in (
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/api/v1/mcp",
    ):
        resp = api_client.get(path)
        assert _is_json(resp.headers.get("content-type")), (path, resp.text[:200])


# ---------------------------------------------------------------------------
# The real nginx, rendered from the deploy template, in front of the real API.
# ---------------------------------------------------------------------------


def _nginx_bin() -> str:
    explicit = os.environ.get("DATAWRAP_TEST_NGINX_BIN") or ""
    found = explicit if explicit and os.access(explicit, os.X_OK) else shutil.which("nginx")
    if not found:
        pytest.skip("nginx binary not available (set DATAWRAP_TEST_NGINX_BIN)")
    return found


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _start_nginx_values(api_base: str) -> dict[str, str]:
    """Run deploy/start-nginx.sh's own URL normalisation on ``api_base``."""
    script = (REPO / "deploy" / "start-nginx.sh").read_text(encoding="utf-8")
    # Everything up to the template rendering: env -> API_* values.
    head = script.split("# Browser always uses same-origin", 1)[0]
    probe = head + '\nprintf "%s\\n%s\\n%s\\n" "$API_PROXY_PASS" "$API_PROXY_HOST" "$API_ORIGIN"\n'
    out = subprocess.run(  # nosec B603 B607 - local sh with a fixed script
        ["sh", "-c", probe],
        env={"PATH": os.environ.get("PATH", ""), "DATAWRAP_API_BASE": api_base},
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    return {"API_PROXY_PASS": out[0], "API_PROXY_HOST": out[1], "API_ORIGIN": out[2]}


def _render(template: Path, port: int, values: dict[str, str], html_root: Path) -> str:
    text = template.read_text(encoding="utf-8")
    for key, value in {"PORT": str(port), **values}.items():
        text = text.replace("${" + key + "}", value)
    return text.replace("root /usr/share/nginx/html;", f"root {html_root};")


def _main_conf(tmp: Path, server_conf: Path) -> Path:
    for sub in ("client", "proxy", "fastcgi", "uwsgi", "scgi"):
        (tmp / sub).mkdir(exist_ok=True)
    conf = tmp / "nginx.conf"
    conf.write_text(
        f"""
daemon off;
master_process off;
pid {tmp}/nginx.pid;
error_log {tmp}/error.log warn;
events {{ worker_connections 64; }}
http {{
    types {{ text/html html; application/javascript js; text/css css; application/json json; }}
    default_type application/octet-stream;
    access_log off;
    client_body_temp_path {tmp}/client;
    proxy_temp_path {tmp}/proxy;
    fastcgi_temp_path {tmp}/fastcgi;
    uwsgi_temp_path {tmp}/uwsgi;
    scgi_temp_path {tmp}/scgi;
    include {server_conf};
}}
""",
        encoding="utf-8",
    )
    return conf


def _wait_port(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(f"port {port} never opened")


@pytest.fixture(scope="module")
def real_api():
    """The real FastAPI app on a local port (the nginx upstream)."""
    pytest.importorskip("uvicorn")
    import uvicorn

    mp = pytest.MonkeyPatch()
    mp.setattr("src.services.auth_service.auth_required", lambda: False)
    from src.main import app

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        _wait_port(port, timeout=60)
        yield port
    finally:
        server.should_exit = True
        thread.join(timeout=15)
        mp.undo()


def _run_nginx(tmp: Path, template: Path, api_base: str):
    nginx = _nginx_bin()
    html = tmp / "html"
    html.mkdir()
    (html / "index.html").write_text(
        "<!DOCTYPE html><html><head></head><body>spa</body></html>", encoding="utf-8"
    )
    port = _free_port()
    server_conf = tmp / "default.conf"
    server_conf.write_text(_render(template, port, _start_nginx_values(api_base), html), encoding="utf-8")
    conf = _main_conf(tmp, server_conf)
    check = subprocess.run(  # nosec B603 - local nginx -t
        [nginx, "-t", "-c", str(conf), "-p", str(tmp)], capture_output=True, text=True
    )
    assert check.returncode == 0, check.stderr
    proc = subprocess.Popen(  # nosec B603 - local nginx under test
        [nginx, "-c", str(conf), "-p", str(tmp)], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    _wait_port(port)
    return proc, port


def _fetch(port: int, method: str, path: str, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )

    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):  # noqa: D401
            return None

    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(req, timeout=60) as resp:  # nosec B310 - local loopback
            return resp.status, resp.headers.get("Content-Type"), resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.headers.get("Content-Type"), err.read().decode()


@pytest.fixture
def web_nginx(tmp_path, real_api):
    proc, port = _run_nginx(tmp_path, TEMPLATE, f"http://127.0.0.1:{real_api}/api/v1")
    try:
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_nginx_template_routes_every_mcp_path_to_json(web_nginx):
    failures = []
    for method, path, body in _MCP_CALLS:
        headers = _MCP_ACCEPT if body is _LIST else None
        status, ctype, text = _fetch(web_nginx, method, path, body, headers)
        if status != 200 or not _is_json(ctype) or text.lstrip().startswith("<"):
            failures.append((method, path, status, ctype, text[:80]))
    assert not failures, failures


def test_nginx_template_never_serves_the_spa_for_api_or_discovery(web_nginx):
    failures = []
    for path in [
        *_JSON_404,
        "/api",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/api/v1/mcp",
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-authorization-server/api/v1/mcp",
        "/.well-known/openid-configuration",
    ]:
        status, ctype, text = _fetch(web_nginx, "GET", path)
        if status == 200 or not _is_json(ctype) or "<!doctype" in text.lower():
            failures.append((path, status, ctype, text[:80]))
    assert not failures, failures
    # The SPA itself still serves its client-side routes.
    status, ctype, text = _fetch(web_nginx, "GET", "/jobs/123")
    assert status == 200 and "spa" in text


def test_nginx_template_answers_json_when_the_api_is_down(tmp_path):
    dead = _free_port()
    proc, port = _run_nginx(tmp_path, TEMPLATE, f"http://127.0.0.1:{dead}/api/v1")
    try:
        for path in ("/api/v1/mcp", "/.well-known/oauth-protected-resource"):
            status, ctype, text = _fetch(port, "POST" if path.endswith("mcp") else "GET", path, _LIST)
            assert status == 502, (path, status)
            assert _is_json(ctype), (path, ctype, text[:120])
            json.loads(text)
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_compose_nginx_conf_has_the_same_json_guards():
    """deploy/nginx.conf (docker compose) carries the same locations."""
    text = COMPOSE_CONF.read_text(encoding="utf-8")
    for needle in (
        "location ^~ /.well-known/oauth-protected-resource",
        "location ^~ /.well-known/",
        "location = /api",
        "@api_unavailable",
    ):
        assert needle in text, needle
