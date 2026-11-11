"""Audit M-1: the hosted SPA is served with a CSP and frame/referrer/content-type headers.

``deploy/nginx.conf.template`` (the Railway web image) sent none of them, and
``deploy/nginx.conf`` (the compose image) had no CSP. An injected script could
run in the app's origin and read the session token, and the app could be
framed.

Two layers, like ``test_r12_d11_mcp_routes_answer_json``: the policy text
(every inline script the shell really contains is allowed by hash, nothing is
allowed by ``'unsafe-inline'`` / ``'unsafe-eval'``, the origins the app loads
are present), and the real nginx serving a shell with those headers. The nginx
leg skips without an nginx binary.

Token storage is unchanged by this change (still ``localStorage`` when
"remember" is on); the CSP is what now stands between an HTML injection and it.
"""

from __future__ import annotations

import base64
import hashlib
import re
import subprocess  # nosec B404 - stops the local nginx under test
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

REPO = Path(__file__).resolve().parents[3]
TEMPLATE = REPO / "deploy" / "nginx.conf.template"
COMPOSE_CONF = REPO / "deploy" / "nginx.conf"
INDEX_HTML = REPO / "apps" / "web" / "index.html"
START_SCRIPT = REPO / "deploy" / "start-nginx.sh"
WEB_SRC = REPO / "apps" / "web" / "src"

CONFIGS = [TEMPLATE, COMPOSE_CONF]
BASELINE = {
    "X-Frame-Options": "DENY",
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "strict-origin-when-cross-origin",
}


def _csp(conf: Path) -> dict[str, list[str]]:
    """The one policy a config defines, as ``{directive: [sources]}``."""
    found = re.findall(r'^\s*set \$dw_csp "([^"]+)";', conf.read_text(encoding="utf-8"), re.M)
    assert len(found) == 1, f"{conf.name}: expected exactly one CSP definition, found {len(found)}"
    policy: dict[str, list[str]] = {}
    for part in found[0].split(";"):
        name, *sources = part.split()
        assert name not in policy, f"{conf.name}: directive {name} repeated"
        policy[name] = sources
    return policy


def _sha256_source(script_body: str) -> str:
    return "'sha256-" + base64.b64encode(hashlib.sha256(script_body.encode("utf-8")).digest()).decode() + "'"


def _inline_scripts(html: str) -> list[str]:
    """Bodies of the scripts a browser would execute inline (not data blocks, not ``src``)."""
    bodies = []
    for attrs, body in re.findall(r"<script([^>]*)>(.*?)</script>", html, re.S):
        script_type = re.search(r'type="([^"]+)"', attrs)
        executable = script_type is None or script_type.group(1) in ("module", "text/javascript")
        if "src=" not in attrs and executable and body.strip():
            bodies.append(body)
    return bodies


def _injected_script() -> str:
    """The inline script ``start-nginx.sh`` writes into the shell at container start."""
    script = START_SCRIPT.read_text(encoding="utf-8")
    base = re.search(r'^BROWSER_API_BASE="([^"]+)"', script, re.M).group(1)
    template = re.search(r"<script>(window\.DATAWRAP_API_BASE=[^<]+)</script>", script).group(1)
    return template.replace('\\"', '"').replace("${BROWSER_API_BASE}", base)


@pytest.mark.parametrize("conf", CONFIGS, ids=[c.name for c in CONFIGS])
def test_policy_allows_scripts_by_origin_or_hash_only(conf):
    policy = _csp(conf)
    assert policy["default-src"] == ["'self'"]
    for directive in ("default-src", "script-src", "connect-src", "frame-src", "img-src", "font-src"):
        sources = policy[directive]
        assert "'unsafe-eval'" not in sources and "*" not in sources, (directive, sources)
        assert not any(s in ("https:", "http:") for s in sources), (directive, sources)
    assert "'unsafe-inline'" not in policy["script-src"]
    assert "data:" not in policy["script-src"] and "blob:" not in policy["script-src"]
    assert policy["object-src"] == ["'none'"]
    assert policy["base-uri"] == ["'self'"]
    assert policy["form-action"] == ["'self'"]
    assert policy["frame-ancestors"] == ["'none'"]
    assert policy["connect-src"] == ["'self'"], "the SPA calls its own origin only (nginx proxies /api/)"


@pytest.mark.parametrize("conf", CONFIGS, ids=[c.name for c in CONFIGS])
def test_every_inline_script_of_the_shell_is_allowed_by_hash(conf):
    """Editing an inline script changes its hash: this fails until the policy follows."""
    script_src = _csp(conf)["script-src"]
    inline = _inline_scripts(INDEX_HTML.read_text(encoding="utf-8"))
    assert inline, "index.html no longer has the inline theme script: drop its hash from the CSP"
    for body in inline:
        assert _sha256_source(body) in script_src, f"inline script not allowed by {conf.name}:\n{body[:200]}"
    assert _sha256_source(_injected_script()) in script_src, "start-nginx.sh's API-base script is not allowed"
    hashes = [s for s in script_src if s.startswith("'sha256-")]
    assert len(hashes) == len(inline) + 1, "a hash in the policy matches no script the shell contains"


@pytest.mark.parametrize("conf", CONFIGS, ids=[c.name for c in CONFIGS])
def test_origins_the_app_loads_are_in_the_policy(conf):
    policy = _csp(conf)
    turnstile = (WEB_SRC / "components" / "TurnstileWidget.tsx").read_text(encoding="utf-8")
    script_origin = re.search(r'script\.src = "(https://[^/"]+)', turnstile).group(1)
    assert script_origin in policy["script-src"] and script_origin in policy["frame-src"]
    video = (WEB_SRC / "components" / "marketing" / "VideoPlayer.tsx").read_text(encoding="utf-8")
    for origin in set(re.findall(r"`(https://[^/`]+)/embed/", video)):
        assert origin in policy["frame-src"], origin
    for origin in set(re.findall(r"`(https://[^/`]+)/vi/", video)):
        assert origin in policy["img-src"], origin
    # Bundled fonts and images are same-origin files or data: URIs in the CSS.
    assert {"'self'", "data:"} <= set(policy["font-src"]) and {"'self'", "data:"} <= set(policy["img-src"])


@pytest.mark.parametrize("conf", CONFIGS, ids=[c.name for c in CONFIGS])
def test_spa_locations_send_the_headers_and_api_locations_do_not_get_the_spa_policy(conf):
    text = conf.read_text(encoding="utf-8")
    blocks = dict(re.findall(r"\n    location (/assets/|/) \{\n(.*?)\n    \}", text, re.S))
    assert set(blocks) == {"/assets/", "/"}
    for path, body in blocks.items():
        assert "add_header Content-Security-Policy $dw_csp always;" in body, path
        for name, value in BASELINE.items():
            assert f'add_header {name} "{value}" always;' in body, (path, name)
    # Not server-wide: an /api/ response keeps the API's own policy, and a
    # second, SPA-shaped CSP on the API's HTML (SSO pages) would break it.
    server_level = re.sub(r"\n    location [^\n]*\{\n.*?\n    \}", "", text, flags=re.S)
    assert "Content-Security-Policy" not in server_level.replace("set $dw_csp", "")
    api_block = re.search(r"\n    location /api/ \{\n(.*?)\n    \}", text, re.S).group(1)
    assert "Content-Security-Policy" not in api_block


def test_static_only_config_sends_the_baseline_headers():
    """``nginx.static.conf`` serves the shell for an API on another origin: no
    CSP (its connect-src is deployment specific), but never frameable or sniffed."""
    text = (REPO / "deploy" / "nginx.static.conf").read_text(encoding="utf-8")
    for path in ("/assets/", "/"):
        body = re.search(rf"\n    location {re.escape(path)} \{{\n(.*?)\n    \}}", text, re.S).group(1)
        for name, value in BASELINE.items():
            assert f'add_header {name} "{value}" always;' in body, (path, name)


# --------------------------------------------------------------------------- real nginx


def _get(port: int, path: str):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # nosec B310 - local loopback
            return resp.status, resp.headers, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.headers, err.read().decode()


@pytest.fixture
def spa_behind_nginx(tmp_path):
    import test_r12_d11_mcp_routes_answer_json as harness

    # No API behind it: /api/ answers nginx's own JSON 502, which is the point.
    dead_api = harness._free_port()
    proc, port = harness._run_nginx(tmp_path, TEMPLATE, f"http://127.0.0.1:{dead_api}/api/v1")
    html = tmp_path / "html"
    shell = INDEX_HTML.read_text(encoding="utf-8").replace(
        "</head>", f"<script>{_injected_script()}</script></head>"
    )
    (html / "index.html").write_text(shell, encoding="utf-8")
    (html / "assets").mkdir()
    (html / "assets" / "index-abc123.js").write_text("console.log('app')", encoding="utf-8")
    try:
        yield port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_real_nginx_serves_the_shell_and_assets_with_the_policy(spa_behind_nginx):
    expected_csp = re.search(r'set \$dw_csp "([^"]+)";', TEMPLATE.read_text(encoding="utf-8")).group(1)
    for path in ("/", "/jobs/123", "/index.html", "/assets/index-abc123.js", "/assets/missing.js"):
        status, headers, body = _get(spa_behind_nginx, path)
        assert headers.get_all("Content-Security-Policy") == [expected_csp], (path, status)
        for name, value in BASELINE.items():
            assert headers.get_all(name) == [value], (path, name)
        assert headers.get("Permissions-Policy"), path
    status, headers, body = _get(spa_behind_nginx, "/")
    assert status == 200 and "window.DATAWRAP_API_BASE" in body
    assert headers.get("Cache-Control") == "no-cache"
    status, headers, _ = _get(spa_behind_nginx, "/assets/index-abc123.js")
    assert status == 200 and "immutable" in headers.get("Cache-Control", "")

    # nginx's own /api/ answer is JSON and carries no SPA policy.
    status, headers, body = _get(spa_behind_nginx, "/api/v1/status")
    assert status == 502 and body.startswith("{")
    assert headers.get("Content-Security-Policy") is None
