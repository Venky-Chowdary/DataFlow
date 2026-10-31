"""QA round 9: "No RBAC rule" errors — unknown routes are 404, not 403.

No permission rule was missing. A request to a route the API does not have was
answered 403 ``"No RBAC rule is configured for this route."`` and the web app
still called six such dead routes (``/api/v1/jobs…``) as fallbacks, so when the
real route answered with an error the page showed the RBAC message instead
(analytics pages broke on it).

Through the real app on the live MongoDB: a signed-in workspace admin gets 404
for every dead route the web used to call, an anonymous caller still gets 401
(no route map for strangers), a known path with the wrong method is 405, and a
real route is still gated by its rule. The web source must not call any route
the API does not serve.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401

_WEB_SRC = Path(__file__).resolve().parents[3] / "apps" / "web" / "src"

# The six fallbacks apps/web/src/lib/api.ts tried after the real route.
DEAD_FALLBACKS = (
    ("GET", "/api/v1/jobs"),
    ("GET", "/api/v1/jobs/{job}"),
    ("PATCH", "/api/v1/jobs/{job}"),
    ("POST", "/api/v1/jobs/{job}/retry"),
    ("POST", "/api/v1/jobs/{job}/resume"),
    ("POST", "/api/v1/jobs/{job}/cancel"),
)


def _job(r9) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job(
        {"status": "failed", "workspace_id": r9.acme, "name": "r9-route-probe"}
    )


def test_dead_routes_are_404_for_a_signed_in_admin_not_no_rbac_rule(r9):
    job_id = _job(r9)
    owner = r9.owner
    for method, template in DEAD_FALLBACKS:
        path = template.format(job=job_id)
        response = owner.request(method, path, json={} if method != "GET" else None)
        assert response.status_code == 404, (method, path, response.status_code, response.text)
        assert "No RBAC rule" not in response.text
        assert response.json().get("reason") != "no_rule"

    # The real routes behind those fallbacks answer for the same job.
    real = owner.get(f"/api/v1/connectors/jobs/{job_id}")
    assert real.status_code == 200, real.text


def test_unknown_route_semantics_around_the_404(r9):
    from fastapi.testclient import TestClient

    # Strangers learn nothing about the route map: authentication comes first.
    anonymous = TestClient(r9.app)
    assert anonymous.get("/api/v1/jobs").status_code == 401
    assert anonymous.get("/api/v1/connectors/jobs").status_code == 401

    owner = r9.owner
    # A real path with a method it does not serve is 405, not "no rule".
    wrong_method = owner.post("/api/v1/usage/summary", json={})
    assert wrong_method.status_code == 405, wrong_method.text
    # A made-up path anywhere is 404.
    assert owner.get("/api/v1/definitely-not-a-route").status_code == 404
    assert owner.post("/api/v1/analytics/does-not-exist", json={}).status_code == 404

    # Real routes are still gated by their RBAC rule.
    r9.world.member("viewer@acme.test", "viewer")
    viewer = r9.login("viewer@acme.test", "Member-Password-2026!", r9.acme)
    denied = viewer.post("/api/v1/workspace/api-keys", json={"name": "x", "role": "viewer"})
    assert denied.status_code == 403, denied.text
    assert denied.json()["required_permission"] != "no_rule"


def _api_routes(app) -> list[tuple[str, re.Pattern[str]]]:
    from services.rbac import _compile_route_template, _walk_route_patterns

    return [(m, _compile_route_template(t)) for m, t in _walk_route_patterns(app.routes)]


_TEMPLATE = re.compile(r"`\$\{(?:API_BASE|resolveApiBase\(\))\}(/[^`]*)`")


def _web_paths() -> list[tuple[str, int, str]]:
    found = []
    for path in sorted(_WEB_SRC.rglob("*.ts*")):
        if ".test." in path.name:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        for match in _TEMPLATE.finditer(text):
            raw = match.group(1)
            # A ${…} glued to a segment builds the query string
            # (``retry${query}``, ``preflight${runId ? `?…` : ""}``): cut there.
            glued = re.search(r"(?<=[^/])\$\{", raw)
            raw = (raw[: glued.start()] if glued else raw).split("?")[0]
            concrete = re.sub(r"\$\{[^}]*\}", "x", raw).rstrip("/")
            if not concrete or concrete == "/x":
                continue
            line = text.count("\n", 0, match.start()) + 1
            found.append((str(path.relative_to(_WEB_SRC)), line, "/api/v1" + concrete))
    return found


def test_web_app_calls_no_route_the_api_does_not_serve(r9):
    routes = _api_routes(r9.app)
    paths = _web_paths()
    assert len(paths) > 100  # the scan really read the client
    dead = [
        f"{file}:{line} {path}"
        for file, line, path in paths
        if not any(pattern.fullmatch(path) or pattern.fullmatch(path + "/") for _m, pattern in routes)
    ]
    assert dead == [], "web calls routes the API does not have:\n" + "\n".join(dead)
