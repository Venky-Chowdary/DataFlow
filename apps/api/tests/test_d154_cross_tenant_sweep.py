"""D154 regression sweep: B never sees A's data on ANY listable GET route.

Every parameterless GET route in the app's route table is called as B (no
header, B's own header, A's workspace forced into the header) with A's
workspace seeded across every tenant-owned store (``tests/d154_world.py``).
All of A's rows carry one marker string and A's workspace id is known, so the
check is the same for every route: neither may appear in a 200 response to B.

Two deployments are swept:
* multi-tenant (signup on, isolation enforced): B must not see A's rows *or*
  the legacy unscoped rows;
* legacy single-tenant (signup off, isolation off): the legacy rows stay
  visible, but a non-admin — including one who belongs to NO workspace —
  never sees another workspace's rows (``''`` must not mean "all").
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import World, build_world, owner_marker  # noqa: E402

# Long-lived streams would hang a synchronous client.
_SKIP = ("stream", "/sse", "/live", "/events/watch")

# Routes that must answer A with A's own marker, so the seeds provably reach
# the sweep (a sweep over unseeded routes would pass vacuously). The transform
# library is not here: it has no RBAC rule yet, so every caller gets a 403
# before the tenant scope is reached (see test_rbac_route_inventory).
_MUST_SHOW_A = (
    "/api/v1/connectors/",
    "/api/v1/connectors/saved",
    "/api/v1/connectors/jobs",
    "/api/v1/schedules/",
    "/api/v1/schedules/export/dataflow",
    "/api/v1/contracts",
    "/api/v1/transforms/",
    "/api/v1/audit/events",
    "/api/v1/ops/dlq",
    "/api/v1/preflight/runs",
    "/api/v1/transfer/plans",
)


def get_routes() -> list[str]:
    from src.main import app

    paths: set[str] = set()
    for included in app.routes:
        router = getattr(included, "original_router", None)
        context = getattr(included, "include_context", None)
        if router is None:
            continue
        prefix = str(getattr(context, "prefix", "") or "")
        for route in router.routes:
            path = str(getattr(route, "path", "") or "")
            if "GET" in (getattr(route, "methods", None) or ()) and "{" not in path:
                paths.add(prefix + path)
    return sorted(p for p in paths if not any(s in p for s in _SKIP))


def _body(client, path: str, headers: dict[str, str]):
    response = client.get(path, headers=headers)
    return response.status_code, response.text


def _leaks(world: World, client, headers: dict[str, str], needles: tuple[str, ...]) -> list[str]:
    found = []
    for path in get_routes():
        status, text = _body(client, path, headers)
        if status == 200 and any(n in text for n in needles):
            found.append(path)
    return found


@pytest.fixture(params=["multi_tenant", "legacy_single_tenant"])
def world(request, tmp_path, monkeypatch) -> World:
    return build_world(tmp_path, monkeypatch, signup=request.param == "multi_tenant")


def _is_multi_tenant(world: World) -> bool:
    return world.m is None


def test_owner_sees_own_rows_on_the_seeded_routes(world):
    marker = owner_marker("a", world.tag)
    for client, headers in ((world.a, {}), (world.a, {"X-Workspace-Id": world.ws_a})):
        for path in _MUST_SHOW_A:
            status, text = _body(client, path, headers)
            assert status == 200, (path, status, text[:200])
            assert marker in text, (path, headers)
    # The platform admin sees the legacy rows too (and A's, unfiltered).
    legacy = owner_marker("legacy", world.tag)
    for path in ("/api/v1/connectors/saved", "/api/v1/connectors/jobs", "/api/v1/schedules/"):
        _status, text = _body(world.admin, path, {})
        assert marker in text and legacy in text, path


def test_b_sees_none_of_a_on_any_get_route(world):
    needles = (owner_marker("a", world.tag), world.ws_a)
    for headers in ({}, {"X-Workspace-Id": world.ws_b}):
        assert _leaks(world, world.b, headers, needles) == [], headers
    # A's workspace in the header is refused, never served. (/auth/me echoes
    # the id the caller typed, with no role and no data, so only A's rows are
    # the needle here.)
    forced = {"X-Workspace-Id": world.ws_a}
    assert _leaks(world, world.b, forced, needles[:1]) == []
    assert world.b.get("/api/v1/schedules/", headers=forced).status_code == 403


def test_member_of_no_workspace_sees_none_of_a_on_any_get_route(world):
    if _is_multi_tenant(world):
        pytest.skip("multi-tenant accounts always have a signup workspace")
    needles = (owner_marker("a", world.tag), world.ws_a)
    assert _leaks(world, world.m, {}, needles) == []
    forced = {"X-Workspace-Id": world.ws_a}
    assert _leaks(world, world.m, forced, needles[:1]) == []
    # Legacy single-tenant behaviour is kept: the unscoped rows stay visible.
    legacy = owner_marker("legacy", world.tag)
    for path in ("/api/v1/connectors/saved", "/api/v1/connectors/jobs", "/api/v1/schedules/",
                 "/api/v1/contracts", "/api/v1/transforms/"):
        _status, text = _body(world.m, path, {})
        assert legacy in text, path


def test_unscoped_rows_are_hidden_from_tenants_only_when_isolation_is_enforced(world):
    legacy = owner_marker("legacy", world.tag)
    leaked = _leaks(world, world.b, {}, (legacy,))
    if _is_multi_tenant(world):
        assert leaked == []
    else:
        assert "/api/v1/connectors/jobs" in leaked  # legacy view intact


def test_id_addressed_reads_of_a_are_refused_for_b(world):
    ids = world.ids
    reads = (
        f"/api/v1/connectors/saved/{ids['a_connector']}",
        f"/api/v1/connectors/jobs/{ids['a_job']}",
        f"/api/v1/schedules/{ids['a_schedule']}",
        f"/api/v1/contracts/{ids['a_contract']}",
        f"/api/v1/transforms/{ids['a_project']}",
        f"/api/v1/transform-library/{ids['a_recipe']}",
        f"/api/v1/preflight/runs/{ids['a_preflight_run']}",
        f"/api/v1/transfer/plans/{ids['a_plan']}",
    )
    clients = [(world.b, {}), (world.b, {"X-Workspace-Id": world.ws_b})]
    if world.m is not None:
        clients.append((world.m, {}))
    for client, headers in clients:
        for path in reads:
            assert client.get(path, headers=headers).status_code in {403, 404}, (path, headers)
    # Mutations by id as well.
    assert world.b.delete(f"/api/v1/connectors/saved/{ids['a_connector']}").status_code in {403, 404}
    assert world.b.post(f"/api/v1/contracts/{ids['a_contract']}/deprecate").status_code in {403, 404}


def test_b_cannot_plan_or_apply_a_resource_through_gitops(world):
    manifest = {
        "kind": "PipelineSchedule",
        "spec": {"id": world.ids["a_schedule"], "name": "stolen"},
    }
    clients = [world.b] + ([world.m] if world.m is not None else [])
    for client in clients:
        for path in ("/api/v1/schedules/gitops/plan", "/api/v1/schedules/gitops/apply"):
            response = client.post(path, json=manifest)
            assert response.status_code in {403, 404}, (path, response.status_code, response.text)


def test_shape_drafts_are_per_workspace(world):
    route = f"{owner_marker('a', world.tag)}-src→dst"
    mine = world.a.get("/api/v1/shape/drafts", params={"route_key": route})
    assert mine.json()["draft"] is not None
    # B reading, overwriting and deleting the same route name touches only B's.
    assert world.b.get("/api/v1/shape/drafts", params={"route_key": route}).json()["draft"] is None
    saved = world.b.put("/api/v1/shape/drafts", json={"route_key": route, "recipe": {"steps": []}})
    assert saved.status_code == 200, saved.text
    assert world.b.delete("/api/v1/shape/drafts", params={"route_key": route}).status_code == 200
    assert world.a.get("/api/v1/shape/drafts", params={"route_key": route}).json()["draft"] is not None

