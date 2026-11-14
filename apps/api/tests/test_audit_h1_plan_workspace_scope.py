"""Audit H-1: a transfer plan is changed only from inside its own workspace.

``GET /transfer/plans/{id}`` already answered 404 for another workspace's plan,
but PATCH, POST mappings, POST approve and GET preflight went through store
mutators that looked the plan up by id alone. An editor of workspace B who knew
a plan id of workspace A could rename it (and read its sample rows and source
endpoint off the response), write a mapping revision, approve it, and poll its
Validate verdict.

Through the real REST routes with two workspaces: every plan route answers B
exactly as it answers an id that does not exist, A's plan is unchanged, and A
keeps full use of its own plan.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import EDITOR_A, tenancy_world  # noqa: E402,F401

MISSING_PLAN = "00000000-0000-4000-8000-000000000000"
SECRET_ROW = {"ssn": "A-SAMPLE-ROW-SENTINEL"}
SECRET_HOST = "a-internal-host-sentinel"
SENTINELS = ("A-SAMPLE-ROW-SENTINEL", SECRET_HOST, "A-PLAN-NAME-SENTINEL", "A-VERDICT-SENTINEL")


@pytest.fixture
def plan_a(tenancy_world):  # noqa: F811
    from services.plan_preflight_job import reset_plan_preflight_jobs

    reset_plan_preflight_jobs()
    made = tenancy_world.a.post(
        "/api/v1/transfer/plans",
        json={
            "name": "A-PLAN-NAME-SENTINEL",
            "source": {"kind": "database", "format": "postgresql", "host": SECRET_HOST},
            "destination": {"kind": "database", "format": "postgresql", "host": "a-dw"},
            "source_columns": ["id", "ssn"],
            "target_columns": ["id", "ssn"],
            "sample_rows": [SECRET_ROW],
        },
    )
    assert made.status_code == 200, made.text
    yield made.json()["plan"]["id"]
    reset_plan_preflight_jobs()


def _stored(plan_id: str):
    """The plan as the store holds it, read in system scope."""
    from services import transfer_plan_store

    return next(p for p in transfer_plan_store._load_all() if p.id == plan_id)


def _same_as_missing(client, method: str, suffix: str, plan_id: str, **kwargs):
    foreign = client.request(method, f"/api/v1/transfer/plans/{plan_id}{suffix}", **kwargs)
    missing = client.request(method, f"/api/v1/transfer/plans/{MISSING_PLAN}{suffix}", **kwargs)
    assert foreign.status_code == missing.status_code, (foreign.text, missing.text)
    assert foreign.status_code in {400, 404}, foreign.text
    # safe_http_exception appends a per-response support reference.
    # ... and the error envelope carries the same reference as a field (W3).
    strip = lambda text: re.sub(r'( Reference: |"reference":")[0-9a-f-]{36}', r"\1", text)  # noqa: E731
    assert strip(foreign.text) == strip(missing.text), (foreign.text, missing.text)
    for sentinel in SENTINELS:
        assert sentinel not in foreign.text, f"{method} {suffix} leaked {sentinel}"
    return foreign


def test_patch_plan_from_another_workspace_is_a_404_and_changes_nothing(tenancy_world, plan_a):  # noqa: F811
    before = _stored(plan_a).to_dict()
    refused = _same_as_missing(
        tenancy_world.b, "PATCH", "", plan_a, json={"name": "renamed-by-b"}
    )
    assert refused.status_code == 404
    assert _stored(plan_a).to_dict() == before

    own = tenancy_world.a.patch(f"/api/v1/transfer/plans/{plan_a}", json={"name": "renamed-by-a"})
    assert own.status_code == 200, own.text
    assert _stored(plan_a).name == "renamed-by-a"


def test_sync_mappings_from_another_workspace_is_a_404_and_writes_no_revision(tenancy_world, plan_a):  # noqa: F811
    body = {"mappings": [{"source": "id", "target": "id"}]}
    refused = _same_as_missing(tenancy_world.b, "POST", "/mappings", plan_a, json=body)
    assert refused.status_code == 404
    assert _stored(plan_a).revisions == []
    assert _stored(plan_a).status == "draft"

    own = tenancy_world.a.post(f"/api/v1/transfer/plans/{plan_a}/mappings", json=body)
    assert own.status_code == 200, own.text
    assert len(_stored(plan_a).revisions) == 1


def test_approve_from_another_workspace_is_a_404_and_does_not_approve(tenancy_world, plan_a):  # noqa: F811
    refused = _same_as_missing(tenancy_world.b, "POST", "/approve", plan_a)
    assert refused.status_code == 404
    assert _stored(plan_a).status == "draft"

    own = tenancy_world.a.post(f"/api/v1/transfer/plans/{plan_a}/approve")
    assert own.status_code == 200, own.text
    assert _stored(plan_a).status == "approved"


def test_validate_verdict_is_not_readable_from_another_workspace(tenancy_world, plan_a, monkeypatch):  # noqa: F811
    import time

    from services import transfer_plan_service

    def fake_run(plan_id, *, acknowledgments=None, shape_recipe=None):
        return {"passed": True, "run_id": "pf_a_verdict", "blockers": ["A-VERDICT-SENTINEL"]}

    monkeypatch.setattr(transfer_plan_service, "run_plan_preflight", fake_run)
    started = tenancy_world.a.post(
        f"/api/v1/transfer/plans/{plan_a}/preflight", json={"async_run": True}
    )
    assert started.status_code == 202, started.text
    run_id = started.json()["run_id"]
    deadline = time.time() + 5
    own = None
    while time.time() < deadline:
        own = tenancy_world.a.get(
            f"/api/v1/transfer/plans/{plan_a}/preflight", params={"run_id": run_id}
        )
        if own.status_code == 200 and own.json()["status"] == "complete":
            break
        time.sleep(0.02)
    assert own is not None and own.json()["status"] == "complete", own.text
    assert "A-VERDICT-SENTINEL" in own.text

    # Latest run of A's plan, and the same run addressed by its id.
    _same_as_missing(tenancy_world.b, "GET", "/preflight", plan_a)
    _same_as_missing(tenancy_world.b, "GET", "/preflight", plan_a, params={"run_id": run_id})

    # A run id is not a bearer handle: B's own plan must not serve A's run.
    own_b = tenancy_world.b.post("/api/v1/transfer/plans", json={"name": "b plan"})
    assert own_b.status_code == 200, own_b.text
    plan_b = own_b.json()["plan"]["id"]
    via_own = tenancy_world.b.get(
        f"/api/v1/transfer/plans/{plan_b}/preflight", params={"run_id": run_id}
    )
    assert via_own.status_code == 404, via_own.text
    assert "A-VERDICT-SENTINEL" not in via_own.text


def test_starting_validate_on_a_foreign_plan_does_not_displace_the_owners_run(
    tenancy_world, plan_a, monkeypatch  # noqa: F811
):
    import threading

    from services import transfer_plan_service

    release = threading.Event()

    def slow_run(plan_id, *, acknowledgments=None, shape_recipe=None):
        release.wait(5)
        return {"passed": True, "run_id": "pf_a_slow", "blockers": []}

    monkeypatch.setattr(transfer_plan_service, "run_plan_preflight", slow_run)
    started = tenancy_world.a.post(
        f"/api/v1/transfer/plans/{plan_a}/preflight", json={"async_run": True}
    )
    assert started.status_code == 202, started.text
    try:
        tenancy_world.b.post(
            f"/api/v1/transfer/plans/{plan_a}/preflight", json={"async_run": True}
        )
        latest = tenancy_world.a.get(f"/api/v1/transfer/plans/{plan_a}/preflight")
        assert latest.status_code == 200, latest.text
        assert latest.json()["run_id"] == started.json()["run_id"]
    finally:
        release.set()


def test_every_other_plan_route_answers_a_foreign_plan_like_a_missing_one(tenancy_world, plan_a):  # noqa: F811
    assert tenancy_world.b.get(f"/api/v1/transfer/plans/{plan_a}").status_code == 404
    _same_as_missing(tenancy_world.b, "GET", "", plan_a)
    _same_as_missing(tenancy_world.b, "GET", "/run-payload", plan_a)
    _same_as_missing(tenancy_world.b, "POST", "/map", plan_a, json={"use_llm": False})
    _same_as_missing(tenancy_world.b, "POST", "/preflight", plan_a, json={})
    listed = tenancy_world.b.get("/api/v1/transfer/plans")
    assert listed.status_code == 200, listed.text
    assert plan_a not in listed.text
    assert _stored(plan_a).status == "draft"
    assert _stored(plan_a).preflight_runs == []


def test_store_mutators_refuse_a_plan_outside_the_bound_scope(tenancy_world, plan_a):  # noqa: F811
    """Pilot and MCP reach the store without a route: the store itself refuses."""
    from services import transfer_plan_store as store

    with tenancy_world.as_workspace(tenancy_world.workspace_b, actor="b@example.com"):
        assert store.update_plan(plan_a, {"name": "x"}) is None
        assert store.sync_ui_mappings(plan_a, [{"source": "id", "target": "id"}]) is None
        assert store.add_mapping_revision(plan_a, {"mappings": [{"source": "id", "target": "id"}]}) is None
        assert store.approve_plan_version(plan_a) is None
        assert store.add_preflight_run(plan_a, {"passed": True}) is None
        assert store.attach_job(plan_a, "job-b") is None
    stored = _stored(plan_a)
    assert (stored.name, stored.status, stored.revisions, stored.job_ids, stored.preflight_runs) == (
        "A-PLAN-NAME-SENTINEL", "draft", [], [], [],
    )
    # System scope (workers, schedulers: nothing bound) still reaches it.
    assert store.attach_job(plan_a, "job-a") is not None


def test_a_viewer_of_the_plans_workspace_cannot_change_it(tenancy_world, plan_a):  # noqa: F811
    """Write needs an editor role in the plan's own workspace, not merely a
    credential that is an editor somewhere else."""
    tenancy_world.add_membership("editor-b@tenancy.example.com", tenancy_world.workspace_a, "viewer")
    both = tenancy_world.b
    both.headers.pop("X-Workspace-Id", None)
    assert both.get(f"/api/v1/transfer/plans/{plan_a}").status_code in {200, 403}
    for method, suffix, body in (
        ("PATCH", "", {"name": "renamed-by-viewer"}),
        ("POST", "/mappings", {"mappings": [{"source": "id", "target": "id"}]}),
        ("POST", "/approve", None),
    ):
        refused = both.request(method, f"/api/v1/transfer/plans/{plan_a}{suffix}", json=body)
        assert refused.status_code in {403, 404}, (method, suffix, refused.text)
    stored = _stored(plan_a)
    assert (stored.name, stored.status, stored.revisions) == ("A-PLAN-NAME-SENTINEL", "draft", [])
    assert EDITOR_A  # the owner account is untouched by the viewer membership
