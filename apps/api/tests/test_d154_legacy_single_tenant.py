"""D154 in a legacy single-tenant deployment (signup off, isolation off).

Unscoped rows stay visible there — that is the legacy behaviour — but ``""``
must never mean "every workspace" for a non-admin: an account that belongs to
NO workspace sees the unscoped rows only, and a member of workspace B sees B's
rows plus the unscoped ones, never workspace A's.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import build_world, owner_marker  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    return build_world(tmp_path, monkeypatch, signup=False)


def _marked(client, path: str, marker: str, headers: dict[str, str] | None = None) -> bool:
    response = client.get(path, headers=headers or {})
    assert response.status_code == 200, (path, response.status_code, response.text[:200])
    return marker in response.text


LISTS = (
    "/api/v1/connectors/",
    "/api/v1/connectors/saved",
    "/api/v1/connectors/jobs",
    "/api/v1/schedules/",
    "/api/v1/schedules/approvals/open",
    "/api/v1/schedules/export/dataflow",
    "/api/v1/contracts",
    "/api/v1/transforms/",
)


@pytest.mark.parametrize("who", ["m", "b"])
def test_non_admin_never_sees_another_workspaces_rows(world, who):
    client = getattr(world, who)
    a_marker = owner_marker("a", world.tag)
    for path in LISTS:
        assert not _marked(client, path, a_marker), (who, path)
        assert not _marked(client, path, world.ws_a), (who, path)


def test_legacy_rows_remain_visible_to_a_member_of_no_workspace(world):
    legacy = owner_marker("legacy", world.tag)
    for path in (
        "/api/v1/connectors/saved",
        "/api/v1/connectors/jobs",
        "/api/v1/schedules/",
        "/api/v1/schedules/export/dataflow",
        "/api/v1/contracts",
        "/api/v1/transforms/",
    ):
        assert _marked(world.m, path, legacy), path
    assert world.m.get(f"/api/v1/connectors/jobs/{world.ids['legacy_job']}").status_code == 200


def test_header_less_member_reads_and_writes_land_in_their_own_workspace(world):
    created = world.b.post("/api/v1/connectors/saved", json={
        "name": f"b-conn-{world.tag}", "type": "postgresql", "host": "b.example.com",
        "port": 5432, "database": "b", "username": "u", "password": "p",
    })
    assert created.status_code == 200, created.text
    assert created.json()["workspace_id"] == world.ws_b  # not a legacy unscoped row
    # ... so a member of no workspace does not inherit it.
    assert f"b-conn-{world.tag}" not in world.m.get("/api/v1/connectors/saved").text


def test_gitops_cannot_reach_another_workspaces_schedule(world):
    manifest = {"kind": "PipelineSchedule", "spec": {"id": world.ids["a_schedule"], "name": "x"}}
    for client in (world.m, world.b):
        plan = client.post("/api/v1/schedules/gitops/plan", json=manifest)
        assert plan.status_code in {403, 404}, plan.text
