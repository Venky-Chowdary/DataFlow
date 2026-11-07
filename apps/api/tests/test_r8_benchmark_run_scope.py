"""A benchmark run belongs to the workspace that started it (round-7 routes).

``POST /workspace/benchmark/runs`` and ``GET .../runs/{run_id}`` kept runs in
one process-wide dict with no workspace: any workspace admin could poll another
workspace's run by id, and a second workspace starting a run was handed the
first workspace's run in flight.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import PASSWORD, world  # noqa: E402,F401


def _other_workspace_admin(world):
    from services import team_store

    other = team_store.create_workspace(name="Other", created_by="boss@other.test")
    world.account("boss@other.test")
    boss = world.login("boss@other.test", PASSWORD)
    boss.headers["X-Workspace-Id"] = other.id
    return other.id, boss


def test_run_id_from_another_workspace_reads_as_absent(world, monkeypatch):
    from services import benchmark_runs

    other_id, boss = _other_workspace_admin(world)
    monkeypatch.setitem(
        benchmark_runs._RUNS,
        "c" * 32,
        {"id": "c" * 32, "rows": 1000, "status": "completed",
         "report": {"target": "OTHER_SENTINEL"}, "workspace_id": other_id},
    )
    own = boss.get(f"/api/v1/workspace/benchmark/runs/{'c' * 32}")
    assert own.status_code == 200, own.text
    foreign = world.owner.get(f"/api/v1/workspace/benchmark/runs/{'c' * 32}")
    assert foreign.status_code == 404, foreign.text
    assert "OTHER_SENTINEL" not in foreign.text
    md = world.owner.get(f"/api/v1/workspace/benchmark/runs/{'c' * 32}?format=md")
    assert md.status_code == 404
    # A platform admin reads every workspace's run.
    assert world.root.get(f"/api/v1/workspace/benchmark/runs/{'c' * 32}").status_code == 200
    # Starting and polling runs is workspace administration (workspace.manage).
    for role in ("viewer", "editor"):
        key = world.as_key(world.mint_key(role))
        assert key.post("/api/v1/workspace/benchmark/runs", json={"rows": 1000}).status_code == 403
        assert key.get(f"/api/v1/workspace/benchmark/runs/{'c' * 32}").status_code == 403


def test_starting_a_run_never_hands_back_another_workspaces_run(world, monkeypatch):
    from services import benchmark_runs

    other_id, _boss = _other_workspace_admin(world)
    monkeypatch.setitem(
        benchmark_runs._RUNS,
        "d" * 32,
        {"id": "d" * 32, "rows": 1000, "status": "running", "report": None,
         "workspace_id": other_id},
    )
    started = world.owner.post("/api/v1/workspace/benchmark/runs", json={"rows": 1000})
    assert started.status_code == 409, started.text
    assert "d" * 32 not in started.text

