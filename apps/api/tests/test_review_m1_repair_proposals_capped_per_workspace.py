"""Security review M1: one workspace cannot evict another's repair proposals.

The file store kept the newest 200 proposals of the whole deployment. Workspace
B posting 205 proposals pushed workspace A's out of the file: A's own proposal
answered 404 and A's list was empty (the reviewer's sequence, below). The cap
is per workspace now, as the MongoDB store's scoped page already was.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

QUARANTINE = {"rejected_details": [{"column": "amount", "reason": "value is not a number"}]}


def _propose(client, job_id: str = "") -> dict:
    made = client.post("/api/v1/repair/propose/quarantine", json={**QUARANTINE, "job_id": job_id})
    assert made.status_code == 200, made.text
    return made.json()


def _stored(world) -> list[dict]:
    return json.loads((world.data_dir / "agentic_repair_proposals.json").read_text())["proposals"]


def test_a_flood_from_one_workspace_leaves_the_others_proposals_in_place(tenancy_world):  # noqa: F811
    world = tenancy_world
    mine = _propose(world.a, world.job(world.workspace_a, name="acme orders"))
    job_b = world.job(world.workspace_b, name="b orders")

    for _ in range(205):
        _propose(world.b, job_b)

    kept = world.a.get(f"/api/v1/repair/proposals/{mine['id']}")
    assert kept.status_code == 200, kept.text
    assert [p["id"] for p in world.a.get("/api/v1/repair/proposals").json()["proposals"]] == [mine["id"]]
    # A can still decide it.
    admin_a = world.member("admin-a@tenancy.example.com", world.workspace_a, "admin")
    decided = admin_a.post(f"/api/v1/repair/proposals/{mine['id']}/decide", json={"approve": False})
    assert decided.status_code == 200, decided.text

    # B's own history is still bounded: its newest 200, in order.
    rows = _stored(world)
    of_b = [r for r in rows if r.get("workspace_id") == world.workspace_b]
    assert len(of_b) == 200
    assert [r["created_at"] for r in of_b] == sorted(r["created_at"] for r in of_b)
    assert len(world.b.get("/api/v1/repair/proposals").json()["proposals"]) == 200
    assert [r["id"] for r in rows if r.get("workspace_id") == world.workspace_a] == [mine["id"]]


def test_cap_keeps_the_newest_of_each_workspace_and_of_unscoped_rows():
    from services.agentic_repair import _capped_per_workspace

    rows = (
        [{"id": f"a{i}", "workspace_id": "A"} for i in range(3)]
        + [{"id": f"b{i}", "workspace_id": "B"} for i in range(250)]
        + [{"id": f"u{i}"} for i in range(210)]
        + [{"id": "a3", "workspace_id": "A"}]
    )
    kept = _capped_per_workspace(rows)
    assert [r["id"] for r in kept if r.get("workspace_id") == "A"] == ["a0", "a1", "a2", "a3"]
    assert [r["id"] for r in kept if r.get("workspace_id") == "B"] == [f"b{i}" for i in range(50, 250)]
    assert [r["id"] for r in kept if "workspace_id" not in r] == [f"u{i}" for i in range(10, 210)]
    assert [r["id"] for r in kept] == [r["id"] for r in rows if r in kept]  # stored order unchanged
