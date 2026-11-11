"""Audit H-3: repair proposals belong to a workspace.

``RepairProposal`` had no tenant dimension: ``GET /repair/proposals`` listed
every workspace's proposals (job ids, summaries, column names, proposed
actions) to anyone holding ``job.read``, and a ``job.manage`` holder could
approve or reject another tenant's proposal by id.

A proposal now carries the workspace of its job (the caller's own workspace
when it names no job the caller can see), and list / get / decide are scoped to
it. Through the real REST routes with two workspaces. Proposals stored before
the field existed follow their job; one whose job cannot be found is unscoped,
which only a platform administrator sees.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401

QUARANTINE = {
    "rejected_details": [
        {"column": "A_SECRET_COLUMN_SENTINEL", "reason": "value is not a number"},
    ],
}
SENTINEL = "A_SECRET_COLUMN_SENTINEL"


def _propose(client, job_id: str = "") -> dict:
    made = client.post("/api/v1/repair/propose/quarantine", json={**QUARANTINE, "job_id": job_id})
    assert made.status_code == 200, made.text
    return made.json()


def _stored(world, proposal_id: str) -> dict:
    rows = json.loads((world.data_dir / "agentic_repair_proposals.json").read_text())["proposals"]
    return next(r for r in rows if r["id"] == proposal_id)


def test_proposal_is_listed_and_read_only_inside_its_workspace(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a, name="acme orders")
    proposal = _propose(tenancy_world.a, job_a)

    listed_b = tenancy_world.b.get("/api/v1/repair/proposals")
    assert listed_b.status_code == 200, listed_b.text
    assert listed_b.json() == {"proposals": []}
    by_job = tenancy_world.b.get("/api/v1/repair/proposals", params={"job_id": job_a})
    assert by_job.json() == {"proposals": []}
    assert SENTINEL not in listed_b.text + by_job.text

    foreign = tenancy_world.b.get(f"/api/v1/repair/proposals/{proposal['id']}")
    missing = tenancy_world.b.get("/api/v1/repair/proposals/repair_000000000000")
    assert foreign.status_code == missing.status_code == 404
    assert foreign.json() == missing.json() == {"detail": "Proposal not found"}

    listed_a = tenancy_world.a.get("/api/v1/repair/proposals")
    assert [p["id"] for p in listed_a.json()["proposals"]] == [proposal["id"]]
    assert tenancy_world.a.get(f"/api/v1/repair/proposals/{proposal['id']}").status_code == 200
    assert proposal["workspace_id"] == tenancy_world.workspace_a


def test_another_workspace_cannot_decide_a_proposal(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    proposal = _propose(tenancy_world.a, job_a)
    # ``job.manage`` in its own workspace: an admin of B, not merely an editor.
    admin_b = tenancy_world.member("admin-b@tenancy.example.com", tenancy_world.workspace_b, "admin")

    for approve in (False, True):
        foreign = admin_b.post(
            f"/api/v1/repair/proposals/{proposal['id']}/decide", json={"approve": approve}
        )
        missing = admin_b.post(
            "/api/v1/repair/proposals/repair_000000000000/decide", json={"approve": approve}
        )
        assert foreign.status_code == missing.status_code == 404, foreign.text
        assert foreign.json() == missing.json() == {"detail": "Proposal not found"}
    stored = _stored(tenancy_world, proposal["id"])
    assert (stored["status"], stored["decided_by"]) == ("proposed", "")

    admin_a = tenancy_world.member("admin-a@tenancy.example.com", tenancy_world.workspace_a, "admin")
    decided = admin_a.post(f"/api/v1/repair/proposals/{proposal['id']}/decide", json={"approve": False})
    assert decided.status_code == 200, decided.text
    assert decided.json()["status"] == "rejected"
    assert decided.json()["decided_by"] == "admin-a@tenancy.example.com"


def test_proposal_cannot_be_planted_on_another_workspaces_job(tenancy_world):  # noqa: F811
    """Naming a job the caller cannot see is the same as naming no real job:
    the proposal stays in the caller's own workspace, out of the owner's queue."""
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    planted = _propose(tenancy_world.b, job_a)
    unknown = _propose(tenancy_world.b, "0123456789abcdef01234567")
    assert planted["workspace_id"] == unknown["workspace_id"] == tenancy_world.workspace_b

    queue_a = tenancy_world.a.get("/api/v1/repair/proposals", params={"job_id": job_a})
    assert queue_a.json() == {"proposals": []}
    queue_b = tenancy_world.b.get("/api/v1/repair/proposals")
    assert {p["id"] for p in queue_b.json()["proposals"]} == {planted["id"], unknown["id"]}


def test_proposal_without_a_job_belongs_to_the_callers_workspace(tenancy_world):  # noqa: F811
    proposal = _propose(tenancy_world.a)
    assert proposal["workspace_id"] == tenancy_world.workspace_a
    assert tenancy_world.b.get(f"/api/v1/repair/proposals/{proposal['id']}").status_code == 404
    assert tenancy_world.a.get(f"/api/v1/repair/proposals/{proposal['id']}").status_code == 200


def _legacy_row(proposal_id: str, job_id: str) -> dict:
    """A proposal exactly as it was stored before the workspace field existed."""
    return {
        "id": proposal_id, "job_id": job_id, "source": "quarantine", "status": "proposed",
        "confidence": "medium", "auto_applicable": False, "summary": f"legacy {SENTINEL}",
        "actions": [], "diagnosis": {}, "created_at": 1_700_000_000.0, "decided_at": 0.0,
        "decided_by": "", "apply_result": {},
    }


def test_legacy_proposals_follow_their_job_and_orphans_are_admin_only(tenancy_world):  # noqa: F811
    job_a = tenancy_world.job(tenancy_world.workspace_a)
    (tenancy_world.data_dir / "agentic_repair_proposals.json").write_text(
        json.dumps({"proposals": [
            _legacy_row("repair_legacy_a", job_a),
            _legacy_row("repair_legacy_orphan", "job-that-no-longer-exists"),
            _legacy_row("repair_legacy_nojob", ""),
        ]}),
        encoding="utf-8",
    )

    # B sees neither: A's follows its job, the orphans are nobody's.
    assert tenancy_world.b.get("/api/v1/repair/proposals").json() == {"proposals": []}
    for pid in ("repair_legacy_a", "repair_legacy_orphan", "repair_legacy_nojob"):
        assert tenancy_world.b.get(f"/api/v1/repair/proposals/{pid}").status_code == 404

    # A sees its own legacy proposal, and not the orphans.
    listed_a = tenancy_world.a.get("/api/v1/repair/proposals").json()["proposals"]
    assert [p["id"] for p in listed_a] == ["repair_legacy_a"]
    assert listed_a[0]["workspace_id"] == tenancy_world.workspace_a
    assert tenancy_world.a.get("/api/v1/repair/proposals/repair_legacy_orphan").status_code == 404
    # The resolved workspace is written back, so it is resolved once.
    assert _stored(tenancy_world, "repair_legacy_a")["workspace_id"] == tenancy_world.workspace_a

    # The platform administrator (default scope: every workspace and the
    # unassigned rows) still reaches all three.
    listed_admin = tenancy_world.admin.get("/api/v1/repair/proposals").json()["proposals"]
    assert {p["id"] for p in listed_admin} == {
        "repair_legacy_a", "repair_legacy_orphan", "repair_legacy_nojob",
    }
    assert tenancy_world.admin.get("/api/v1/repair/proposals/repair_legacy_orphan").status_code == 200


def test_store_scopes_reads_and_decisions_without_a_route(tenancy_world):  # noqa: F811
    """Pilot, MCP and Parity Studio reach the store directly: it refuses too."""
    from services import agentic_repair as ar

    job_a = tenancy_world.job(tenancy_world.workspace_a)
    with tenancy_world.as_workspace(tenancy_world.workspace_a, actor="a@example.com"):
        proposal = ar.propose_from_quarantine(QUARANTINE["rejected_details"], job_id=job_a)
        parity = ar.propose_from_parity(job_id=job_a, fix={"id": "fix-1", "action": "accept", "title": "t"})
    assert proposal.workspace_id == parity.workspace_id == tenancy_world.workspace_a

    with tenancy_world.as_workspace(tenancy_world.workspace_b, actor="b@example.com"):
        assert ar.list_proposals() == []
        assert ar.get_proposal(proposal.id) is None
        try:
            ar.decide_proposal(proposal.id, approve=False, actor="b@example.com")
        except KeyError:
            pass
        else:
            raise AssertionError("another workspace decided the proposal")
        assert ar.record_outcome(parity.id, applied=True, result="x", actor="b@example.com") is None

    # System scope (a background re-sync settling the fix) still closes it.
    ar.decide_proposal(parity.id, approve=True, actor="a@example.com")
    closed = ar.record_outcome(parity.id, applied=True, result="re-synced", actor="a@example.com")
    assert closed is not None and closed.status == "applied"
    assert ar.get_proposal(proposal.id).status == "proposed"


def test_mongo_backed_proposals_are_scoped_and_legacy_rows_adopted(tenancy_world, monkeypatch):  # noqa: F811
    """The replicated (Mongo) backend applies the same scope in its queries."""
    import os
    import uuid

    import pytest

    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient(
        os.environ.get("MONGODB_URI", "mongodb://127.0.0.1:27017"), serverSelectionTimeoutMS=800
    )
    try:
        client.admin.command("ping")
    except pymongo.errors.PyMongoError:
        pytest.skip("MongoDB is not running")
    from services import agentic_repair as ar

    db = client[f"audit_h3_{uuid.uuid4().hex[:10]}"]
    coll = db["repair_proposals"]
    monkeypatch.setattr(ar, "_proposals_coll", lambda: coll)
    try:
        job_a = tenancy_world.job(tenancy_world.workspace_a)
        legacy = _legacy_row("repair_legacy_a", job_a)
        orphan = _legacy_row("repair_legacy_orphan", "job-that-no-longer-exists")
        coll.insert_many([{**legacy, "_id": legacy["id"]}, {**orphan, "_id": orphan["id"]}])
        fresh = _propose(tenancy_world.a, job_a)

        assert tenancy_world.b.get("/api/v1/repair/proposals").json() == {"proposals": []}
        for pid in (fresh["id"], "repair_legacy_a", "repair_legacy_orphan"):
            assert tenancy_world.b.get(f"/api/v1/repair/proposals/{pid}").status_code == 404
        refused = tenancy_world.member(
            "admin-b@tenancy.example.com", tenancy_world.workspace_b, "admin"
        ).post(f"/api/v1/repair/proposals/{fresh['id']}/decide", json={"approve": False})
        assert refused.status_code == 404, refused.text
        assert coll.find_one({"_id": fresh["id"]})["status"] == "proposed"

        listed_a = tenancy_world.a.get("/api/v1/repair/proposals").json()["proposals"]
        assert {p["id"] for p in listed_a} == {fresh["id"], "repair_legacy_a"}
        assert coll.find_one({"_id": "repair_legacy_a"})["workspace_id"] == tenancy_world.workspace_a
        assert "workspace_id" not in coll.find_one({"_id": "repair_legacy_orphan"})
        listed_admin = tenancy_world.admin.get("/api/v1/repair/proposals").json()["proposals"]
        assert {p["id"] for p in listed_admin} == {fresh["id"], "repair_legacy_a", "repair_legacy_orphan"}
    finally:
        client.drop_database(db.name)
        client.close()
