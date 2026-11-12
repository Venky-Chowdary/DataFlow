"""Security review (low): a legacy repair proposal is stamped only when its job has a workspace.

Proposals stored before the workspace field existed take their job's
workspace on first read, and the stamp is written back. A job that had no
workspace yet at that read was stamped ``""`` for good: once the job was
assigned to workspace B, B still could not see the proposal. Now an unassigned
job leaves the row unstamped, so it follows its job on a later read.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


def _stored(world) -> dict:
    rows = json.loads((world.data_dir / "agentic_repair_proposals.json").read_text())["proposals"]
    return next(r for r in rows if r["id"] == "repair_legacy_late")


def test_legacy_proposal_of_an_unassigned_job_follows_the_job_once_it_is_assigned(tenancy_world):  # noqa: F811
    from services.mongodb_service import get_mongodb_service

    job_id = tenancy_world.job("", name="unassigned at first read")
    (tenancy_world.data_dir / "agentic_repair_proposals.json").write_text(json.dumps({"proposals": [{
        "id": "repair_legacy_late", "job_id": job_id, "source": "quarantine", "status": "proposed",
        "confidence": "medium", "auto_applicable": False, "summary": "legacy", "actions": [],
        "diagnosis": {}, "created_at": 1_700_000_000.0, "decided_at": 0.0, "decided_by": "", "apply_result": {},
    }]}), encoding="utf-8")

    # First read while the job has no workspace: unscoped, platform admin only, and not pinned.
    assert tenancy_world.b.get("/api/v1/repair/proposals/repair_legacy_late").status_code == 404
    assert tenancy_world.admin.get("/api/v1/repair/proposals/repair_legacy_late").status_code == 200
    assert "workspace_id" not in _stored(tenancy_world)

    # The job is assigned to B: the proposal follows it.
    get_mongodb_service().update_job_fields(job_id, {"workspace_id": tenancy_world.workspace_b})
    seen = tenancy_world.b.get("/api/v1/repair/proposals/repair_legacy_late")
    assert seen.status_code == 200, seen.text
    assert _stored(tenancy_world)["workspace_id"] == tenancy_world.workspace_b
    assert tenancy_world.a.get("/api/v1/repair/proposals/repair_legacy_late").status_code == 404
