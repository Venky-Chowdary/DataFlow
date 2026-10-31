"""``GET /api/v1/jobs`` for every role, on the Mongo job store (QA enterprise #50).

QA (production 557f556c): ``GET /api/v1/jobs`` answered ``403 No RBAC rule is
configured`` for viewer, operator, editor and admin keys alike. Root cause: no
route existed at that path (jobs live under ``/api/v1/connectors/jobs``) and
``services.rbac`` fails closed on an unruled path before routing can 404. The
path is now a read-only alias of the jobs handlers, gated by ``job.read`` (a
permission every role holds) and scoped to the caller's workspace.
"""

from __future__ import annotations

import pytest

from r8_ent_harness import ent  # noqa: F401


def _job(ent, workspace_id: str, name: str) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job(
        {"name": name, "workspace_id": workspace_id, "source": {"type": "csv"},
         "destination": {"type": "sqlite"}}
    )


@pytest.mark.parametrize("role", ["viewer", "operator", "editor", "admin"])
def test_every_role_reads_its_workspace_jobs(ent, role):
    ws = ent.workspace("Jobs RBAC")
    other = ent.workspace("Jobs elsewhere")
    mine = _job(ent, ws, "mine")
    theirs = _job(ent, other, "theirs")
    key_role = role
    client = ent.bearer(ent.api_key(role=key_role, workspace_id=ws), workspace_id=ws)

    listed = client.get("/api/v1/jobs")
    assert listed.status_code == 200, listed.text
    ids = {str(j.get("id") or j.get("_id") or j.get("job_id")) for j in listed.json()["jobs"]}
    assert mine in ids
    assert theirs not in ids
    assert listed.json()["total"] == 1

    one = client.get(f"/api/v1/jobs/{mine}")
    assert one.status_code == 200, one.text
    assert client.get(f"/api/v1/jobs/{theirs}").status_code == 404


def test_member_session_reads_jobs(ent):
    ws = ent.workspace("Jobs session")
    _job(ent, ws, "session job")
    viewer = ent.member(email="jobs-viewer-r8@example.com", workspace_id=ws, role="viewer")
    res = viewer.get("/api/v1/jobs")
    assert res.status_code == 200, res.text
    assert res.json()["total"] == 1
    assert ent.anonymous().get("/api/v1/jobs").status_code == 401
