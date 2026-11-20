"""U2: every REST error body carries ``error.reference``, in one envelope.

QA: GET /api/v1/jobs/000... answered ``{"detail": "Job not found"}`` with no
reference. 8d2b01127/1d73523ea added the reference to the HTTPException
handler, proven on a toy FastAPI app. This sweep runs the real app with
authentication enforced and reaches each status through a real route or
middleware: 400, 401 (keeps WWW-Authenticate), 403, 404 (a missing job and
an unknown route), 405, 409, 422 (validation), 500. ``detail`` keeps its
shape (string or object).
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from r8_ent_harness import ent  # noqa: F401 - fixture


def _ref(res) -> str:
    """The reference, from the envelope; a legacy string ``error`` code (read by
    existing clients, e.g. the 500's "Internal Server Error" pinned by
    test_safe_errors_r3) keeps its shape and carries ``reference`` (the 500:
    ``correlation_id``) beside it."""
    body = res.json()
    error = body.get("error")
    if isinstance(error, str):
        reference = str(body.get("reference") or body.get("correlation_id") or "")
    else:
        assert isinstance(error, dict), (res.status_code, body)
        assert error.get("code"), (res.status_code, body)
        reference = str(error.get("reference") or "")
    assert reference, (res.status_code, body)
    # The reference is the one the server log and X-Correlation-ID carry.
    assert res.headers.get("X-Correlation-ID") == reference or not res.headers.get("X-Correlation-ID"), (
        reference, res.headers.get("X-Correlation-ID"))
    return reference


def test_every_error_status_carries_a_reference(ent, monkeypatch):
    admin = ent.admin
    ws = ent.workspace("U2 sweep")
    viewer = ent.member(email=f"u2-viewer-{uuid.uuid4().hex[:6]}@example.com", workspace_id=ws, role="viewer")

    seen: dict[int, str] = {}

    res = admin.post("/api/v1/team/users", json={"email": "not-an-email"})
    assert res.status_code == 400, res.text
    seen[400] = _ref(res)

    res = ent.anonymous().get("/api/v1/connectors/saved")
    assert res.status_code == 401, res.text
    seen[401] = _ref(res)
    assert res.headers.get("WWW-Authenticate"), dict(res.headers)

    res = viewer.post("/api/v1/team/workspaces", json={"name": "not allowed"})
    assert res.status_code == 403, res.text
    seen[403] = _ref(res)

    res = admin.get("/api/v1/jobs/000000000000000000000000")
    assert res.status_code == 404, res.text
    seen[404] = _ref(res)
    assert isinstance(res.json().get("detail"), (str, dict))

    res = admin.get(f"/api/v1/no-such-route-{uuid.uuid4().hex[:6]}")
    assert res.status_code == 404, res.text
    _ref(res)

    res = admin.get("/api/v1/transfer/run")
    assert res.status_code == 405, res.text
    seen[405] = _ref(res)

    from services import workspace_teardown

    def _busy(*_a, **_k):
        raise workspace_teardown.WorkspaceBusy("A transfer in this workspace is still running.")

    monkeypatch.setattr(workspace_teardown, "teardown_workspace", _busy)
    res = admin.delete(f"/api/v1/team/workspaces/{ws}", params={"dry_run": "true"})
    assert res.status_code == 409, res.text
    seen[409] = _ref(res)

    res = admin.post("/api/v1/team/workspaces", json={})
    assert res.status_code == 422, res.text
    seen[422] = _ref(res)
    assert isinstance(res.json().get("detail"), list)  # validation detail keeps its list shape

    from src.routers import team_router

    def _boom(*_a, **_k):
        raise RuntimeError("synthetic failure for the sweep")

    monkeypatch.setattr(team_router, "list_workspaces", _boom)
    crash = TestClient(ent.app, raise_server_exceptions=False)
    crash.headers.update(admin.headers)
    res = crash.get("/api/v1/team/workspaces")
    assert res.status_code == 500, res.text
    seen[500] = _ref(res)
    assert "synthetic failure" not in res.text

    assert sorted(seen) == [400, 401, 403, 404, 405, 409, 422, 500]
