"""Id-addressed job reads follow the same tenant scope as ``list_jobs``.

D154: unscoped (legacy) jobs used to be readable by every account because the
list endpoint OR-ed them into every workspace. Under isolation the list no
longer returns them to non-admins, so the id-addressed read must not either.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.routers.connectors_router import _can_access_job


def test_unscoped_job_hidden_from_non_admin_under_workspace_isolation(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    req = MagicMock()
    req.state = MagicMock(spec=[])
    req.state.user_email = "ops@example.com"
    req.state.user = {"email": "ops@example.com", "role": "member"}
    assert _can_access_job(req, {"_id": "abc", "workspace_id": ""}) is False
    assert _can_access_job(req, {"_id": "abc"}) is False

    admin = MagicMock()
    admin.state = MagicMock(spec=[])
    admin.state.user_email = "root@example.com"
    admin.state.user = {"email": "root@example.com", "role": "admin"}
    assert _can_access_job(admin, {"_id": "abc", "workspace_id": ""}) is True


def test_named_workspace_still_gated(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    req = MagicMock()
    # _actor_email falls back when state has no user
    req.state = MagicMock(spec=[])
    # Without membership, named workspace should deny
    assert _can_access_job(req, {"_id": "abc", "workspace_id": "ws-secret"}) is False
