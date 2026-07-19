"""Unscoped jobs must be readable when they appear in list_jobs (Job Theater)."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.routers.connectors_router import _can_access_job


def test_unscoped_job_accessible_under_workspace_isolation(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    req = MagicMock()
    req.state = MagicMock()
    req.state.user = {"email": "ops@example.com"}
    assert _can_access_job(req, {"_id": "abc", "workspace_id": ""}) is True
    assert _can_access_job(req, {"_id": "abc"}) is True


def test_named_workspace_still_gated(monkeypatch):
    monkeypatch.setenv("DATAFLOW_REQUIRE_WORKSPACE", "1")
    req = MagicMock()
    # _actor_email falls back when state has no user
    req.state = MagicMock(spec=[])
    # Without membership, named workspace should deny
    assert _can_access_job(req, {"_id": "abc", "workspace_id": "ws-secret"}) is False
