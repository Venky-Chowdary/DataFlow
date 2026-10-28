"""D164: the session check (GET /auth/me) does not echo or act on a workspace the
caller does not belong to."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import World, build_world  # noqa: E402


@pytest.fixture(params=["multi_tenant", "legacy_single_tenant"])
def world(request, tmp_path, monkeypatch) -> World:
    return build_world(tmp_path, monkeypatch, signup=request.param == "multi_tenant")


def test_session_check_does_not_echo_a_workspace_the_user_is_not_in(world):
    forced = world.b.get("/api/v1/auth/me", headers={"X-Workspace-Id": world.ws_a}).json()
    # Answered as if no header were sent: B's own single membership, never A's.
    assert forced["workspace_id"] == world.ws_b
    assert world.ws_a not in json.dumps(forced)
    assert forced["workspace_header_rejected"] is True
    own = world.b.get("/api/v1/auth/me", headers={"X-Workspace-Id": world.ws_b}).json()
    assert own["workspace_id"] == world.ws_b and own["workspace_header_rejected"] is False
    # A platform admin may name any workspace.
    admin = world.admin.get("/api/v1/auth/me", headers={"X-Workspace-Id": world.ws_a}).json()
    assert admin["workspace_id"] == world.ws_a
