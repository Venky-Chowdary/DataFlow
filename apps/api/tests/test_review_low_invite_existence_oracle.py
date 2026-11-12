"""Security review (low): another workspace's invitation id answers like a missing one.

``POST .../invites/{id}/resend`` and ``DELETE .../invites/{id}`` looked the
invitation up before asking whether the caller belongs to the workspace, so an
outsider got 403 for a real invitation id and 404 for an unknown one. Both now
answer the same 404 to anyone with no role in the workspace.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from tenancy_world import tenancy_world  # noqa: E402,F401


@pytest.mark.parametrize("method,suffix", [("POST", "/resend"), ("DELETE", "")])
def test_outsider_gets_the_same_404_for_a_real_and_a_missing_invite(tenancy_world, method, suffix):  # noqa: F811
    from services import workspace_invites

    world = tenancy_world
    admin_a = world.member("admin-a@invite.example.com", world.workspace_a, "admin")
    admin_b = world.member("admin-b@invite.example.com", world.workspace_b, "admin")
    made = admin_a.post(f"/api/v1/team/workspaces/{world.workspace_a}/invites",
                        json={"email": "new@invite.example.com", "role": "viewer"})
    assert made.status_code == 200, made.text
    invite_id = made.json()["invite"]["id"]
    base = f"/api/v1/team/workspaces/{world.workspace_a}/invites"

    real = admin_b.request(method, f"{base}/{invite_id}{suffix}")
    missing = admin_b.request(method, f"{base}/inv_000000000000000000000000{suffix}")
    assert (real.status_code, real.json()) == (missing.status_code, missing.json()), (real.text, missing.text)
    assert real.status_code == 404
    assert workspace_invites.store.get(invite_id)["status"] == "pending"

    # A member of A without the right to manage invitations is still told why.
    viewer_a = world.member("viewer-a@invite.example.com", world.workspace_a, "viewer")
    assert viewer_a.request(method, f"{base}/{invite_id}{suffix}").status_code == 403
