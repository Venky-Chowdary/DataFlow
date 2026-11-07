"""A notification channel belongs to the workspace it was created in (QA enterprise #45).

Production 557f556c: ``POST /workspace/notifications`` without a body
``workspace_id`` stored ``workspace_id=''``, so ``GET /workspace/notifications``
(scoped to the caller's workspace) answered ``channels: []``. An unscoped
channel was then editable and deletable by any workspace admin.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import PASSWORD, world  # noqa: E402,F401

HOOK = {"kind": "webhook", "label": "qa-hook", "config": {"url": "https://example.com/hook"}}


def _listed(c) -> list[dict]:
    response = c.get("/api/v1/workspace/notifications")
    assert response.status_code == 200, response.text
    return response.json()["channels"]


def test_created_channel_is_stamped_and_listed(world):
    created = world.owner.post("/api/v1/workspace/notifications", json=HOOK)
    assert created.status_code == 200, created.text
    assert created.json()["workspace_id"] == world.workspace_id
    assert [c["id"] for c in _listed(world.owner)] == [created.json()["id"]]

    key = world.as_key(world.mint_key("admin"))
    by_key = key.post("/api/v1/workspace/notifications", json={**HOOK, "label": "by-key"})
    assert by_key.status_code == 200, by_key.text
    assert by_key.json()["workspace_id"] == world.workspace_id
    assert {c["label"] for c in _listed(key)} == {"qa-hook", "by-key"}


def test_channels_of_other_workspaces_and_unscoped_ones_are_not_writable(world):
    from services import notification_store, team_store

    other = team_store.create_workspace(name="Other", created_by="boss@other.test")
    world.account("boss@other.test")
    boss = world.login("boss@other.test", PASSWORD)
    boss.headers["X-Workspace-Id"] = other.id
    theirs = boss.post("/api/v1/workspace/notifications", json=HOOK).json()
    assert theirs["workspace_id"] == other.id
    assert _listed(world.owner) == []
    # Not writable, and answered like a missing id so the id is not confirmed (D222).
    assert world.owner.patch(
        f"/api/v1/workspace/notifications/{theirs['id']}", json={"label": "pwned"}
    ).status_code == 404
    assert world.owner.delete(f"/api/v1/workspace/notifications/{theirs['id']}").status_code == 404
    # Naming another workspace in the body is refused, not silently stamped.
    named = world.owner.post("/api/v1/workspace/notifications", json={**HOOK, "workspace_id": other.id})
    assert named.status_code == 403

    legacy = notification_store.create_channel(
        workspace_id="", kind="webhook", label="legacy", config={"url": "https://example.com/x"}
    )
    assert world.owner.patch(
        f"/api/v1/workspace/notifications/{legacy.id}", json={"label": "pwned"}
    ).status_code == 404
    assert world.owner.delete(f"/api/v1/workspace/notifications/{legacy.id}").status_code == 404
    assert world.root.delete(f"/api/v1/workspace/notifications/{legacy.id}").status_code == 200
