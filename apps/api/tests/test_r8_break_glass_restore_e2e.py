"""Audited break-glass: a platform admin restores a workspace admin (QA enterprise, owner action).

On production an editor key demoted the owner to viewer and nobody with a login
was admin any more, so the API could not undo it and support would have had to
edit the database. ``POST /team/workspaces/{id}/break-glass/restore-admin`` is
the recovery path: platform administrators only, signed in as themselves (no API
key), a written reason, a target who can actually sign in, and an audit event
for every attempt — granted or refused.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import OWNER, PASSWORD, ROOT, world  # noqa: E402,F401

REASON = "QA demoted the owner with a temporary editor key (ticket SUP-1030)"


def _qa_lockout(world) -> dict:
    """Production's state after QA: owner is viewer, the only admin cannot sign in."""
    owner_admin_key = world.mint_key("admin")  # minted while the owner was still admin
    world.member("qa-e2@example.com", "admin", with_account=False)
    world.member(OWNER, "viewer", with_account=False)
    assert world.role_of(OWNER) == "viewer"
    return owner_admin_key


def _restore(c, world, email: str, reason: str | None = REASON):
    body = {"email": email}
    if reason is not None:
        body["reason"] = reason
    return c.post(
        f"/api/v1/team/workspaces/{world.workspace_id}/break-glass/restore-admin", json=body
    )


def test_platform_admin_restores_the_owner_and_it_is_audited(world):
    _qa_lockout(world)
    restored = _restore(world.root, world, OWNER)
    assert restored.status_code == 200, restored.text
    assert restored.json()["membership"]["role"] == "admin"
    assert world.role_of(OWNER) == "admin"

    event = world.audit("team.break_glass.restore_admin")[-1]
    assert event["actor"] == ROOT
    assert event["level"] == "warn"
    details = event["details"]
    assert details["target"] == OWNER
    assert details["from_role"] == "viewer"
    assert details["to_role"] == "admin"
    assert details["reason"] == REASON
    assert details["workspace_id"] == world.workspace_id
    assert details["principal"]["kind"] == "session"

    # The owner is back in charge and can clean up QA's leftovers from the app.
    owner = world.login(OWNER, "Owner-Password-2026!")
    assert world.remove(owner, "qa-e2@example.com").status_code == 200


def test_nobody_else_can_use_break_glass(world):
    owner_admin_key = _qa_lockout(world)
    # The demoted owner cannot restore themselves…
    assert world.set_role(world.owner, OWNER, "admin").status_code == 403
    assert _restore(world.owner, world, OWNER).status_code == 403
    # …nor with the admin key they minted before the demotion (capped at viewer)…
    key = world.as_key(owner_admin_key)
    assert world.set_role(key, OWNER, "admin").status_code == 403
    assert _restore(key, world, OWNER).status_code == 403
    # …nor can an admin of another workspace, nor any API key — even one a
    # platform admin minted: break-glass needs the person, signed in.
    from services import team_store

    other = team_store.create_workspace(name="Elsewhere", created_by="lead@other.test")
    world.account("lead@other.test")
    lead = world.login("lead@other.test", PASSWORD)
    lead.headers["X-Workspace-Id"] = other.id
    assert _restore(lead, world, OWNER).status_code == 403
    root_key = world.as_key(world.mint_key("admin", by=world.root))
    assert _restore(root_key, world, OWNER).status_code == 403

    assert world.role_of(OWNER) == "viewer"
    denied = world.audit("team.break_glass.denied")
    assert len(denied) >= 4
    assert {d["details"]["principal"]["kind"] for d in denied} == {"session", "api_key"}
    assert all(d["details"]["target"] == OWNER for d in denied)


def test_break_glass_requires_a_reason_and_a_target_who_can_sign_in(world):
    _qa_lockout(world)
    assert _restore(world.root, world, OWNER, reason=None).status_code == 422
    assert _restore(world.root, world, OWNER, reason="  ").status_code == 422
    # Restoring an admin who cannot log in would not end the lockout.
    no_login = _restore(world.root, world, "qa-ghost@example.com")
    assert no_login.status_code == 400, no_login.text
    assert "sign in" in no_login.json()["detail"]
    assert world.role_of("qa-ghost@example.com") == ""
    assert world.audit("team.break_glass.denied")[-1]["details"]["reason_code"] == "target_cannot_sign_in"
    missing = world.root.post(
        "/api/v1/team/workspaces/does-not-exist/break-glass/restore-admin",
        json={"email": OWNER, "reason": REASON},
    )
    assert missing.status_code == 404
