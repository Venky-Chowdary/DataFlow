"""C6 (QA r14): member administration is the workspace admin's, for sessions and keys.

QA (Oct 30 / Oct 31 rerun): an editor-role API key added a workspace member
(HTTP 200). Root cause: ``services.rbac`` gave the editor role
``member.invite`` and ``services.team_store._authorize_change`` let any holder
of it add or remove a non-admin member. The secure default adopted in round 14:
only a workspace admin (the role older builds named ``owner``) adds, removes or
re-roles members; editors, operators and viewers cannot, whether they sign in
or present an API key — judged by the key's *own* role, never its creator's.

Every request goes through the real REST routes, on the file store and on the
live MongoDB (``r8_security_world``). Every refusal is an audited
``team.member.denied`` naming the principal.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import OWNER, PASSWORD, world  # noqa: E402,F401

NON_ADMIN = ("editor", "operator", "viewer")


def _callers(world, role: str):
    """A session member with ``role`` and an owner-minted key with ``role``."""
    email = f"{role}-c6@acme.test"
    world.member(email, role)
    return {
        "session": world.login(email, PASSWORD),
        # Minted by the owner (an admin): the key must not borrow that authority.
        "api_key": world.as_key(world.mint_key(role)),
    }


@pytest.mark.parametrize("role", NON_ADMIN)
def test_non_admin_sessions_and_keys_cannot_add_remove_or_rerole(world, role):
    world.member("target-c6@acme.test", "viewer")
    for kind, caller in _callers(world, role).items():
        new = f"new-{role}-{kind}@example.com"
        for wanted in ("viewer", "editor", "admin"):
            added = world.add_member(caller, new, wanted)
            assert added.status_code == 403, (kind, wanted, added.text)
        assert world.role_of(new) == ""
        assert world.set_role(caller, "target-c6@acme.test", "editor").status_code == 403
        assert world.remove(caller, "target-c6@acme.test").status_code == 403
        # The owner's role cannot be touched either.
        assert world.set_role(caller, OWNER, "viewer").status_code == 403
        assert world.remove(caller, OWNER).status_code == 403
        denied = [
            e for e in world.audit("team.member.denied")
            if e["details"]["target"] == new
        ]
        assert denied, "refusal was not audited"
        principal = denied[-1]["details"]["principal"]
        assert principal["kind"] == ("session" if kind == "session" else "api_key")
        # ``operator`` is a key role only; a membership stores admin/editor/viewer.
        expected = "viewer" if (kind == "session" and role == "operator") else role
        assert principal["effective_role"] == expected
    assert world.role_of("target-c6@acme.test") == "viewer"
    assert world.role_of(OWNER) == "admin"


def test_editor_refusal_names_the_admin_role(world):
    editor = _callers(world, "editor")["api_key"]
    refused = world.add_member(editor, "qa-c6@example.com", "viewer")
    assert refused.status_code == 403
    assert "workspace admin role" in refused.json()["detail"]


def test_admin_session_and_admin_key_manage_members(world):
    admin_key = world.as_key(world.mint_key("admin"))
    for caller, email in ((world.owner, "by-owner@example.com"), (admin_key, "by-key@example.com")):
        assert world.add_member(caller, email, "viewer").status_code == 200
        assert world.set_role(caller, email, "editor").status_code == 200
        assert world.role_of(email) == "editor"
        assert world.remove(caller, email).status_code == 200
        assert world.role_of(email) == ""
    assert world.audit("team.member.add") and world.audit("team.member.remove")


def test_admin_key_minted_by_an_editor_has_no_member_authority(world):
    """A key is capped at its creator's role: an editor cannot mint its way in."""
    world.member("ed-c6@acme.test", "editor")
    editor = world.login("ed-c6@acme.test", PASSWORD)
    minted = editor.post(
        "/api/v1/workspace/api-keys", json={"name": "esc", "role": "admin", "expires_in": "7d"}
    )
    # Minting keys is IAM administration: an editor gets no key at all.
    assert minted.status_code == 403, minted.text
    assert "key" not in minted.json()
    assert world.add_member(editor, "esc-c6@example.com", "viewer").status_code == 403
    assert world.role_of("esc-c6@example.com") == ""


def test_editor_and_operator_roles_no_longer_hold_member_invite():
    from services.rbac import Permission, role_permissions

    assert Permission.MEMBER_INVITE in role_permissions("admin")
    for role in NON_ADMIN:
        assert Permission.MEMBER_INVITE not in role_permissions(role)
        assert Permission.WORKSPACE_MANAGE not in role_permissions(role)
