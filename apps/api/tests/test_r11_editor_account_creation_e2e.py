"""RB3 (QA handover, build 9c75ada4): an editor minted platform logins.

QA: an editor key could add members; the route also created a platform account
for a new email and returned its one-time password to the editor
(src/routers/team_router.py ``post_member``: ``create_account`` was honoured
for anyone ``add_workspace_member`` let through, i.e. ``member.invite``).

Contract: creating an account and receiving its temporary password needs the
workspace admin role. An editor can still bring in people within their own rank
— existing users, or an invite (membership only) for an email with no login.
Checked through the real routes on the live MongoDB (``r9_workspace_world``).
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import PASSWORD, r9  # noqa: E402,F401


def _post(client, ws: str, email: str, *, role: str = "viewer", create: bool = False):
    return client.post(
        f"/api/v1/team/workspaces/{ws}/members",
        json={"email": email, "role": role, "create_account": create},
    )


def test_editor_cannot_create_a_login_or_receive_its_password(r9):
    from services import team_store, user_store

    editor = "editor-rb3@example.test"
    r9.world.member(editor, "editor")
    session = r9.login(editor, PASSWORD, r9.acme)
    key = r9.world.as_key(r9.world.mint_key("editor"))

    for i, client in enumerate((session, key)):
        target = f"brand-new-rb3-{i}@example.test"
        res = _post(client, r9.acme, target, create=True)
        assert res.status_code == 403, res.text
        assert "temporary_password" not in res.text
        assert "admin" in res.json()["detail"].lower()
        # Nothing was half-done: no login, no membership.
        assert user_store.get_user(target) is None
        assert team_store.get_workspace_role(workspace_id=r9.acme, email=target) == ""
    denied = [e for e in r9.world.audit("team.member.denied")
              if e["details"].get("reason") == "account_creation_requires_admin"]
    assert len(denied) == 2


def test_editor_still_adds_existing_users_and_invites_within_rank(r9):
    editor = "editor-rb3b@example.test"
    r9.world.member(editor, "editor")
    session = r9.login(editor, PASSWORD, r9.acme)

    existing = "existing-rb3@example.test"
    r9.world.account(existing)
    added = _post(session, r9.acme, existing, role="editor")
    assert added.status_code == 200, added.text
    assert added.json()["has_account"] is True
    assert added.json()["temporary_password"] is None

    invited = _post(session, r9.acme, "invite-only-rb3@example.test", role="viewer")
    assert invited.status_code == 200, invited.text
    assert invited.json()["has_account"] is False
    assert invited.json()["temporary_password"] is None

    # Above their own rank is still refused.
    assert _post(session, r9.acme, "admin-wanted-rb3@example.test", role="admin").status_code == 403


def test_workspace_admin_creates_the_login_and_gets_the_password(r9):
    res = _post(r9.owner, r9.acme, "admin-made-rb3@example.test", create=True)
    assert res.status_code == 200, res.text
    assert res.json()["has_account"] is True
    assert res.json()["temporary_password"]
