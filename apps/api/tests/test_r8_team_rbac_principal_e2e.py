"""Team routes decide with the caller's own effective principal (QA enterprise #24, #25).

Production 557f556c: the team routes read the *key creator's* membership, so a
viewer API key minted by the owner could add members and change roles, and an
editor key demoted the owner to viewer. ``_assert_not_last_admin`` counted an
invited admin who has no account, so that demotion left no admin who can sign
in. Every request here goes through the real REST routes with real keys.
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import OWNER, world  # noqa: E402,F401


def _denial(world, *, target: str, attempted: str | None) -> dict:
    events = world.audit("team.member.denied")
    assert events, "refusal was not audited"
    event = events[-1]
    details = event["details"]
    assert details["target"] == target
    if attempted is not None:
        assert details["attempted_role"] == attempted
    principal = details["principal"]
    assert principal["kind"] and "effective_role" in principal
    assert event["level"] == "warn"
    return details


def test_viewer_and_operator_keys_are_refused_on_every_team_mutation(world):
    world.member("alice@acme.test", "viewer")
    for role in ("viewer", "operator"):
        key = world.as_key(world.mint_key(role))
        added = world.add_member(key, f"new-{role}@example.com", "viewer")
        assert added.status_code == 403, (role, added.text)
        details = _denial(world, target=f"new-{role}@example.com", attempted="viewer")
        assert details["principal"]["kind"] == "api_key"
        assert details["principal"]["effective_role"] == role
        promoted = world.set_role(key, "alice@acme.test", "editor")
        assert promoted.status_code == 403, (role, promoted.text)
        _denial(world, target="alice@acme.test", attempted="editor")
        removed = world.remove(key, "alice@acme.test")
        assert removed.status_code == 403, (role, removed.text)
        _denial(world, target="alice@acme.test", attempted=None)
    assert world.role_of("alice@acme.test") == "viewer"
    assert world.role_of("new-viewer@example.com") == ""


def test_editor_key_cannot_grant_admin_promote_or_demote_the_owner(world):
    """QA's exact repro: invite as admin, promote to admin, demote the owner."""
    world.member("bob@acme.test", "editor")
    editor = world.as_key(world.mint_key("editor"))

    invited = world.add_member(editor, "qa-e2@example.com", "admin")
    assert invited.status_code == 403, invited.text
    _denial(world, target="qa-e2@example.com", attempted="admin")

    promoted = world.set_role(editor, "bob@acme.test", "admin")
    assert promoted.status_code == 403, promoted.text
    _denial(world, target="bob@acme.test", attempted="admin")

    demoted = world.set_role(editor, OWNER, "viewer")
    assert demoted.status_code == 403, demoted.text
    _denial(world, target=OWNER, attempted="viewer")

    removed = world.remove(editor, OWNER)
    assert removed.status_code == 403, removed.text

    assert world.role_of(OWNER) == "admin"
    assert world.role_of("bob@acme.test") == "editor"
    assert world.role_of("qa-e2@example.com") == ""
    # An editor still brings in a peer at or below its own role.
    assert world.add_member(editor, "peer@example.com", "viewer").status_code == 200


def test_key_minted_by_a_platform_admin_keeps_its_own_role_on_team_routes(world):
    """The creator's platform role must not leak into a lesser key."""
    viewer = world.as_key(world.mint_key("viewer", by=world.root))
    assert world.add_member(viewer, "x@example.com", "viewer").status_code == 403
    assert world.set_role(viewer, OWNER, "viewer").status_code == 403
    # …nor into the deployment-level account routes.
    made = viewer.post("/api/v1/team/users", json={"email": "evil@example.com", "platform_role": "admin"})
    assert made.status_code == 403, made.text
    assert world.role_of(OWNER) == "admin"


def test_key_role_is_capped_at_its_creators_current_role(world):
    world.member("carol@acme.test", "admin")
    carol = world.login("carol@acme.test", "Member-Password-2026!")
    carol_admin_key = world.as_key(world.mint_key("admin", by=carol))
    assert world.add_member(carol_admin_key, "first@example.com", "viewer").status_code == 200

    assert world.set_role(world.owner, "carol@acme.test", "viewer").status_code == 200
    # Carol is a viewer now; the admin key she minted can do no more than she can.
    added = world.add_member(carol_admin_key, "second@example.com", "viewer")
    assert added.status_code == 403, added.text
    assert _denial(world, target="second@example.com", attempted="viewer")["principal"][
        "effective_role"
    ] == "viewer"
    settings = carol_admin_key.patch("/api/v1/workspace/settings", json={"org_name": "pwned"})
    assert settings.status_code == 403, settings.text


def test_scoped_service_account_is_held_to_its_scopes_on_team_routes(world):
    sa = world.as_key(world.mint_service_account("admin", ["connector.read", "workspace.read"]))
    added = world.add_member(sa, "sa-added@example.com", "viewer")
    assert added.status_code == 403, added.text
    details = _denial(world, target="sa-added@example.com", attempted="viewer")
    assert details["principal"]["kind"] == "service_account"
    assert world.set_role(sa, OWNER, "viewer").status_code == 403
    assert world.role_of(OWNER) == "admin"


def test_admin_key_manages_roles_within_its_own_rank(world):
    world.member("dan@acme.test", "viewer")
    admin = world.as_key(world.mint_key("admin"))
    assert world.set_role(admin, "dan@acme.test", "editor").status_code == 200
    assert world.role_of("dan@acme.test") == "editor"
    # Role management needs admin: an editor key cannot re-role even downwards.
    editor = world.as_key(world.mint_key("editor"))
    assert world.set_role(editor, "dan@acme.test", "viewer").status_code == 403
    assert world.role_of("dan@acme.test") == "editor"


def test_last_admin_who_can_sign_in_cannot_be_demoted_or_removed(world):
    """An invited admin with no account cannot sign in, so it is not a spare admin."""
    world.member("qa-e2@example.com", "admin", with_account=False)
    admin = world.as_key(world.mint_key("admin"))

    demoted = world.set_role(admin, OWNER, "viewer")
    assert demoted.status_code == 409, demoted.text
    details = _denial(world, target=OWNER, attempted="viewer")
    assert details["reason"] == "last_admin"
    removed = world.remove(world.root, OWNER)
    assert removed.status_code == 409, removed.text
    assert world.role_of(OWNER) == "admin"
    # The admin who cannot sign in may go; the owner stays.
    assert world.remove(admin, "qa-e2@example.com").status_code == 200


def test_concurrent_demotions_never_leave_no_admin_who_can_sign_in(world):
    world.member("erin@acme.test", "admin")
    world.member("qa-e2@example.com", "admin", with_account=False)
    root = world.root
    results: dict[str, int] = {}
    barrier = threading.Barrier(2)

    def demote(email: str) -> None:
        c = world.client(root.headers["Authorization"].split(" ", 1)[1])
        barrier.wait()
        results[email] = world.set_role(c, email, "viewer").status_code

    threads = [threading.Thread(target=demote, args=(e,)) for e in (OWNER, "erin@acme.test")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results.values()) == [200, 409], results
    survivors = [e for e in (OWNER, "erin@acme.test") if world.role_of(e) == "admin"]
    assert len(survivors) == 1, results
