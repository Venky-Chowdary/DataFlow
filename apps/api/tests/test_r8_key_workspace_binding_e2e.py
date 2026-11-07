"""An API key reads only its own workspace; a key with no accountable creator is refused.

aeacb02a: the tenant scope (``services.tenant_scope._base_scope``) gave an API
key its *creator's* memberships, so a key minted for workspace B by someone who
also administers A read A's jobs, connectors, schedules, quarantine, proofs and
audit. A key whose creator has no account and no membership (a deleted or
never-existing identity) still authenticated with its full role.

Every parameterless GET route is swept, as in the D154 cross-tenant sweep, with
A's workspace seeded in every tenant-owned store (``tests/d154_world.py``).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from d154_world import build_world, owner_marker  # noqa: E402
from test_d154_cross_tenant_sweep import _MUST_SHOW_A, _leaks  # noqa: E402


def _mint(client, workspace_id: str, role: str = "admin") -> str:
    response = client.post(
        "/api/v1/workspace/api-keys",
        headers={"X-Workspace-Id": workspace_id},
        json={"name": f"bound-{role}", "role": role, "expires_in": "7d"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["workspace_id"] == workspace_id
    return response.json()["key"]


@pytest.mark.parametrize("signup", [True, False], ids=["multi-tenant", "single-tenant"])
def test_key_bound_to_b_never_reads_a_even_when_its_creator_administers_both(
    tmp_path, monkeypatch, signup
):
    world = build_world(tmp_path, monkeypatch, signup=signup)
    from services import team_store
    from src.main import app
    from fastapi.testclient import TestClient

    # Alice administers A (seeded) and B.
    for ws in (world.ws_a, world.ws_b):
        team_store._write_membership(
            team_store.Membership(workspace_id=ws, email=world.a_email, role="admin")
        )
    marker = owner_marker("a", world.tag)
    # Positive control: Alice herself sees A, and a key bound to A sees A.
    a_key = TestClient(app, headers={"Authorization": f"Bearer {_mint(world.a, world.ws_a)}"})
    shown = a_key.get("/api/v1/connectors/saved")
    assert shown.status_code == 200 and marker in shown.text, shown.text

    b_key = TestClient(app, headers={"Authorization": f"Bearer {_mint(world.a, world.ws_b)}"})
    for headers in ({}, {"X-Workspace-Id": world.ws_b}, {"X-Workspace-Id": world.ws_a}):
        leaks = _leaks(world, b_key, headers, (marker, world.ws_a))
        assert leaks == [], (headers, leaks)
    # The routes that provably carry A's seeds answer A's own key with them.
    for path in _MUST_SHOW_A[:4]:
        response = a_key.get(path)
        assert response.status_code == 200 and marker in response.text, (path, response.text[:300])
    # Naming A is refused outright, not silently widened.
    forced = b_key.get("/api/v1/connectors/saved", headers={"X-Workspace-Id": world.ws_a})
    assert forced.status_code in (403, 404), forced.text


def test_key_whose_creator_is_no_identity_is_refused(tmp_path, monkeypatch):
    world = build_world(tmp_path, monkeypatch, signup=True)
    from fastapi.testclient import TestClient

    from services import integrations_store, user_store
    from src.main import app

    ghost = integrations_store.create_api_key(
        "orphan", "ghost-deleted@example.com", role="admin", workspace_id=world.ws_a
    )
    c = TestClient(app, headers={"Authorization": f"Bearer {ghost['key']}"})
    for path in ("/api/v1/connectors/saved", "/api/v1/connectors/jobs", "/api/v1/audit/events"):
        response = c.get(path)
        assert response.status_code == 401, (path, response.status_code, response.text[:200])
        assert owner_marker("a", world.tag) not in response.text

    # A key minted by an account that is later disabled stops working too.
    key = _mint(world.a, world.ws_a, role="editor")
    k = TestClient(app, headers={"Authorization": f"Bearer {key}"})
    assert k.get("/api/v1/connectors/saved").status_code == 200
    user_store.update_user(email=world.a_email, status="disabled")
    assert k.get("/api/v1/connectors/saved").status_code == 401
