"""Workspace API keys honour the scopes they were minted with (QA enterprise #29).

Production 557f556c: ``POST /workspace/api-keys`` with ``scopes=[connector.read]``
stored ``null`` and the key had full editor permissions. Service-account scopes
already worked; workspace keys now carry and enforce scopes the same way, and
unknown scopes are refused.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import world  # noqa: E402,F401


def test_scoped_workspace_key_is_stored_listed_and_enforced(world):
    key = world.mint_key("editor", scopes=["connector.read"])
    assert key["scopes"] == ["connector.read"]
    listed = next(
        k for k in world.owner.get("/api/v1/workspace/api-keys").json()["keys"] if k["id"] == key["id"]
    )
    assert listed["scopes"] == ["connector.read"]

    c = world.as_key(key)
    assert c.get("/api/v1/connectors/saved").status_code == 200
    write = c.post("/api/v1/connectors/saved", json={"name": "x", "type": "postgresql"})
    assert write.status_code == 403, write.text
    assert write.json()["required_permission"] == "connector.write"
    assert c.get("/api/v1/schedules/").status_code == 403
    assert world.add_member(c, "scoped@example.com", "viewer").status_code == 403

    # The same role without scopes still writes (auth passes; body is validated).
    unscoped = world.as_key(world.mint_key("editor"))
    assert unscoped.post("/api/v1/connectors/saved", json={}).status_code != 403


def test_unknown_empty_or_out_of_role_scopes_are_refused(world):
    for scopes, needle in (
        (["connector.read", "connector.everything"], "unknown scope"),
        ([], "no scopes"),
        (["connector.write"], "outside the viewer role"),
    ):
        response = world.owner.post(
            "/api/v1/workspace/api-keys",
            json={"name": "bad", "role": "viewer", "scopes": scopes, "expires_in": "7d"},
        )
        assert response.status_code == 400, (scopes, response.text)
        assert needle in response.json()["detail"], response.text
    assert not [k for k in world.owner.get("/api/v1/workspace/api-keys").json()["keys"] if k["name"] == "bad"]


def test_scoped_key_cannot_mint_a_key_with_more_than_its_scopes(world):
    minter = world.as_key(world.mint_key("admin", scopes=["iam.manage", "connector.read"]))
    response = minter.post(
        "/api/v1/workspace/api-keys",
        json={"name": "wider", "role": "editor", "scopes": ["connector.write"], "expires_in": "7d"},
    )
    assert response.status_code == 403, response.text
    narrower = minter.post(
        "/api/v1/workspace/api-keys",
        json={"name": "narrow", "role": "viewer", "scopes": ["connector.read"], "expires_in": "7d"},
    )
    assert narrower.status_code == 200, narrower.text
    assert narrower.json()["scopes"] == ["connector.read"]
