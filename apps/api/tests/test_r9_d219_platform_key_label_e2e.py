"""QA D219: the platform key says what it is.

An ``admin`` key a platform admin minted with no workspace is unrestricted — it
reads and acts in every workspace (``tenant_scope.actor_is_unrestricted``) —
yet the key list showed it like any other key. Keys minted in a workspace (by
anyone, including a platform admin) are held to that workspace. The fix is only the label:
every listed (and freshly minted) key now carries ``access`` and
``access_label`` that match what the key can actually do.

Through the real REST routes on the live MongoDB: the label is checked against
the key's real reach (a request into another tenant's workspace).
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401


def _listed(c, key_id: str) -> dict:
    response = c.get("/api/v1/workspace/api-keys")
    assert response.status_code == 200, response.text
    return next(k for k in response.json()["keys"] if k["id"] == key_id)


def test_platform_key_is_labelled_platform_wide_and_scoped_keys_workspace_only(r9):
    root = r9.world.root  # header: Acme
    root_platform = r9.login("root@example.test", "Bootstrap-Admin-2026!", None)
    platform_key = r9.world.mint_key("admin", by=root_platform, name="ops-automation")
    owner_key = r9.world.mint_key("admin", name="acme-admin")
    editor_key = r9.world.mint_key("editor", by=root, name="root-editor")
    root_in_acme_key = r9.world.mint_key("admin", by=root, name="root-in-acme")

    assert platform_key["access"] == "platform"
    assert "every workspace" in platform_key["access_label"].lower()
    for listing_client in (root,):
        row = _listed(listing_client, platform_key["id"])
        assert row["access"] == "platform"
        assert row["access_label"] == platform_key["access_label"]
    for key in (owner_key, editor_key, root_in_acme_key):
        assert key["access"] == "workspace"
        row = _listed(r9.owner, key["id"])
        assert row["access"] == "workspace"
        assert "this workspace" in row["access_label"].lower()
        assert "every workspace" not in row["access_label"].lower()

    # The label is the truth: the platform key reaches another tenant, the
    # workspace keys do not.
    def into_bravo(key: dict):
        c = r9.world.as_key(key)
        c.headers["X-Workspace-Id"] = r9.bravo_workspace_id
        return c.get("/api/v1/connectors/jobs").status_code

    assert into_bravo(platform_key) == 200
    assert into_bravo(owner_key) == 403
    assert into_bravo(editor_key) == 403
    assert into_bravo(root_in_acme_key) == 403
