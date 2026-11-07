"""API key rotation leaves exactly the keys an operator can see and revoke (QA enterprise #32).

Production 557f556c, ``POST /iam/api-keys/{id}/rotate``: the old key kept
working (a silent 24h overlap), the new key carried no workspace so it was in
no list, and DELETE answered 404 for it while it still authenticated. QA's two
rotated keys (276d4753…, 1e68a5e6…) are live orphans nobody can revoke.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r8_security_world import OWNER, PASSWORD, world  # noqa: E402,F401


def _alive(world, raw: str) -> bool:
    response = world.as_key(raw).get("/api/v1/connectors/saved")
    assert response.status_code in (200, 401), response.text
    return response.status_code == 200


def _listed(world, key_id: str) -> dict | None:
    keys = world.owner.get("/api/v1/workspace/api-keys").json()["keys"]
    return next((k for k in keys if k["id"] == key_id), None)


def test_rotation_revokes_the_old_key_and_the_new_one_is_listed_and_revocable(world):
    old = world.mint_key("editor")
    rotated = world.owner.post(f"/api/v1/iam/api-keys/{old['id']}/rotate", json={})
    assert rotated.status_code == 200, rotated.text
    new = rotated.json()
    assert new["workspace_id"] == world.workspace_id
    assert new["created_by"] == OWNER
    assert new["role"] == "editor"

    assert not _alive(world, old["key"]), "old key still authenticates after rotation"
    assert _alive(world, new["key"])
    assert _listed(world, new["id"]) is not None
    assert _listed(world, old["id"]) is None

    revoked = world.owner.delete(f"/api/v1/workspace/api-keys/{new['id']}")
    assert revoked.status_code == 200, revoked.text
    assert not _alive(world, new["key"])
    events = world.audit("iam.api_key.rotate")
    assert events and events[-1]["details"]["rotated_to"] == new["id"]


def test_explicit_grace_is_short_listed_and_both_keys_revocable(world):
    old = world.mint_key("viewer")
    too_long = world.owner.post(
        f"/api/v1/iam/api-keys/{old['id']}/rotate", json={"overlap_seconds": 7 * 86400}
    )
    assert too_long.status_code == 422, too_long.text
    rotated = world.owner.post(
        f"/api/v1/iam/api-keys/{old['id']}/rotate", json={"overlap_seconds": 300}
    )
    assert rotated.status_code == 200, rotated.text
    new = rotated.json()
    assert _alive(world, old["key"]) and _alive(world, new["key"])
    row = _listed(world, old["id"])
    assert row is not None and row["rotated_to"] == new["id"] and row["grace_until"]
    assert world.owner.delete(f"/api/v1/workspace/api-keys/{old['id']}").status_code == 200
    assert world.owner.delete(f"/api/v1/workspace/api-keys/{new['id']}").status_code == 200
    assert not _alive(world, old["key"]) and not _alive(world, new["key"])


def test_another_workspaces_admin_cannot_rotate_the_key(world):
    from services import team_store

    old = world.mint_key("editor")
    other = team_store.create_workspace(name="Other", created_by="boss@other.test")
    world.account("boss@other.test")
    boss = world.login("boss@other.test", PASSWORD)
    boss.headers["X-Workspace-Id"] = other.id
    response = boss.post(f"/api/v1/iam/api-keys/{old['id']}/rotate", json={})
    assert response.status_code == 404, response.text
    assert _alive(world, old["key"])


def _seed_qa_orphan(world) -> tuple[dict, dict]:
    """The production shape: a rotated key with no workspace, its source revoked."""
    from services import integrations_store

    source = world.mint_key("editor", name="qa-tmp-editor")
    raw = integrations_store._mint_raw_key("276d4753-0000-4000-8000-000000000001")
    records = {r["id"]: r for r in integrations_store.load_api_key_records()}
    src = records[source["id"]]
    src["rotated_to"] = "276d4753-0000-4000-8000-000000000001"
    src["revoked_at"] = integrations_store._now()
    integrations_store._upsert_api_key_record(src)
    orphan = {
        "id": "276d4753-0000-4000-8000-000000000001",
        "name": "qa-tmp-editor",
        "prefix": raw[:12],
        "role": "editor",
        "key_hash": integrations_store._hash_api_key(raw),
        "created_at": integrations_store._now(),
        "created_by": OWNER,
        "last_used_at": None,
        "lifetime": "7d",
        "expires_at": src["expires_at"],
        "scopes": None,
        "kind": "api_key",
        "rotated_from": source["id"],
    }
    integrations_store._upsert_api_key_record(orphan)
    return source, {**orphan, "key": raw}


def _stored_workspace(key_id: str) -> str:
    from services import integrations_store

    record = next(r for r in integrations_store.load_api_key_records() if r["id"] == key_id)
    return str(record.get("workspace_id") or "")


def test_orphaned_rotated_keys_are_repaired_listed_and_revocable(world):
    _source, orphan = _seed_qa_orphan(world)
    assert _alive(world, orphan["key"])
    # K1 (round 11): the orphan's creator (the owner who rotated it) sees it in
    # Settings -> API keys even before any repair, and could revoke it there.
    assert _listed(world, orphan["id"]) is not None

    # Workspace admins cannot run the deployment-wide repair.
    assert world.owner.post("/api/v1/iam/api-keys/repair-rotated", json={}).status_code == 403

    dry = world.root.post("/api/v1/iam/api-keys/repair-rotated", json={})
    assert dry.status_code == 200, dry.text
    assert dry.json()["dry_run"] is True
    assert [r["id"] for r in dry.json()["repairs"]] == [orphan["id"]]
    assert dry.json()["repairs"][0]["workspace_id"] == world.workspace_id
    assert _stored_workspace(orphan["id"]) == ""  # dry run changed nothing

    applied = world.root.post("/api/v1/iam/api-keys/repair-rotated", json={"dry_run": False})
    assert applied.status_code == 200, applied.text
    assert world.audit("iam.api_key.repair_rotated")
    assert _stored_workspace(orphan["id"]) == world.workspace_id
    assert _listed(world, orphan["id"]) is not None
    assert world.owner.delete(f"/api/v1/workspace/api-keys/{orphan['id']}").status_code == 200
    assert not _alive(world, orphan["key"])
    again = world.root.post("/api/v1/iam/api-keys/repair-rotated", json={"dry_run": False})
    assert again.json()["repairs"] == []
