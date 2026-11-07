"""K1 (QA handover, build 9c75ada4): orphaned API keys nobody could revoke.

QA's two editor keys (ids 276d4753… and 1e68a5e6…) were minted by the
pre-round-8 rotate endpoint, which wrote the new key with ``workspace_id ""``
and the rotating caller as ``created_by``. On 9c75ada4 they still authenticated
(``verify_workspace_api_key`` does not look at the workspace), yet
``DELETE /workspace/api-keys/{id}`` and ``DELETE /iam/service-accounts/{id}``
answered 404 to their owner: ``_own_workspace_key`` (workspace_router.py)
showed a key with no workspace to platform admins only, and the repair
(``POST /iam/api-keys/repair-rotated``) was platform-admin only and never ran.

Contract checked here through the real REST routes on the live MongoDB
(``r9_workspace_world``):

* the key's creator, or an admin of a workspace the creator belongs to, sees an
  empty-workspace key in Settings -> API keys and revokes it with the normal
  DELETE; another tenant's admin still gets 404;
* a one-time, idempotent, audited startup migration gives orphaned keys their
  workspace back: through the rotation chain first, else the creator's sole
  workspace; a key whose owner is ambiguous stays revocable by its creator and
  visible to platform admins;
* QA's two production keys end up revocable by the owner through
  ``DELETE /workspace/api-keys/{id}``.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import BRAVO, OWNER, PASSWORD, r9  # noqa: E402,F401

PROD_IDS = ("276d4753", "1e68a5e6")


def _seed_key(
    *,
    created_by: str,
    workspace_id: str = "",
    rotated_from: str | None = None,
    key_id: str | None = None,
    kind: str = "api_key",
    revoked: bool = False,
    role: str = "editor",
) -> tuple[str, str]:
    """Write a key record the way the pre-round-8 rotate endpoint did."""
    from services import integrations_store as store

    kid = key_id or str(uuid.uuid4())
    raw = store._mint_raw_key(kid)
    now = datetime.now(timezone.utc)
    record = {
        "id": kid,
        "name": "qa-rotated",
        "prefix": raw[:12],
        "role": role,
        "key_hash": store._hash_api_key(raw),
        "created_at": now.isoformat(),
        "created_by": created_by,
        "last_used_at": None,
        "lifetime": "7d",
        "expires_at": (now + timedelta(days=7)).isoformat(),
        "scopes": None,
        "kind": kind,
        "workspace_id": workspace_id,
    }
    if rotated_from:
        record["rotated_from"] = rotated_from
    if revoked:
        record["revoked_at"] = now.isoformat()
    store._upsert_api_key_record(record)
    return kid, raw


def _listed_ids(client) -> set[str]:
    res = client.get("/api/v1/workspace/api-keys")
    assert res.status_code == 200, res.text
    return {k["id"] for k in res.json()["keys"]}


def _authenticates(r9, raw: str) -> bool:
    return r9.world.client(raw).get("/api/v1/jobs").status_code != 401


def _stored(key_id: str) -> dict:
    from services import integrations_store as store

    return next(r for r in store.load_api_key_records() if r["id"] == key_id)


def test_creator_sees_and_revokes_an_empty_workspace_key(r9):
    kid, raw = _seed_key(created_by=OWNER)
    assert _authenticates(r9, raw)
    # 9c75ada4: not listed, and DELETE answered 404 on both routes.
    assert kid in _listed_ids(r9.owner)
    res = r9.owner.delete(f"/api/v1/workspace/api-keys/{kid}")
    assert res.status_code == 200, res.text
    assert not _authenticates(r9, raw)
    assert r9.owner.delete(f"/api/v1/workspace/api-keys/{kid}").status_code == 404


def test_creator_revokes_an_orphaned_service_account_on_the_iam_route(r9):
    kid, raw = _seed_key(created_by=OWNER, kind="service_account")
    res = r9.owner.delete(f"/api/v1/iam/service-accounts/{kid}")
    assert res.status_code == 200, res.text
    assert not _authenticates(r9, raw)


def test_admin_of_a_creator_workspace_revokes_it_and_another_tenant_cannot(r9):
    admin2 = "second-admin-k1@example.test"
    r9.world.member(admin2, "admin")
    kid, raw = _seed_key(created_by=OWNER)

    bravo = r9.bravo()
    assert kid not in _listed_ids(bravo)
    assert bravo.delete(f"/api/v1/workspace/api-keys/{kid}").status_code == 404
    assert bravo.delete(f"/api/v1/iam/service-accounts/{kid}").status_code == 404

    second = r9.login(admin2, PASSWORD, r9.acme)
    assert kid in _listed_ids(second)
    assert second.delete(f"/api/v1/workspace/api-keys/{kid}").status_code == 200
    assert not _authenticates(r9, raw)


def test_an_orphan_cannot_be_rotated_into_another_orphan(r9):
    kid, _raw = _seed_key(created_by=OWNER)
    res = r9.owner.post(f"/api/v1/iam/api-keys/{kid}/rotate", json={})
    # Revocation, not re-issue: rotating would mint a second credential with
    # no workspace. The migration gives it a workspace first.
    assert res.status_code == 404, res.text


def test_startup_migration_repairs_assigns_and_leaves_ambiguous_keys_revocable(r9):
    from services import integrations_store as store
    from services.orphaned_api_keys import run_orphaned_api_key_migration

    # QA's two production keys: rotated from keys minted in the workspace.
    sources = [r9.world.mint_key("editor", name=f"qa-src-{i}") for i in range(2)]
    prod = []
    for prefix, src in zip(PROD_IDS, sources):
        store.revoke_api_key(src["id"])  # rotation revoked the source
        prod.append(_seed_key(
            created_by=OWNER,
            rotated_from=src["id"],
            key_id=f"{prefix}-{uuid.uuid4().hex[:4]}-4000-8000-{uuid.uuid4().hex[:12]}",
        ))
    # A chain that never reaches a workspace; the creator has one workspace.
    chain_root, _ = _seed_key(created_by=OWNER, revoked=True)
    sole, sole_raw = _seed_key(created_by=OWNER, rotated_from=chain_root)
    # A creator in two workspaces: ambiguous, left as it is.
    multi = "two-spaces-k1@example.test"
    r9.world.member(multi, "admin")
    from services import team_store

    team_store._write_membership(team_store.Membership(
        workspace_id=r9.bravo_workspace_id, email=multi, role="admin", added_by=BRAVO,
    ))
    ambiguous, amb_raw = _seed_key(created_by=multi)

    first = run_orphaned_api_key_migration()
    assert first["ran"] is True
    by_id = {row["id"]: row for row in first["repairs"]}
    for (kid, _), src in zip(prod, sources):
        assert by_id[kid]["workspace_id"] == r9.acme
        assert by_id[kid]["via"] == "rotation_chain"
        assert _stored(kid)["workspace_id"] == r9.acme
    assert by_id[sole]["via"] == "creator_sole_workspace"
    assert _stored(sole)["workspace_id"] == r9.acme
    assert ambiguous not in by_id
    assert [row["id"] for row in first["unresolved"]] == [ambiguous]
    assert _stored(ambiguous)["workspace_id"] == ""
    events = r9.world.audit("iam.api_key.orphan_migration")
    assert len(events) == 1
    assert events[0]["details"]["repaired"] == len(by_id)

    # One-time and idempotent: a second startup changes and audits nothing.
    again = run_orphaned_api_key_migration()
    assert again["ran"] is False
    forced = run_orphaned_api_key_migration(force=True)
    assert forced["repairs"] == []
    assert len(r9.world.audit("iam.api_key.orphan_migration")) == 1

    # The production keys: listed in Settings -> API keys, revoked by the owner.
    listed = _listed_ids(r9.owner)
    for kid, raw in prod:
        assert kid in listed
        assert r9.owner.delete(f"/api/v1/workspace/api-keys/{kid}").status_code == 200
        assert not _authenticates(r9, raw)
    assert sole in listed

    # The ambiguous key: its creator revokes it; platform admins see it.
    root_platform = r9.login("root@example.test", "Bootstrap-Admin-2026!", None)
    assert ambiguous in _listed_ids(root_platform)
    creator = r9.login(multi, PASSWORD, r9.bravo_workspace_id)
    assert ambiguous in _listed_ids(creator)
    assert creator.delete(f"/api/v1/workspace/api-keys/{ambiguous}").status_code == 200
    assert not _authenticates(r9, amb_raw)
    del sole_raw


def test_startup_migration_is_wired_into_the_app_lifespan():
    import inspect

    from src import main

    assert "run_orphaned_api_key_migration" in inspect.getsource(main.lifespan)
