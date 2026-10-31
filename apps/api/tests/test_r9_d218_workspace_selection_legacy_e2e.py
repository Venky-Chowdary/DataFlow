"""QA D218: sign-in lands in the right workspace; legacy unscoped data has an owner.

1. An admin signed in and landed in the OLDEST workspace. The API never named
   a workspace for a platform admin (``/auth/me`` -> ``workspace_id: ""``), so
   the web app took the first row of ``GET /team/workspaces``, which is sorted
   by creation date. The API now names one, deterministically: the workspace
   this account last used (remembered server-side; the browser forgets it at
   sign-out), else the account's own (personal first, then its earliest
   membership), else the oldest only when the account belongs to none.
2. Rows written before workspace scoping (``workspace_id`` missing, null, ""
   or whitespace) were visible to nobody once the UI always names a workspace.
   The owner claims them with the dry-run-first, idempotent
   ``scripts/backfill_legacy_workspaces.py --workspace-id <id>``; whitespace
   ids, which no Mongo query matched, are found too. Reading them in place is
   opt-in for platform admins only (``DATAFLOW_LEGACY_UNSCOPED_ADMIN_VIEW``,
   off by default, audited); no other workspace ever sees them.

All through the real REST routes, on the live MongoDB (teams, users, jobs).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_TESTS = Path(__file__).resolve().parent
_API = _TESTS.parent
for p in (_TESTS, _API, _API / "scripts"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from r9_workspace_world import PASSWORD, r9  # noqa: E402,F401

ROOT_PASSWORD = "Bootstrap-Admin-2026!"
OWNER_PASSWORD = "Owner-Password-2026!"
LEGACY_SPELLINGS = ("missing", None, "", "   ")


@pytest.fixture
def legacy_world(r9, tmp_path, monkeypatch):
    """Point every store the backfill touches at this test's own files/db."""
    from services import (
        preflight_run_store,
        quarantine_dlq,
        shape_draft_store,
        transfer_plan_store,
        transform_library,
        transform_store,
        usage_metering,
    )
    from src.services.contract_store import reset_contract_store

    monkeypatch.setenv("DATAFLOW_CONTRACTS_PATH", str(tmp_path / "contracts.json"))
    monkeypatch.setenv("DATAFLOW_TRANSFORMS_PATH", str(tmp_path / "transforms.json"))
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "library.json"))
    monkeypatch.setattr(usage_metering, "STORE_PATH", tmp_path / "usage.json")
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(preflight_run_store, "STORE_PATH", tmp_path / "preflight_runs.jsonl")
    monkeypatch.setattr(transfer_plan_store, "STORE_PATH", tmp_path / "plans.json")
    monkeypatch.setattr(shape_draft_store, "STORE_PATH", tmp_path / "shape_drafts.json")
    reset_contract_store()
    transform_store.reset_transform_store()
    transform_library.reset_transform_library()
    yield r9
    reset_contract_store()
    transform_store.reset_transform_store()
    transform_library.reset_transform_library()


def _old_corp() -> str:
    """A workspace older than everything else, unrelated to the admin."""
    from services import team_store

    ws = team_store.Workspace(
        id="old-corp-0000", name="Old Corp", created_at="2020-01-01T00:00:00+00:00",
        created_by="someone@old.test",
    )
    team_store._write_workspace(ws)
    return ws.id


def _signed_in(r9, email: str, password: str):
    return r9.login(email, password, None)


def _default(c) -> tuple[str, str]:
    listed = c.get("/api/v1/team/workspaces")
    assert listed.status_code == 200, listed.text
    me = c.get("/api/v1/auth/me")
    assert me.status_code == 200, me.text
    return listed.json()["default_workspace_id"], me.json()["default_workspace_id"]


def test_sign_in_lands_in_last_used_then_own_workspace_never_the_oldest(r9):
    old = _old_corp()
    root = _signed_in(r9, "root@example.test", ROOT_PASSWORD)
    made = root.post("/api/v1/team/workspaces", json={"name": "Root HQ"})
    assert made.status_code == 200, made.text
    root_hq = made.json()["workspace"]["id"]

    listed = root.get("/api/v1/team/workspaces").json()["workspaces"]
    assert listed[0]["id"] == old  # what the web used to land in
    # First sign-in: the admin's own workspace, not the oldest; stable across reads.
    assert _default(root) == (root_hq, root_hq)
    assert _default(root) == (root_hq, root_hq)

    # The admin works in Acme, signs out (the browser forgets the id), signs in.
    in_acme = root.get("/api/v1/auth/me", headers={"X-Workspace-Id": r9.acme})
    assert in_acme.status_code == 200 and in_acme.json()["workspace_id"] == r9.acme
    again = _signed_in(r9, "root@example.test", ROOT_PASSWORD)
    assert _default(again) == (r9.acme, r9.acme)

    # A remembered workspace that is gone falls back to the admin's own.
    assert again.delete(f"/api/v1/team/workspaces/{r9.acme}").status_code == 200
    assert _default(_signed_in(r9, "root@example.test", ROOT_PASSWORD)) == (root_hq, root_hq)

    # A header the account may not use is never remembered.
    bravo = r9.bravo()
    bravo.headers["X-Workspace-Id"] = root_hq
    bravo.get("/api/v1/auth/me")
    bravo.headers.pop("X-Workspace-Id")
    assert _default(bravo) == (r9.bravo_workspace_id, r9.bravo_workspace_id)


def test_members_land_in_their_own_workspace(r9):
    _old_corp()
    assert _default(_signed_in(r9, "owner@acme.test", OWNER_PASSWORD)) == (r9.acme, r9.acme)
    bravo = _signed_in(r9, "bravo-signup@example.test", PASSWORD)
    assert _default(bravo) == (r9.bravo_workspace_id, r9.bravo_workspace_id)


def _seed_legacy_jobs(r9) -> list[str]:
    from bson import ObjectId

    ids = []
    for spelling in LEGACY_SPELLINGS:
        doc = {"_id": ObjectId(), "status": "completed", "name": f"legacy-{spelling!r}"}
        if spelling != "missing":
            doc["workspace_id"] = spelling
        r9.db["transfer_jobs"].insert_one(doc)
        ids.append(str(doc["_id"]))
    return ids


def _job_ids(c) -> set[str]:
    response = c.get("/api/v1/connectors/jobs")
    assert response.status_code == 200, response.text
    return {str(j.get("_id") or j.get("id")) for j in response.json()["jobs"]}


def _claim(*args: str) -> dict:
    import contextlib
    import io

    from backfill_legacy_workspaces import main

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        assert main(list(args)) == 0
    return json.loads(out.getvalue())


def test_owner_claims_legacy_rows_dry_run_first_idempotent_and_isolated(legacy_world):
    r9 = legacy_world
    legacy = _seed_legacy_jobs(r9)
    root = _signed_in(r9, "root@example.test", ROOT_PASSWORD)
    root_hq = root.post("/api/v1/team/workspaces", json={"name": "Root HQ"}).json()["workspace"]["id"]
    in_hq = r9.login("root@example.test", ROOT_PASSWORD, root_hq)
    owner, bravo = r9.owner, r9.bravo()

    # Before: invisible in every named workspace — the UI always names one.
    for c in (in_hq, owner, bravo):
        assert not (_job_ids(c) & set(legacy))

    dry = _claim("--workspace-id", root_hq)
    assert dry["applied"] is False
    assert dry["counts"]["jobs"]["assigned"] == len(LEGACY_SPELLINGS)
    assert not (_job_ids(in_hq) & set(legacy))  # a dry run writes nothing

    applied = _claim("--apply", "--workspace-id", root_hq)
    assert applied["applied"] is True
    assert applied["counts"]["jobs"]["assigned"] == len(LEGACY_SPELLINGS)
    rerun = _claim("--apply", "--workspace-id", root_hq)
    assert rerun["counts"]["jobs"]["assigned"] == 0  # idempotent

    assert set(legacy) <= _job_ids(in_hq)
    for c in (owner, bravo):
        assert not (_job_ids(c) & set(legacy))
        assert c.get(f"/api/v1/connectors/jobs/{legacy[0]}").status_code == 404

    # A target that does not exist is refused before anything is written.
    import contextlib
    import io

    from backfill_legacy_workspaces import main

    with contextlib.redirect_stderr(io.StringIO()):
        assert main(["--apply", "--workspace-id", "no-such-workspace"]) == 2


def test_legacy_admin_view_flag_is_off_by_default_admin_only_and_audited(legacy_world, monkeypatch):
    r9 = legacy_world
    from services import audit_log

    events = []
    real_append = audit_log.append_audit_event

    def capture(**event):
        events.append(event)
        return real_append(**event)

    monkeypatch.setattr(audit_log, "append_audit_event", capture)
    legacy = _seed_legacy_jobs(r9)
    root = _signed_in(r9, "root@example.test", ROOT_PASSWORD)
    root_hq = root.post("/api/v1/team/workspaces", json={"name": "Root HQ"}).json()["workspace"]["id"]
    in_hq = r9.login("root@example.test", ROOT_PASSWORD, root_hq)
    assert not (_job_ids(in_hq) & set(legacy))

    monkeypatch.setenv("DATAFLOW_LEGACY_UNSCOPED_ADMIN_VIEW", "1")
    assert set(legacy) <= _job_ids(r9.login("root@example.test", ROOT_PASSWORD, root_hq))
    for c in (r9.owner, r9.bravo()):
        assert not (_job_ids(c) & set(legacy))
    flagged = [e for e in events if e.get("action") == "tenant_scope.legacy_admin_view.used"]
    assert flagged and flagged[0]["details"]["flag"] == "DATAFLOW_LEGACY_UNSCOPED_ADMIN_VIEW"
