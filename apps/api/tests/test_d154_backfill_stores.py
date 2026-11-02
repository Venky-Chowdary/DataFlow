"""D154 backfill covers every tenant-owned store, not just connectors and jobs.

Contracts, transform projects, library recipes, preflight runs, transfer plans
and shape drafts carry no owner; the backfill places them by what uses them
(or with the platform owner) and never touches scoped rows.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from d154_world import ADMIN, build_world, owner_marker  # noqa: E402


@pytest.fixture
def world(tmp_path, monkeypatch):
    return build_world(tmp_path, monkeypatch, signup=True)


def _unscoped_everywhere(world) -> dict[str, int]:
    """How many legacy rows each store still holds."""
    from services import preflight_run_store, shape_draft_store, transfer_plan_store
    from services.transform_library import get_transform_library
    from services.transform_store import get_transform_store
    from services.workspace_filter import WorkspaceFilter
    from src.services.contract_store import get_contract_store

    unscoped = WorkspaceFilter.only("")
    return {
        "contracts": sum(
            1 for c in get_contract_store().list_contracts(limit=1000)
            if unscoped.matches((c.metadata or {}).get("workspace_id"))
        ),
        "projects": sum(1 for p in get_transform_store().list() if unscoped.matches(p.workspace_id)),
        "recipes": sum(1 for r in get_transform_library().list() if unscoped.matches(r.workspace_id)),
        "runs": sum(1 for r in preflight_run_store._read_all() if unscoped.matches(r.get("workspace_id"))),
        "plans": sum(1 for p in transfer_plan_store._load_all() if unscoped.matches(p.workspace_id)),
        "drafts": sum(
            1 for d in shape_draft_store._load().values() if unscoped.matches(d.get("workspace_id"))
        ),
    }


def test_backfill_places_every_store_and_is_idempotent(world):
    from services.legacy_workspace_backfill import backfill_legacy_workspaces

    before = _unscoped_everywhere(world)
    assert all(n == 1 for n in before.values()), before

    dry = backfill_legacy_workspaces()
    for name in ("contracts", "transform_projects", "transform_recipes", "preflight_runs",
                 "transfer_plans", "shape_drafts"):
        assert dry.counts[name]["assigned"] == 1, (name, dry.counts)
    assert _unscoped_everywhere(world) == before  # dry run writes nothing

    applied = backfill_legacy_workspaces(apply=True)
    assert all(c["skipped"] == 0 for c in applied.counts.values()), applied.counts
    assert _unscoped_everywhere(world) == {k: 0 for k in before}

    again = backfill_legacy_workspaces(apply=True)
    assert all(c["assigned"] == 0 for c in again.counts.values()), again.counts

    # Rows with no owner on record went to the platform owner's workspace: the
    # admin sees them, the tenants still do not.
    legacy = owner_marker("legacy", world.tag)
    for path in ("/api/v1/contracts", "/api/v1/preflight/runs", "/api/v1/transfer/plans"):
        assert legacy in world.admin.get(path).text, path
        for client in (world.a, world.b):
            assert legacy not in client.get(path).text, path
    assert applied.platform_owner == ADMIN.lower()
    # A transform project follows its destination connector, whose owner (A,
    # through the legacy job that used it) is known.
    assert legacy in world.a.get("/api/v1/transforms/").text
    assert legacy not in world.b.get("/api/v1/transforms/").text


def test_scoped_rows_are_not_touched_by_the_backfill(world):
    from services.legacy_workspace_backfill import backfill_legacy_workspaces
    from src.services.contract_store import get_contract_store

    a_contract = world.ids["a_contract"]
    backfill_legacy_workspaces(apply=True)
    kept = get_contract_store().get_contract(a_contract)
    assert (kept.metadata or {}).get("workspace_id") == world.ws_a
