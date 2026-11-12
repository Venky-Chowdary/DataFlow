"""Security review round 3 (H5): quarantine replay is judged like a run.

``POST /connectors/jobs/{id}/quarantine/replay`` wrote the parent job's
quarantined rows straight to its destination with
``write_destination_database``. Nothing judged the destination's region, so a
saved destination connector moved out of region after the job ran, or an
inline destination stored on a job from before the workspace was fenced,
received the fenced workspace's rows while ``/transfer/execute`` to the same
place answered 403.

The replay now asks the same run-time gate every tracked run asks
(``job_scope.start_refusal``) on the destination as it resolves now, in the
parent job's workspace, before any connect or write. Unfenced workspaces
replay exactly as before.
"""

from __future__ import annotations

import pytest
from tenancy_world import tenancy_world  # noqa: F401

_EU_HOST = "x.eu-west-1.rds.amazonaws.com"
_DETAILS = [{
    "row": 1, "column": "c", "value": "x", "reason": "bad",
    "record": {"id": 1, "c": "EU-PERSONAL-ROW"},
    "row_data": {"id": 1, "c": "EU-PERSONAL-ROW"},
    "values": {"id": 1, "c": "EU-PERSONAL-ROW"},
}]


@pytest.fixture
def replay_world(tenancy_world, monkeypatch):  # noqa: F811
    for name in ("RESIDENCY_STRICT", "DEFAULT_REGION"):
        monkeypatch.delenv(f"DATAWRAP_{name}", raising=False)
        monkeypatch.delenv(f"DATAFLOW_{name}", raising=False)
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "review3-h5-secrets-key-distinct-from-auth-0001")
    import src.transfer.adapters as adapters

    writes: list[str] = []

    def fake_write(dest, records, *_a, **_k):
        writes.append(str(adapters.resolve_connector_config(dest).get("host") or ""))
        return 1, [], {"rows_written": 1}

    monkeypatch.setattr(adapters, "write_destination_database", fake_write)
    # The mapping-type refresh reads the destination's columns; no live engine here.
    monkeypatch.setattr(
        "services.dest_physical_types.refresh_mapping_target_types",
        lambda mappings, *_a, **_k: mappings,
    )
    # Gate-8 reads the destination back; no live engine here either.
    monkeypatch.setattr(
        "src.transfer.reconcile_step.run_reconciliation",
        lambda *_a, **_k: {"passed": True},
    )
    tenancy_world.writes = writes
    return tenancy_world


def _fence(world, workspace_id):
    from services import tenant_store

    tenant_store.create_tenant(workspace_id=workspace_id, name="EU", data_region="eu-west-1")


def _saved(world, workspace_id, host):
    from services import connector_store

    return connector_store.create_connector({
        "name": "dst", "type": "postgresql", "host": host, "database": "d",
        "username": "u", "password": "p", "workspace_id": workspace_id,
    })


def _replay(world, client, workspace_id, destination):
    job_id = world.job(
        workspace_id, name="qjob", status="completed", rows_written=1,
        rejected_details=list(_DETAILS),
        transfer_request={
            "source": {"kind": "database", "format": "postgresql", "host": _EU_HOST, "table": "t"},
            "destination": destination,
            "mappings": [{"source": "id", "target": "id"}, {"source": "c", "target": "c"}],
        },
    )
    return client.post(f"/api/v1/connectors/jobs/{job_id}/quarantine/replay", json={})


def test_replay_to_a_saved_connector_moved_out_of_region_is_refused(replay_world):
    w = replay_world
    _fence(w, w.workspace_a)
    conn = _saved(w, w.workspace_a, _EU_HOST)
    moved = w.a.put(f"/api/v1/connectors/saved/{conn.id}", json={
        "name": "dst", "type": "postgresql", "host": "52.1.2.3", "port": 5432,
        "database": "d", "username": "u", "password": "p",
    })
    assert moved.status_code == 200, moved.text
    r = _replay(w, w.a, w.workspace_a, {"kind": "database", "format": "postgresql",
                                         "connector_id": conn.id, "table": "t"})
    assert r.status_code == 403, r.text
    assert "residency" in r.text.lower()
    assert w.writes == []


def test_replay_of_a_pre_fence_job_with_an_inline_foreign_destination_is_refused(replay_world):
    w = replay_world
    _fence(w, w.workspace_a)
    r = _replay(w, w.a, w.workspace_a, {"kind": "database", "format": "postgresql",
                                         "host": "52.1.2.3", "database": "d", "table": "t"})
    assert r.status_code == 403, r.text
    assert "residency" in r.text.lower()
    assert w.writes == []


def test_replay_creates_no_child_job_when_refused(replay_world):
    from services.mongodb_service import get_mongodb_service

    w = replay_world
    _fence(w, w.workspace_a)
    before = len(get_mongodb_service().list_jobs(limit=500) or [])
    r = _replay(w, w.a, w.workspace_a, {"kind": "database", "format": "postgresql",
                                         "host": "52.1.2.3", "database": "d", "table": "t"})
    assert r.status_code == 403, r.text
    # Only the parent job seeded by _replay exists; no running child was left behind.
    assert len(get_mongodb_service().list_jobs(limit=500) or []) == before + 1


def test_replay_to_an_in_region_destination_still_writes(replay_world):
    w = replay_world
    _fence(w, w.workspace_a)
    conn = _saved(w, w.workspace_a, _EU_HOST)
    r = _replay(w, w.a, w.workspace_a, {"kind": "database", "format": "postgresql",
                                         "connector_id": conn.id, "table": "t"})
    assert r.status_code == 200, r.text
    assert w.writes == [_EU_HOST]


def test_replay_in_an_unfenced_workspace_is_unchanged(replay_world):
    w = replay_world
    r = _replay(w, w.b, w.workspace_b, {"kind": "database", "format": "postgresql",
                                         "host": "52.1.2.3", "database": "d", "table": "t"})
    assert r.status_code == 200, r.text
    assert w.writes == ["52.1.2.3"]
