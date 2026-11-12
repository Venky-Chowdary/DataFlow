"""Security review round 2 (M2): CDC snapshot signals never cross workspaces.

``POST /cdc/snapshots`` accepted any ``source_key`` and ``claim_next_signal``
claimed by key and table only. B queued a signal for ``connector:<A id>`` (or
A's guessable inline fingerprint); A's CDC runner claimed it, re-snapshotted
A's table and wrote ``last_pk`` and the row count into B's signal, which B
then read.

Now the route resolves ``source_key`` from a connector or job the caller can
write (an arbitrary key is refused) and stamps the signal with that
resource's workspace; ``claim_next_signal`` only claims signals of the
claiming job's workspace; a signal created inside a running job carries the
job's workspace; a job's signal list shows only its own workspace's signals.
"""

from __future__ import annotations

import pytest
from tenancy_world import tenancy_world  # noqa: F401


@pytest.fixture
def world(tenancy_world, monkeypatch):
    from services import cdc_incremental_snapshot as snapshots
    from services import connector_store

    w = tenancy_world
    monkeypatch.setattr(snapshots, "_signals_coll", lambda: None)
    monkeypatch.setattr(snapshots, "_DATA_DIR", str(w.data_dir))
    monkeypatch.setattr(snapshots, "_PATH", str(w.data_dir / "cdc_incremental_snapshots.json"))
    w.conn_a = connector_store.create_connector({
        "name": "a-src", "type": "postgresql", "host": "db.a-corp.example", "port": 5432,
        "database": "app", "workspace_id": w.workspace_a,
    })
    w.job_a = w.job(w.workspace_a, status="running", sync_mode="cdc", transfer_request={
        "source": {"kind": "database", "format": "postgresql", "connector_id": w.conn_a.id, "table": "orders"},
        "destination": {"kind": "database", "format": "postgresql", "host": "dst.a-corp.example", "table": "orders"},
        "sync_mode": "cdc",
    })
    w.viewer_a = w.member("viewer-a@tenancy.example.com", w.workspace_a, "viewer")
    return w


def _post(client, **body):
    return client.post("/api/v1/cdc/snapshots", json={"table": "orders", "primary_key": "id", **body})


def test_b_cannot_queue_a_signal_on_a_workspace_a_source(world):
    from services.cdc_schema_history import connection_fingerprint

    w = world
    inline = connection_fingerprint({"type": "postgresql", "host": "db.a-corp.example", "port": 5432, "database": "app"})
    answers = [
        _post(w.b, source_key=f"connector:{w.conn_a.id}"),
        _post(w.b, source_key=inline),
        _post(w.b, connector_id=w.conn_a.id),
        _post(w.b, job_id=w.job_a),
        _post(w.viewer_a, connector_id=w.conn_a.id),
    ]
    assert [r.status_code for r in answers] == [404, 400, 404, 404, 403], [r.text[:160] for r in answers]
    from services.cdc_incremental_snapshot import list_signals

    assert list_signals() == []


def test_the_owner_queues_signals_from_its_connector_or_job(world):
    w = world
    by_connector = _post(w.a, connector_id=w.conn_a.id)
    assert by_connector.status_code == 200, by_connector.text
    assert by_connector.json()["source_key"] == f"connector:{w.conn_a.id}"
    assert by_connector.json()["workspace_id"] == w.workspace_a
    by_key = _post(w.a, source_key=f"connector:{w.conn_a.id}")
    assert by_key.status_code == 200, by_key.text
    by_job = _post(w.a, job_id=w.job_a)
    assert by_job.status_code == 200, by_job.text
    assert by_job.json()["workspace_id"] == w.workspace_a
    mismatch = _post(w.a, connector_id=w.conn_a.id, source_key="postgresql:elsewhere:5432/x")
    assert mismatch.status_code == 400, mismatch.text


def test_a_job_claims_only_its_own_workspaces_signals(world):
    from services import cdc_incremental_snapshot as snapshots
    from services.export_object_store import export_job_scope

    w = world
    key = f"connector:{w.conn_a.id}"
    foreign = snapshots.request_incremental_snapshot(key, "orders", primary_key="id", workspace_id=w.workspace_b)
    with export_job_scope(w.job_a, w.workspace_a):
        assert snapshots.claim_next_signal(key, table="orders") is None
    assert snapshots.get_signal(foreign.id).status == "pending"
    own = snapshots.request_incremental_snapshot(key, "orders", primary_key="id", workspace_id=w.workspace_a)
    with export_job_scope(w.job_a, w.workspace_a):
        claimed = snapshots.claim_next_signal(key, table="orders")
    assert claimed is not None and claimed.id == own.id


def test_a_signal_created_inside_a_job_carries_the_jobs_workspace(world):
    from services import cdc_incremental_snapshot as snapshots
    from services.export_object_store import export_job_scope

    with export_job_scope(world.job_a, world.workspace_a):
        sig = snapshots.request_incremental_snapshot("connector:x", "orders", primary_key="id")
    assert sig.workspace_id == world.workspace_a


def test_signal_results_are_visible_only_in_the_owning_workspace(world):
    from services import cdc_incremental_snapshot as snapshots

    w = world
    key = f"connector:{w.conn_a.id}"
    foreign = snapshots.request_incremental_snapshot(key, "orders", primary_key="id", workspace_id=w.workspace_b)
    snapshots.mark_chunk(foreign.id, last_pk="A-CUSTOMER-PK-998877", rows=10)
    listed = w.a.get(f"/api/v1/transfer/{w.job_a}/cdc/snapshots")
    assert listed.status_code == 200, listed.text
    assert foreign.id not in listed.text and "A-CUSTOMER-PK-998877" not in listed.text
    own = _post(w.a, connector_id=w.conn_a.id).json()
    snapshots.mark_chunk(own["id"], last_pk="A-OWN-PK", rows=3)
    assert w.b.get(f"/api/v1/cdc/snapshots/{own['id']}").status_code == 404
    assert "A-OWN-PK" not in w.b.get("/api/v1/cdc/snapshots").text
    assert w.a.get(f"/api/v1/cdc/snapshots/{own['id']}").json()["last_pk"] == "A-OWN-PK"
