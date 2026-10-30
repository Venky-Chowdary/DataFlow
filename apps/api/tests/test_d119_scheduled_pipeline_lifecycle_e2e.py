"""D119 — a scheduled pipeline onto a new table survives run 2+, drift-ack, backfill, propagate.

QA (production): ``name 'ci_get' is not defined``. Every scheduled pipeline
created onto a new table died on its second and every later run, and every
drift-ack, backfill or propagate preflight crashed. The earlier fix imported
the name and proved it through ``execute_tracked`` with SQLite only.

This drives the pipeline the way a user does, on real PostgreSQL:

1. MCP ``create_schedule`` (new destination table) -> ``/api/v1/copilot/confirm``.
2. Three runs through the scheduler beat (``_run_due_schedules``), the path a
   cadence fires; runs 2 and 3 land into the table run 1 created.
3. The source gains a column: the run parks on a source-drift finding (caught
   by the scheduler's drift guard, or inside the job by G13). The operator
   acknowledges it (``POST /schedules/{id}/approvals/{aid}/approve`` with
   ``schema_drift``), which declares the column omitted, and the next beat runs.
4. Backfill: ``backfill_new_fields`` on the schedule (``PUT /schedules/{id}``)
   and the beat adds and fills the new column.
5. Propagate: ``schema_policy=propagate_columns``, another source column, and
   the beat carries it to the destination.

Each step also runs the REST preflight (``/api/v1/preflight/run``) with the
flag that arms the widen block, which is where ``ci_get`` crashed.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h

# Live engines: a hung run fails instead of holding a session (pytest-timeout).
pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _beat(schedule_id: str) -> dict:
    """Make the schedule due and fire one scheduler beat. Returns the job."""
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})
    started = schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    job_id = str(getattr(sched, "running_job_id", "") or getattr(sched, "last_job_id", "") or "")
    if not started:
        return {"started": 0, "schedule": sched}
    job = h.wait_job(job_id)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        sched = get_schedule(schedule_id)
        if not str(getattr(sched, "running_job_id", "") or "").strip():
            break
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    return {"started": started, "job": job, "schedule": get_schedule(schedule_id)}


def _completed(run: dict) -> dict:
    assert run["started"] == 1, {
        "last_status": getattr(run["schedule"], "last_status", None),
        "last_error": getattr(run["schedule"], "last_error", None),
        "approval": getattr(run["schedule"], "approval_request", None),
    }
    job = run["job"]
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"),
        "error": job.get("error") or job.get("message"),
    }
    return job


def _open_approval(schedule_id: str) -> dict:
    from services.schedule_store import get_schedule

    req = getattr(get_schedule(schedule_id), "approval_request", None) or {}
    assert str(req.get("status") or "") == "open", req
    return dict(req)


def _rest_preflight(client, sched, **flags) -> dict:
    """The REST preflight with the flags that arm the widen block."""
    from services.schedule_mapping_contract import persisted_mapping_rows

    mappings = [
        {"source": m["source"], "target": m.get("target") or m["source"],
         "source_type": m.get("source_type") or "", "target_type": m.get("target_type") or ""}
        for m in persisted_mapping_rows(sched.mappings)
    ]
    live = h.query(
        "postgresql",
        "SELECT column_name, data_type FROM information_schema.columns "  # nosec B608
        f"WHERE table_schema = 'public' AND table_name = '{sched.source_table}' "
        "ORDER BY ordinal_position",
    )
    body = {
        "dataset_name": sched.source_table,
        "columns": [r[0] for r in live],
        "column_types": {r[0]: r[1] for r in live},
        "mappings": mappings,
        "source_connector_id": sched.source_connector_id,
        "source_kind": "database",
        "source_type": "postgresql",
        "source_table": sched.source_table,
        "connector_id": sched.dest_connector_id,
        "dest_kind": "database",
        "dest_type": "postgresql",
        "dest_table": sched.dest_table,
        "sync_mode": sched.sync_mode,
        "validation_mode": "balanced",
        **flags,
    }
    if flags.get("schema_drift_acknowledged"):
        body.update(acknowledgment_actor="operator", acknowledgment_reason="d119 drift ack")
    resp = client.post("/api/v1/preflight/run", json=body)
    assert resp.status_code == 200, resp.text
    text = resp.text
    assert "is not defined" not in text, text[:2000]
    return resp.json()


def _dest(table: str) -> tuple[list[str], dict[int, tuple]]:
    cols = [
        r[0] for r in h.query(
            "postgresql",
            "SELECT column_name FROM information_schema.columns "  # nosec B608
            f"WHERE table_schema = 'public' AND table_name = '{table}' "
            "ORDER BY ordinal_position",
        )
    ]
    sel = ", ".join(f'"{c}"' for c in cols)
    rows = h.query("postgresql", f'SELECT {sel} FROM public."{table}"')  # nosec B608
    return cols, {int(r[cols.index("id")]): dict(zip(cols, r)) for r in rows}


# The new column is caught either by the scheduler's drift guard (a baseline
# exists) or inside the job by G13 (no baseline recorded yet). Both park on the
# same finding, and the drift-ack must clear both.
@pytest.mark.parametrize("caught_by", ["drift_guard", "job_g13"])
def test_scheduled_pipeline_onto_new_table_runs_three_times_then_ack_backfill_propagate(
    client, caught_by
):
    from services.connector_store import create_connector
    from services.schedule_store import get_schedule

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table = f"d119_src_{token}"
    dest_table = f"d119_dst_{token}"
    try:
        cur.execute(
            f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT, "  # nosec B608
            "updated_at BIGINT NOT NULL)"
        )
        cur.executemany(
            f"INSERT INTO {src_table} VALUES (%s, %s, %s)",  # nosec B608
            [(1, "Ada", 1), (2, "", 2), (3, None, 3)],
        )
        src = create_connector(
            {"name": f"d119-src-{token}", "type": "postgresql", "role": "source",
             "workspace_id": "", "ssl": False, **h.PG, "schema": "public"}
        )
        dst = create_connector(
            {"name": f"d119-dst-{token}", "role": "destination", "workspace_id": "",
             "ssl": False, **h.dest_config("postgresql")}
        )

        # 1. create_schedule through MCP onto a table that does not exist yet.
        ok, staged = h.mcp_call(client, "create_schedule", {
            "source_connector_id": src.id,
            "source_table": src_table,
            "dest_connector_id": dst.id,
            "dest_table": dest_table,
            "cadence": "every 15 minutes",
            "sync_mode": "incremental_upsert",
            "upsert_key": "id",
            "cursor_column": "updated_at",
            "cursor_semantics": "monotonic_sequence",
            "name": f"d119-{token}",
        })
        assert ok, staged
        assert not h.blockers_of(staged), h.blockers_of(staged)
        resp = client.post(
            "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "d119"}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("ok") is True, body
        schedule_id = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")
        assert get_schedule(schedule_id) is not None, body

        # 2. Three scheduler beats. Run 1 creates the table; 2 and 3 land into it.
        _completed(_beat(schedule_id))
        cur.execute(f"UPDATE {src_table} SET name = 'Zoë', updated_at = 10 WHERE id = 2")  # nosec B608
        _completed(_beat(schedule_id))
        cur.execute(f"INSERT INTO {src_table} VALUES (4, '   ', 11)")  # nosec B608
        _completed(_beat(schedule_id))
        cols, rows = _dest(dest_table)
        assert sorted(rows) == [1, 2, 3, 4]
        assert (rows[2]["name"], rows[3]["name"], rows[4]["name"]) == ("Zoë", None, "   ")
        _rest_preflight(client, get_schedule(schedule_id))

        # 3. Drift: the source gains a column. The beat parks; the operator accepts.
        cur.execute(f"ALTER TABLE {src_table} ADD COLUMN region TEXT")  # nosec B608
        cur.execute(f"UPDATE {src_table} SET region = 'EU', updated_at = 20 WHERE id = 1")  # nosec B608
        _rest_preflight(client, get_schedule(schedule_id), schema_drift_acknowledged=True)
        from services.schedule_store import update_schedule

        if caught_by == "job_g13":
            update_schedule(schedule_id, {"source_schema": {}, "source_schema_fingerprint": ""})
        else:
            assert get_schedule(schedule_id).source_schema, "no baseline after three runs"
        held = _beat(schedule_id)
        assert held["started"] == (1 if caught_by == "job_g13" else 0), held
        held_status = str((held.get("job") or {}).get("status") or "parked")
        # manual_review: the new column is held for a decision, never dropped.
        assert held_status != "completed", held
        approval = _open_approval(schedule_id)
        assert approval.get("kind") == "source_drift", approval
        # The drift-ack: the operator approves the finding, attesting the drift.
        resp = client.post(
            f"/api/v1/schedules/{schedule_id}/approvals/{approval['id']}/approve",
            json={"reason": "d119 drift acknowledged by the operator", "schema_drift": True},
            headers={"X-Actor": "qa-operator"},
        )
        assert resp.status_code == 200, resp.text
        acked = _beat(schedule_id)
        assert acked["started"] == 1 and str(acked["job"].get("status")) == "completed", {
            "held": (held_status, (held.get("job") or {}).get("error")),
            "after_ack": (acked.get("job") or {}).get("error"),
            "approval": getattr(acked["schedule"], "approval_request", None),
        }

        # The acknowledged column is a recorded omission, not a silent drop.
        cols, rows = _dest(dest_table)
        assert "region" not in cols, cols
        omitted = [
            m for m in get_schedule(schedule_id).mappings
            if m.get("source") == "region" and m.get("intentional_omit")
        ]
        assert omitted and omitted[0]["omit_approved_by"] == "qa-operator", omitted

        # 4. Backfill: a new column, and backfill_new_fields adds and fills it.
        cur.execute(f"ALTER TABLE {src_table} ADD COLUMN segment TEXT")  # nosec B608
        cur.execute(
            f"UPDATE {src_table} SET segment = 'Großkunde', updated_at = 30 WHERE id = 1"  # nosec B608
        )
        _rest_preflight(client, get_schedule(schedule_id), backfill_new_fields=True)
        resp = client.put(f"/api/v1/schedules/{schedule_id}", json={"backfill_new_fields": True})
        assert resp.status_code == 200, resp.text
        _completed(_beat(schedule_id))
        cols, rows = _dest(dest_table)
        assert "segment" in cols and "region" not in cols, cols
        assert rows[1]["segment"] == "Großkunde"

        # 5. Propagate: another column, carried by the policy itself.
        resp = client.put(
            f"/api/v1/schedules/{schedule_id}", json={"schema_policy": "propagate_columns"}
        )
        assert resp.status_code == 200, resp.text
        cur.execute(f"ALTER TABLE {src_table} ADD COLUMN tier TEXT")  # nosec B608
        cur.execute(f"UPDATE {src_table} SET tier = 'gold', updated_at = 40 WHERE id = 3")  # nosec B608
        _rest_preflight(client, get_schedule(schedule_id), schema_policy="propagate_columns")
        _completed(_beat(schedule_id))
        cols, rows = _dest(dest_table)
        assert "tier" in cols, cols
        assert rows[3]["tier"] == "gold"
        assert sorted(rows) == [1, 2, 3, 4]
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')  # nosec B608
        pg.close()
