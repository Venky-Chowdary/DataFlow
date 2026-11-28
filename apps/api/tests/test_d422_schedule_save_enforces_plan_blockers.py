"""D422 — saving a schedule must enforce the plan blockers Execute enforces.

QA rounds 61-75: Kafka -> PostgreSQL incremental_append with cursor
``updated_at``. plan_transfer correctly said safe_to_start=false with blocker
g9_sync_contract ("Kafka cannot page an incremental read on cursor
'updated_at'"), yet POST /schedules/ saved the route and runs 1-3 each failed
at 0 rows on that same blocker.

Root cause: the schedule store asked only the recipe question (D361) of the
plan blockers Execute refuses up front. It now asks the one shared function
Execute asks (``services.plan_execute_blockers``) on create and on any edit of
what the plan judged, and refuses with the blocker id (422
SCHEDULE_PLAN_BLOCKED); a schedule saved before this check parks as "needs
decision" before a job exists. REST and MCP share the store, so they agree.
Live in-process SQLite -> SQLite for the runs; the Kafka connector is never
dialled (the refusal comes before any read).
"""

from __future__ import annotations

import sqlite3

import pytest

from tests.test_mx324_parked_schedule_releases_claim import _isolate, _wait

_COLUMNS = ("id", "name", "city")


def _world(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_ACK_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    from services.connector_store import create_connector
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down

    ensure_memory_job_store_if_mongo_down()
    src_db, dst_db = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    with sqlite3.connect(src_db) as c:
        c.execute('CREATE TABLE "d422_src" (id INTEGER PRIMARY KEY, name TEXT, city TEXT)')
        c.executemany('INSERT INTO "d422_src" VALUES (?,?,?)', [(i, f"n{i}", f"c{i}") for i in range(5)])
    src = create_connector({"name": "D422 S", "type": "sqlite", "role": "source",
                            "connection_string": f"sqlite:///{src_db}", "workspace_id": ""})
    dst = create_connector({"name": "D422 D", "type": "sqlite", "role": "destination",
                            "connection_string": f"sqlite:///{dst_db}", "workspace_id": ""})
    return src, dst, dst_db


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from services.api_errors import ApiError, api_error_handler
    from src.routers.schedules_router import router

    app = FastAPI()
    app.add_exception_handler(ApiError, api_error_handler)
    app.include_router(router)
    return TestClient(app)


def _assigned():
    return [{"source": c, "target": c, "confidence": 1.0, "target_type": "TEXT",
             "assignment_strategy": "identity_passthrough"} for c in _COLUMNS]


def _pending():
    return [{"source": c, "target": c, "confidence": 1.0, "target_type": "",
             "assignment_strategy": "pending_dest_schema"} for c in _COLUMNS]


def _body(src, dst, dest_table, **extra):
    return {
        "name": f"d422 {dest_table}", "source_connector_id": src.id,
        "source_table": "d422_src", "dest_connector_id": dst.id,
        "dest_table": dest_table, "interval": "daily",
        "sync_mode": "full_refresh_overwrite", "primary_key": "id", "enabled": True,
        "mappings": _assigned(), **extra,
    }


def _blocked(resp, blocker: str) -> dict:
    assert resp.status_code == 422, resp.text
    err = resp.json()["error"]
    assert err["code"] == "SCHEDULE_PLAN_BLOCKED", err
    assert err["blocker_id"] == blocker, err
    # The id is data for clients; the sentence an operator reads carries none.
    assert blocker not in err["message"], err["message"]
    assert err["blocker_message"] in err["message"]
    return err


def test_rest_create_refuses_a_kafka_column_cursor_and_saves_nothing(tmp_path, monkeypatch):
    src, dst, _ = _world(tmp_path, monkeypatch)
    from services.connector_store import create_connector
    from services.schedule_store import list_schedules
    from src.transfer.engine_kafka import kafka_unpageable_cursor

    kafka = create_connector({"name": "D422 K", "type": "kafka", "role": "source",
                              "host": "127.0.0.1", "port": 9, "workspace_id": ""})
    body = _body(kafka, dst, "k_ia", sync_mode="incremental_append", cursor_column="updated_at",
                 source_table="r61_kfa")
    before = len(list_schedules())
    err = _blocked(_client().post("/schedules/", json=body), "g9_sync_contract")
    # The plan's own text, so the operator reads one reason everywhere.
    assert kafka_unpageable_cursor("kafka", "updated_at") in err["message"]
    assert len(list_schedules()) == before, "nothing may be saved"


def test_rest_create_refuses_map_rows_waiting_on_an_unread_destination(tmp_path, monkeypatch):
    src, dst, _ = _world(tmp_path, monkeypatch)
    client = _client()
    _blocked(client.post("/schedules/", json=_body(src, dst, "p1", mappings=_pending())),
             "g_dest_schema_unread")
    # A draft is refused too: enabling it could only fail.
    _blocked(client.post("/schedules/", json=_body(src, dst, "p2", mappings=_pending(),
                                                   enabled=False)),
             "g_dest_schema_unread")
    ok = client.post("/schedules/", json=_body(src, dst, "p3"))
    assert ok.status_code == 201, ok.text  # AC-2: no blocker, saved


def test_rest_patch_that_introduces_a_blocker_is_refused(tmp_path, monkeypatch):
    src, dst, _ = _world(tmp_path, monkeypatch)
    client = _client()
    created = client.post("/schedules/", json=_body(src, dst, "patch1"))
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    _blocked(client.patch(f"/schedules/{sid}", json={"mappings": _pending()}),
             "g_dest_schema_unread")
    from services.schedule_store import get_schedule

    stored = get_schedule(sid)
    assert all(m.get("assignment_strategy") != "pending_dest_schema" for m in stored.mappings)
    # A cadence-only edit is not refused.
    assert client.patch(f"/schedules/{sid}", json={"interval": "hourly"}).status_code == 200


def test_mcp_create_schedule_agrees_with_rest_through_the_store(tmp_path, monkeypatch):
    """Every save path meets the same store check (MCP Confirm, GitOps, REST)."""
    src, dst, _ = _world(tmp_path, monkeypatch)
    from services.schedule_mapping_contract import SchedulePlanBlocked
    from services.schedule_store import create_schedule

    payload = _body(src, dst, "store1", mappings=_pending())
    payload.pop("primary_key")
    with pytest.raises(SchedulePlanBlocked) as refused:
        create_schedule(payload, refuse_unrunnable_mappings=True)
    assert refused.value.extra["blocker_id"] == "g_dest_schema_unread"
    assert isinstance(refused.value, ValueError)


def test_schedule_saved_before_the_check_parks_before_a_job_then_runs(tmp_path, monkeypatch):
    """AC-3: a route that gained a blocker is refused before reading, needs a decision."""
    src, dst, dst_db = _world(tmp_path, monkeypatch)
    from services import schedule_runner
    from services import schedule_store as store
    from services.schedule_runner import ScheduleStartError

    client = _client()
    created = client.post("/schedules/", json=_body(src, dst, "legacy1"))
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    rows = store._load_all()
    for i, s in enumerate(rows):
        if s.id == sid:
            rows[i] = store.PipelineSchedule.from_dict({**s.to_dict(), "mappings": _pending()})
    store._save_all(rows)

    with pytest.raises(ScheduleStartError) as refused:
        schedule_runner._run_schedule(sid, manual=True)
    message = str(refused.value)
    assert message.startswith("Schedule cannot run — The destination schema could not be read")
    assert "g_dest_schema_unread" not in message
    parked = store.get_schedule(sid)
    assert parked.last_status == "needs_approval"
    # The blocker id is the run record's structured reason, not prose.
    assert parked.run_history[-1]["reason_code"] == "g_dest_schema_unread", parked.run_history[-1]
    assert not parked.last_job_id, "the refusal must come before a job exists"

    fixed = client.patch(f"/schedules/{sid}", json={"mappings": _assigned()})
    assert fixed.status_code == 200, fixed.text
    doc = _wait(schedule_runner._run_schedule(sid, manual=True))
    assert doc.get("status") == "completed", doc.get("error")
    with sqlite3.connect(dst_db) as c:
        assert c.execute('SELECT COUNT(*) FROM "legacy1"').fetchone()[0] == 5


def test_studio_form_cadence_edit_resending_route_keys_still_saves(tmp_path, monkeypatch):
    """Review finding 3: ScheduleForm resends source_table and cursor_column with
    every edit. Unchanged values are not a route edit, so a legacy row's
    cadence change is not re-planned; a real route change still is."""
    src, dst, _ = _world(tmp_path, monkeypatch)
    from services import schedule_store as store

    client = _client()
    created = client.post("/schedules/", json=_body(src, dst, "form1", cursor_column="id"))
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    rows = store._load_all()
    for i, s in enumerate(rows):  # a row saved before the D422 check
        if s.id == sid:
            rows[i] = store.PipelineSchedule.from_dict({**s.to_dict(), "mappings": _pending()})
    store._save_all(rows)
    form_patch = {"interval": "hourly", "source_table": "d422_src", "cursor_column": "id",
                  "source_connector_id": src.id}
    resp = client.patch(f"/schedules/{sid}", json=form_patch)
    assert resp.status_code == 200, resp.text
    assert store.get_schedule(sid).interval == "hourly"
    _blocked(client.patch(f"/schedules/{sid}", json={**form_patch, "cursor_column": "city"}),
             "g_dest_schema_unread")


@pytest.mark.parametrize("top,stream,refused", [
    ("incremental_append", "full_refresh_append", False),
    ("full_refresh_append", "incremental_append", True),
    ("incremental_append", "", True),
])
def test_kafka_cursor_plan_and_execute_judge_the_same_stream_mode(top, stream, refused):
    """Review finding 2: the plan's g9 used the request's mode, Execute each
    stream's own. Both now resolve the effective mode one way."""
    from types import SimpleNamespace

    from services.plan_execute_blockers import execute_plan_refusal
    from services.preflight_cursor_gate import build_sync_contract_gate
    from services.sync_cursor import normalize_sync_mode

    contract = {"name": "r61_kfa", "cursor_field": "updated_at", "selected": True,
                **({"sync_mode": stream} if stream else {})}
    gate = build_sync_contract_gate(
        [dict(contract)], sync=normalize_sync_mode(top), validation="strict",
        dest="postgresql", src="kafka", kind="database", source_columns=["id", "updated_at"],
        pass_status="pass", block_status="block", source_table="r61_kfa",
    )
    plan_refuses = any("Kafka cannot page" in i for i in (gate.get("details") or {}).get("issues") or [])
    request = SimpleNamespace(source=SimpleNamespace(format="kafka"), sync_mode=top,
                              stream_contracts=[dict(contract)], shape_recipe=None,
                              mappings=[], destination=None)
    execute_refuses = bool(execute_plan_refusal(request))
    assert plan_refuses is refused, gate
    assert execute_refuses is refused
