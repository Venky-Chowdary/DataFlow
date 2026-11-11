"""D361 — an scd2 schedule accepted transforms at create, then failed its first run.

A column pipeline (``column_transforms={"name": ["trim", "upper"]}``) compiles
to a pre-load ``shape_recipe``. Execute applies one on the scd2 route only when
it declares the history contract as an ``scd2`` step
(``src/transfer/engine_shape._shape_stream_refusal``); the preflight gate that
refuses the same recipe on mirror / cdc skipped scd2 whole, so MCP
create_schedule, REST create and PATCH all saved a schedule whose first run then
failed with "Transform (pre-load) is not applied on the scd2 route".

Now every surface asks Execute's question up front: MCP create_schedule and
plan_transfer (the preflight gate), REST create / PATCH (the schedule store),
and a schedule saved earlier parks before a job exists with the way out.
Live in-process SQLite -> SQLite, no services.
"""

from __future__ import annotations

import sqlite3
import time

import pytest

from tests.test_mx324_parked_schedule_releases_claim import _isolate, _wait

_PIPELINE = {"name": ["trim", "upper"]}
_RECIPE = {
    "steps": [
        {"op": "trim", "options": {}, "column": "name"},
        {"op": "case", "options": {"mode": "upper"}, "column": "name"},
    ]
}
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
        c.execute('CREATE TABLE "d361_src" (id INTEGER PRIMARY KEY, name TEXT, city TEXT)')
        c.executemany(
            'INSERT INTO "d361_src" VALUES (?,?,?)',
            [(i, f" n{i} ", f"c{i}") for i in range(5)],
        )
    src = create_connector({"name": "D361 S", "type": "sqlite", "role": "source",
                            "connection_string": f"sqlite:///{src_db}", "workspace_id": ""})
    dst = create_connector({"name": "D361 D", "type": "sqlite", "role": "destination",
                            "connection_string": f"sqlite:///{dst_db}", "workspace_id": ""})
    return src, dst, dst_db


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers.schedules_router import router

    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _body(src, dst, mode, dest_table, **extra):
    return {
        "name": f"d361 {dest_table}", "source_connector_id": src.id,
        "source_table": "d361_src", "dest_connector_id": dst.id,
        "dest_table": dest_table, "interval": "daily", "sync_mode": mode,
        "primary_key": "id", "enabled": True,
        "mappings": [{"source": c, "target": c, "confidence": 1.0} for c in _COLUMNS],
        **extra,
    }


def _mcp(src, dst, mode, column_transforms):
    from src.ai.copilot.schedule_tools import create_schedule

    return create_schedule(
        source_connector_id=src.id, source_table="d361_src",
        dest_connector_id=dst.id, dest_table=f"d361_{mode}",
        sync_mode=mode, primary_key="id", cadence="daily at 02:00",
        column_transforms=column_transforms,
    )


@pytest.mark.parametrize("mode", ["scd2", "mirror"])
def test_mcp_create_schedule_refuses_a_pipeline_on_a_history_route(tmp_path, monkeypatch, mode):
    src, dst, _ = _world(tmp_path, monkeypatch)
    out = _mcp(src, dst, mode, _PIPELINE)
    assert out.success is False
    assert "g_shape_route" in out.error
    assert f"Transform (pre-load) is not applied on the {mode} route" in out.error


def test_mcp_scd2_keeps_a_single_map_transform(tmp_path, monkeypatch):
    """A one-step Map transform rides the mapping, not a recipe — not refused."""
    src, dst, _ = _world(tmp_path, monkeypatch)
    out = _mcp(src, dst, "scd2", {"name": "upper"})
    assert out.success is True, out.error


def test_preflight_gate_asks_executes_scd2_question():
    from services.multi_stream_plan import history_shape_route_gate

    gate = history_shape_route_gate(_RECIPE, "scd2")
    assert gate and gate["status"] == "block" and gate["id"] == "g_shape_route"
    # The declared history contract is honoured, as Execute honours it.
    contract = {"steps": [
        {"op": "scd2", "options": {"keys": ["id"], "tracked": ["city"]}},
        {"op": "case", "options": {"mode": "upper"}, "column": "name"},
    ]}
    assert history_shape_route_gate(contract, "scd2") is None
    rewrites_tracked = {"steps": [contract["steps"][0], dict(contract["steps"][1], column="city")]}
    gate = history_shape_route_gate(rewrites_tracked, "scd2")
    assert gate and "rewrites history column(s) city" in gate["message"]


@pytest.mark.parametrize("mode", ["scd2", "mirror"])
def test_rest_create_refuses_a_recipe_on_a_history_route(tmp_path, monkeypatch, mode):
    src, dst, _ = _world(tmp_path, monkeypatch)
    resp = _client().post("/schedules/", json=_body(src, dst, mode, f"r_{mode}", shape_recipe=_RECIPE))
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert "Recipe steps [trim, case]" in detail
    assert f"not applied on the {mode} route" in detail
    assert 'shape_recipe {"steps": []}' in detail


def test_rest_patch_refuses_a_recipe_or_a_mode_switch_onto_scd2(tmp_path, monkeypatch):
    src, dst, _ = _world(tmp_path, monkeypatch)
    client = _client()
    plain = client.post("/schedules/", json=_body(src, dst, "scd2", "p_scd2"))
    assert plain.status_code == 201, plain.text
    resp = client.patch(f"/schedules/{plain.json()['id']}", json={"shape_recipe": _RECIPE})
    assert resp.status_code == 400, resp.text
    assert "not applied on the scd2 route" in resp.json()["detail"]

    shaped = client.post(
        "/schedules/",
        json=_body(src, dst, "full_refresh_overwrite", "p_overwrite", shape_recipe=_RECIPE),
    )
    assert shaped.status_code == 201, shaped.text
    resp = client.patch(f"/schedules/{shaped.json()['id']}", json={"sync_mode": "scd2"})
    assert resp.status_code == 400, resp.text
    assert "not applied on the scd2 route" in resp.json()["detail"]


def _save_legacy(store, sched_id: str) -> None:
    """A schedule stored before create/update refused the combination."""
    from services.shape_models import ShapeRecipe

    rows = store._load_all()
    for i, s in enumerate(rows):
        if s.id == sched_id:
            rows[i] = store.PipelineSchedule.from_dict({
                **s.to_dict(), "shape_recipe": _RECIPE,
                "approved_shape_recipe_hash": ShapeRecipe.parse(_RECIPE).recipe_hash,
            })
    store._save_all(rows)


def test_earlier_saved_scd2_schedule_parks_with_the_way_out_then_runs(tmp_path, monkeypatch):
    src, dst, dst_db = _world(tmp_path, monkeypatch)
    from services import schedule_runner
    from services import schedule_store as store
    from services.schedule_runner import ScheduleStartError

    client = _client()
    created = client.post("/schedules/", json=_body(src, dst, "scd2", "legacy_scd2"))
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    _save_legacy(store, sid)

    # An unrelated edit of the legacy row is not locked out.
    assert client.patch(f"/schedules/{sid}", json={"name": "d361 legacy"}).status_code == 200

    with pytest.raises(ScheduleStartError) as refused:
        schedule_runner._run_schedule(sid, manual=True)
    message = str(refused.value)
    assert message.startswith("Schedule cannot run — Recipe steps [trim, case]:")
    assert "not applied on the scd2 route" in message
    assert 'PATCH this schedule with shape_recipe {"steps": []}' in message
    assert len(message) <= 500
    parked = store.get_schedule(sid)
    assert parked.last_status == "needs_approval"
    assert parked.running is False and not parked.running_job_id
    assert not parked.last_job_id, "the refusal must come before a job exists"

    # The way out the message names: drop the steps, keep scd2.
    fixed = client.patch(f"/schedules/{sid}", json={"shape_recipe": {"steps": []}})
    assert fixed.status_code == 200, fixed.text
    assert store.get_schedule(sid).approved_shape_recipe_hash == ""
    doc = _wait(schedule_runner._run_schedule(sid, manual=True))
    assert doc.get("status") == "completed", doc.get("error")
    time.sleep(0.5)
    with sqlite3.connect(dst_db) as c:
        cols = [r[1] for r in c.execute('PRAGMA table_info("legacy_scd2")')]
        rows = c.execute('SELECT id, name, is_current FROM "legacy_scd2" ORDER BY id').fetchall()
    assert {"valid_from", "valid_to", "is_current", "row_hash"} <= set(cols)
    assert rows == [(i, f" n{i} ", 1) for i in range(5)]
