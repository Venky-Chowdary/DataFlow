"""Incremental append honours its watermark — exactly once, on every route.

Owner's report (round 5): "Incremental append ignores its watermark." Runs
re-read rows already loaded and append them again.

Measured on production (origin/feature/fix-qa-bug-fixes 557f556c), QA's route
(MCP ``start_transfer`` -> ``/api/v1/copilot/confirm`` -> job), real engines:

* PG -> MySQL, ``source_timezone=UTC``: run 2 appended the tie group at the old
  watermark again (10 rows, 8 distinct) and run 3, with no source change,
  appended the last row again (11 rows). The inclusive run seed re-reads the
  tie group at the watermark; the already-landed peers are meant to be dropped
  against the destination, but the drop remapped them with the destination's
  *physical* types (``TIMESTAMP_NTZ``), quarantined every tz-aware peer in its
  own remap and failed open — so every peer was delivered again. Gate-8 passed
  both runs: the append window was compared with extra rows allowed.
* PG -> PG (no ``source_timezone``): run 2 was refused at Validate
  ("Destination already stores these keys"). Validate looked the watermark up
  under ``host:127.0.0.1:5432`` while the run had stored it under
  ``id:<connector>``; Execute-time gates used a third key (``host:localhost``).
  With no watermark, every pre-write check judged the whole table.

Contract pinned here, per route and engine path: one durable watermark (the
route's ``sync_cursors`` entry) read under one key by Validate, Execute and the
reader; the tie group re-read and already-landed peers dropped; the watermark
advanced with the committed rows and reported on the job (old -> new, rows
selected); Gate-8 failing a window that holds a row twice.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Source tables
# --------------------------------------------------------------------------- #

_TS = {"postgresql": "TIMESTAMP", "pg_tstz": "TIMESTAMPTZ", "mysql": "DATETIME",
       "sqlserver": "DATETIME2(0)"}
_TXT = {"postgresql": "TEXT", "mysql": "VARCHAR(40)", "sqlserver": "NVARCHAR(40)"}

# NULL, '', whitespace and unicode ride on every run.
_RUN1 = [(1, "a", 1), (2, None, 1), (3, "", 2), (4, "  ", 3), (5, "Łukasz 山田", 3)]
# Ties at the stored watermark (cursor 3): one above the last key read (6) and
# one below it (0); plus a strictly newer row.
_RUN2 = [(6, "tie", 3), (0, "tie-low 🎉", 3), (7, "new", 4)]


def _engine(spec: str) -> str:
    return "postgresql" if spec == "pg_tstz" else spec


def _ts(n: int, spec: str) -> str:
    base = f"2024-01-01 00:00:{n:02d}"
    return base + "+00" if spec == "pg_tstz" else base


def _insert(spec: str, table: str, rows) -> None:
    kind = _engine(spec)
    for rid, note, n in rows:
        if note is None:
            lit = "NULL"
        else:
            esc = note.replace("'", "''")
            lit = f"N'{esc}'" if kind == "sqlserver" else f"'{esc}'"
        h.execute(
            kind,
            f"INSERT INTO {h.quote(kind, table)} (id, note, updated_at) "  # nosec B608
            f"VALUES ({rid}, {lit}, '{_ts(n, spec)}')",
        )


def _create_source(spec: str, table: str) -> None:
    kind = _engine(spec)
    h.execute(
        kind,
        f"CREATE TABLE {h.quote(kind, table)} (id INT PRIMARY KEY, "  # nosec B608
        f"note {_TXT[kind]} NULL, updated_at {_TS[spec]} NOT NULL)",
    )


def _dest_rows(kind: str, table: str) -> list[tuple]:
    rows = h.query(kind, f"SELECT id, note FROM {h.quote(kind, table)}")  # nosec B608
    return sorted(((int(r[0]), r[1]) for r in rows), key=lambda r: (r[0], str(r[1])))


def _expected(*batches) -> list[tuple]:
    out = [(rid, note) for batch in batches for rid, note, _n in batch]
    return sorted(out, key=lambda r: (r[0], str(r[1])))


def _connectors(token: str, src_spec: str, dst_kind: str):
    from services.connector_store import create_connector

    src = create_connector({
        "name": f"r5wm-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
        **h.dest_config(_engine(src_spec)),
    })
    dst = create_connector({
        "name": f"r5wm-dst-{token}", "role": "destination", "workspace_id": "", "ssl": False,
        **h.dest_config(dst_kind),
    })
    return src, dst


def _report(job: dict) -> dict:
    """The watermark the job reports: old -> new and the rows it selected."""
    rep = job.get("incremental_watermark")
    assert isinstance(rep, dict), {k: job.get(k) for k in ("cursor_value", "cursor_key")}
    return rep


def _assert_report(job: dict, *, before: bool, rows_selected: int, moved: bool) -> None:
    rep = _report(job)
    assert rep.get("cursor_column") == "updated_at", rep
    assert rep.get("cursor_key"), rep
    assert (rep.get("before") not in (None, "")) is before, rep
    assert rep.get("after") not in (None, ""), rep
    assert int(rep.get("rows_selected")) == rows_selected, rep
    assert bool(rep.get("advanced")) is moved, rep
    # The job, the route store and the report agree on the new watermark.
    from services.sync_cursor import get_watermark

    assert get_watermark(str(rep["cursor_key"])) == rep["after"], rep
    assert str(job.get("cursor_value") or "") == rep["after"], rep


def _three_runs(run, src_spec: str, src_table: str, dst_kind: str, dest_table: str):
    """Run 1, ties + new rows, run 2, quiet run 3 — exact contents each time."""
    j1 = run()
    assert _dest_rows(dst_kind, dest_table) == _expected(_RUN1)
    _assert_report(j1, before=False, rows_selected=len(_RUN1), moved=True)

    _insert(src_spec, src_table, _RUN2)
    j2 = run()
    got = _dest_rows(dst_kind, dest_table)
    assert got == _expected(_RUN1, _RUN2), {"run": 2, "rows": got}
    _assert_report(j2, before=True, rows_selected=len(_RUN2), moved=True)
    assert _report(j2)["before"] == _report(j1)["after"]

    j3 = run()  # quiet poll: nothing new at the source
    got = _dest_rows(dst_kind, dest_table)
    assert got == _expected(_RUN1, _RUN2), {"run": 3, "rows": got}
    _assert_report(j3, before=True, rows_selected=0, moved=False)
    assert _report(j3)["after"] == _report(j2)["after"]


# --------------------------------------------------------------------------- #
# 1. QA's route: MCP start_transfer -> /api/v1/copilot/confirm -> job
# --------------------------------------------------------------------------- #

_MCP_ROUTES = [
    ("postgresql", "postgresql"),
    ("postgresql", "mysql"),
    ("pg_tstz", "mysql"),
    ("mysql", "postgresql"),
    ("sqlserver", "postgresql"),
]


@pytest.mark.parametrize("source_timezone", ["UTC", ""], ids=["tz_utc", "tz_none"])
@pytest.mark.parametrize(
    "src_spec,dst_kind", _MCP_ROUTES, ids=[f"{s}_to_{d}" for s, d in _MCP_ROUTES]
)
def test_mcp_incremental_append_is_exactly_once_across_three_runs(
    client, src_spec, dst_kind, source_timezone
):
    token = uuid.uuid4().hex[:8]
    src_table = f"r5wm_s_{token}"
    dest_table = f"r5wm_d_{token}"
    src_kind = _engine(src_spec)
    try:
        _create_source(src_spec, src_table)
        _insert(src_spec, src_table, _RUN1)
        src, dst = _connectors(token, src_spec, dst_kind)
        args: dict[str, Any] = {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "sync_mode": "incremental_append", "validation_mode": "strict",
            "cursor_column": "updated_at", "cursor_semantics": "monotonic_sequence",
        }
        if source_timezone:
            args["source_timezone"] = source_timezone

        def run() -> dict:
            return h.stage_and_run(client, "start_transfer", args)

        _three_runs(run, src_spec, src_table, dst_kind, dest_table)
    finally:
        h.drop_table(src_kind, src_table)
        h.drop_table(dst_kind, dest_table)


# --------------------------------------------------------------------------- #
# 2. Scheduled runs: MCP create_schedule -> confirm -> scheduler beats
# --------------------------------------------------------------------------- #


def _beat(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})
    started = schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    assert started == 1, {
        "last_status": getattr(sched, "last_status", None),
        "last_error": getattr(sched, "last_error", None),
        "approval": getattr(sched, "approval_request", None),
    }
    job_id = str(getattr(sched, "running_job_id", "") or getattr(sched, "last_job_id", "") or "")
    job = h.wait_job(job_id)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not str(getattr(get_schedule(schedule_id), "running_job_id", "") or "").strip():
            break
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    assert str(job.get("status")) == "completed", {
        "status": job.get("status"), "error": job.get("error") or job.get("message"),
    }
    return job


_SCHED_ROUTES = [("postgresql", "postgresql"), ("pg_tstz", "mysql")]


@pytest.mark.parametrize(
    "src_spec,dst_kind", _SCHED_ROUTES, ids=[f"{s}_to_{d}" for s, d in _SCHED_ROUTES]
)
def test_scheduled_incremental_append_is_exactly_once_across_three_beats(
    client, src_spec, dst_kind
):
    from services.schedule_store import get_schedule

    token = uuid.uuid4().hex[:8]
    src_table = f"r5wm_s_{token}"
    dest_table = f"r5wm_d_{token}"
    try:
        _create_source(src_spec, src_table)
        _insert(src_spec, src_table, _RUN1)
        src, dst = _connectors(token, src_spec, dst_kind)
        ok, staged = h.mcp_call(client, "create_schedule", {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "cadence": "every 15 minutes", "sync_mode": "incremental_append",
            "cursor_column": "updated_at", "cursor_semantics": "monotonic_sequence",
            "name": f"r5wm-{token}",
        })
        assert ok, staged
        assert not h.blockers_of(staged), h.blockers_of(staged)
        resp = client.post(
            "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r5wm"}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("ok") is True, body
        schedule_id = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")
        assert get_schedule(schedule_id) is not None, body

        jobs: list[dict] = []

        def run() -> dict:
            jobs.append(_beat(schedule_id))
            return jobs[-1]

        _three_runs(run, src_spec, src_table, dst_kind, dest_table)
        # The schedule's own bookkeeping follows the route watermark.
        assert get_schedule(schedule_id).cursor_value == _report(jobs[-1])["after"]
    finally:
        h.drop_table(_engine(src_spec), src_table)
        h.drop_table(dst_kind, dest_table)


# --------------------------------------------------------------------------- #
# 3. The UI's REST route: /api/v1/preflight/run then /api/v1/transfer/execute
# --------------------------------------------------------------------------- #


def test_rest_preflight_and_execute_read_the_route_watermark(client):
    token = uuid.uuid4().hex[:8]
    src_table = f"r5wm_s_{token}"
    dest_table = f"r5wm_d_{token}"
    contracts = [{
        "name": src_table, "selected": True, "sync_mode": "incremental_append",
        "cursor_field": "updated_at", "cursor_semantics": "monotonic_sequence",
    }]
    mappings = [{"source": c, "target": c, "confidence": 1.0}
                for c in ("id", "note", "updated_at")]
    try:
        _create_source("postgresql", src_table)
        _insert("postgresql", src_table, _RUN1)
        src, dst = _connectors(token, "postgresql", "postgresql")

        def preflight() -> dict:
            resp = client.post("/api/v1/preflight/run", json={
                "dataset_name": src_table, "columns": ["id", "note", "updated_at"],
                "column_types": {"id": "integer", "note": "text",
                                 "updated_at": "timestamp without time zone"},
                "mappings": mappings, "source_connector_id": src.id,
                "source_kind": "database", "source_type": "postgresql",
                "source_table": src_table, "connector_id": dst.id,
                "dest_kind": "database", "dest_type": "postgresql",
                "dest_table": dest_table, "sync_mode": "incremental_append",
                "stream_contracts": contracts, "validation_mode": "strict",
            })
            assert resp.status_code == 200, resp.text
            return resp.json()

        def run() -> dict:
            pf = preflight()
            blockers = [b for b in (pf.get("blockers") or []) if isinstance(b, dict)]
            assert not [b for b in blockers if "collision" in str(b.get("id") or "")], blockers
            resp = client.post("/api/v1/transfer/execute", json={
                "source": {"kind": "database", "format": "postgresql",
                           "connector_id": src.id, "table": src_table},
                "destination": {"kind": "database", "format": "postgresql",
                                "connector_id": dst.id, "table": dest_table},
                "mappings": mappings, "sync_mode": "incremental_append",
                "stream_contracts": contracts, "validation_mode": "strict",
                "async_mode": False, "skip_preflight": True,
            })
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body.get("success") is True, body
            from services.mongodb_service import get_mongodb_service

            return get_mongodb_service().get_job(str(body["job_id"])) or {}

        _three_runs(run, "postgresql", src_table, "postgresql", dest_table)
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("postgresql", dest_table)


# --------------------------------------------------------------------------- #
# 4. Gate-8 verifies the append window on its own
# --------------------------------------------------------------------------- #


def test_gate8_fails_an_append_window_that_holds_a_row_twice(client, monkeypatch):
    """The tie-peer drop is one line of defence; Gate-8 is the proof.

    With the drop disabled (the production defect: it failed open and every
    peer was delivered again), the destination window holds the old peers
    twice. Gate-8 must fail that run, restore the destination and keep the
    watermark where the last good run left it — the next run then re-reads
    the same window instead of skipping past a bad load.
    """
    import src.transfer.stream  # noqa: F401  (stream_db_page imports it)
    import src.transfer.stream_db_page as page

    from services.sync_cursor import get_watermark, list_cursor_keys

    token = uuid.uuid4().hex[:8]
    src_table = f"r5wm_s_{token}"
    dest_table = f"r5wm_d_{token}"
    try:
        _create_source("pg_tstz", src_table)
        _insert("pg_tstz", src_table, _RUN1)
        src, dst = _connectors(token, "pg_tstz", "mysql")
        args = {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "sync_mode": "incremental_append", "validation_mode": "strict",
            "cursor_column": "updated_at", "cursor_semantics": "monotonic_sequence",
        }
        h.stage_and_run(client, "start_transfer", args)
        (key,) = list_cursor_keys()
        mark = get_watermark(key)
        assert mark

        _insert("pg_tstz", src_table, _RUN2)
        with monkeypatch.context() as fault:
            fault.setattr(page, "_boundary_rows_already_landed", lambda *a, **k: set())
            ok, staged = h.mcp_call(client, "start_transfer", args)
            assert ok and not h.blockers_of(staged), staged
            resp = client.post(
                "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "r5wm"}
            )
            assert resp.status_code == 200, resp.text
            job = h.wait_job(str(resp.json().get("job_id") or ""))
        assert str(job.get("status")) == "failed", {
            "status": job.get("status"), "rows": _dest_rows("mysql", dest_table),
        }
        assert "twice" in str(job.get("error") or ""), job.get("error")
        assert _dest_rows("mysql", dest_table) == _expected(_RUN1)
        assert get_watermark(key) == mark

        # The fault gone, the next run re-reads the same window exactly once.
        again = h.stage_and_run(client, "start_transfer", args)
        assert _dest_rows("mysql", dest_table) == _expected(_RUN1, _RUN2)
        assert _report(again)["before"] == mark
    finally:
        h.drop_table("postgresql", src_table)
        h.drop_table("mysql", dest_table)
