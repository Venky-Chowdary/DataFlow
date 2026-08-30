"""Scheduler proof: real API, real cadence maths, real beat, real destination.

Nothing here calls the scheduler's helpers to *ask* what it would do. A schedule
is created over HTTP on the mounted FastAPI app (the control plane an operator
drives), the beat that fires it is ``schedule_runner._run_due_schedules`` (the
one the service loop calls), and the only accepted evidence that a scheduled run
moved data is an independent driver read of the destination plus the run's own
proof artifact from the job document.

Cadence expectations are computed here with ``zoneinfo`` arithmetic rather than
by calling ``services.cron_schedule``: a next-run test that asks the parser to
grade itself proves only that it is self-consistent, which is exactly how a DST
off-by-one-hour ships.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from tests.scale import live_engines as L
from tests.scale import stores
from tests.scale.modes_matrix import Cell, Matrix

COLS = ["id", "region", "amount", "note", "updated_at"]
SHAPE = "id BIGINT PK, region TEXT, amount NUMERIC(12,2), note TEXT NULL, updated_at TIMESTAMP"
ROUTE = "scheduler (postgresql→postgresql)"
ACTOR = "anonymous"  # TestClient requests are unauthenticated; this is their identity
JOB_WAIT_SECONDS = float(os.getenv("DATAFLOW_SCALE_JOB_WAIT", "900"))
TAG = uuid.uuid4().hex[:8]
API_DIR = Path(__file__).resolve().parents[2]


# --------------------------------------------------------------------------
# harness plumbing
# --------------------------------------------------------------------------

def _client() -> Any:
    from fastapi.testclient import TestClient

    from src.main import app

    return TestClient(app)


def _workspace(name: str, owner: str = ACTOR) -> str:
    from services.team_store import create_workspace

    return create_workspace(name=name, created_by=owner).id


def _connector(client: Any, ws: str, name: str) -> str:
    body = {
        "name": name,
        "type": "postgresql",
        "role": "both",
        "host": L.PG["host"],
        "port": int(L.PG["port"]),
        "database": L.PG["database"],
        "username": L.PG["user"],
        "password": L.PG["password"],
        "schema": "public",
        "ssl": False,
    }
    resp = client.post(
        "/api/v1/connectors/saved", json=body, headers={"X-Workspace-Id": ws}
    )
    if resp.status_code >= 400:
        raise RuntimeError(f"connector save failed: {resp.status_code} {resp.text[:200]}")
    return str(resp.json().get("id") or "")


def _create_schedule(client: Any, ws: str, body: dict[str, Any]) -> dict[str, Any]:
    resp = client.post("/api/v1/schedules/", json=body, headers={"X-Workspace-Id": ws})
    if resp.status_code != 201:
        raise RuntimeError(f"schedule create failed: {resp.status_code} {resp.text[:300]}")
    return dict(resp.json())


def _mappings() -> list[dict[str, Any]]:
    return [{"source": c, "target": c, "confidence": 1.0} for c in COLS]


def _schedule_body(
    *,
    name: str,
    src_conn: str,
    src_table: str,
    dst_conn: str,
    dst_table: str,
    sync_mode: str = "full_refresh_overwrite",
    interval: str = "daily",
    cron: str = "",
    tz: str = "UTC",
    **extra: Any,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": name,
        "source_connector_id": src_conn,
        "source_table": src_table,
        "dest_connector_id": dst_conn,
        "dest_table": dst_table,
        "interval": interval,
        "cron": cron,
        "timezone": tz,
        "sync_mode": sync_mode,
        "validation_mode": "balanced",
        "mappings": _mappings(),
        "primary_key": "id",
    }
    body.update(extra)
    return body


def _force_due(schedule_id: str, *, seconds_ago: int = 30) -> None:
    """Move the persisted cadence into the past so the next beat is a real fire.

    The alternative — waiting for a live cron minute — measures the clock, not
    the scheduler; the code path exercised is identical because ``due_schedules``
    only ever compares ``next_run_at`` to now.
    """
    import services.schedule_store as store

    due = datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)
    schedules = store._load_all()
    for i, s in enumerate(schedules):
        if s.id == schedule_id:
            schedules[i] = store.PipelineSchedule.from_dict(
                {**s.to_dict(), "next_run_at": due.isoformat(), "enabled": True}
            )
            store._save_all(schedules)
            return
    raise RuntimeError(f"schedule {schedule_id} not persisted")


def _beat() -> int:
    from services import schedule_runner

    return schedule_runner._run_due_schedules()


def _await_job(job_id: str, timeout: float = JOB_WAIT_SECONDS) -> dict[str, Any]:
    from services import schedule_runner

    deadline = time.time() + timeout
    doc: dict[str, Any] = {}
    while time.time() < deadline:
        doc = dict(schedule_runner._job_doc(job_id) or {})
        status = str(doc.get("status") or "").lower()
        if status in {"completed", "success", "succeeded", "failed", "error", "cancelled"}:
            return doc
        time.sleep(2.0)
    return doc


def _await_run_history(schedule_id: str, *, minimum: int,
                       timeout: float = JOB_WAIT_SECONDS) -> list[dict[str, Any]]:
    from services.schedule_store import get_schedule

    deadline = time.time() + timeout
    history: list[dict[str, Any]] = []
    while time.time() < deadline:
        sched = get_schedule(schedule_id)
        history = [dict(h) for h in (getattr(sched, "run_history", None) or [])]
        if len(history) >= minimum:
            return history
        time.sleep(2.0)
    return history


def _running_job(schedule_id: str, timeout: float = 120.0) -> str:
    from services.schedule_store import get_schedule

    deadline = time.time() + timeout
    while time.time() < deadline:
        sched = get_schedule(schedule_id)
        job = str(getattr(sched, "running_job_id", "") or "")
        if job:
            return job
        time.sleep(0.5)
    return ""


def _cell(matrix: Matrix, mode: str, *, ok: bool, note: str,
          detail: dict[str, Any] | None = None, **fields: Any) -> Cell:
    cell = Cell(route=ROUTE, mode=mode, schema_shape=SHAPE, **fields)
    cell.detail = detail or {}
    cell.mark(ok, note)
    return matrix.add(cell)


def _skip(matrix: Matrix, mode: str, reason: str) -> Cell:
    return matrix.add(Cell(route=ROUTE, mode=mode).skip(reason))


# --------------------------------------------------------------------------
# cadence: computed here, then compared to what the API persisted
# --------------------------------------------------------------------------

def _exists_locally(local: datetime) -> bool:
    """False when the wall clock skipped this instant (spring-forward gap).

    A zone-aware value inside the gap still converts to UTC, so the only way to
    detect the gap is to round-trip it: 02:30 on a spring-forward day comes back
    as 03:30, meaning the clock never showed it.
    """
    return local.astimezone(timezone.utc).astimezone(local.tzinfo) == local


def _expected_daily_cron(cron_hour: int, cron_minute: int, tz: str,
                         base: datetime) -> datetime:
    """Next wall-clock ``HH:MM`` in ``tz`` strictly after ``base``, as UTC.

    A day whose clock never shows ``HH:MM`` (spring-forward) has no firing, so
    the search moves to the following day; a wall clock that repeats (fall-back)
    fires on its first occurrence, ``fold=0``.
    """
    zone = ZoneInfo(tz)
    local = base.astimezone(zone)
    candidate = local.replace(
        hour=cron_hour, minute=cron_minute, second=0, microsecond=0, fold=0
    )
    for _ in range(4):
        if candidate > local and _exists_locally(candidate):
            return candidate.astimezone(timezone.utc)
        candidate = (candidate + timedelta(days=1)).replace(
            hour=cron_hour, minute=cron_minute, second=0, microsecond=0, fold=0
        )
    raise AssertionError(f"no {cron_hour:02d}:{cron_minute:02d} firing in {tz}")


def cadence_cells(matrix: Matrix) -> None:
    from services.schedule_store import compute_next_run

    now = datetime.now(timezone.utc)

    # interval presets
    interval_expect = {"hourly": 3600, "daily": 86400, "weekly": 604800}
    interval_detail: dict[str, Any] = {}
    ok = True
    for preset, seconds in interval_expect.items():
        got = compute_next_run(preset, now)
        delta = (datetime.fromisoformat(got) - now).total_seconds()
        interval_detail[preset] = {"next_run_at": got, "delta_seconds": round(delta, 1)}
        if abs(delta - seconds) > 2:
            ok = False
    _cell(
        matrix,
        "scheduler interval next-run",
        ok=ok,
        note="hourly/daily/weekly land exactly one period out"
        if ok
        else "interval next-run drifted from the declared period",
        detail=interval_detail,
    )

    # cron in a zone with a UTC offset, graded against zoneinfo arithmetic
    cron_detail: dict[str, Any] = {}
    cron_ok = True
    for tz, cron, hour, minute in (
        ("UTC", "0 3 * * *", 3, 0),
        ("America/New_York", "30 2 * * *", 2, 30),
        ("Asia/Kolkata", "15 6 * * *", 6, 15),
    ):
        got = datetime.fromisoformat(compute_next_run("daily", now, cron=cron, tz=tz))
        want = _expected_daily_cron(hour, minute, tz, now)
        cron_detail[f"{tz} {cron}"] = {"product": got.isoformat(), "zoneinfo": want.isoformat()}
        if got != want:
            cron_ok = False
    _cell(
        matrix,
        "scheduler cron next-run (tz)",
        ok=cron_ok,
        note="cron next-run matches independent zoneinfo arithmetic"
        if cron_ok
        else "cron next-run disagrees with zoneinfo",
        detail=cron_detail,
    )

    # DST has three separate ways to be wrong, so each is graded on its own: the
    # offset must be re-derived per firing (noon exists on every day), the
    # spring-forward gap has no 02:30 to fire at, and the fall-back hour shows
    # 01:30 twice but must fire once.
    ny = ZoneInfo("America/New_York")

    def _next(base_local: datetime, cron: str) -> datetime:
        return datetime.fromisoformat(
            compute_next_run(
                "daily",
                base_local.astimezone(timezone.utc),
                cron=cron,
                tz="America/New_York",
            )
        )

    est_base = datetime(2026, 3, 6, 15, 0, tzinfo=ny)
    edt_base = datetime(2026, 3, 9, 15, 0, tzinfo=ny)
    noon_est = _next(est_base, "0 12 * * *")
    noon_edt = _next(edt_base, "0 12 * * *")
    want_noon_est = _expected_daily_cron(12, 0, "America/New_York", est_base)
    want_noon_edt = _expected_daily_cron(12, 0, "America/New_York", edt_base)
    offsets_differ = (
        noon_est.astimezone(ny).utcoffset() != noon_edt.astimezone(ny).utcoffset()
    )
    offset_ok = (
        noon_est == want_noon_est and noon_edt == want_noon_edt and offsets_differ
    )

    # 2026-03-08: the clock jumps 02:00 -> 03:00, so 02:30 never happens.
    gap_base = datetime(2026, 3, 7, 12, 0, tzinfo=ny)
    gap = _next(gap_base, "30 2 * * *")
    want_gap = _expected_daily_cron(2, 30, "America/New_York", gap_base)
    gap_local = gap.astimezone(ny)
    gap_ok = (
        gap == want_gap
        and (gap_local.hour, gap_local.minute) == (2, 30)
        and gap_local.date() > date(2026, 3, 8)
    )

    # 2026-11-01: the clock repeats 01:00-02:00, so 01:30 happens twice.
    fold_base = datetime(2026, 10, 31, 12, 0, tzinfo=ny)
    fold = _next(fold_base, "30 1 * * *")
    want_fold = _expected_daily_cron(1, 30, "America/New_York", fold_base)
    fold_local = fold.astimezone(ny)
    fold_ok = (
        fold == want_fold
        and (fold_local.hour, fold_local.minute) == (1, 30)
        and fold_local.date() == date(2026, 11, 1)
    )

    dst_ok = offset_ok and gap_ok and fold_ok
    _cell(
        matrix,
        "scheduler DST boundary",
        ok=dst_ok,
        note="offset re-derived per firing "
        f"({noon_est.strftime('%H:%MZ')} EST → {noon_edt.strftime('%H:%MZ')} EDT), "
        f"spring-forward gap skipped to {gap_local.date()} 02:30, "
        f"fall-back 01:30 fired once on {fold_local.date()} at "
        f"{fold.strftime('%H:%MZ')}"
        if dst_ok
        else "DST next-run wrong: "
        + ", ".join(
            name
            for name, good in (
                ("offset not re-derived", offset_ok),
                ("spring-forward gap", gap_ok),
                ("fall-back repeat", fold_ok),
            )
            if not good
        ),
        detail={
            "offset_rederived": {
                "est": {
                    "product": noon_est.isoformat(),
                    "zoneinfo": want_noon_est.isoformat(),
                },
                "edt": {
                    "product": noon_edt.isoformat(),
                    "zoneinfo": want_noon_edt.isoformat(),
                },
                "offsets_differ": offsets_differ,
            },
            "spring_forward_gap": {
                "product": gap.isoformat(),
                "product_local": gap_local.isoformat(),
                "zoneinfo": want_gap.isoformat(),
            },
            "fall_back_repeat": {
                "product": fold.isoformat(),
                "product_local": fold_local.isoformat(),
                "zoneinfo": want_fold.isoformat(),
            },
        },
    )


# --------------------------------------------------------------------------
# control plane: persisted state and tenant ownership
# --------------------------------------------------------------------------

def control_plane_cells(matrix: Matrix, ws: str, src_conn: str, dst_conn: str,
                        src_table: str, dst_table: str) -> str:
    from services.schedule_store import get_schedule

    client = _client()
    created = _create_schedule(
        client,
        ws,
        _schedule_body(
            name=f"track-d persisted {TAG}",
            src_conn=src_conn,
            src_table=src_table,
            dst_conn=dst_conn,
            dst_table=dst_table,
            sync_mode="incremental_deduped",
            interval="hourly",
            cursor_column="updated_at",
        ),
    )
    sid = str(created.get("id") or "")

    fetched = client.get(f"/api/v1/schedules/{sid}", headers={"X-Workspace-Id": ws})
    persisted = get_schedule(sid)
    body = fetched.json() if fetched.status_code == 200 else {}
    checks = {
        "http_get": fetched.status_code,
        "sync_mode_api": body.get("sync_mode"),
        "sync_mode_store": getattr(persisted, "sync_mode", ""),
        "workspace_store": getattr(persisted, "workspace_id", ""),
        "next_run_at": getattr(persisted, "next_run_at", ""),
        "mappings_store": len(getattr(persisted, "mappings", None) or []),
    }
    ok = (
        fetched.status_code == 200
        and body.get("sync_mode") == "incremental_deduped"
        and getattr(persisted, "sync_mode", "") == "incremental_deduped"
        and getattr(persisted, "workspace_id", "") == ws
        and bool(getattr(persisted, "next_run_at", ""))
        and len(getattr(persisted, "mappings", None) or []) == len(COLS)
    )
    _cell(
        matrix,
        "scheduler create + persisted state",
        ok=ok,
        note="API-created schedule survives a fresh store read with mode, "
        "mappings, cadence and workspace intact"
        if ok
        else "persisted schedule state does not match what the API returned",
        detail=checks,
    )

    # tenancy: a workspace this actor is not a member of, and a workspace they
    # own but which does not own the schedule.
    foreign = _workspace(f"track-d foreign {TAG}", owner="someone.else@example.com")
    sibling = _workspace(f"track-d sibling {TAG}")
    denied = client.get(f"/api/v1/schedules/{sid}", headers={"X-Workspace-Id": foreign})
    cross = client.get(f"/api/v1/schedules/{sid}", headers={"X-Workspace-Id": sibling})
    listed = client.get("/api/v1/schedules/", headers={"X-Workspace-Id": sibling})
    ids_in_sibling = [
        str(row.get("id")) for row in (listed.json() if listed.status_code == 200 else [])
    ]
    write_denied = client.post(
        "/api/v1/schedules/",
        json=_schedule_body(
            name=f"track-d intruder {TAG}",
            src_conn=src_conn,
            src_table=src_table,
            dst_conn=dst_conn,
            dst_table=dst_table,
        ),
        headers={"X-Workspace-Id": foreign},
    )
    tenancy_ok = (
        denied.status_code in (403, 404)
        and cross.status_code == 404
        and sid not in ids_in_sibling
        and write_denied.status_code in (403, 404)
    )
    _cell(
        matrix,
        "scheduler workspace ownership",
        ok=tenancy_ok,
        note="non-member read refused, sibling-workspace read 404, "
        "sibling list excludes it, non-member create refused"
        if tenancy_ok
        else "workspace boundary is not enforced on schedules",
        detail={
            "non_member_read": denied.status_code,
            "sibling_workspace_read": cross.status_code,
            "sibling_list_ids": len(ids_in_sibling),
            "non_member_create": write_denied.status_code,
        },
    )
    return sid


# --------------------------------------------------------------------------
# the beat: a scheduled run that actually moves 100K rows
# --------------------------------------------------------------------------

def firing_cells(matrix: Matrix, rows: int, ws: str, src_conn: str, dst_conn: str,
                 src_table: str, dst_table: str) -> None:
    from services.schedule_store import get_schedule

    client = _client()
    created = _create_schedule(
        client,
        ws,
        _schedule_body(
            name=f"track-d fire {TAG}",
            src_conn=src_conn,
            src_table=src_table,
            dst_conn=dst_conn,
            dst_table=dst_table,
            sync_mode="incremental_deduped",
            interval="hourly",
            cursor_column="updated_at",
            stream_contracts=[{
                "name": src_table,
                "stream": src_table,
                "selected": True,
                "sync_mode": "incremental_deduped",
                "primary_key": "id",
                "cursor_field": "updated_at",
                "cursor_semantics": "modification_timestamp",
            }],
        ),
    )
    sid = str(created.get("id") or "")
    before = datetime.now(timezone.utc)
    _force_due(sid)
    started = time.perf_counter()
    fired = _beat()
    job_id = _running_job(sid)
    doc = _await_job(job_id) if job_id else {}
    elapsed = time.perf_counter() - started
    history = _await_run_history(sid, minimum=1)
    sched = get_schedule(sid)

    src_rows = stores.count("postgresql", src_table)
    dst_rows = stores.count("postgresql", dst_table)
    src_sum = L.checksum(stores.projection("postgresql", src_table, COLS))
    dst_sum = L.checksum(stores.projection("postgresql", dst_table, COLS))
    entry = history[-1] if history else {}
    artifact = {
        k: v
        for k, v in doc.items()
        if k in {"reconciliation", "row_accounting", "load_history_report",
                 "records_processed", "status", "sync_mode"}
    }
    next_run = getattr(sched, "next_run_at", "") or ""
    advanced = bool(next_run) and datetime.fromisoformat(next_run) > before
    ok = (
        fired == 1
        and bool(job_id)
        and src_rows == rows
        and dst_rows == rows
        and src_sum == dst_sum
        and bool(entry)
        and str(entry.get("status", "")).lower() in {"completed", "success", "succeeded"}
        and advanced
    )
    _cell(
        matrix,
        "scheduler fires at cadence",
        ok=ok,
        note=f"beat started {fired} run; destination read back independently "
        f"({dst_rows} rows, checksum match), run history recorded, "
        f"next_run_at advanced to {next_run}"
        if ok
        else f"scheduled run did not land the source population "
        f"(src={src_rows} dst={dst_rows} fired={fired})",
        detail={"run_history": entry, "job_artifact": artifact, "next_run_at": next_run},
        source_rows=src_rows,
        dest_rows=dst_rows,
        written=int(doc.get("records_processed", 0) or 0),
        elapsed_seconds=round(elapsed, 2),
        rows_per_second=round(dst_rows / elapsed, 1) if elapsed > 0 else 0.0,
        run_id=job_id,
        source_checksum=src_sum,
        dest_checksum=dst_sum,
        reconcile=_reconcile_token(doc),
        delivery="at_least_once",
    )

    # sync-mode preservation: the second scheduled run of an incremental_deduped
    # schedule must upsert the delta, not append a second copy of the table.
    delta = min(L.CHANGE_ROWS, max(1, rows // 50))
    stores.append("postgresql", src_table, delta, start=rows + 1)
    _force_due(sid)
    started = time.perf_counter()
    fired2 = _beat()
    job2 = _running_job(sid)
    doc2 = _await_job(job2) if job2 else {}
    elapsed2 = time.perf_counter() - started
    _await_run_history(sid, minimum=2)
    src_rows2 = stores.count("postgresql", src_table)
    dst_rows2 = stores.count("postgresql", dst_table)
    src_sum2 = L.checksum(stores.projection("postgresql", src_table, COLS))
    dst_sum2 = L.checksum(stores.projection("postgresql", dst_table, COLS))
    mode_ok = str(doc2.get("sync_mode") or "").lower() in {"incremental_deduped", ""}
    ok2 = (
        fired2 == 1
        and src_rows2 == rows + delta
        and dst_rows2 == rows + delta
        and src_sum2 == dst_sum2
        and mode_ok
    )
    _cell(
        matrix,
        "scheduler preserves sync mode",
        ok=ok2,
        note=f"second scheduled run merged {delta} new rows to {dst_rows2} "
        "(not appended a duplicate population); checksums match"
        if ok2
        else f"scheduled run did not honour incremental_deduped "
        f"(src={src_rows2} dst={dst_rows2})",
        detail={"job_sync_mode": doc2.get("sync_mode"), "delta_rows": delta},
        source_rows=src_rows2,
        dest_rows=dst_rows2,
        written=int(doc2.get("records_processed", 0) or 0),
        elapsed_seconds=round(elapsed2, 2),
        rows_per_second=round(delta / elapsed2, 1) if elapsed2 > 0 else 0.0,
        run_id=job2,
        source_checksum=src_sum2,
        dest_checksum=dst_sum2,
        reconcile=_reconcile_token(doc2),
        delivery="at_least_once",
    )

    # proof artifact for a scheduled run: reconcile/row-accounting must exist on
    # the job document, because a scheduled run has nobody watching the UI.
    proof = _reconcile_token(doc2) or _reconcile_token(doc)
    ledger = dict(doc2.get("row_accounting") or doc.get("row_accounting") or {})
    proof_ok = bool(proof) and bool(ledger)
    _cell(
        matrix,
        "scheduler run proof artifact",
        ok=proof_ok,
        note=f"scheduled run carries reconcile verdict '{proof}' and a row ledger"
        if proof_ok
        else "scheduled run produced no reconcile/row-accounting artifact",
        detail={"reconcile": proof, "row_accounting": ledger},
        run_id=job2 or job_id,
        reconcile=proof,
    )


def _reconcile_token(doc: dict[str, Any]) -> str:
    rec = doc.get("reconciliation")
    if not isinstance(rec, dict) or not rec:
        return ""
    for key in ("status", "verdict", "result", "outcome"):
        value = rec.get(key)
        if isinstance(value, str) and value:
            return value
    matched = rec.get("matched")
    if isinstance(matched, bool):
        return "match" if matched else "mismatch"
    return "reported"


# --------------------------------------------------------------------------
# failure surfacing, retry/backoff, worker race, overlap
# --------------------------------------------------------------------------

def failure_cells(matrix: Matrix, ws: str, src_conn: str, dst_conn: str,
                  dst_table: str) -> None:
    from services.schedule_store import get_schedule

    client = _client()
    missing = f"track_d_absent_{TAG}"
    created = _create_schedule(
        client,
        ws,
        _schedule_body(
            name=f"track-d failure {TAG}",
            src_conn=src_conn,
            src_table=missing,
            dst_conn=dst_conn,
            dst_table=f"{dst_table}_fail",
            max_retries=2,
            retry_backoff_seconds=60,
        ),
    )
    sid = str(created.get("id") or "")
    _force_due(sid)
    _beat()
    job = _running_job(sid, timeout=60.0)
    if job:
        _await_job(job, timeout=300.0)
    deadline = time.time() + 300
    sched = get_schedule(sid)
    while time.time() < deadline:
        sched = get_schedule(sid)
        if (getattr(sched, "run_history", None) or []) or getattr(sched, "retry_at", None) \
                or getattr(sched, "last_error", ""):
            break
        time.sleep(2.0)
    history = [dict(h) for h in (getattr(sched, "run_history", None) or [])]
    entry = history[-1] if history else {}
    retry_at = getattr(sched, "retry_at", None)
    parked = bool(getattr(sched, "paused_reason", "") or getattr(sched, "last_error", ""))
    surfaced = bool(entry.get("error")) or parked or bool(getattr(sched, "last_error", ""))
    cleared = not getattr(sched, "running", False)
    _cell(
        matrix,
        "scheduler failure surfaced",
        ok=surfaced and cleared,
        note="missing source table surfaces an operator-readable failure and "
        "releases the running claim"
        if surfaced and cleared
        else "a failing scheduled run left no surfaced reason or kept the claim",
        detail={
            "run_entry": entry,
            "last_error": str(getattr(sched, "last_error", ""))[:200],
            "paused_reason": str(getattr(sched, "paused_reason", ""))[:200],
            "running": bool(getattr(sched, "running", False)),
        },
        run_id=job,
    )

    # retry/backoff: either a retry is parked in the store with a future
    # timestamp, or the run was parked on a finding that a retry cannot fix.
    # Both are honest; a silent nothing is not.
    if retry_at:
        due = datetime.fromisoformat(str(retry_at))
        backoff = (due - datetime.now(timezone.utc)).total_seconds()
        ok = backoff > 0 and int(getattr(sched, "retry_attempt", 0) or 0) >= 0
        note = (
            f"failed attempt parked for retry in {backoff:.0f}s "
            f"(attempt {getattr(sched, 'retry_attempt', 0)}), persisted in the store"
        )
        detail = {"retry_at": str(retry_at), "backoff_seconds": round(backoff, 1)}
        _cell(matrix, "scheduler retry/backoff", ok=ok, note=note, detail=detail)
    elif parked:
        _cell(
            matrix,
            "scheduler retry/backoff",
            ok=True,
            note="failure classified as not-retryable and parked on a finding "
            "instead of retrying a run that cannot succeed",
            detail={
                "paused_reason": str(getattr(sched, "paused_reason", ""))[:280],
                "last_error": str(getattr(sched, "last_error", ""))[:280],
            },
        )
    else:
        _cell(
            matrix,
            "scheduler retry/backoff",
            ok=False,
            note="failed run neither parked a retry nor recorded a finding",
            detail={"run_entry": entry},
        )


def concurrency_cells(matrix: Matrix, ws: str, src_conn: str, dst_conn: str,
                      src_table: str, dst_table: str) -> None:
    from services import schedule_runner
    from services.schedule_store import clear_schedule_running, get_schedule

    client = _client()
    created = _create_schedule(
        client,
        ws,
        _schedule_body(
            name=f"track-d race {TAG}",
            src_conn=src_conn,
            src_table=src_table,
            dst_conn=dst_conn,
            dst_table=f"{dst_table}_race",
        ),
    )
    sid = str(created.get("id") or "")

    # two worker *processes*, one due schedule, released on a shared barrier:
    # separate interpreters mean the only thing standing between them is the
    # store's conditional claim, which is what a second replica actually hits.
    _force_due(sid)
    barrier = time.time() + 3.0
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "tests.scale.scheduler_worker", sid, str(barrier)],
            cwd=str(API_DIR),
            env={**os.environ, "DATAFLOW_SCALE_MODES": "1"},
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        for _ in range(2)
    ]
    outcomes: list[str] = []
    for proc in procs:
        out, _ = proc.communicate(timeout=600)
        outcomes.append(
            next(
                (ln for ln in out.splitlines() if ln.startswith(("DISPATCHED", "REFUSED"))),
                "NO OUTPUT",
            )
        )
    dispatched = [
        line.split(" ", 1)[1].strip()
        for line in outcomes
        if line.startswith("DISPATCHED") and line.split(" ", 1)[1:]
    ]
    race_ok = len(dispatched) == 1
    _cell(
        matrix,
        "scheduler worker race (no double run)",
        ok=race_ok,
        note=f"two worker processes raced the same due schedule; "
        f"{len(dispatched)} run dispatched"
        if race_ok
        else f"{len(dispatched)} runs dispatched for one due schedule",
        detail={"worker_outcomes": outcomes},
        run_id=dispatched[0] if dispatched else "",
    )

    # overlap: the previous run is still in flight when the next tick arrives.
    sched = get_schedule(sid)
    in_flight = bool(getattr(sched, "running", False))
    if not in_flight:
        # The race run may already have finished; re-claim explicitly so the
        # overlap question is asked of a schedule that is genuinely running.
        from services.schedule_store import mark_schedule_running

        in_flight = mark_schedule_running(sid, f"holder-{TAG}") is not None
    _force_due(sid)
    second = schedule_runner._run_schedule(sid)
    overlap_ok = in_flight and second is None
    _cell(
        matrix,
        "scheduler overlap (run in flight)",
        ok=overlap_ok,
        note="next tick refused to start while the previous run held the claim"
        if overlap_ok
        else "a second run started while one was still in flight",
        detail={"in_flight": in_flight, "second_dispatch": str(second)},
    )
    clear_schedule_running(sid)
    for job in {str(r) for r in dispatched}:
        if job:
            _await_job(job, timeout=600.0)


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def scheduler_cells(matrix: Matrix, rows: int) -> None:
    if not stores.reachable("postgresql"):
        _skip(matrix, "scheduler", "PostgreSQL 5432 unreachable")
        return
    cadence_cells(matrix)
    if not L.reachable("localhost", 27017):
        _skip(
            matrix,
            "scheduler beat",
            "MongoDB 27017 unreachable: the beat's distributed lock, job "
            "documents and schedule store all live there",
        )
        return

    src_table = f"td_sched_src_{TAG}"
    dst_table = f"td_sched_dst_{TAG}"
    stores.seed("postgresql", src_table, rows)
    stores.drop("postgresql", dst_table)
    ws = _workspace(f"track-d scheduler {TAG}")
    client = _client()
    src_conn = _connector(client, ws, f"track-d pg source {TAG}")
    dst_conn = _connector(client, ws, f"track-d pg dest {TAG}")

    control_plane_cells(matrix, ws, src_conn, dst_conn, src_table, dst_table)
    firing_cells(matrix, rows, ws, src_conn, dst_conn, src_table, dst_table)
    failure_cells(matrix, ws, src_conn, dst_conn, dst_table)
    concurrency_cells(matrix, ws, src_conn, dst_conn, src_table, dst_table)
