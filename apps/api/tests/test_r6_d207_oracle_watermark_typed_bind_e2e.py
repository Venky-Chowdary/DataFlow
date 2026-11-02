"""D207 — an incremental pipeline with an Oracle source keeps running after run 1.

QA (production 557f556c): an Oracle -> PostgreSQL schedule loaded 30 rows on
run 1; after source edits, runs 2 and 3 both failed with 0 rows. The saved
watermark went to Oracle as text (``'2026-10-01T00:30:00'``) inside
``CAST(:param AS DATE)`` — a conversion Oracle performs with the session's
``NLS_DATE_FORMAT`` (``DD-MON-RR``), so it raised ORA-01843 "not a valid month".

Contract pinned here, on QA's route (MCP ``create_schedule`` -> Confirm ->
scheduler beats) against real Oracle Free and PostgreSQL: the watermark is a
*typed* bind matching the cursor column's declared type (DATE, TIMESTAMP,
TIMESTAMP WITH TIME ZONE compared as an instant), so run 2 and run 3 read
exactly the edited and new rows, and the destination equals the source cell
by cell after every run. NULL, whitespace and unicode ride on every run (Oracle
stores '' as NULL).
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

UTC = timezone.utc


def _oracle_cfg() -> dict[str, Any]:
    pw = os.environ.get("DATAFLOW_TEST_ORACLE_PASSWORD", "")
    if not pw or not h.port_up(1521):
        pytest.skip("Oracle not configured (DATAFLOW_TEST_ORACLE_PASSWORD, :1521)")
    pytest.importorskip("oracledb")
    svc = os.environ.get("DATAFLOW_TEST_ORACLE_SERVICE", "FREEPDB1")
    return dict(
        type="oracle", host=os.environ.get("DATAFLOW_TEST_ORACLE_HOST", "127.0.0.1"),
        port=1521, database=svc, service_name=svc,
        username=os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system"), password=pw,
        schema=os.environ.get(
            "DATAFLOW_TEST_ORACLE_SCHEMA",
            os.environ.get("DATAFLOW_TEST_ORACLE_USER", "system").upper(),
        ),
    )


def _ora(sql: str, params: Any = None) -> list[tuple]:
    import oracledb

    cfg = _oracle_cfg()
    conn = oracledb.connect(
        user=cfg["username"], password=cfg["password"],
        dsn=f"{cfg['host']}:{cfg['port']}/{cfg['database']}",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or [])
            return [tuple(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


# --------------------------------------------------------------------------- #
# Source rows. Cursor values are Python datetimes; the TZ leg stores each one
# with its own offset (an instant), the DATE/TIMESTAMP legs store the wall clock.
# --------------------------------------------------------------------------- #

IST = timezone(timedelta(hours=5, minutes=30))
EDT = timezone(timedelta(hours=-4))

_T0 = datetime(2026, 10, 1, 0, 0, 0)


def _at(minutes: int, tz: timezone | None = None) -> datetime:
    value = _T0 + timedelta(minutes=minutes)
    return value.replace(tzinfo=tz) if tz else value


# Run 1: 30 rows like QA's, cursor 00:01 .. 00:30; NULL / whitespace / unicode.
_NOTES = [None, "  ", "Łukasz 山田 🎉", "plain", "x"]
_RUN1 = [(i, _NOTES[i % len(_NOTES)], _at(i)) for i in range(1, 31)]


def _create(table: str, cursor_type: str) -> None:
    _ora(
        f'CREATE TABLE "{table}" (ID NUMBER(10) PRIMARY KEY, NOTE VARCHAR2(60) NULL, '
        f"UPDATED_AT {cursor_type} NOT NULL)"
    )


def _bind_ts(value: datetime, cursor_type: str) -> tuple[str, Any]:
    """An INSERT value expression for one cursor cell, offset kept for TZ."""
    if "TIME ZONE" in cursor_type:
        aware = value if value.tzinfo else value.replace(tzinfo=UTC)
        off = aware.utcoffset() or timedelta(0)
        sign = "-" if off < timedelta(0) else "+"
        mins = int(abs(off).total_seconds() // 60)
        text = aware.strftime("%Y-%m-%d %H:%M:%S") + f" {sign}{mins // 60:02d}:{mins % 60:02d}"
        return "TO_TIMESTAMP_TZ(:ts, 'YYYY-MM-DD HH24:MI:SS TZH:TZM')", text
    return ":ts", value.replace(tzinfo=None)


def _upsert_source(table: str, cursor_type: str, rows) -> None:
    for rid, note, ts in rows:
        expr, ts_value = _bind_ts(ts, cursor_type)
        binds = {"id": rid, "note": note, "ts": ts_value}
        exists = _ora(f'SELECT COUNT(*) FROM "{table}" WHERE ID = :id', {"id": rid})[0][0]
        if exists:
            _ora(f'UPDATE "{table}" SET NOTE = :note, UPDATED_AT = {expr} WHERE ID = :id', binds)
        else:
            _ora(f'INSERT INTO "{table}" (ID, NOTE, UPDATED_AT) VALUES (:id, :note, {expr})', binds)


def _instant(value: Any, cursor_type: str) -> datetime:
    """One comparable cursor cell: an aware UTC instant for TZ, else the wall clock."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if "TIME ZONE" in cursor_type:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    return value.replace(tzinfo=None)


def _source_cells(table: str, cursor_type: str) -> list[tuple]:
    if "TIME ZONE" in cursor_type:
        rows = _ora(
            f"SELECT ID, NOTE, TO_CHAR(SYS_EXTRACT_UTC(UPDATED_AT), "
            f"'YYYY-MM-DD\"T\"HH24:MI:SS') FROM \"{table}\" ORDER BY ID"
        )
        return [
            (int(r[0]), r[1], datetime.fromisoformat(r[2]).replace(tzinfo=UTC)) for r in rows
        ]
    rows = _ora(f'SELECT ID, NOTE, UPDATED_AT FROM "{table}" ORDER BY ID')
    return [(int(r[0]), r[1], _instant(r[2], cursor_type)) for r in rows]


def _dest_cells(table: str, cursor_type: str) -> list[tuple]:
    rows = h.query(
        "postgresql",
        "SELECT * FROM " + h.quote("postgresql", table),  # nosec B608
    )
    cols = [
        r[0].lower()
        for r in h.query(
            "postgresql",
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{table}' ORDER BY ordinal_position",
        )
    ]
    out = []
    for r in rows:
        rec = dict(zip(cols, r))
        rid = rec["id"]
        rid = int(rid) if not isinstance(rid, Decimal) else int(rid)
        out.append((rid, rec["note"], _instant(rec["updated_at"], cursor_type)))
    return sorted(out, key=lambda r: r[0])


def _beat(schedule_id: str) -> dict:
    from services import schedule_runner
    from services.schedule_store import get_schedule, update_schedule

    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})
    started = schedule_runner._run_due_schedules()
    sched = get_schedule(schedule_id)
    assert started == 1, {
        "last_status": getattr(sched, "last_status", None),
        "last_error": getattr(sched, "last_error", None),
    }
    job_id = str(getattr(sched, "running_job_id", "") or getattr(sched, "last_job_id", "") or "")
    job = h.wait_job(job_id, timeout_s=300)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not str(getattr(get_schedule(schedule_id), "running_job_id", "") or "").strip():
            break
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)
    return job


def _assert_completed(job: dict, run: int) -> None:
    assert str(job.get("status")) == "completed", {
        "run": run, "status": job.get("status"),
        "error": str(job.get("error") or job.get("message") or "")[:600],
    }


# TIMESTAMP WITH TIME ZONE: run 2 stores a row whose *wall clock* is earlier
# than the watermark's but whose instant is later. Comparing the watermark's
# wall clock (offset dropped) would skip it; an instant comparison reads it.
_CASES = {
    "date": ("DATE", None),
    "timestamp": ("TIMESTAMP(6)", None),
    "timestamp_tz": ("TIMESTAMP(6) WITH TIME ZONE", IST),
}


@pytest.mark.parametrize("case", list(_CASES), ids=list(_CASES))
def test_scheduled_oracle_to_pg_incremental_keeps_running_after_run_one(client, case):
    from services.connector_store import create_connector

    cursor_type, tz = _CASES[case]
    token = uuid.uuid4().hex[:8]
    src_table = f"D207_{token.upper()}"
    dest_table = f"d207_{case}_{token}"
    ora_cfg = _oracle_cfg()
    pg_cfg = h.dest_config("postgresql")
    try:
        _create(src_table, cursor_type)
        run1 = [(i, n, ts.replace(tzinfo=tz) if tz else ts) for i, n, ts in _RUN1]
        _upsert_source(src_table, cursor_type, run1)
        src = create_connector({
            "name": f"d207-src-{token}", "role": "source", "workspace_id": "", "ssl": False,
            **ora_cfg,
        })
        dst = create_connector({
            "name": f"d207-dst-{token}", "role": "destination", "workspace_id": "",
            "ssl": False, **pg_cfg,
        })
        ok, staged = h.mcp_call(client, "create_schedule", {
            "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table,
            "cadence": "every 15 minutes", "sync_mode": "incremental_upsert",
            "cursor_column": "UPDATED_AT", "cursor_semantics": "modification_timestamp",
            "primary_key": "ID", "name": f"d207-{token}",
        })
        assert ok, staged
        assert not h.blockers_of(staged), h.blockers_of(staged)
        resp = client.post(
            "/api/v1/copilot/confirm", json={"ack_id": staged["ack_id"], "reason": "d207"}
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body.get("ok") is True, body
        schedule_id = str(body.get("schedule_id") or (body.get("schedule") or {}).get("id") or "")

        # Run 1: all 30 rows.
        j1 = _beat(schedule_id)
        _assert_completed(j1, 1)
        assert _dest_cells(dest_table, cursor_type) == _source_cells(src_table, cursor_type)
        wm1 = (j1.get("incremental_watermark") or {}).get("after")

        # Source edits: two rows edited (cursor moves forward), two new rows.
        late = _at(45, tz) if tz else _at(45)
        edits2 = [
            (3, "edited ✓", late),
            (7, None, late),
            (31, "new  row", _at(50, tz) if tz else _at(50)),
            (32, "Ünïcødé", _at(55, tz) if tz else _at(55)),
        ]
        if tz is not None:
            # 2026-09-30 17:00-04:00 = 21:00Z: after the watermark's instant
            # (00:30+05:30 = 19:00Z), before its wall clock read as UTC.
            edits2.append((33, "instant later", datetime(2026, 9, 30, 17, 0, tzinfo=EDT)))
        _upsert_source(src_table, cursor_type, edits2)
        j2 = _beat(schedule_id)
        _assert_completed(j2, 2)
        assert _dest_cells(dest_table, cursor_type) == _source_cells(src_table, cursor_type)
        rep2 = j2.get("incremental_watermark") or {}
        assert wm1, j1.get("incremental_watermark")
        assert rep2.get("before") == wm1, rep2
        # The tie group at the watermark is re-read; at least the edits moved.
        assert int(rep2.get("rows_selected") or 0) >= len(edits2), rep2
        assert rep2.get("advanced") is True, rep2

        # More edits, then run 3.
        # (10:00+05:30 = 04:30Z: past run 2's instant watermark on the TZ leg.)
        edits3 = [
            (1, "edited again", _at(600, tz) if tz else _at(600)),
            (34, "", _at(605, tz) if tz else _at(605)),
        ]
        _upsert_source(src_table, cursor_type, edits3)
        j3 = _beat(schedule_id)
        _assert_completed(j3, 3)
        assert _dest_cells(dest_table, cursor_type) == _source_cells(src_table, cursor_type)
        rep3 = j3.get("incremental_watermark") or {}
        assert rep3.get("before") == rep2.get("after"), rep3
        assert int(rep3.get("rows_selected") or 0) >= len(edits3), rep3
        assert int(rep3.get("rows_selected") or 0) < 30, rep3  # incremental, not a full read
    finally:
        try:
            _ora(f'DROP TABLE "{src_table}" PURGE')
        except Exception:
            pass
        h.drop_table("postgresql", dest_table)
