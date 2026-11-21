"""Parity at Scale phase 3, end to end on live PostgreSQL→PostgreSQL.

A completed job whose table Gate-8 could not compare in full (coverage
limited) is verified by a scan after the write (owner decision 2):

* while the scan runs the Trust Report says "verifying" and sign-off is
  blocked; the write status is never rewritten;
* a clean copy ends ``passed``: the verdict is proven (the pair and kinds are
  certified) and the proof pack's claim is a full population proof
  (``engine_segment_digest``);
* a copy with faults ends ``failed``: the verdict is unexplained with the
  exact rows; the explorer's Columns / Keys / Values name them; the repair SQL
  (which Datawrap never runs) is run here by the "reviewer", and re-verifying
  the table then passes;
* a keyless table is "Content match (no key)";
* a schedule's watermark is held while verification runs and released only
  when it passes; the next tick waits while it runs.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

import parity_engines  # noqa: E402

pytestmark = pytest.mark.skipif(not parity_engines.available("postgresql"),
                                reason="PostgreSQL not reachable (DATAFLOW_PG_TEST_DSN)")

N = 3000


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("DATAWRAP_PARITY_SCALE", "1")
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    import services.mongodb_service as ms

    monkeypatch.setattr(ms, "_mongodb_service", None)
    yield
    monkeypatch.setattr(ms, "_mongodb_service", None)


def _sql(*statements: str) -> None:
    conn = parity_engines.connect("postgresql")
    conn.autocommit = True
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)
    conn.close()


@pytest.fixture()
def tables():
    src, dst = f"psd1_v_s_{uuid.uuid4().hex[:8]}", f"psd1_v_d_{uuid.uuid4().hex[:8]}"
    ddl = "id bigint PRIMARY KEY, name varchar(40), amt numeric(12,2), ts timestamp(6)"
    fill = (f"SELECT g, 'n' || g, g * 1.25, timestamp '2024-01-01' + g * interval '1 second' "
            f"FROM generate_series(1, {N}) g")
    _sql(f'CREATE TABLE "{src}" ({ddl})', f'INSERT INTO "{src}" {fill}',
         f'CREATE TABLE "{dst}" ({ddl})', f'INSERT INTO "{dst}" SELECT * FROM "{src}"')
    yield src, dst
    _sql(f'DROP TABLE IF EXISTS "{src}"', f'DROP TABLE IF EXISTS "{dst}"')


def _endpoint(table: str) -> dict[str, Any]:
    c = parity_engines.cfg("postgresql")
    return {"kind": "database", "format": "postgresql", "host": c["host"], "port": c["port"],
            "database": c["database"], "username": c["username"], "password": c["password"],
            "schema": "public", "table": table}


def _job(src: str, dst: str, *, key: bool = True) -> str:
    from services.mongodb_service import get_mongodb_service

    cols = ["id", "name", "amt", "ts"]
    return get_mongodb_service().create_transfer_job({
        "status": "completed",
        "transfer_request": {
            "source": _endpoint(src), "destination": _endpoint(dst),
            "mappings": [{"source": c, "target": c} for c in cols], "sync_mode": "full_refresh_overwrite",
        },
        "destination_summary": {"table": dst, **({"primary_key_columns": ["id"]} if key else {})},
        # What a real run records for the certificate (attribution and the Map hash).
        "mapping_proof": {"mapping_hash": "m" * 64},
        "connector_versions": {"source": "postgresql 16.15", "destination": "postgresql 16.15"},
        "reconciliation": {
            "passed": True, "source_rows": N, "target_rows": N, "phase": "post_write",
            "coverage_limit": {"rows": N, "limit": 100, "note": "above the ladder limit"},
            "verification_verdict_qualifier": "coverage_limited",
        },
    })


def _live_lease(job_id: str, *, owner: str = "another-worker:1") -> None:
    """Another worker holds this job's verification right now (a live lease)."""
    from datetime import datetime, timedelta, timezone

    from services.mongodb_service import get_mongodb_service

    now = datetime.now(timezone.utc)
    get_mongodb_service().update_job_fields(job_id, {"parity_scale_lease": {
        "token": "other", "owner": owner, "claimed_at": now.isoformat(),
        "heartbeat_at": now.isoformat(), "expires_at": (now + timedelta(minutes=10)).isoformat()}})


def _report(job_id: str) -> dict[str, Any]:
    from services.mongodb_service import get_mongodb_service
    from services.parity_report import build_parity_report

    return build_parity_report(get_mongodb_service().get_job(job_id), reread=False, job_id=job_id)


def _verify(job_id: str, **kwargs: Any) -> dict[str, Any]:
    from services.parity_scale.verification import start_for_job

    return start_for_job(job_id, background=False, **kwargs)


def test_flag_off_starts_nothing(tables, monkeypatch) -> None:
    from services.parity_scale.verification import start_for_job

    monkeypatch.delenv("DATAWRAP_PARITY_SCALE")
    job_id = _job(*tables)
    assert start_for_job(job_id, background=False) == {}
    assert _report(job_id)["verdict"] != "verifying"


def test_running_verification_blocks_signoff_and_is_not_a_verdict(tables) -> None:
    from services.mongodb_service import get_mongodb_service

    job_id = _job(*tables)
    get_mongodb_service().update_job_fields(job_id, {"parity_scale": {
        "state": "running", "tables": [{"name": tables[1], "status": "running", "scan_id": "x",
                                        "progress": {"items_done": 3, "items_queued": 9}}]}})
    _live_lease(job_id)
    report = _report(job_id)
    assert report["verdict"] == "verifying"
    assert report["signoff"]["status"] == "blocked"
    assert "running" in report["signoff"]["blocked_reason"].lower()
    assert report["tables"][0]["status"] == "verifying"
    assert report["verification"]["state"] == "running"
    # Nothing Gate-8 left open is restated as a difference while every row is being compared.
    assert report["parity"]["rows_differ"] == 0 and report["differences"] == []
    assert "being compared now" in report["confidence"]["statement"]
    assert report["verification"]["tables"][0]["status"] == "running"
    assert get_mongodb_service().get_job(job_id)["status"] == "completed"  # the write status is untouched


def test_clean_copy_is_proven_and_the_proof_pack_agrees(tables) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.signed_proof_pack import classify_post_write_assurance

    job_id = _job(*tables)
    before = classify_post_write_assurance(get_mongodb_service().get_job(job_id)["reconciliation"])
    assert before["migration_proven"] is False  # coverage-limited Gate-8 alone
    block = _verify(job_id)
    assert block["state"] == "passed", block
    report = _report(job_id)
    assert report["verification"]["tables"][0]["status"] == "passed"
    assert report["verdict"] == "proven", report["verdict_reason"]
    assert report["tables"][0]["check"] == "segment_diff"
    assert report["tables"][0]["certified"] is True
    assert report["signoff"]["status"] == "open"
    after = classify_post_write_assurance(get_mongodb_service().get_job(job_id)["reconciliation"])
    assert after["migration_proven"] is True
    assert after["source_checksum_provenance"] == "engine_segment_digest"


def test_faults_are_named_explored_and_repaired_by_the_reviewer(tables) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import explorer

    src, dst = tables
    _sql(f"UPDATE \"{dst}\" SET name = 'wrong' WHERE id = 1500",
         f'DELETE FROM "{dst}" WHERE id = 20',
         f"INSERT INTO \"{dst}\" VALUES (99999, 'extra', 1, timestamp '2024-01-01')")
    job_id = _job(src, dst)
    assert _verify(job_id)["state"] == "failed"
    report = _report(job_id)
    assert report["verdict"] == "unexplained"
    assert report["signoff"]["status"] == "blocked"
    kinds = {(d["kind"], d.get("column")) for d in report["differences"]}
    assert {("value", "name"), ("missing_in_destination", None), ("extra_in_destination", None)} <= kinds
    job = get_mongodb_service().get_job(job_id)
    cols = {c["column"]: c for c in explorer.columns(job, dst)["columns"]}
    assert cols["name"]["rows_differing"] == 1 and cols["name"]["status"] == "unexplained"
    assert cols["amt"]["status"] == "match"
    keys = explorer.keys(job, dst)
    assert keys["missing_in_destination"]["keys"] == ["20"]
    assert keys["extra_in_destination"]["keys"] == ["99999"]
    values = explorer.values(job, dst)
    row = values["rows"][0]
    assert row["key"] == ["1500"]
    name = next(c for c in row["cells"] if c["column"] == "name")
    assert (name["source"], name["destination"], name["differs"]) == ("n1500", "wrong", True)
    assert not next(c for c in row["cells"] if c["column"] == "amt")["differs"]
    repair = explorer.repair_sql(job, dst)
    assert repair["available"] and repair["executed_by_datawrap"] is False
    assert repair["statements"] == {"insert": 1, "delete": 1, "update": 1}
    assert "Datawrap does not run these" in repair["sql"]
    # The reviewer runs the statements themselves; re-verifying the table now passes.
    _sql(*[line for line in repair["sql"].splitlines() if line and not line.startswith("--")])
    assert _verify(job_id, only_table=dst, force=True)["state"] == "passed"
    assert _report(job_id)["verdict"] == "proven"


# Protected columns: tests/test_parity_scale_protected_live.py (rows that really differ,
# the stored job document and the job API payload are inspected).


def test_keyless_table_is_a_content_match(tables) -> None:
    job_id = _job(*tables, key=False)
    assert _verify(job_id)["state"] == "passed"
    report = _report(job_id)
    assert report["tables"][0]["check"] == "multiset_digest"
    assert report["tables"][0]["check_label"] == "Content match (no key)"
    assert report["verdict"] == "proven"
    assert "content match (no key)" in report["verdict_reason"].lower()


def test_watermark_waits_for_verification_and_moves_only_when_it_passes(tables, monkeypatch) -> None:
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification

    job_id = _job(*tables)
    job = get_mongodb_service().get_job(job_id)
    # Finalizing the schedule run before verification ran: the watermark is held.
    assert verification.hold_cursor_until_verified("sched-1", job_id, job, "2024-06-01") is None
    assert get_mongodb_service().get_job(job_id)["parity_scale_pending_cursor"]["cursor_value"] == "2024-06-01"
    released: list[dict[str, Any]] = []

    class Sched:
        id = "sched-1"
        last_job_id = job_id
        wait_for_verification = True

    import services.schedule_store as store

    monkeypatch.setattr(store, "get_schedule", lambda _sid: Sched())
    monkeypatch.setattr(store, "update_schedule", lambda sid, data: released.append(data))
    get_mongodb_service().update_job_fields(job_id, {"parity_scale": {"state": "running", "tables": []}})
    _live_lease(job_id)
    assert verification.schedule_waits_for_verification(Sched()).startswith("Waiting")
    get_mongodb_service().update_job_fields(job_id, {"parity_scale": {}, "parity_scale_lease": None})
    assert _verify(job_id)["state"] == "passed"
    assert released == [{"cursor_value": "2024-06-01"}]
    assert verification.schedule_waits_for_verification(Sched()) == ""


class _Schedule:
    """A stored schedule as schedule_store returns it (the run under test may not be its latest yet)."""

    def __init__(self, sid: str, cursor_value: str, last_job_id: str = "") -> None:
        self.id = sid
        self.cursor_value = cursor_value
        self.last_job_id = last_job_id
        self.wait_for_verification = True


def _schedules(monkeypatch, *schedules: _Schedule) -> list[tuple[str, dict[str, Any]]]:
    import services.schedule_store as store

    by_id = {s.id: s for s in schedules}
    released: list[tuple[str, dict[str, Any]]] = []

    def update(sid: str, data: dict[str, Any]) -> None:
        released.append((sid, data))
        by_id[sid].cursor_value = data.get("cursor_value", by_id[sid].cursor_value)

    monkeypatch.setattr(store, "get_schedule", lambda sid: by_id.get(sid))
    monkeypatch.setattr(store, "update_schedule", update)
    return released


def test_a_failed_verification_never_moves_the_watermark(tables, monkeypatch) -> None:
    """The schedule exists and holds an older watermark: a failure still never moves it."""
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification

    src, dst = tables
    _sql(f'DELETE FROM "{dst}" WHERE id = 5')
    job_id = _job(src, dst)
    released = _schedules(monkeypatch, _Schedule("sched-2", "2024-01-01", last_job_id=job_id))
    assert verification.hold_cursor_until_verified(
        "sched-2", job_id, get_mongodb_service().get_job(job_id), "2024-06-01") is None
    assert _verify(job_id)["state"] == "failed"
    assert released == []
    job = get_mongodb_service().get_job(job_id)
    assert job.get("parity_scale_pending_cursor") is None  # dropped, never applied later
    assert verification.hold_cursor_until_verified("sched-2", job_id, job, "2024-07-01") is None


def test_the_watermark_moves_even_before_the_run_is_recorded_on_the_schedule(tables, monkeypatch) -> None:
    """Review 11 (race): verification can pass before mark_schedule_run sets last_job_id."""
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification

    job_id = _job(*tables)
    released = _schedules(monkeypatch, _Schedule("sched-3", "2024-01-01", last_job_id="the-previous-run"))
    verification.hold_cursor_until_verified("sched-3", job_id, get_mongodb_service().get_job(job_id), "2024-06-01")
    assert _verify(job_id)["state"] == "passed"
    assert released == [("sched-3", {"cursor_value": "2024-06-01"})]


def test_with_no_wait_a_later_run_neither_freezes_nor_rewinds_the_watermark(tables, monkeypatch) -> None:
    """Review 11: wait_for_verification off, a later run already started (and maybe advanced)."""
    from services.mongodb_service import get_mongodb_service
    from services.parity_scale import verification

    job_id = _job(*tables)
    behind = _Schedule("sched-4", "2024-01-01", last_job_id="a-later-run")
    ahead = _Schedule("sched-5", "2024-09-01", last_job_id="a-later-run")
    released = _schedules(monkeypatch, behind, ahead)
    svc = get_mongodb_service()
    verification.hold_cursor_until_verified("sched-4", job_id, svc.get_job(job_id), "2024-06-01")
    assert _verify(job_id)["state"] == "passed"
    assert released == [("sched-4", {"cursor_value": "2024-06-01"})]  # not frozen behind the later run
    job_id = _job(*tables)
    verification.hold_cursor_until_verified("sched-5", job_id, svc.get_job(job_id), "2024-06-01")
    assert _verify(job_id)["state"] == "passed"
    assert ahead.cursor_value == "2024-09-01"  # never moved backwards
