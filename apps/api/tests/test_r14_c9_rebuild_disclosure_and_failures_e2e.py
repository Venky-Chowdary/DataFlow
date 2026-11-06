"""QA C9 review: a destructive rebuild is disclosed and confirmed, and a rebuild
that cannot start leaves the finding open.

03b263d9d's rebuild of a schedule with no key ran full_refresh_overwrite (it
replaces the table) on one click: MCP labelled it destructive: false, the
drawer had no confirm, and the docs did not say so. It also audited
"succeeded" and re-armed the schedule (next_run_at = now) before the run
started, so a ScheduleStartError / PermissionError 500'd with the finding
already closed. Live PostgreSQL.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(400)
_ACTOR = {"X-Actor": "qa@example.com"}


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _parked(sid: str) -> bool:
    import time

    from services.schedule_store import get_schedule, has_open_approval

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if has_open_approval(get_schedule(sid)):
            return True
        time.sleep(0.2)
    return False


def _drop_and_park(cur, sid: str, table: str) -> None:
    cur.execute(f'DROP TABLE public."{table}"')  # nosec B608
    for _attempt in (1, 2):  # an unreadable catalog is retried once (D266)
        _beat(sid)
        if _parked(sid):
            return
    raise AssertionError("not parked")


def _schedule(client, cur, token: str, *, keyed: bool):
    from services.connector_store import create_connector

    src_table, dest = f"c9r_src_{token}", f"c9r_dst_{token}"
    cur.execute(f"CREATE TABLE {src_table} (id BIGINT {'PRIMARY KEY' if keyed else ''}, "  # nosec B608
                "name TEXT, seq BIGINT)")
    cur.executemany(f"INSERT INTO {src_table} VALUES (%s, %s, %s)",  # nosec B608
                    [(1, "Ada", 1), (2, None, 2), (3, "", 3), (4, "Zoë ✓", 4)])
    src = create_connector({"name": f"c9r-src-{token}", "type": "postgresql", "role": "source",
                            "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
    dst = create_connector({"name": f"c9r-dst-{token}", "role": "destination", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    body = {"name": f"c9r {token}", "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest, "interval": "hourly",
            "sync_mode": "incremental_append", "cursor_column": "seq",
            "cursor_semantics": "monotonic_sequence",
            "mappings": [{"source": c, "target": c, "confidence": 0.99} for c in ("id", "name", "seq")],
            "enabled": True}
    if keyed:
        body["primary_key"] = "id"
    resp = client.post("/api/v1/schedules/", json=body)
    assert resp.status_code == 201, resp.text
    sid = resp.json()["id"]
    run = _beat(sid)
    assert run["job"].get("status") == "completed", run["job"].get("error")
    return sid, src_table, dest


def test_a_keyless_rebuild_is_disclosed_as_a_replace_and_needs_confirm(client):
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src_table, dest = _schedule(client, cur, token, keyed=False)
    try:
        _drop_and_park(cur, sid, dest)

        dry = client.post(f"/api/v1/schedules/{sid}/recover",
                          json={"action": "rebuild", "dry_run": True})
        assert dry.status_code == 200, dry.text
        plan = dry.json()["plan"]
        assert plan["destructive"] is True and f"Replaces {dest}" in plan["effect"], plan
        assert _parked(sid)  # a dry run changes nothing

        refused = client.post(f"/api/v1/schedules/{sid}/recover", headers=_ACTOR,
                              json={"action": "rebuild", "reason": "table dropped"})
        assert refused.status_code == 409 and "confirm_replace" in refused.text, refused.text
        assert _parked(sid)

        base = client.post(f"/api/v1/schedules/{sid}/recover",
                           json={"action": "rebaseline", "dry_run": True}).json()["plan"]
        assert base["destructive"] is False
        assert f"recreates {dest} with rows after seq" in base["effect"], base

        ok, staged = h.mcp_call(client, "recover_schedule_destination",
                                {"schedule_id": sid, "action": "rebuild", "reason": "rebuild it"})
        assert ok, staged
        assert staged["destructive"] is True and staged["preview"]["destructive"] is True, staged
        assert f"Replaces {dest}" in staged["preview"]["effect"], staged["preview"]
        confirmed = client.post("/api/v1/copilot/confirm", headers=_ACTOR,
                                json={"ack_id": staged["ack_id"], "reason": "c9r"})
        assert confirmed.status_code == 200 and confirmed.json().get("job_id"), confirmed.text
        job = h.wait_job(confirmed.json()["job_id"])
        assert job.get("status") == "completed", job.get("error")
        got = h.query("postgresql", f'SELECT COUNT(*) FROM public."{dest}"')  # nosec B608
        assert int(got[0][0]) == 4
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest)


def test_a_rebuild_that_cannot_start_leaves_the_finding_open_and_audits_the_failure(client, monkeypatch):
    import services.audit_coverage as audit_coverage
    import services.schedule_runner as runner
    from services.schedule_store import get_schedule

    events: list[dict] = []
    real = audit_coverage.record_security_event

    def _capture(action, **kw):
        if action == "schedule.destination.recovered":
            events.append({"action": action, **kw})
        return real(action, **kw)

    monkeypatch.setattr(audit_coverage, "record_security_event", _capture)
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src_table, dest = _schedule(client, cur, token, keyed=True)
    try:
        _drop_and_park(cur, sid, dest)
        real_run = runner._run_schedule
        for exc, status in (
            (runner.ScheduleStartError("Already running: job j-1", http_status=409,
                                       code="already_running"), 409),
            (PermissionError("This key may not use the destination connector."), 403),
        ):
            def _refuse(*_a, _exc=exc, **_k):
                raise _exc

            monkeypatch.setattr(runner, "_run_schedule", _refuse)
            before = get_schedule(sid).next_run_at
            resp = client.post(f"/api/v1/schedules/{sid}/recover", headers=_ACTOR,
                               json={"action": "rebuild", "reason": "rebuild it"})
            assert resp.status_code == status, resp.text
            assert _parked(sid), get_schedule(sid).approval_request
            assert get_schedule(sid).next_run_at == before
            assert events[-1]["outcome"] == "failed", events[-1]
        # Review 2: a generic exception is audited (and still surfaced), and a
        # start that returns no job is a failed start — the finding stays open.
        for refusal, status in ((RuntimeError("worker pool unavailable"), 500), ("", 409)):
            def _odd(*_a, _r=refusal, **_k):
                if isinstance(_r, BaseException):
                    raise _r
                return _r

            monkeypatch.setattr(runner, "_run_schedule", _odd)
            try:
                resp = client.post(f"/api/v1/schedules/{sid}/recover", headers=_ACTOR,
                                   json={"action": "rebuild", "reason": "rebuild it"})
                got = resp.status_code
            except RuntimeError:
                got = 500  # TestClient re-raises an unhandled server error
            assert got == status, got
            assert _parked(sid)
            assert events[-1]["outcome"] == "failed", events[-1]
        monkeypatch.setattr(runner, "_run_schedule", real_run)

        # A rebuild that starts leaves the finding open until its job SUCCEEDS;
        # then the runner closes it and the cadence resumes at its next slot —
        # never "now", which would race the rebuild for the claim.
        resp = client.post(f"/api/v1/schedules/{sid}/recover", headers=_ACTOR,
                           json={"action": "rebuild", "reason": "rebuild it"})
        assert resp.status_code == 200 and resp.json()["job_id"], resp.text
        assert resp.json()["plan"]["destructive"] is False  # keyed: upsert, rows kept
        assert (get_schedule(sid).approval_request or {}).get("status") == "open"
        assert (get_schedule(sid).approval_request or {}).get("recovery", {}).get("job_id") == resp.json()["job_id"]
        job = h.wait_job(resp.json()["job_id"])
        assert job.get("status") == "completed", job.get("error")
        _settle(sid)
        assert not _parked_now(sid)
        nxt = datetime.fromisoformat(str(get_schedule(sid).next_run_at))
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=timezone.utc)
        assert nxt > datetime.now(timezone.utc) + timedelta(minutes=30), nxt
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest)


def _settle(sid: str) -> None:
    import time

    from services import schedule_runner
    from services.schedule_store import get_schedule

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if not str(getattr(get_schedule(sid), "running_job_id", "") or "").strip():
            return
        schedule_runner._finalize_finished_schedule_claims()
        time.sleep(0.2)


def _parked_now(sid: str) -> bool:
    from services.schedule_store import get_schedule, has_open_approval

    return has_open_approval(get_schedule(sid))


def test_a_rebuild_that_starts_and_then_fails_keeps_the_finding_open(client, monkeypatch):
    """Review 2: the finding was closed when the rebuild STARTED, so a rebuild
    that then failed left an armed schedule with nothing to recover. The failed
    run is kept from opening a fresh park here, so only the original finding
    can still hold the schedule."""
    import services.schedule_runner as runner

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src_table, dest = _schedule(client, cur, token, keyed=True)
    try:
        _drop_and_park(cur, sid, dest)
        original = _finding(sid)
        cur.execute(f"DROP TABLE {src_table}")  # nosec B608 - the rebuild's read will fail
        monkeypatch.setattr(runner, "_park_reason", lambda *_a, **_k: "")
        resp = client.post(f"/api/v1/schedules/{sid}/recover", headers=_ACTOR,
                           json={"action": "rebuild", "reason": "rebuild it"})
        assert resp.status_code == 200 and resp.json()["job_id"], resp.text
        job = h.wait_job(resp.json()["job_id"])
        assert job.get("status") == "failed", job.get("status")
        _settle(sid)
        assert _parked(sid), "a failed rebuild must leave the schedule parked"
        after = _finding(sid)
        assert after.get("id") == original.get("id") and after.get("code") == "DESTINATION_CONTINUITY", after
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest)


def _finding(sid: str) -> dict:
    from services.schedule_store import get_schedule

    return dict(get_schedule(sid).approval_request or {})
