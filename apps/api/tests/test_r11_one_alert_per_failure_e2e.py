"""One alert per failure per channel (follow-up to QA round 11 SC9).

The delivery log made it visible: a scheduled job that failed paged every
channel twice — the engine's generic job alert from inside the job, then the
scheduler's run alert from the finalize callback. Now:

* a schedule-dispatched job's failure sends only the schedule-aware alert
  (schedule name, parked state);
* a manual job keeps its job alert, once;
* a run refused before any job sends the schedule alert, once;
* a repeated send for the same failure (a second finalize) is dropped.

The dedupe is one claim per (job or run, event, channel) in
``notify_workspace`` — every sender comes through it.

Real scheduler beats and a real engine run on live MongoDB and PostgreSQL,
two webhook channels on a local receiver.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from r8_ent_harness import ent  # noqa: F401
from r11_live_world import drop_pg, live, pg_table  # noqa: F401
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(300)

_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


@pytest.fixture
def receiver(monkeypatch):
    received: list[dict] = []

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            received.append({"path": self.path, "body": json.loads(body or b"{}")})
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    from services import notification_service

    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", received
    finally:
        server.shutdown()


def _channels(live, base: str) -> list[str]:
    ids = []
    for label in ("ops", "oncall ✓"):
        res = live.admin.post("/api/v1/workspace/notifications", json={
            "workspace_id": live.workspace_id, "kind": "webhook", "label": label,
            "config": {"url": f"{base}/{uuid.uuid4().hex[:6]}"}})
        assert res.status_code == 200, res.text
        ids.append(res.json()["id"])
    return ids


def _deliveries(live, channel_id: str) -> list[dict]:
    res = live.admin.get(f"/api/v1/workspace/notifications/{channel_id}/deliveries")
    assert res.status_code == 200, res.text
    return res.json()["deliveries"]


def _settle(schedule_id: str, runs: int, timeout: float = 120.0) -> None:
    from services import schedule_runner
    from services.schedule_store import get_schedule

    schedule_runner._run_due_schedules()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sched = get_schedule(schedule_id)
        done = [r for r in sched.run_history if r.get("status") not in ("pending", "running")]
        if not sched.running and len(done) >= runs:
            time.sleep(1.0)  # the finalize callback alerts after recording
            return
        time.sleep(0.5)
    raise AssertionError(f"schedule {schedule_id} did not settle")


def _due(schedule_id: str) -> None:
    from services.schedule_store import update_schedule

    update_schedule(schedule_id, {
        "next_run_at": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        "retry_at": None,
    })


def test_a_failed_scheduled_job_alerts_each_channel_once(live, receiver):
    base, received = receiver
    channels = _channels(live, base)
    table = pg_table()
    try:
        src = live.pg_connector()
        sid = live.admin.post("/api/v1/schedules/", json={
            "name": f"r11 nightly ünï {uuid.uuid4().hex[:4]}", "source_connector_id": src,
            "source_table": table, "dest_connector_id": src, "dest_table": f"{table}_out",
            "interval": "hourly", "mappings": _MAPS,
        }).json()["id"]
        drop_pg(table)
        _due(sid)
        _settle(sid, runs=1)

        for cid in channels:
            rows = _deliveries(live, cid)
            assert [r["event"] for r in rows] == ["schedule.run.failed"], rows
            assert rows[0]["job_id"] and rows[0]["schedule_id"] == sid
        assert len(received) == 2, [r["body"].get("title") for r in received]
        body = received[0]["body"]
        assert body["schedule_id"] == sid and body["schedule_name"].startswith("r11 nightly")
        assert body["parked"] is True and body["approval_code"] == "SOURCE_TABLE_MISSING", body

        # A second finalize of the same failure is not a second alert.
        from services import schedule_runner
        from services.schedule_store import get_schedule

        sched = get_schedule(sid)
        schedule_runner._notify_schedule(sched, sched.last_job_id, "failed", {"error": "again"})
        assert len(received) == 2
        assert all(len(_deliveries(live, cid)) == 1 for cid in channels)
    finally:
        drop_pg(table)


def test_a_run_refused_before_any_job_alerts_each_channel_once(live, receiver):
    base, received = receiver
    channels = _channels(live, base)
    table = pg_table()
    try:
        dst = live.pg_connector()
        doomed = live.pg_connector()
        sid = live.admin.post("/api/v1/schedules/", json={
            "name": f"r11 refused {uuid.uuid4().hex[:4]}", "source_connector_id": doomed,
            "source_table": table, "dest_connector_id": dst, "dest_table": f"{table}_out",
            "interval": "hourly", "mappings": _MAPS,
        }).json()["id"]
        assert live.admin.delete(f"/api/v1/connectors/saved/{doomed}").status_code == 200
        _due(sid)
        _settle(sid, runs=1)
        for cid in channels:
            rows = _deliveries(live, cid)
            assert [(r["event"], r["job_id"]) for r in rows] == [("schedule.run.failed", "")], rows
        assert len(received) == 2
    finally:
        drop_pg(table)


def test_a_failed_manual_job_keeps_its_job_alert_once(live, receiver):
    base, received = receiver
    channels = _channels(live, base)
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    missing = f"r11_gone_{uuid.uuid4().hex[:6]}"
    cfg = dict(host=h.PG["host"], port=h.PG["port"], database=h.PG["database"],
               username=h.PG["username"], password=h.PG["password"])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", schema="public",
                              table=missing, **cfg),
        destination=EndpointConfig(kind="database", format="postgresql", schema="public",
                                   table=f"{missing}_out", **cfg),
        mappings=[{"source": "id", "target": "id", "approved": True, "confidence": 0.99}],
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        workspace_id=live.workspace_id,
    )
    engine = UniversalTransferEngine()
    job_id = engine._create_pending_job(request)
    result = engine.execute_tracked(request, job_id)
    assert not result.success
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and len(received) < 2:
        time.sleep(0.2)
    time.sleep(1.0)
    for cid in channels:
        rows = _deliveries(live, cid)
        assert [(r["event"], r["job_id"]) for r in rows] == [("job.failed", job_id)], rows
    assert len(received) == 2
