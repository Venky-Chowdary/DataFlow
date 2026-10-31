"""Every alert delivery attempt is recorded and listable (QA round 11, SC9).

QA (production 9c75ada4): "failure alert via webhook — UNTESTABLE end to end.
The API has no delivery log." ``log_job_notifications`` wrote results onto the
Mongo job document only: a run refused before a job existed (``job_id ""``)
raised in ``ObjectId("")`` and left nothing, test-sends were never recorded,
and no route listed deliveries.

Through the real routes on live MongoDB: a webhook channel is created, test-
sent, and alerted by real scheduler beats (one refused before any job, one
whose job failed on a dropped PostgreSQL table, one to a receiver answering
500). ``GET /workspace/notifications/{id}/deliveries`` and
``GET /schedules/{id}/notifications`` list them, scoped to the workspace, and
never return the URL, headers or signing secret.
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

pytestmark = pytest.mark.timeout(300)

_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


@pytest.fixture
def receiver(monkeypatch):
    received: list[dict] = []
    state = {"status": 200}

    class Hook(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            received.append({"path": self.path, "body": json.loads(body or b"{}")})
            self.send_response(state["status"])
            self.end_headers()
            self.wfile.write(b"receiver says ops@example.com")

        def log_message(self, *args):
            return

    server = HTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    from services import notification_service

    # The SSRF guard refuses loopback; the receiver is local by necessity.
    monkeypatch.setattr(notification_service, "_allowed_url", lambda url: True)
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/hook/s3cr3t-path", received, state
    finally:
        server.shutdown()


def _settle(schedule_id: str, runs: int, timeout: float = 120.0) -> None:
    from services import schedule_runner
    from services.schedule_store import get_schedule

    schedule_runner._run_due_schedules()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sched = get_schedule(schedule_id)
        done = [r for r in sched.run_history if r.get("status") not in ("pending", "running")]
        if not sched.running and len(done) >= runs:
            time.sleep(0.5)  # the finalize callback notifies after recording
            return
        time.sleep(0.5)
    sched = get_schedule(schedule_id)
    raise AssertionError(
        f"schedule {schedule_id} did not settle: running={sched.running} "
        f"history={sched.run_history} approval={sched.approval_request} "
        f"next={sched.next_run_at} enabled={sched.enabled} status={sched.last_status}"
    )


def _due(schedule_id: str) -> None:
    from services.schedule_store import update_schedule

    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    update_schedule(schedule_id, {"next_run_at": past, "retry_at": None})


def test_sc9_every_delivery_is_logged_and_listed_per_workspace(live, receiver):
    url, received, state = receiver
    admin = live.admin
    made = admin.post(
        "/api/v1/workspace/notifications",
        json={"workspace_id": live.workspace_id, "kind": "webhook", "label": "ops 中文 ✓",
              "config": {"url": url}},
    )
    assert made.status_code == 200, made.text
    channel_id = made.json()["id"]
    secret = made.json()["signing_secret"]

    # 1. Test-send.
    sent = admin.post(f"/api/v1/workspace/notifications/{channel_id}/test")
    assert sent.status_code == 200 and sent.json()["success"] is True, sent.text

    # 2. A run refused before any job existed: its source connector was deleted.
    table = pg_table()
    try:
        dst = live.pg_connector()
        doomed = live.pg_connector()
        sid = admin.post("/api/v1/schedules/", json={
            "name": f"r11 alert {uuid.uuid4().hex[:6]}", "source_connector_id": doomed,
            "source_table": table, "dest_connector_id": dst, "dest_table": f"{table}_out",
            "interval": "hourly", "mappings": _MAPS,
        }).json()["id"]
        assert admin.delete(f"/api/v1/connectors/saved/{doomed}").status_code == 200
        _due(sid)
        _settle(sid, runs=1)

        # 3. A run whose job failed: the source table is gone; the receiver is down.
        from services.schedule_store import update_schedule

        update_schedule(sid, {"source_connector_id": dst, "approval_request": {}})
        drop_pg(table)
        state["status"] = 500
        _due(sid)
        _settle(sid, runs=2)
    finally:
        drop_pg(table)

    # Test-send, the refused run, and one alert for the failed job (the
    # schedule's; the engine's job alert stands down for a scheduled job).
    assert len(received) == 3, [r["body"].get("status") for r in received]

    listed = admin.get(f"/api/v1/workspace/notifications/{channel_id}/deliveries")
    assert listed.status_code == 200, listed.text
    rows = listed.json()["deliveries"]
    assert len(rows) == 3, rows
    assert rows == sorted(rows, key=lambda r: r["attempted_at"], reverse=True)
    test = [r for r in rows if r["event"] == "test"]
    sched_rows = [r for r in rows if r["event"] == "schedule.run.failed"]
    job_rows = [r for r in rows if r["event"] == "job.failed"]
    assert len(test) == 1 and len(sched_rows) == 2 and job_rows == [], rows
    test = test[0]
    newest, refused = sched_rows
    assert test["ok"] is True and test["http_status"] == 200 and test["signed"] is True
    assert test["signature_id"].isdigit(), test
    assert refused["job_id"] == "" and refused["schedule_id"] == sid and refused["ok"] is True
    assert newest["job_id"] and newest["schedule_id"] == sid
    assert newest["ok"] is False and newest["http_status"] == 500, newest
    assert "ops@example.com" not in json.dumps(rows)
    for row in rows:
        assert row["channel_id"] == channel_id and row["kind"] == "webhook"
        assert row["workspace_id"] == live.workspace_id
        assert row["attempted_at"] and row["duration_ms"] >= 0
    # Never the URL, path secret or signing secret.
    assert "s3cr3t-path" not in listed.text and "127.0.0.1" not in listed.text
    assert secret not in listed.text

    per_schedule = admin.get(f"/api/v1/schedules/{sid}/notifications")
    assert per_schedule.status_code == 200, per_schedule.text
    assert [r["id"] for r in per_schedule.json()["deliveries"]] == [newest["id"], refused["id"]]
    assert admin.get(f"/api/v1/schedules/{sid}/notifications", params={"limit": 1}).json()[
        "deliveries"
    ][0]["id"] == newest["id"]

    # The job record carries its own result too (Mongo job store).
    job = live.db["transfer_jobs"].find_one({"_id": __import__("bson").ObjectId(newest["job_id"])})
    assert job and job.get("notifications"), job and list(job)

    # Stored in MongoDB with a TTL, and nothing secret on disk.
    docs = list(live.db["notification_deliveries"].find({"channel_id": channel_id}))
    assert len(docs) == 3
    assert all(isinstance(d["expires_at"], datetime) for d in docs)
    indexes = live.db["notification_deliveries"].index_information()
    assert any(ix.get("expireAfterSeconds") == 0 for ix in indexes.values()), indexes

    # Another workspace sees none of it.
    other_ws = live.ent.workspace("R11 other")
    other = live.ent.member(email=f"r11-other-{uuid.uuid4().hex[:6]}@example.com",
                            workspace_id=other_ws, role="admin")
    assert other.get(f"/api/v1/workspace/notifications/{channel_id}/deliveries").status_code == 404
    assert other.get(f"/api/v1/schedules/{sid}/notifications").status_code in (403, 404)
    viewer = live.ent.member(email=f"r11-viewer-{uuid.uuid4().hex[:6]}@example.com",
                             workspace_id=live.workspace_id, role="viewer")
    assert viewer.get(f"/api/v1/workspace/notifications/{channel_id}/deliveries").status_code == 403


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, ""),
        ("", ""),
        ("   ", ""),
        ("refused by https://hooks.slack.com/services/T0/B0/XYZsecret ok", "refused by <url> ok"),
        ("bounce for ops@example.com — naïve 中文 ✓", "bounce for <redacted-email> — naïve 中文 ✓"),
    ],
)
def test_sc9_delivery_errors_are_redacted(raw, expected):
    from services.notification_delivery_log import redact_error

    assert redact_error(raw) == expected


def test_sc9_file_backed_log_when_mongo_is_absent(tmp_path, monkeypatch):
    """Single-node deployments without MongoDB keep the same log in JSONL."""
    from services import notification_delivery_log as log
    from services.notification_store import NotificationChannel

    monkeypatch.setattr(log, "LOG_PATH", tmp_path / "deliveries.jsonl")
    monkeypatch.setattr(log, "_collection", lambda: None)
    channel = NotificationChannel(id="ch-1", workspace_id="ws-1", kind="webhook", label="x",
                                  enabled=False, config={})
    from services.notification_service import send_to_channel

    with log.delivery_context(schedule_id="sched-1"):
        out = send_to_channel(channel, {"status": "failed", "job_id": ""})
    assert out["ok"] is False
    rows = log.list_deliveries(workspace_id="ws-1", channel_id="ch-1")
    assert len(rows) == 1 and rows[0]["schedule_id"] == "sched-1" and rows[0]["job_id"] == ""
    assert rows[0]["error"] == "Channel disabled"
    assert log.list_deliveries(workspace_id="ws-2", channel_id="ch-1") == []
