"""D19 — get_job reads only the stored record; long phases pulse heartbeats.

Repro: after cancelling a job whose source was slow, get_job timed out
3x (MCP -32001) because _get_job re-introspected the live source for
live_source_schema. And a slow MySQL source left the job frozen on one
message for 8 minutes with no heartbeat.
"""
from __future__ import annotations

import time

import pytest


def test_get_job_never_introspects_live(monkeypatch):
    """get_job must return the stored record without touching the source."""
    from src.ai.copilot import tools as copilot_tools
    from src.ai.copilot import schema_tools

    calls = []

    def _boom(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("live introspection must not run inside get_job")

    monkeypatch.setattr(
        schema_tools, "introspect_connector_schema", _boom, raising=True
    )
    monkeypatch.setattr(
        copilot_tools.DataPilotTools,
        "_get_job",
        copilot_tools.DataPilotTools._get_job,
    )
    job_doc = {
        "_id": "job-1",
        "status": "cancelled",
        "source_type": "mysql",
        "transfer_request": {
            "source": {"connector_id": "c1", "table": "orders", "format": "mysql"},
            "destination": {"connector_id": "c2", "table": "orders", "format": "sqlite"},
            "mappings": [],
            "sync_mode": "incremental_append",
        },
        "live_source_schema": {"column_count": 3},
        "records_processed": 10,
    }
    monkeypatch.setattr(
        "src.ai.copilot.job_reads.read_transfer_job",
        lambda job_id: job_doc,
        raising=True,
    )
    # _get_job imports read_transfer_job from .job_reads at call time.
    import src.ai.copilot.tools as t

    res = t.DataPilotTools().execute("get_job", {"job_id": "job-1"})
    assert res.success is True
    assert calls == [], "get_job introspected the live source"
    assert res.output["live_source_schema"] == {"column_count": 3}
    assert res.output["status"] == "cancelled"


def test_progress_heartbeat_during_slow_source():
    """A stalled phase emits elapsed-time pulses instead of freezing."""
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    class _Mongo:
        def __init__(self):
            self.events = []

        def update_job_status(self, job_id, status, **kw):
            self.events.append((job_id, status, kw))

    mongo = _Mongo()
    with job_phase_heartbeat(
        mongo,
        "job-42",
        phase="reading",
        message="Reading source data…",
        interval_s=0.05,
        progress_pct=5,
    ):
        time.sleep(0.3)

    pulses = [e for e in mongo.events if e[2].get("phase") == "reading"]
    assert len(pulses) >= 3, f"expected repeated heartbeats, got {len(pulses)}"
    assert all("has not stalled" in e[2]["message"] for e in pulses)
    assert all(e[2].get("progress_pct") == 5 for e in pulses)


def test_progress_heartbeat_stops_when_phase_ends():
    """No pulses after the block exits — and one bad update must not leak."""
    from src.transfer.reconcile_heartbeat import job_phase_heartbeat

    class _Mongo:
        def __init__(self):
            self.events = []

        def update_job_status(self, job_id, status, **kw):
            self.events.append(kw)

    mongo = _Mongo()
    with job_phase_heartbeat(
        mongo, "j", phase="writing", message="Writing…", interval_s=0.05
    ):
        time.sleep(0.12)
    seen = len(mongo.events)
    time.sleep(0.15)
    assert len(mongo.events) == seen, "heartbeat kept firing after the phase"
