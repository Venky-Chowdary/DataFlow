"""D6 — a confirmed rerun and a confirmed schedule run-now carry their approval.

QA: jobs started from Pilot/MCP ``confirm_action`` (rerun confirmed with an
ack; run_schedule_now confirmed) read back ``governance.approval = null`` and
``triggered_by = ""``, while start_transfer jobs carried ack_id/approved_by/
reason. ``/copilot/confirm`` stamped the confirmation only on the
start_transfer branch; the run_schedule branch called ``_run_schedule`` with
no ``triggered_by`` and the lifecycle branch (rerun_job) stamped nothing.

Routes exercised: the real ``POST /api/v1/copilot/confirm`` with the owning
REST handlers underneath; the scheduler and engine are stubbed where they
would need live connectors. The job store is the in-memory Mongo service.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: False)
    import services.mongodb_service as mongo_mod
    import src.ai.copilot.ack_ledger as ack_mod
    from fastapi.testclient import TestClient
    from services.mongodb_service import MemoryMongoDBService
    from src.main import app

    memory = MemoryMongoDBService()
    memory.connect()
    monkeypatch.setattr(mongo_mod, "_mongodb_service", memory)
    monkeypatch.setattr(ack_mod, "_ledger", None)
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, mongo=memory, ledger=ack_mod.get_ack_ledger())


def _governance(job: dict) -> dict:
    from src.ai.copilot.job_reads import job_governance

    return job_governance(job)


def test_confirmed_run_schedule_now_records_trigger_and_approval(world, monkeypatch):
    import services.schedule_runner as runner
    from services import schedule_store

    sched = SimpleNamespace(id="sched-1", name="nightly", enabled=True,
                            contract_id="", require_signed_contract=False)
    monkeypatch.setattr(schedule_store, "get_schedule", lambda sid: sched if sid == "sched-1" else None)
    seen: dict = {}

    def _fake_run(schedule_id, *, manual=False, resync_mode="", triggered_by=""):
        # What build_schedule_request would stamp on the pending job.
        seen.update({"schedule_id": schedule_id, "manual": manual, "triggered_by": triggered_by})
        return world.mongo.create_transfer_job({
            "status": "pending", "source": "s", "destination": "d",
            "triggered_by": triggered_by or f"schedule:{sched.name}",
            "transfer_request": {"schedule_id": schedule_id},
        })

    monkeypatch.setattr(runner, "_run_schedule", _fake_run)
    # copilot_router imports the name through the src.services shim, which
    # bound it at import time.
    import src.services.schedule_runner as shim

    monkeypatch.setattr(shim, "_run_schedule", _fake_run)
    ack_id = world.ledger.put(kind="run_schedule", payload={"schedule_id": "sched-1", "name": "nightly"},
                              preview={"name": "nightly"})
    res = world.client.post("/api/v1/copilot/confirm",
                            json={"ack_id": ack_id, "actor": "ops@example.com", "reason": "backfill"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is True and body["job_id"], body
    assert seen["manual"] is True
    assert seen["triggered_by"] == "ops@example.com"
    job = world.mongo.get_job(body["job_id"])
    gov = _governance(job)
    assert gov["triggered_by"] == "ops@example.com"
    assert gov["approval"] is not None, job
    assert gov["approval"]["ack_id"] == ack_id
    assert gov["approval"]["approved_by"] == "ops@example.com"
    assert gov["approval"]["reason"] == "backfill"
    assert gov["approval"]["action"] == "run_schedule"
    assert gov["approval"]["schedule_id"] == "sched-1"
    assert gov["approval"]["confirmed_at"].endswith("Z")


def test_schedule_request_names_its_trigger_actor_or_the_schedule():
    from services.schedule_runner import build_schedule_request
    from services.schedule_store import PipelineSchedule

    sched = PipelineSchedule(
        id="sched-2", name="hourly", source_connector_id="c-src", source_table="t",
        dest_connector_id="c-dst", dest_table="t_out", interval="hourly",
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    src = {"type": "sqlite", "kind": "database", "database": ":memory:"}
    dst = {"type": "sqlite", "kind": "database", "database": ":memory:"}
    assert build_schedule_request(sched, src, dst, triggered_by="ops@example.com").triggered_by == "ops@example.com"
    assert build_schedule_request(sched, src, dst).triggered_by == "schedule:hourly"


def test_confirmed_rerun_merges_ack_over_the_route_confirmation(world, monkeypatch):
    import src.transfer.engine as engine_mod

    parent = {
        "job_id": "job-r13-parent", "_id": "job-r13-parent", "status": "completed",
        "records_processed": 10,
        "transfer_request": {
            "source": {"kind": "database", "type": "sqlite", "table": "src"},
            "destination": {"kind": "database", "type": "sqlite", "table": "dst"},
            "sync_mode": "full_refresh_overwrite", "skip_preflight": True,
        },
    }
    world.mongo._jobs["job-r13-parent"] = parent  # the finished job being re-run

    class _Engine:
        def _create_pending_job(self, req):
            return world.mongo.create_transfer_job({
                "status": "pending", "source": "s", "destination": "d",
                "triggered_by": (req.triggered_by or "").strip(),
                "transfer_request": {"sync_mode": req.sync_mode},
            })

    monkeypatch.setattr(engine_mod, "get_transfer_engine", lambda: _Engine())
    monkeypatch.setattr("src.transfer.background.run_transfer_async", lambda *a, **k: None)
    ack_id = world.ledger.put(kind="rerun_job", payload={"job_id": "job-r13-parent"},
                              preview={"label": "Re-run"})
    res = world.client.post("/api/v1/copilot/confirm",
                            json={"ack_id": ack_id, "actor": "ops@example.com", "reason": "second pass"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is True
    new_id = str(body["job_id"])
    assert new_id and new_id != "job-r13-parent"
    job = world.mongo.get_job(new_id)
    gov = _governance(job)
    # The owning REST route names the trigger (the session actor; "anonymous"
    # while auth is off in this harness) — never "".
    assert gov["triggered_by"], job
    approval = gov["approval"]
    assert approval is not None, job
    # The route's own record survives the stamp...
    assert approval["action"] == "rerun"
    assert approval["of"] == "job-r13-parent"
    # ...and the ack's identity is added to it.
    assert approval["ack_id"] == ack_id
    assert approval["approved_by"] == "ops@example.com"
    assert approval["reason"] == "second pass"
    # The parent keeps whatever it had: acting on a job never rewrites its approval.
    assert "confirmation" not in parent
