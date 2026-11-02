"""Quarantine + reconcile persist bounded lineage on the job document."""

from __future__ import annotations

from services.lineage_telemetry import (
    clear_events,
    emit_reconciliation,
    emit_run_completed,
    persist_event_on_job,
)
from services.quarantine_dlq import persist_rejected_rows


class _FakeMongo:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {
            "job-lineage": {"_id": "job-lineage", "lineage_events": []},
        }

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        rec = self.jobs.setdefault(job_id, {"_id": job_id, "lineage_events": []})
        rec.update(fields)
        return True


def test_persist_event_on_job_caps_and_strips_summaries(monkeypatch):
    mongo = _FakeMongo()
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: mongo)
    event = emit_reconciliation(
        run_id="run-1",
        job_id="job-lineage",
        source_count=10,
        target_count=10,
        checksum_ok=True,
    )
    persist_event_on_job("job-lineage", event)
    stored = mongo.jobs["job-lineage"]["lineage_events"]
    assert stored[-1]["event_type"] == "reconciliation"
    assert stored[-1]["payload"]["source_count"] == 10
    assert "source_summary" not in stored[-1]["payload"]


def test_persist_rejected_rows_emits_quarantine_lineage(tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    mongo = _FakeMongo()
    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: mongo)
    clear_events()
    persist_rejected_rows(
        job_id="job-lineage",
        rejected_details=[{"row": 1, "reason": "bad_int", "values": {"id": "x"}}],
        workspace_id="ws1",
    )
    types = [e["event_type"] for e in mongo.jobs["job-lineage"]["lineage_events"]]
    assert "quarantine" in types


def test_emit_run_completed_carries_cdc_lag_when_present(monkeypatch):
    mongo = _FakeMongo()
    monkeypatch.setattr("services.mongodb_service.get_mongodb_service", lambda: mongo)
    event = emit_run_completed(
        run_id="run-cdc",
        job_id="job-lineage",
        records_transferred=3,
        destination_summary={"cdc_lag_seconds": 1.5, "cdc_lag_basis": "commit_ts"},
    )
    assert event["payload"]["cdc_lag_seconds"] == 1.5
    assert event["payload"]["cdc_lag_basis"] == "commit_ts"
    stored = mongo.jobs["job-lineage"]["lineage_events"][-1]
    assert stored["payload"]["cdc_lag_seconds"] == 1.5
    assert "destination_summary" not in stored["payload"]
