"""Round 14 regressions.

R14-001: Run now twice. The second call must name the job the first call
already created. It must not say the claim has no job bound.
"""

from __future__ import annotations

import services.schedule_runner as runner
import services.schedule_store as store


def _isolate(tmp_path, monkeypatch):
    import services.connector_store as connectors

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(store, "_job_is_live", lambda _job_id: True)
    monkeypatch.setattr(connectors, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connectors, "_use_mongo", lambda: False)
    monkeypatch.setattr(connectors, "_resolve_backend", lambda: "file")
    monkeypatch.setattr(connectors, "_backend_choice", "file")


def test_claim_write_carries_the_job_id(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    sched = store.create_schedule({
        "name": "QA_R14",
        "source_connector_id": "src",
        "source_table": "s",
        "dest_connector_id": "dst",
        "dest_table": "d",
        "cron": "*/5 * * * *",
        "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
    })
    claimed = store.mark_schedule_running(sched.id, "inst", "job-6ad00409")
    assert claimed is not None
    assert claimed.running_job_id == "job-6ad00409"
    assert store.mark_schedule_running(sched.id, "other", "job-other") is None
    holder = store.last_claim_refusal(sched.id)
    assert holder is not None
    assert holder["job_id"] == "job-6ad00409"
    text = store.describe_claim_holder(holder)
    assert "job-6ad00409" in text
    assert "no job bound yet" not in text


def test_second_run_now_names_the_job_already_bound(tmp_path, monkeypatch):
    _isolate(tmp_path, monkeypatch)
    sched = store.create_schedule({
        "name": "QA_R14_run",
        "source_connector_id": "src",
        "source_table": "s",
        "dest_connector_id": "dst",
        "dest_table": "d",
        "cron": "*/5 * * * *",
        "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0}],
    })

    def _bind(schedule_id, *_args, **_kwargs):
        store.set_running_job(schedule_id, "job-6ad00409")
        return "job-6ad00409"

    monkeypatch.setattr(runner, "_dispatch_transfer", _bind)
    first = runner._run_schedule(sched.id, manual=True)
    assert first == "job-6ad00409"
    assert store.get_schedule(sched.id).running_job_id == "job-6ad00409"
    try:
        runner._run_schedule(sched.id, manual=True)
    except runner.ScheduleStartError as exc:
        text = str(exc)
        status = exc.http_status
    else:
        raise AssertionError("second Run now started another job")
    assert status == 409
    assert "job-6ad00409" in text
    assert "no job bound yet" not in text
    assert store.get_schedule(sched.id).running_job_id == "job-6ad00409"
