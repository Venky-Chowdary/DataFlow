"""DEF-C-032: a completed transfer is newer health evidence than a failed probe."""

from __future__ import annotations

import time

import pytest

import services.connector_store as store


@pytest.fixture
def file_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(store, "_use_mongo", lambda: False)
    monkeypatch.delenv("DATAFLOW_CONNECTOR_STORE", raising=False)
    return store


def _mysql(file_store, name="QA MySQL LIVE"):
    return file_store.create_connector({
        "name": name, "type": "mysql", "host": "db", "port": 3306,
        "database": "qa", "username": "u", "password": "p",
    })


def _health(cid):
    return store.connector_health(store.get_connector(cid))


def test_a_completed_job_overrules_an_older_failed_probe(file_store, monkeypatch):
    conn = _mysql(file_store)
    store.mark_tested(conn.id, False)
    assert _health(conn.id) == "failed"

    import services.mongodb_service as mongo_mod
    from services.job_terminal_effects import apply_job_terminal_effects

    class _Jobs:
        def get_job(self, _job_id):
            return {"source_connector_id": "pg-src", "dest_connector_id": conn.id}

    monkeypatch.setattr(mongo_mod, "get_mongodb_service", lambda: _Jobs())
    monkeypatch.setattr(
        "services.schedule_runner.record_schedule_for_finished_job", lambda _j: None
    )
    time.sleep(0.002)
    apply_job_terminal_effects("job-ok", "completed")

    saved = store.get_connector(conn.id)
    assert saved.last_test_ok is False
    assert saved.last_transfer_ok_at
    assert store.connector_health(saved) == "passed"
    assert store.connector_ui_status(saved) == "configured"


def test_a_failed_job_does_not_clear_a_failed_probe(file_store, monkeypatch):
    conn = _mysql(file_store)
    store.mark_tested(conn.id, False)
    import services.mongodb_service as mongo_mod
    from services.job_terminal_effects import apply_job_terminal_effects

    monkeypatch.setattr(
        mongo_mod,
        "get_mongodb_service",
        lambda: type("J", (), {"get_job": lambda self, _j: {"dest_connector_id": conn.id}})(),
    )
    monkeypatch.setattr(
        "services.schedule_runner.record_schedule_for_finished_job", lambda _j: None
    )
    apply_job_terminal_effects("job-bad", "failed")
    assert _health(conn.id) == "failed"


def test_a_probe_that_fails_after_the_transfer_is_failed_again(file_store):
    conn = _mysql(file_store)
    store.note_transfer_succeeded(conn.id)
    time.sleep(0.002)
    store.mark_tested(conn.id, False)
    assert _health(conn.id) == "failed"
    assert store.connector_ui_status(store.get_connector(conn.id)) == "error"


def test_untested_stays_untested_and_passed_stays_passed(file_store):
    fresh = _mysql(file_store, "fresh")
    assert _health(fresh.id) == "untested"
    store.note_transfer_succeeded(fresh.id)
    assert _health(fresh.id) == "untested"
    ok = _mysql(file_store, "ok")
    store.mark_tested(ok.id, True)
    assert _health(ok.id) == "passed"


def test_pilot_list_and_briefing_bucket_on_the_same_rule(file_store, monkeypatch):
    stale = _mysql(file_store, "QA MySQL LIVE")
    store.mark_tested(stale.id, False)
    time.sleep(0.002)
    store.note_transfer_succeeded(stale.id)
    broken = _mysql(file_store, "Broken MySQL")
    store.mark_tested(broken.id, False)

    from src.ai.copilot.tools import DataPilotTools

    tools = DataPilotTools.__new__(DataPilotTools)
    failed = tools._list_connectors(health="failed").output["connectors"]
    assert [c["name"] for c in failed] == ["Broken MySQL"]
    passed = tools._list_connectors(health="passed").output["connectors"]
    assert [c["name"] for c in passed] == ["QA MySQL LIVE"]
    assert passed[0]["health"] == "passed"
    assert passed[0]["last_transfer_ok_at"]

    import src.ai.copilot.workspace_briefing as briefing

    monkeypatch.setattr(briefing, "_load_jobs", lambda _w: ([], {"total": 0, "by_status": {}}))
    monkeypatch.setattr(briefing, "_load_schedules", lambda _w: [])
    monkeypatch.setattr(briefing, "_load_contracts", lambda _w: [])
    monkeypatch.setattr(briefing, "_contract_census", lambda _w, _c: (0, 0))
    brief = briefing.collect_workspace_briefing(workspace_id="")
    assert brief["connectors_failed"] == 1
    assert brief["connectors_passed"] == 1
    assert brief["failed_connector_names"] == ["Broken MySQL"]
