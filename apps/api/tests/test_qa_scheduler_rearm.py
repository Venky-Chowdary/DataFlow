"""QA scheduler parks: resume, a later success, and cadence timezone.

R2-080 pause_on_change failed every run with "Schema change policy incomplete"
because a create-new mapping implied backfill, which that policy forbids.
R2-107 / R2-109 a non-approvable finding stayed open across pause/resume, so
the beat never fired. R2-108 next_run_at stayed in the past for the same
reason. TC50 / RT5-09 editing the cadence without a zone wrote UTC.
"""

from __future__ import annotations

import pytest

import services.schedule_store as store


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield


def _parked(*, approvable: bool) -> store.PipelineSchedule:
    sched = store.create_schedule({
        "name": "QA parked",
        "source_connector_id": "src",
        "source_table": "t",
        "dest_connector_id": "dst",
        "dest_table": "t",
        "interval": "hourly",
        "cron": "*/15 * * * *",
        "timezone": "America/New_York",
        "mappings": [{"source": "id", "target": "id"}],
    })
    past = "2020-01-01T00:00:00+00:00"
    updated = store.update_schedule(sched.id, {
        "enabled": False,
        "next_run_at": past,
        "last_status": "needs_approval",
        "approval_request": {
            "id": "apr-1",
            "status": "open",
            "approvable": approvable,
            "code": "RUN_REFUSED",
            "finding": "Schema change policy incomplete",
        },
    })
    assert updated is not None
    return updated


def test_resume_rearms_a_non_approvable_finding_and_waits_for_the_next_slot(temp_store):
    """Resume closes a non-approvable finding and waits for the next cron slot."""
    from datetime import datetime, timezone

    parked = _parked(approvable=False)
    resumed = store.update_schedule(parked.id, {"enabled": True})
    assert resumed is not None
    assert resumed.last_status == "rearmed"
    assert resumed.approval_request["status"] == "approved"
    assert resumed.approval_request["resolved_by"] == "operator:resume"
    due = store._parse_ts(resumed.next_run_at)
    assert due is not None
    assert due > datetime.now(timezone.utc)
    assert resumed.next_run_at != parked.next_run_at
    assert not store.has_open_approval(resumed)
    assert resumed.id not in {item.id for item in store.due_schedules()}


def test_resume_does_not_grant_an_approvable_finding(temp_store):
    parked = _parked(approvable=True)
    resumed = store.update_schedule(parked.id, {"enabled": True})
    assert resumed is not None
    assert store.has_open_approval(resumed)
    assert resumed.last_status == "needs_approval"
    assert resumed.id not in {item.id for item in store.due_schedules()}


def test_successful_run_closes_the_park_and_keeps_the_cadence(temp_store):
    parked = _parked(approvable=False)
    # Pass the request in the same write so resume's re-arm does not close it.
    store.update_schedule(parked.id, {
        "enabled": True,
        "approval_request": {
            "id": "apr-2",
            "status": "open",
            "approvable": False,
            "code": "RUN_REFUSED",
            "finding": "still parked",
        },
        "last_status": "needs_approval",
        "next_run_at": "2030-01-01T00:00:00+00:00",
    })
    from services.schedule_approvals import close_finding_after_successful_run

    close_finding_after_successful_run(parked.id)
    done = store.get_schedule(parked.id)
    assert done is not None
    assert not store.has_open_approval(done)
    assert done.next_run_at == "2030-01-01T00:00:00+00:00"
    assert done.approval_request["resolved_by"] == "system:run-succeeded"


def test_pause_on_change_does_not_imply_backfill_from_create_new():
    from services.batch_progress import effective_backfill_new_fields
    from services.preflight_service import run_transfer_policy_gates

    mappings = [{
        "source": "id",
        "target": "id",
        "create_new": True,
        "assignment_strategy": "create_compatible_new",
    }]
    assert effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="pause_on_change",
        mappings=mappings,
    ) is False
    assert effective_backfill_new_fields(
        backfill_new_fields=False,
        schema_policy="manual_review",
        mappings=mappings,
    ) is True
    gates = run_transfer_policy_gates(
        schema_policy="pause_on_change",
        backfill_new_fields=False,
        mappings=mappings,
    )
    schema = next(g for g in gates if g["id"] == "g10_schema_policy")
    assert schema["status"] == "pass"
    assert "incomplete" not in schema["message"].lower()


def test_cadence_edit_keeps_the_schedule_timezone():
    from src.ai.copilot import lifecycle_tools as lt

    class _Sched:
        id = "s-ny"
        name = "NY load"
        enabled = True
        interval = "hourly"
        cron = ""
        timezone = "America/New_York"

    staged: list[dict] = []

    def _stage(kind, **kwargs):
        staged.append({"kind": kind, **kwargs})
        return type("R", (), {
            "success": True,
            "output": {"requires_confirm": True},
            "error": None,
        })()

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(lt, "_stage", _stage)
    try:
        result = lt.update_schedule(
            lambda _sid, _name: (_Sched(), None),
            name="NY load",
            cadence="every 15 minutes",
        )
    finally:
        monkeypatch.undo()
    assert result.success
    payload = staged[0]["payload"]
    assert payload["timezone"] == "America/New_York"
    assert payload["cron"] == "*/15 * * * *"
    assert "America/New_York" in staged[0]["preview"]["timezone_note"]
