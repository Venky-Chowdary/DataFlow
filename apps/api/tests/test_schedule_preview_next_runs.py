"""Re-review F: the schedule form's "Next 5 runs" comes from the scheduler itself.

The browser's own copy of the cron rules can still disagree with the server
on time-zone data (Africa/Casablanca, Africa/El_Aaiun: the browser's ICU and
this server's zoneinfo differ by an hour). POST /schedules/preview answers
with the functions save and the beat use, so the times it lists are the
times the scheduler will run.
"""

from __future__ import annotations

from datetime import datetime, timezone

from services.cron_schedule import next_run
from services.schedule_store import compute_next_run, preview_next_runs

NOW = datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)


def test_a_cron_lists_the_schedulers_own_next_runs_in_any_zone():
    for zone in ("Africa/Casablanca", "Africa/El_Aaiun", "Australia/Lord_Howe", "America/New_York"):
        out = preview_next_runs("daily", "30 1 * * *", zone, now=NOW)
        assert out["kind"] == "clock"
        cursor, want = NOW, []
        for _ in range(5):
            cursor = next_run("30 1 * * *", cursor, zone)
            want.append(cursor.isoformat())
        assert out["runs"] == want, zone


def test_rolling_cadences_anchor_like_save_does():
    fresh = preview_next_runs("hourly", "", "UTC", now=NOW)
    assert fresh["kind"] == "rolling"
    assert fresh["runs"][0] == compute_next_run("hourly", NOW)  # what create/update store
    kept = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T10:20:00+00:00",
                             cadence_changed=False, now=NOW)
    assert kept["runs"][0] == "2026-11-21T10:20:00+00:00" and kept["from_stored"] is True
    changed = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T10:20:00+00:00",
                                cadence_changed=True, now=NOW)
    assert changed["runs"][0] == "2026-11-21T11:00:00+00:00"
    uneven = preview_next_runs("daily", "*/7 * * * *", "UTC", now=NOW)
    assert uneven["kind"] == "rolling" and uneven["every_minutes"] == 7


def test_paused_and_invalid():
    assert preview_next_runs("daily", "0 9 * * *", "UTC", enabled=False, now=NOW) == {"kind": "paused", "runs": []}
    assert preview_next_runs("daily", "0 0 * * * *", "UTC", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("daily", "0 0 30 2 *", "UTC", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("daily", "0 9 * * *", "Mars/Olympus", now=NOW)["kind"] == "invalid"
    assert preview_next_runs("fortnightly", "", "UTC", now=NOW)["kind"] == "invalid"
    assert len(preview_next_runs("daily", "* * * * *", "UTC", count=99, now=NOW)["runs"]) == 10


def test_route_is_a_read():
    from services.rbac import Permission, _required_permission

    assert _required_permission("POST", "/api/v1/schedules/preview") == Permission.SCHEDULE_READ
    assert _required_permission("POST", "/api/v1/schedules/") == Permission.SCHEDULE_MANAGE


def test_the_preview_is_fast_even_for_rare_dates_and_runs_off_the_event_loop():
    """Review: the route ran next_run's minute walk on the event loop (21 s for Feb 29 x 10)."""
    import inspect
    import time

    from src.routers.schedules_router import preview_schedule_next_runs

    assert not inspect.iscoroutinefunction(preview_schedule_next_runs), "sync: FastAPI runs it on the thread pool"
    for zone in ("UTC", "Africa/Casablanca", "Australia/Lord_Howe"):
        start = time.perf_counter()
        out = preview_next_runs("daily", "0 0 29 2 *", zone, count=10, now=NOW)
        assert out["kind"] == "clock" and len(out["runs"]) == 10
        assert time.perf_counter() - start < 0.5, zone
    refused = preview_next_runs("daily", "0 0 31 2 *", "UTC", count=10, now=NOW)
    assert refused["kind"] == "invalid" and "can never fire" in refused["message"]


def test_an_overdue_rolling_cadence_runs_now_then_counts_from_now_like_the_beat():
    """Review: the beat fires an overdue schedule at its next check, then next = now + delta."""
    out = preview_next_runs("hourly", "", "UTC", stored_next_run_at="2026-11-21T07:20:00+00:00",
                            cadence_changed=False, now=NOW)
    assert out["due_now"] is True
    assert out["runs"][:3] == [NOW.isoformat(), "2026-11-21T11:00:00+00:00", "2026-11-21T12:00:00+00:00"]
    assert out["runs"][1] == compute_next_run("hourly", NOW)  # what claim_due_schedules stores
    cron = preview_next_runs("daily", "0 9 * * *", "UTC", stored_next_run_at="2026-11-20T09:00:00+00:00",
                             cadence_changed=False, now=NOW)
    assert cron["due_now"] is True and cron["runs"][1] == "2026-11-22T09:00:00+00:00"


def test_a_list_cut_by_the_time_budget_says_so(monkeypatch):
    """Review round 4 #2: fewer runs than asked must not look like the whole answer."""
    import services.schedule_store as store

    full = preview_next_runs("daily", "0 9 * * *", "UTC", count=5, now=NOW)
    assert len(full["runs"]) == 5 and "truncated" not in full
    monkeypatch.setattr(store, "PREVIEW_TIME_BUDGET_SECONDS", -1.0)
    cut = preview_next_runs("daily", "0 9 * * *", "UTC", count=5, now=NOW)
    assert cut["truncated"] is True and 1 <= len(cut["runs"]) < 5
    overdue = preview_next_runs("daily", "0 9 * * *", "UTC", stored_next_run_at="2026-11-20T09:00:00+00:00",
                                cadence_changed=False, count=5, now=NOW)
    assert overdue["truncated"] is True and overdue["runs"] == [NOW.isoformat()]


def _request(email: str | None, host: str = "10.0.0.7"):
    from starlette.requests import Request

    req = Request({"type": "http", "method": "POST", "path": "/api/v1/schedules/preview", "headers": [],
                   "client": (host, 5000)})
    if email:
        req.state.user_email = email
    return req


def test_each_caller_gets_ten_previews_per_ten_seconds():
    """Review round 4 #2: the preview is CPU on the worker pool; a loop must not monopolise it."""
    import pytest
    from fastapi import HTTPException

    from src.routers.schedules_router import NextRunsPreviewBody, _preview_rate_store, preview_schedule_next_runs

    _preview_rate_store().clear()
    body = NextRunsPreviewBody(interval="daily", cron="0 9 * * *", timezone="UTC")
    for _ in range(10):
        assert preview_schedule_next_runs(body, _request("ana@example.com"))["kind"] == "clock"
    with pytest.raises(HTTPException) as refused:
        preview_schedule_next_runs(body, _request("ana@example.com"))
    assert refused.value.status_code == 429 and int(refused.value.headers["Retry-After"]) >= 1
    # Another account, and an anonymous caller by address, have their own allowance.
    assert preview_schedule_next_runs(body, _request("bo@example.com"))["kind"] == "clock"
    assert preview_schedule_next_runs(body, _request(None))["kind"] == "clock"
    _preview_rate_store().clear()
