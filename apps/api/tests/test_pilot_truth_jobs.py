"""Pilot ground truth — the job ledger (audit C1, C2, H2).

Each case seeds real jobs, asks Pilot the question an operator asks, and
requires the numbers and the listed job ids to equal what the job store says.
Expected values are read from the store (``job_truth`` / ``job_ids``) for the
same status and window, so nothing here passes on a matching substring.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = pt.FROZEN_NOW  # 2026-11-11 02:30 UTC


def _at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=timezone.utc)


@pytest.fixture
def ledger(monkeypatch):
    """36 jobs: failures older than the newest 25 rows, mixed classes and days."""
    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    ids = {
        # Three days old: outside every page Pilot reads by default.
        "failed_old": [pt.seed_job("failed", at=_at(8, 10, i), error=f"cast failure #{i}") for i in range(3)],
        "warn": [pt.seed_job("completed_with_warnings", at=_at(9, 9))],
        "cancelled": [pt.seed_job("cancelled", at=_at(9, 10))],
        # 19:00 UTC on the 10th is already the 11th in Asia/Kolkata.
        "failed_eve": [pt.seed_job("failed", at=_at(10, 19), error="destination table dropped")],
        "ok_eve": [pt.seed_job("completed", at=_at(10, 20, i)) for i in range(5)],
        "ok_today": [pt.seed_job("completed", at=_at(11, 0, 10 + i)) for i in range(20)],
        "verify_failed": [
            pt.seed_job("completed", at=_at(11, 1, i), reconciliation={"passed": False, "rows_differ": 7})
            for i in range(2)
        ],
        "in_flight": [
            pt.seed_job(status, at=_at(11, 2, 20 + i))
            for i, status in enumerate(("queued", "retrying", "running"))
        ],
    }
    return ids


def _utc_day(day: int) -> dict:
    return {"date_from": _at(day, 0), "date_to": _at(day, 0) + timedelta(days=1)}


def test_fixture_is_what_the_cases_assume(ledger):
    truth = pt.job_truth()
    assert truth["total"] == 36
    assert truth["by_status"]["failed"] == 4
    assert truth["attention"] == 6


# ---- C1: a failed-jobs answer lists failed jobs, and only those -------------


@pytest.mark.parametrize("question", ["show me my failed jobs", "how many jobs failed", "which jobs failed?"])
def test_failed_jobs_answer_lists_only_failed_jobs(ledger, question):
    truth = pt.job_truth()
    failed_ids = set(pt.job_ids(status="failed"))
    response = pt.ask(pt.agent(), question)

    assert pt.bold_ints(response.answer)[0] == truth["by_status"]["failed"] == 4
    listed = pt.listed_job_ids(response.answer)
    assert listed, response.answer
    assert set(listed) <= failed_ids, f"non-failed job listed as failed: {response.answer}"
    assert "completed" not in response.answer.lower()
    assert pt.grounded(response)


def test_failed_question_when_the_last_job_did_not_fail(ledger):
    response = pt.ask(pt.agent(), "why did my last job fail")
    newest = pt.job_ids()[0]
    assert newest in response.answer
    assert "did not fail" in response.answer.lower()
    assert set(pt.listed_job_ids(response.answer)) - {newest} <= set(pt.job_ids(status="failed"))


def test_last_job_that_completed_but_failed_verification_is_not_reported_clean(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.seed_job("completed", minutes_ago=9)
    last = pt.seed_job("completed", minutes_ago=1, reconciliation={"passed": False, "rows_differ": 7})
    response = pt.ask(pt.agent(), "what is the status of my last transfer")
    assert last in response.answer and "COMPLETED" in response.answer
    assert "verification **failed**" in response.answer


def test_no_failures_never_lists_other_jobs_as_failed(monkeypatch):
    pt.reset_workspace(monkeypatch)
    for i in range(4):
        pt.seed_job("completed", minutes_ago=i)
    response = pt.ask(pt.agent(), "show me my failed jobs")
    assert pt.listed_job_ids(response.answer) == []
    assert pt.bold_ints(response.answer) in ([], [4])  # "none of your 4 job(s) failed"
    assert "no" in response.answer.lower()


# ---- C2: time windows are applied by the store, in the workspace zone -------


def test_failed_today_is_counted_inside_today_utc(ledger):
    in_window = pt.job_truth(**_utc_day(11))
    assert in_window["by_status"].get("failed", 0) == 0  # every failure is older
    response = pt.ask(pt.agent(), "how many jobs failed today")

    assert pt.listed_job_ids(response.answer) == []
    assert 4 not in pt.bold_ints(response.answer), "whole-history failures stated for 'today'"
    assert "today (Wed 11 Nov 2026, UTC)" in response.answer
    assert "As of 2026-11-11 02:30 UTC" in response.answer
    assert pt.grounded(response)


def test_failed_today_follows_the_workspace_time_zone(ledger, monkeypatch):
    pt.freeze_clock(monkeypatch, NOW, "Asia/Kolkata")
    # Kolkata's 11 Nov runs 10 Nov 18:30 UTC to 11 Nov 18:30 UTC.
    window = {"date_from": _at(10, 18, 30), "date_to": _at(11, 18, 30)}
    truth = pt.job_truth(**window)
    assert truth["by_status"]["failed"] == 1
    response = pt.ask(pt.agent(), "how many jobs failed today")

    assert pt.bold_ints(response.answer)[0] == 1
    assert pt.listed_job_ids(response.answer) == ledger["failed_eve"]
    assert "Asia/Kolkata" in response.answer


@pytest.mark.parametrize(
    "zone, window",
    [
        ("UTC", {"date_from": _at(11, 0), "date_to": _at(12, 0)}),
        ("Asia/Kolkata", {"date_from": _at(10, 18, 30), "date_to": _at(11, 18, 30)}),
        ("America/Los_Angeles", {"date_from": _at(10, 8), "date_to": _at(11, 8)}),
    ],
)
def test_jobs_ran_today_equals_the_store_count_for_that_day(ledger, monkeypatch, zone, window):
    pt.freeze_clock(monkeypatch, NOW, zone)
    truth = pt.job_truth(**window)
    response = pt.ask(pt.agent(), "how many jobs ran today")
    assert pt.bold_ints(response.answer)[0] == truth["total"], response.answer
    assert zone in response.answer
    assert set(pt.listed_job_ids(response.answer)) <= set(pt.job_ids(**window))


def test_failed_this_week_excludes_last_weeks_failures(ledger):
    # Monday 9 Nov 00:00 UTC onwards; the three failures on Sunday the 8th are out.
    window = {"date_from": _at(9, 0), "date_to": _at(16, 0)}
    assert pt.job_truth(**window)["by_status"]["failed"] == 1
    response = pt.ask(pt.agent(), "how many jobs failed this week")
    assert pt.bold_ints(response.answer)[0] == 1
    assert pt.listed_job_ids(response.answer) == ledger["failed_eve"]
    assert "this week (since Mon 9 Nov 2026, UTC)" in response.answer


def test_explicit_date_range_is_applied(ledger):
    window = {"date_from": _at(8, 0), "date_to": _at(10, 0)}
    truth = pt.job_truth(**window)
    response = pt.ask(pt.agent(), "how many jobs failed from 2026-11-08 to 2026-11-09")
    assert pt.bold_ints(response.answer)[0] == truth["by_status"]["failed"] == 3
    assert set(pt.listed_job_ids(response.answer)) == set(ledger["failed_old"])


@pytest.mark.parametrize("question", ["how many jobs failed this quarter", "which jobs failed last night"])
def test_a_window_pilot_cannot_apply_is_refused_not_dropped(ledger, question):
    response = pt.ask(pt.agent(), question)
    assert "can't apply the time window" in response.answer
    assert pt.bold_ints(response.answer) == []
    assert pt.listed_job_ids(response.answer) == []
    assert not pt.grounded(response)
    assert response.confidence <= 0.4


# ---- H2: counts equal the Jobs page ----------------------------------------


@pytest.mark.parametrize("question", ["how many jobs need attention", "which jobs need attention"])
def test_attention_count_is_the_jobs_page_needs_attention_count(ledger, question):
    truth = pt.job_truth()
    response = pt.ask(pt.agent(), question)
    assert pt.bold_ints(response.answer)[0] == truth["attention"] == 6
    listed = pt.listed_job_ids(response.answer)
    assert listed and set(listed) <= set(pt.job_ids(status="attention"))
    assert [t["name"] for t in response.tools_used] == ["list_jobs"]


def test_running_count_is_the_in_flight_class(ledger):
    truth = pt.job_truth()
    in_flight = sum(truth["by_status"].get(s, 0) for s in ("running", "pending", "queued", "retrying"))
    response = pt.ask(pt.agent(), "how many jobs are running")
    assert pt.bold_ints(response.answer)[0] == in_flight == 3
    assert set(pt.listed_job_ids(response.answer)) == set(ledger["in_flight"])


def test_completed_count_is_the_completed_class(ledger):
    completed = len(pt.job_ids(status="completed"))
    response = pt.ask(pt.agent(), "how many jobs completed")
    assert pt.bold_ints(response.answer)[0] == completed == 28


def test_list_jobs_tool_counts_equal_the_jobs_page_payload(monkeypatch, tmp_path):
    from fastapi.testclient import TestClient

    from services.orphan_jobs import stop_orphan_sweeper
    from src.ai.copilot.tools import get_pilot_tools
    from src.main import app

    pt.reset_workspace(monkeypatch)
    # Fresh timestamps: the app's stall watchdog is right to fail a back-dated
    # in-flight job, and that would change the ledger under the comparison.
    for i, status in enumerate(["failed"] * 3 + ["completed"] * 30 + ["running", "queued", "retrying", "cancelled"]):
        pt.seed_job(status, minutes_ago=i / 10)
    for i in range(2):
        pt.seed_job("completed", minutes_ago=5 + i, reconciliation={"passed": False})
    monkeypatch.setenv("DATAFLOW_TEAM_STORE", str(tmp_path / "teams.json"))
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    stop_orphan_sweeper()
    with TestClient(app) as client:
        for status in ("all", "running", "completed", "failed", "attention"):
            page = client.get(f"/api/v1/jobs?status={status}&limit=25").json()
            out = get_pilot_tools().execute("list_jobs", {"limit": 25, "status": status}).output
            assert page["total"] == 39 and page["attention_count"] == 5
            assert out["total"] == page["total"]
            assert out["status_counts"] == page["status_counts"]
            assert out["attention_count"] == page["attention_count"]
            assert out["filtered_total"] == page["filtered_total"], status
            assert out["truncated"] is (page["filtered_total"] > len(page["jobs"]))
            assert [j["id"] for j in out["jobs"]] == [str(j.get("_id") or j.get("id")) for j in page["jobs"]]
            assert out["degraded"] is False and out["as_of"]
