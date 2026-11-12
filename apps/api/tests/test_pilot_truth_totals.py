"""Pilot ground truth — totals come from counts, never from a page (audit H2, H3).

The page a tool returns is capped (20 pipelines, 8 context jobs, 25 briefing
jobs). Every total Pilot states must be the store's count for the population,
and must equal what the Jobs / Pipelines / Connectors pages show.
"""

from __future__ import annotations

import pytest

from tests.helpers import pilot_truth as pt


def _schedule_truth() -> dict:
    import services.schedule_store as schedule_store

    rows = schedule_store.list_schedules()
    return {"total": len(rows), "enabled": sum(1 for s in rows if s.enabled),
            "paused_names": {s.name for s in rows if not s.enabled},
            "enabled_names": {s.name for s in rows if s.enabled}}


@pytest.fixture
def pipelines(monkeypatch):
    pt.reset_workspace(monkeypatch)
    pt.seed_schedules(30, enabled_every=2)
    truth = _schedule_truth()
    assert truth["total"] == 30 and truth["enabled"] == 15
    return truth


@pytest.fixture
def jobs(monkeypatch):
    """18 jobs across every class the Jobs page pivots on; 1 needs attention."""
    pt.reset_workspace(monkeypatch)
    for i in range(12):
        pt.seed_job("completed", minutes_ago=600 - i)
    for i, status in enumerate(("running", "queued", "retrying", "completed_with_warnings", "cancelled")):
        pt.seed_job(status, minutes_ago=1 + i)
    pt.seed_job("completed", minutes_ago=6, reconciliation={"passed": False})
    return pt.job_truth()


def test_pipeline_count_is_the_population_not_the_page(pipelines):
    response = pt.ask(pt.agent(), "how many pipelines do I have")
    assert pt.bold_ints(response.answer)[:2] == [pipelines["total"], pipelines["enabled"]]
    assert 20 not in pt.bold_ints(response.answer)


def test_list_schedules_tool_reports_total_enabled_and_truncation(pipelines):
    from src.ai.copilot.tools import get_pilot_tools

    out = get_pilot_tools().execute("list_schedules", {}).output
    assert out["count"] == len(out["schedules"]) == 20
    assert out["total"] == pipelines["total"]
    assert out["enabled_count"] == pipelines["enabled"]
    assert out["truncated"] is True
    assert out["degraded"] is False and out["as_of"]


def test_enabled_pipeline_count_is_stated(pipelines):
    response = pt.ask(pt.agent(), "how many pipelines are enabled")
    assert pipelines["enabled"] in pt.bold_ints(response.answer)[:2]
    assert pt.bold_ints(response.answer)[0] == pipelines["total"]


def test_disabled_pipelines_question_lists_only_paused_ones(pipelines):
    import re

    response = pt.ask(pt.agent(), "which schedules are disabled")
    listed = set(re.findall(r"• \*\*(nightly-\d+)\*\*", response.answer))
    assert listed and listed <= pipelines["paused_names"], response.answer
    assert pipelines["total"] - pipelines["enabled"] in pt.bold_ints(response.answer)


def test_greeting_counts_are_whole_workspace_counts(jobs, tmp_path):
    pt.seed_sqlite_connector(tmp_path)
    response = pt.ask(pt.agent(), "hi")
    assert pt.bold_ints(response.answer)[:2] == [1, jobs["total"]]
    assert "recent job" not in response.answer


def test_briefing_job_classes_equal_the_jobs_page(jobs):
    by_status = jobs["by_status"]
    completed = sum(by_status.get(s, 0) for s in ("completed", "completed_with_warnings", "completed_with_quarantine"))
    in_flight = sum(by_status.get(s, 0) for s in ("running", "pending", "queued", "retrying"))
    response = pt.ask(pt.agent(), "give me a workspace briefing")
    line = next(ln for ln in response.answer.splitlines() if "Transfers" in ln)

    assert pt.bold_ints(line)[0] == jobs["total"] == 18
    assert "recent job" not in line
    assert f"{completed} completed" in line and completed == 14
    assert f"{in_flight} in flight" in line and in_flight == 3
    assert f"{by_status.get('failed', 0)} failed" in line
    assert f"{jobs['attention']} need attention" in line and jobs["attention"] == 1


def test_system_prompt_states_the_job_total_not_the_context_page(jobs):
    from src.ai.copilot.context_builder import get_context_builder

    builder = get_context_builder()
    ctx = builder.build({}, "hi")
    assert len(ctx["recent_jobs"]) == 8
    assert ctx["job_counts"]["total"] == jobs["total"]
    text = builder.to_system_context(ctx)
    assert f"{jobs['total']} transfer jobs" in text
    assert "8 recent transfer jobs" not in text


def test_connector_count_is_every_saved_connector(monkeypatch, tmp_path):
    pt.reset_workspace(monkeypatch)
    for i in range(27):
        pt.seed_sqlite_connector(tmp_path, name=f"Store {i:02d}", rows=1)
    from services.connector_store import list_connectors

    truth = len(list_connectors())
    response = pt.ask(pt.agent(), "how many connectors do I have")
    assert truth == 27 and pt.bold_ints(response.answer)[0] == truth


def test_job_count_is_the_whole_history(monkeypatch):
    pt.reset_workspace(monkeypatch)
    for i in range(130):
        pt.seed_job("completed", minutes_ago=i)
    response = pt.ask(pt.agent(), "how many jobs do I have")
    assert pt.bold_ints(response.answer)[0] == pt.job_truth()["total"] == 130
