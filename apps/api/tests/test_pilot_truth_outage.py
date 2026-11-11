"""Pilot ground truth — a store that cannot be read is reported as unread (audit C3).

"No transfer jobs yet" and "this workspace is still empty" are statements
about the workspace. When the job, schedule or connector store raises, Pilot
has no such statement to make: it must say it could not read, state no count,
and not mark the turn grounded.
"""

from __future__ import annotations

import pytest

from tests.helpers import pilot_truth as pt

_SECRET = "secret-internal-host"


def _boom(*_args, **_kwargs):
    raise RuntimeError(f"ServerSelectionTimeoutError: mongo:27017 [{_SECRET}]")


@pytest.fixture
def workspace(monkeypatch):
    """18 jobs and 4 pipelines in healthy stores."""
    pt.reset_workspace(monkeypatch)
    for i in range(12):
        pt.seed_job("completed", minutes_ago=600 - i)
    for i, status in enumerate(("running", "queued", "retrying", "completed_with_warnings", "cancelled")):
        pt.seed_job(status, minutes_ago=1 + i)
    pt.seed_job("completed", minutes_ago=6, reconciliation={"passed": False})
    pt.seed_schedules(4)
    return pt.job_truth()


def _break_job_store(monkeypatch):
    store = pt.mongo()
    for name in ("list_jobs", "count_jobs", "count_job_matches"):
        monkeypatch.setattr(store, name, _boom)
    from src.ai.copilot import context_builder

    context_builder.reset_context_cooldowns()


def _line(answer: str, label: str) -> str:
    return next((ln for ln in answer.splitlines() if label in ln), "")


@pytest.mark.parametrize(
    "question",
    ["how many jobs do I have", "show me my failed jobs", "how many jobs failed today", "show my jobs"],
)
def test_job_question_during_an_outage_says_unreadable(workspace, monkeypatch, question):
    _break_job_store(monkeypatch)
    response = pt.ask(pt.agent(), question)

    assert pt.says_unreadable(response.answer), response.answer
    assert "No transfer jobs yet" not in response.answer
    assert pt.bold_ints(response.answer) == [], "a count was stated from a failed read"
    assert _SECRET not in response.answer
    assert not pt.grounded(response)
    assert response.confidence <= 0.4


def test_list_jobs_tool_marks_the_read_degraded(workspace, monkeypatch):
    from src.ai.copilot.tools import get_pilot_tools

    healthy = get_pilot_tools().execute("list_jobs", {"limit": 5}).output
    assert healthy["degraded"] is False and healthy["total"] == workspace["total"] == 18
    _break_job_store(monkeypatch)
    out = get_pilot_tools().execute("list_jobs", {"limit": 5}).output
    assert out["degraded"] is True
    assert out["degraded_reason"] == "job_store_unreachable"
    assert out["as_of"]


def test_briefing_with_the_job_store_down(workspace, monkeypatch):
    _break_job_store(monkeypatch)
    response = pt.ask(pt.agent(), "give me a workspace briefing")
    transfers = _line(response.answer, "Transfers")

    assert pt.says_unreadable(transfers), response.answer
    assert pt.bold_ints(transfers) == []
    assert "still empty" not in response.answer
    # The stores that did answer are still reported, with their real numbers.
    assert pt.bold_ints(_line(response.answer, "Pipelines"))[0] == 4
    assert _SECRET not in response.answer


def test_briefing_with_the_schedule_store_down(workspace, monkeypatch):
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "list_schedules", _boom)
    response = pt.ask(pt.agent(), "give me a workspace briefing")

    assert "still empty" not in response.answer
    assert pt.says_unreadable(_line(response.answer, "Pipelines")), response.answer
    assert pt.bold_ints(_line(response.answer, "Pipelines")) == []
    assert pt.bold_ints(_line(response.answer, "Transfers"))[0] == workspace["total"]
    assert _SECRET not in response.answer


def test_briefing_of_an_unreadable_workspace_is_never_called_empty(monkeypatch):
    import services.connector_store as connector_store
    import services.schedule_store as schedule_store

    pt.reset_workspace(monkeypatch)
    _break_job_store(monkeypatch)
    monkeypatch.setattr(schedule_store, "list_schedules", _boom)
    monkeypatch.setattr(connector_store, "list_connectors", _boom)
    response = pt.ask(pt.agent(), "give me a workspace briefing")

    assert "still empty" not in response.answer
    for label in ("Connections", "Transfers", "Pipelines"):
        assert pt.says_unreadable(_line(response.answer, label)), (label, response.answer)
    assert not pt.grounded(response)


def test_greeting_states_no_job_count_it_could_not_read(workspace, monkeypatch, tmp_path):
    pt.seed_sqlite_connector(tmp_path)
    _break_job_store(monkeypatch)
    response = pt.ask(pt.agent(), "hi")
    assert "**0**" not in response.answer
    assert "job(s)" not in response.answer or pt.says_unreadable(response.answer)


def test_an_empty_workspace_is_still_reported_as_empty(monkeypatch):
    pt.reset_workspace(monkeypatch)
    response = pt.ask(pt.agent(), "how many jobs do I have")
    assert "No transfer jobs yet" in response.answer
    assert not pt.says_unreadable(response.answer)
