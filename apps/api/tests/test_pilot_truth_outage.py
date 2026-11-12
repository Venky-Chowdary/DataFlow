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


# ---- configured store unreachable or unparseable (review H1) -----------------
#
# The schedule and connector stores fall back to the local file when Mongo does
# not answer, and a corrupt schedules file reads as no schedules. The engine
# keeps that fallback; what Pilot reports must not: "No pipeline schedules
# yet" and "0 connector(s)" are statements about the workspace.

_DEAD_MONGO = "mongodb://127.0.0.1:9/?serverSelectionTimeoutMS=200&connectTimeoutMS=200"


@pytest.fixture
def mongo_down(monkeypatch):
    """A Mongo-backed deployment whose Mongo does not answer, default backends."""
    from services import connector_store, mongodb_service

    pt.reset_workspace(monkeypatch)
    monkeypatch.setenv("MONGODB_URI", _DEAD_MONGO)
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "mongo")
    monkeypatch.delenv("DATAFLOW_CONNECTOR_STORE_BACKEND", raising=False)
    monkeypatch.setattr(mongodb_service, "_mongodb_service", None)
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    from src.ai.copilot import context_builder

    context_builder.reset_context_cooldowns()
    return pt.agent()


@pytest.mark.parametrize("question", [
    "how many pipelines do I have",
    "why are my schedules not running",
    "list my pipelines",
])
def test_pipeline_question_with_mongo_down_says_unreadable(mongo_down, question):
    response = pt.ask(mongo_down, question)
    assert pt.says_unreadable(response.answer), response.answer
    assert "No pipeline" not in response.answer and "No pipelines" not in response.answer
    assert pt.bold_ints(response.answer) == []
    assert not pt.grounded(response)


def test_pipeline_question_does_not_state_a_stale_local_file(mongo_down):
    """A leftover schedules file is not the workspace's pipelines while Mongo is configured."""
    pt.seed_schedules(3)  # written to the file: Mongo is not answering
    response = pt.ask(mongo_down, "how many pipelines do I have")
    assert pt.says_unreadable(response.answer), response.answer
    assert 3 not in pt.bold_ints(response.answer)


def test_briefing_with_mongo_down_states_no_connector_or_pipeline_count(mongo_down):
    response = pt.ask(mongo_down, "give me a workspace briefing")
    for label in ("Connections", "Pipelines"):
        line = _line(response.answer, label)
        assert "0 " not in line.replace("**", ""), f"{label} stated as empty: {line!r}"
    assert "could not read" in response.answer.lower() or "couldn't read" in response.answer.lower()


def test_corrupt_schedules_file_says_unreadable(workspace, monkeypatch):
    import services.schedule_store as schedule_store

    schedule_store.STORE_PATH.write_text("{corrupt", encoding="utf-8")
    try:
        response = pt.ask(pt.agent(), "how many pipelines do I have")
        briefing = pt.ask(pt.agent(), "give me a workspace briefing")
    finally:
        schedule_store._save_all([])
    assert pt.says_unreadable(response.answer), response.answer
    assert "No pipeline" not in response.answer
    assert not pt.grounded(response)
    assert "0 pipeline" not in _line(briefing.answer, "Pipelines").replace("**", "")
