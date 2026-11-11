"""Pilot ground truth — one workspace never sees another's jobs, pipelines or connectors.

The audit found these reads correctly scoped; the cases pin that, so the
answers stay equal to the caller's own store truth as the tools change. (Result
references and uploaded datasets, which were not scoped, have their own files.)
"""

from __future__ import annotations

from contextlib import contextmanager

import pytest

from tests.helpers import pilot_truth as pt


@contextmanager
def acting_as(workspace: str, actor: str, role: str = "viewer"):
    from services.workspace_filter import WorkspaceFilter, bound_workspace_filter
    from src.ai.copilot.tool_permissions import caller_role

    with bound_workspace_filter(WorkspaceFilter.only(workspace), stamp=workspace, actor=actor), caller_role(role):
        yield


@pytest.fixture
def two_workspaces(monkeypatch, tmp_path):
    pt.reset_workspace(monkeypatch)
    a_jobs = [pt.seed_job("failed", minutes_ago=3 + i, workspace_id="ws-A", error="A-only-host.internal refused") for i in range(4)]
    a_jobs += [pt.seed_job("completed", minutes_ago=20 + i, workspace_id="ws-A") for i in range(7)]
    b_jobs = [pt.seed_job("completed", minutes_ago=2 + i, workspace_id="ws-B") for i in range(3)]
    import services.schedule_store as schedule_store

    schedules = pt.seed_schedules(5, workspace_id="ws-A")
    schedule_store._save_all(schedules)
    with acting_as("ws-A", "alice@a.example", role="admin"):
        pt.seed_sqlite_connector(tmp_path, name="A Payroll DB", workspace_id="ws-A")
    return {"a": a_jobs, "b": b_jobs}


def _scope(workspace: str) -> dict:
    from services.workspace_filter import WorkspaceFilter

    return {"workspace_filter": WorkspaceFilter.only(workspace)}


def test_job_count_is_the_callers_workspace_only(two_workspaces):
    truth_b = pt.job_truth(**_scope("ws-B"))
    with acting_as("ws-B", "bob@b.example"):
        response = pt.ask(pt.agent(), "how many jobs do I have")
    assert pt.bold_ints(response.answer)[0] == truth_b["total"] == 3
    assert set(pt.listed_job_ids(response.answer)) <= set(two_workspaces["b"])
    pt.assert_only_these_numbers(response.answer, {truth_b["total"]})


def test_failed_jobs_of_another_workspace_are_not_counted_or_listed(two_workspaces):
    assert pt.job_truth(**_scope("ws-A"))["by_status"]["failed"] == 4
    with acting_as("ws-B", "bob@b.example"):
        response = pt.ask(pt.agent(), "show me my failed jobs")
    assert pt.listed_job_ids(response.answer) == []
    assert "A-only-host" not in response.answer
    pt.assert_only_these_numbers(response.answer, {3})


def test_another_workspaces_job_id_reads_as_not_found(two_workspaces):
    a_job = two_workspaces["a"][0]
    with acting_as("ws-B", "bob@b.example"):
        agent = pt.agent()
        by_id = pt.ask(agent, f"status of job {a_job}")
        by_context = pt.ask(agent, "why did it fail", job_id=a_job)
    for response in (by_id, by_context):
        assert "A-only-host" not in response.answer
        assert "FAILED" not in response.answer
    assert "not found" in by_id.answer.lower()
    assert not pt.grounded(by_id)


def test_briefing_counts_are_the_callers_workspace(two_workspaces):
    with acting_as("ws-B", "bob@b.example"):
        response = pt.ask(pt.agent(), "give me a workspace briefing")
    lines = {label: next(ln for ln in response.answer.splitlines() if label in ln)
             for label in ("Connections", "Transfers", "Pipelines")}
    assert pt.bold_ints(lines["Transfers"])[0] == 3
    assert pt.bold_ints(lines["Pipelines"])[0] == 0
    assert pt.bold_ints(lines["Connections"])[0] == 0
    assert "A Payroll DB" not in response.answer and "nightly-" not in response.answer


def test_pipelines_and_connectors_of_another_workspace_are_not_listed(two_workspaces):
    with acting_as("ws-B", "bob@b.example"):
        agent = pt.agent()
        pipelines = pt.ask(agent, "how many pipelines do I have")
        connectors = pt.ask(agent, "list my connectors")
    assert "nightly-" not in pipelines.answer
    assert "A Payroll DB" not in connectors.answer
    with acting_as("ws-A", "alice@a.example", role="admin"):
        own = pt.ask(pt.agent(), "how many pipelines do I have")
    assert pt.bold_ints(own.answer)[0] == 5
