"""Pilot ground truth — no half-answers, no answer from the wrong subject (review 2, M3).

"how many jobs completed or failed" was answered "6 completed", "how many
failed and how many running jobs" with the failed count only, "show my failed
jobs and my connectors" with the connectors only, all grounded. "did any
schedule fail" was answered with the jobs count, "what pipelines need my
attention" with "25 enabled, 0 paused" (omitting 3 failed and 1 parked), and
"which datasets are mine" with unrelated documentation, grounded. A question
is answered whole from the tool that owns its subject, or refused.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.helpers import pilot_truth as pt

NOW = datetime(2026, 11, 11, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def workspace(monkeypatch, tmp_path):
    import services.schedule_store as ss
    from services import file_parser
    from src.ai.copilot.data_analyst import get_data_analyst

    pt.reset_workspace(monkeypatch)
    pt.freeze_clock(monkeypatch, NOW, "UTC")
    for i in range(3):
        pt.seed_job("failed", at=NOW - timedelta(minutes=10 + i))
    for i in range(2):
        pt.seed_job("running", at=NOW - timedelta(minutes=20 + i))
    for i in range(4):
        pt.seed_job("completed", at=NOW - timedelta(minutes=30 + i))
    pt.seed_job("queued", at=NOW - timedelta(minutes=40))
    rows = pt.seed_schedules(25, enabled_every=1)
    for s in rows[20:23]:
        s.last_status = "failed"
    rows[24].approval_request = {"status": "open", "finding": "schema drift"}
    ss._save_all(rows)
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setenv("DATAFLOW_UPLOAD_DIR", str(uploads))
    monkeypatch.delenv("DATAFLOW_PILOT_DATASET_DIRS", raising=False)
    monkeypatch.setattr(file_parser, "UPLOAD_DIR", uploads)
    monkeypatch.setattr(file_parser, "REGISTRY_PATH", tmp_path / "upload_registry.json")
    monkeypatch.setattr(file_parser, "_file_registry", {})
    analyst = get_data_analyst()
    monkeypatch.setattr(analyst, "feeder", type(analyst.feeder)())
    monkeypatch.setattr(analyst, "_cache", {})
    file_parser.store_upload("payroll_acme.csv", b"employee,salary\nAlice,1\nBob,2\n")
    return pt.agent()


def _count(**filters) -> int:
    return int(pt.mongo().count_job_matches(**filters) or 0)


def _tools(response) -> list[str]:
    return [str(t.get("name")) for t in response.tools_used or []]


@pytest.mark.parametrize("question,statuses", [
    ("how many failed and how many running jobs do I have", ("failed", "running")),
    ("how many jobs failed or are running", ("failed", "running")),
    ("how many jobs failed today and how many completed", ("failed", "completed")),
    ("how many jobs completed or failed", ("completed", "failed")),
    ("how many jobs are running or queued", ("running", "queued")),
])
def test_a_same_subject_compound_is_answered_whole_or_refused(workspace, question, statuses):
    response = pt.ask(workspace, question)
    stated = pt.bold_ints(response.answer)
    if pt.grounded(response):
        counts = {s: int(pt.job_truth()["by_status"].get(s, 0)) for s in statuses}
        assert all(n in stated for n in counts.values()), f"half-answered: {counts} vs {response.answer!r}"
    else:
        assert stated == [], response.answer
        for status in statuses:
            assert status in response.answer, f"the refusal must name {status!r}: {response.answer!r}"


@pytest.mark.parametrize("question", [
    "show my failed jobs and my connectors",
    "list my failed jobs and my pipelines",
])
def test_a_cross_subject_compound_gets_both_parts_or_a_refusal(workspace, question):
    response = pt.ask(workspace, question)
    if pt.grounded(response):
        assert _count(status="failed") in pt.bold_ints(response.answer), response.answer
        assert "connector" in response.answer.lower() or "pipeline" in response.answer.lower()
        assert {"list_jobs"} <= set(_tools(response)), response.tools_used
    else:
        assert pt.bold_ints(response.answer) == [], response.answer
        assert "jobs" in response.answer


@pytest.mark.parametrize("question", [
    "did any schedule fail", "which pipelines failed", "did any pipeline fail last time",
])
def test_a_failing_pipeline_question_reads_the_pipelines(workspace, question):
    response = pt.ask(workspace, question)
    assert "list_schedules" in _tools(response) and "list_jobs" not in _tools(response), response.tools_used
    assert 3 in pt.bold_ints(response.answer), response.answer
    assert _count(status="failed") not in pt.bold_ints(response.answer) or _count(status="failed") == 3
    assert pt.grounded(response)


@pytest.mark.parametrize("question", [
    "what pipelines need my attention", "any pipeline problems?", "do any pipelines need attention",
])
def test_pipelines_needing_attention_names_failed_and_parked(workspace, question):
    response = pt.ask(workspace, question)
    assert "list_schedules" in _tools(response), response.tools_used
    stated = pt.bold_ints(response.answer)
    assert 3 in stated and 1 in stated and "parked" in response.answer, response.answer
    assert "explain_product" not in _tools(response)


@pytest.mark.parametrize("question", [
    "how many datasets did I upload", "which datasets are mine", "do I have any uploads",
    "show my datasets", "what files have I uploaded",
])
def test_my_uploads_are_answered_from_the_dataset_listing(workspace, question):
    response = pt.ask(workspace, question)
    assert _tools(response)[:1] == ["list_datasets"], response.tools_used
    assert "explain_product" not in _tools(response) and "get_schedule" not in _tools(response)
    assert "payroll_acme.csv" in response.answer and 1 in pt.bold_ints(response.answer), response.answer
    assert pt.grounded(response)
