"""No-AI-key Pilot conversations operators actually had (2026-11-02 transcript).

Each turn failed in production: job-status asks answered "Connector not found",
"can you create a transform" recited documentation, and "can you do that?" got
an unrelated job list. These pin the deterministic routing that replaced them.
"""
from __future__ import annotations

import pytest

from src.ai.copilot.dialogue_acts import (
    asks_to_do_that,
    is_create_transform_capability_ask,
    is_transform_templates_ask,
)
from src.ai.copilot.followup import looks_like_consent
from src.ai.copilot.job_narration import narrate_jobs
from src.ai.copilot.tool_permissions import ACK_KIND_PERMISSIONS
from src.ai.copilot.workspace_intents import ground_plan, job_status_asked, match_transform_template

_JOBS_HISTORY = [{"role": "user", "content": "how many jobs do I have"}, {"role": "assistant", "content": "8 jobs"}]
_PIPE_HISTORY = [{"role": "user", "content": "show my pipelines"}, {"role": "assistant", "content": "2 pipelines"}]


@pytest.mark.parametrize("message", ["how many completed", "and failed?", "which of those failed", "any running?"])
def test_job_status_asks_read_the_jobs_ledger(message):
    misread = [("aggregate_data", {"metric": "count", "table": "completed"})]
    assert ground_plan(message, misread, _JOBS_HISTORY) == [("list_jobs", {"limit": 25})]
    assert ground_plan(message, [], _JOBS_HISTORY) == [("list_jobs", {"limit": 25})]


def test_status_word_about_data_stays_a_data_read():
    plan = [("aggregate_data", {"metric": "count", "table": "orders", "name": "Orders PG"})]
    assert ground_plan("how many failed rows in orders", plan, _JOBS_HISTORY) == plan


def test_named_connector_aggregate_is_not_rerouted():
    plan = [("aggregate_data", {"metric": "count", "table": "completed", "name": "Orders PG"})]
    assert ground_plan("how many completed", plan, None) == plan


def test_status_follow_up_after_connectors_is_not_hijacked():
    hist = [{"role": "user", "content": "list my connectors"}]
    assert ground_plan("which failed", [("explain_product", {})], hist) == [("explain_product", {})]


def test_paused_follow_up_after_pipelines_lists_schedules():
    assert ground_plan("are any of them paused", [("list_jobs", {"limit": 10})], _PIPE_HISTORY) == [
        ("list_schedules", {})
    ]


def test_how_to_question_does_not_plan_a_move_against_a_guessed_connector():
    plan = [("explain_product", {"query": "q"}), ("plan_transfer", {"dest_connector_name": "postgres"})]
    assert ground_plan("how do I move a csv into postgres", plan, None) == [("explain_product", {"query": "q"})]


def test_job_status_vocabulary():
    assert job_status_asked("how many succeeded") == "completed"
    assert job_status_asked("any cancelled?") == "cancelled"
    assert job_status_asked("what is a transform") == ""


def _jobs_output():
    jobs = [
        {"id": "a", "source": "s", "destination": "d", "status": "completed", "records": 10},
        {"id": "b", "source": "s", "destination": "d", "status": "failed", "records": 0},
        {"id": "c", "source": "s", "destination": "d", "status": "completed", "records": 5},
    ]
    return {"jobs": jobs, "total": 3, "status_counts": {"completed": 2, "failed": 1}}


def test_status_count_answer_uses_counted_totals():
    text = narrate_jobs(_jobs_output(), "how many completed")
    assert text.startswith("**2** of your **3** job(s) were completed")
    assert "`b`" not in text


def test_status_count_answer_says_none_when_none():
    assert narrate_jobs(_jobs_output(), "any running?") == "None of your **3** job(s) are running."


@pytest.mark.parametrize(
    "message",
    ["can you create a transform", "Could you make me a transformation?", "how do I create a transform recipe", "help me build a transform"],
)
def test_transform_capability_asks(message):
    assert is_create_transform_capability_ask(message)


def test_transform_capability_does_not_swallow_other_creates():
    assert not is_create_transform_capability_ask("can you create a connector")
    assert not is_create_transform_capability_ask("what is a transform")


def test_transform_templates_ask():
    assert is_transform_templates_ask("what templates are there")
    assert is_transform_templates_ask("show transform templates")
    assert not is_transform_templates_ask("show my pipelines")


@pytest.mark.parametrize(
    "message,key",
    [
        ("create a dedupe transform", "dedupe-keep-latest"),
        ("Create the Dedupe Keep Latest transform", "dedupe-keep-latest"),
        ("make an scd2 history transform", "scd2-history"),
        ("create a transform that hashes PII emails", "standardise-email-hash-pii"),
        ("use the safe cast template", "safe-cast-numerics"),
    ],
)
def test_template_named_in_a_create_request(message, key):
    assert match_transform_template(message) == key


def test_template_match_needs_a_create_request():
    assert match_transform_template("what does the dedupe transform do") == ""
    assert match_transform_template("create a dedupe") == ""


def test_can_you_do_that_is_consent_not_a_job_list():
    assert asks_to_do_that("can you do that ?")
    assert looks_like_consent("can you do that?")
    assert not asks_to_do_that("can you do that for kafka and bigquery and more")


def test_transform_confirm_requires_write_permission():
    assert "create_transform_recipe" in ACK_KIND_PERMISSIONS


def test_transform_tool_stages_without_saving(monkeypatch):
    from src.ai.copilot.tools import DataPilotTools

    saved = []
    monkeypatch.setattr("services.transform_library.get_transform_library", lambda: saved.append(1))
    result = DataPilotTools().execute("create_transform_recipe", {"template_key": "dedupe-keep-latest"})
    assert result.success
    assert result.output["requires_confirm"] is True
    assert result.output["ack_id"]
    assert result.output["preview"]["name"]
    assert saved == []


def test_transform_tool_unknown_template_lists_real_choices():
    from src.ai.copilot.tools import DataPilotTools

    result = DataPilotTools().execute("create_transform_recipe", {"template_key": "nope"})
    assert not result.success
    assert "Dedupe" in result.error


from tests.test_mcp_confirm_route import _confirm, _role, isolated  # noqa: E402,F401


def test_confirm_saves_one_transform_and_replay_saves_none(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "editor", "qe@example.com"))
    import services.transform_library as lib_mod
    from src.ai.copilot.tools import DataPilotTools

    saved = []

    class _Library:
        def save(self, recipe):
            recipe.id = recipe.id or f"rcp-{len(saved) + 1}"
            saved.append(recipe)
            return recipe

    monkeypatch.setattr(lib_mod, "get_transform_library", lambda: _Library())
    staged = DataPilotTools().execute("create_transform_recipe", {"template_key": "dedupe-keep-latest"})
    assert staged.success, staged.error
    assert saved == []

    first = _confirm(staged.output["ack_id"])
    assert first["ok"] is True, first
    assert first["kind"] == "create_transform_recipe"
    assert first["recipe_id"] == saved[0].id
    assert first["steps"] >= 1
    assert len(saved) == 1

    replay = _confirm(staged.output["ack_id"])
    assert replay["ok"] is True and replay["idempotent"] is True
    assert len(saved) == 1


def test_viewer_cannot_confirm_a_transform(isolated, monkeypatch):
    monkeypatch.setattr(isolated.router, "_caller", _role(isolated.router, "viewer", "v@example.com"))
    from src.ai.copilot.tools import DataPilotTools

    staged = DataPilotTools().execute("create_transform_recipe", {"template_key": "scd2-history"})
    result = _confirm(staged.output["ack_id"])
    assert result.get("ok") is not True


def test_cancelled_count_is_answered_as_a_count():
    out = _jobs_output()
    out["jobs"].append({"id": "x", "source": "s", "destination": "d", "status": "cancelled", "records": 0})
    out["status_counts"]["cancelled"] = 1
    assert narrate_jobs(out, "how many cancelled jobs").startswith("**1** of your **3** job(s) were cancelled")


def test_overview_of_my_workspace_is_a_briefing_not_a_tab():
    assert ground_plan("overview of my workspace", [("navigate", {"screen": "overview"})], None) == [
        ("brief_workspace", {})
    ]
    assert ground_plan("open overview", [("navigate", {"screen": "overview"})], None) == [
        ("navigate", {"screen": "overview"})
    ]


_FAILED_LIST = (
    "**1** of your **3** job(s) failed:\n"
    "• `0002aaaaaaaaaaaaaaaaaaaa` · orders -> orders_copy_2: **failed** — Destination unreachable"
)


@pytest.mark.parametrize("ask", [
    "how do I fix it?", "retry it", "what was the error?", "what went wrong", "ok, rerun it",
    "why did it fail", "why did it fail?", "so why did that fail", "what happened", "why?",
    "what caused it",
])
def test_fix_or_retry_after_failed_list_reads_that_job(ask):
    hist = [{"role": "user", "content": "which jobs failed"}, {"role": "assistant", "content": _FAILED_LIST}]
    assert ground_plan(ask, [("list_jobs", {})], hist) == [("get_job", {"job_id": "0002aaaaaaaaaaaaaaaaaaaa"})]


def test_fix_it_without_a_failed_job_on_screen_is_left_alone():
    hist = [{"role": "assistant", "content": "You have 4 saved connector(s)."}]
    assert ground_plan("how do I fix it?", [("explain_product", {})], hist) == [("explain_product", {})]


def test_switching_template_while_a_transform_is_staged():
    hist = [{"role": "assistant", "content": "Create transform “Dedupe Keep Latest” — press Confirm."}]
    assert ground_plan("actually make it scd2 history instead", [("list_jobs", {})], hist) == [
        ("create_transform_recipe", {"template_key": "scd2-history"})
    ]
    assert ground_plan("actually make it scd2 history instead", [("list_jobs", {})], None) == [("list_jobs", {})]


def test_status_typos_are_in_the_workspace_vocabulary():
    from src.ai.copilot.spelling import correct_workspace_typos

    assert "failed" in correct_workspace_typos("hw many jobs faild")


def test_unnamed_connector_read_asks_which_instead_of_not_found(monkeypatch):
    from src.ai.copilot import schema_tools

    monkeypatch.setattr(schema_tools, "_connector_dict", lambda *a, **k: None)
    monkeypatch.setattr(schema_tools, "_only_saved_connector", lambda: None)
    _, unnamed = schema_tools._safe_connector("", "", "sample_connector_object")
    _, named = schema_tools._safe_connector("", "Nope DB", "sample_connector_object")
    assert unnamed.error.startswith("Which saved connector")
    assert named.error.startswith("Connector not found")


@pytest.mark.parametrize("ask", ["which jobs failed", "how many jobs failed"])
def test_failed_jobs_ask_reads_the_ledger_without_the_docs_lead(ask):
    planned = [("explain_product", {"query": ask}), ("list_jobs", {"limit": 10})]
    assert ground_plan(ask, planned, None) == [("list_jobs", {"limit": 25})]


def test_what_a_failed_job_means_keeps_the_docs():
    planned = [("explain_product", {"query": "q"}), ("list_jobs", {"limit": 10})]
    assert ground_plan("what does a failed job mean", planned, None) == planned


def test_a_cancelled_job_is_not_counted_as_failed():
    out = {
        "jobs": [
            {"id": "a", "status": "failed", "source": "s", "destination": "d"},
            {"id": "b", "status": "cancelled", "source": "s", "destination": "d"},
        ],
        "total": 2,
        "status_counts": {"failed": 1, "cancelled": 1},
    }
    text = narrate_jobs(out, "which jobs failed")
    assert text.startswith("**1** of your **2** job(s) failed")
    assert "cancelled" not in text


def test_a_why_did_it_fail_question_keeps_the_failure_docs():
    planned = [("explain_product", {"query": "q"}), ("list_jobs", {"limit": 10})]
    ask = "my connector test passed but the transfer failed, why"
    assert ground_plan(ask, planned, None) == planned
