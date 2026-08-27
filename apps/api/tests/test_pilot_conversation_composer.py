"""Conversational Pilot — dialogue acts, briefing facts, no invented counts.

Grounded in the operator bar: RAG does not invent a second confidence, and
the composer never invents a job or connector count the stores did not return.
"""

from __future__ import annotations

from src.ai.copilot.conversation_composer import (
    compose_briefing,
    compose_general,
    compose_greeting,
    compose_history_turn,
    compose_next_action,
    explain_simpler,
    summarize_text,
)
from src.ai.copilot.dialogue_acts import classify_dialogue_act
from src.ai.copilot.tool_permissions import TOOL_PERMISSIONS
from src.ai.copilot.tools import TOOL_DEFINITIONS, infer_tools_from_message
from src.ai.copilot.workspace_briefing import collect_workspace_briefing


def test_short_history_does_not_force_workspace_for_off_topic():
    hist = [{"role": "assistant", "content": "You have **2** jobs."}]
    assert classify_dialogue_act("what is the capital of France", history=hist) == "general"
    assert classify_dialogue_act("and by region", history=hist) == "workspace"
    assert classify_dialogue_act("only paid ones", history=hist) == "workspace"


def test_turn_text_accepts_content_or_text():
    from src.ai.copilot.dialogue_acts import last_assistant_text, turn_text
    from src.ai.copilot.followup import last_assistant_content

    assert turn_text({"role": "assistant", "text": "You have **2** jobs."}) == "You have **2** jobs."
    assert turn_text({"role": "assistant", "content": "ok"}) == "ok"
    hist = [{"role": "assistant", "text": "Job `abc` failed."}]
    assert last_assistant_text(hist) == "Job `abc` failed."
    assert last_assistant_content(hist) == "Job `abc` failed."


def test_dialogue_acts_cover_copilot_turns():
    assert classify_dialogue_act("hi") == "greeting"
    assert classify_dialogue_act("hello there") == "greeting"
    assert classify_dialogue_act("thanks") == "thanks"
    assert classify_dialogue_act("give me a workspace briefing") == "briefing"
    assert classify_dialogue_act("what's going on") == "briefing"
    assert classify_dialogue_act("catch me up") == "briefing"
    assert classify_dialogue_act("how do I cook rice") == "general"
    assert classify_dialogue_act("what is the capital of France") == "general"
    assert classify_dialogue_act("show my jobs") == "workspace"
    assert classify_dialogue_act("plan a transfer of orders") == "workspace"


def test_tell_me_everything_about_a_table_is_not_a_sitrep():
    # "tell me everything about airports" is a dataset ask, not a workspace briefing.
    assert classify_dialogue_act("tell me everything about airports") != "briefing"
    assert classify_dialogue_act("tell me everything") == "briefing"


def test_summarize_that_needs_history():
    assert classify_dialogue_act("summarize that") != "summarize_last"
    hist = [{"role": "assistant", "content": "You have **2** jobs. One failed."}]
    assert classify_dialogue_act("summarize that", history=hist) == "summarize_last"
    assert classify_dialogue_act("explain that more simply", history=hist) == "explain_simpler"
    assert classify_dialogue_act("what should I do next") == "next_action"


def test_compose_greeting_empty_workspace_does_not_invent_counts():
    text = compose_greeting({"connectors": [], "recent_jobs": []})
    assert "Datawrap Pilot" in text
    assert "0" not in text or "still empty" in text.lower() or "Start anywhere" in text
    # No fake inventory.
    assert "650" not in text
    assert "99%" not in text


def test_compose_greeting_uses_only_ctx_counts():
    text = compose_greeting({
        "connectors": [{"name": "Local PG"}, {"name": "Warehouse"}],
        "recent_jobs": [
            {"status": "completed"},
            {"status": "failed"},
        ],
    })
    assert "**2**" in text
    assert "failed" in text.lower()
    assert "Local PG" not in text  # greeting counts, does not invent names unless we add them


def test_compose_briefing_empty_and_live_facts():
    empty = compose_briefing({"empty_workspace": True})
    assert "empty" in empty.lower()
    assert "3 connector" not in empty

    live = compose_briefing({
        "empty_workspace": False,
        "connector_count": 2,
        "connectors_passed": 1,
        "connectors_failed": 1,
        "connectors_untested": 0,
        "connector_names": ["Sales PG", "Warehouse"],
        "job_count": 4,
        "jobs_ok": 3,
        "jobs_failed": 1,
        "jobs_running": 0,
        "latest_failed_job": "`abc123` orders → dest",
        "schedule_count": 1,
        "schedules_enabled": 1,
        "schedules_parked": 0,
        "next_schedule": "",
        "parked_names": [],
        "contract_count": 0,
        "contracts_unsigned": 0,
        "attention": ["1 failed transfer job(s)"],
    })
    assert "**2**" in live
    assert "Sales PG" in live
    assert "`abc123`" in live
    assert "1 failed transfer" in live
    # Composer must not grow a count the facts did not give.
    assert "**9**" not in live
    assert "650" not in live


def test_compose_general_refuses_guesswork():
    text = compose_general("how do I cook rice", {"connectors": [], "recent_jobs": []})
    assert "will not answer it from guesswork" in text
    assert "Settings → AI" in text
    assert "rice recipe" not in text.lower()
    assert "boil" not in text.lower()


def test_summarize_and_explain_are_extractive():
    src = (
        "Job **`j1`** failed. Rows processed: 12. "
        "Error: destination table EMPLOYEE_DB.tree rejected the population."
    )
    short = summarize_text(src)
    assert "Short version" in short
    assert "j1" in short or "failed" in short.lower()
    # No new warehouse claim.
    assert "EMPLOYEE_DB" in short or "failed" in short.lower()
    plain = explain_simpler(src)
    assert "plain" in plain.lower()
    assert "I only report" in plain


def test_next_action_prefers_confirm_then_attention():
    pending = compose_next_action(pending_labels=["Start transfer"])
    assert "Confirm" in pending
    assert "Start transfer" in pending
    attn = compose_next_action(facts={"attention": ["2 failed transfer job(s)"]})
    assert "2 failed" in attn
    idle = compose_next_action()
    assert "briefing" in idle.lower()


def test_history_turn_does_not_invent_facts():
    hist = [
        {"role": "user", "content": "show my jobs"},
        {"role": "assistant", "content": "You have **3** jobs. **1** failed (`job_aa`)."},
    ]
    resp = compose_history_turn("summarize_last", history=hist, message="summarize that")
    assert resp.method == "pilot_conversation"
    assert "3" in resp.answer
    assert "99%" not in resp.answer
    thanks = compose_history_turn("thanks", history=hist, message="thanks")
    assert "welcome" in thanks.answer.lower()


def test_infer_tools_briefing_and_general():
    names = [n for n, _ in infer_tools_from_message("give me a workspace briefing")]
    assert names == ["brief_workspace"]

    names = [n for n, _ in infer_tools_from_message("what's going on in my workspace")]
    assert names == ["brief_workspace"]

    names = [n for n, _ in infer_tools_from_message("catch me up")]
    assert names == ["brief_workspace"]

    # Inventory verbs keep the existing router.
    names = [n for n, _ in infer_tools_from_message("show my jobs")]
    assert "list_jobs" in names
    assert "brief_workspace" not in names

    # Off-topic must not become product RAG.
    names = [n for n, _ in infer_tools_from_message("how do I cook rice tonight")]
    assert "search_knowledge" not in names
    assert names == []

    names = [n for n, _ in infer_tools_from_message("what is the capital of France")]
    assert "search_knowledge" not in names


def test_brief_workspace_is_permissioned_like_other_reads():
    assert "brief_workspace" in {d["name"] for d in TOOL_DEFINITIONS}
    perm, effect = TOOL_PERMISSIONS["brief_workspace"]
    assert effect == "read"
    assert "workspace" in perm or perm.endswith("read")


def test_collect_workspace_briefing_never_invents_when_stores_fail(monkeypatch):
    import src.ai.copilot.workspace_briefing as wb

    monkeypatch.setattr(wb, "_load_connectors", lambda _ws: [])
    monkeypatch.setattr(wb, "_load_jobs", lambda _ws: [])
    monkeypatch.setattr(wb, "_load_schedules", lambda _ws: [])
    monkeypatch.setattr(wb, "_load_contracts", lambda _ws: [])
    facts = collect_workspace_briefing()
    assert facts["connector_count"] == 0
    assert facts["job_count"] == 0
    assert facts["empty_workspace"] is True
    assert facts["attention"] == []
    assert facts["latest_failed_job"] == ""


def test_collect_workspace_briefing_counts_only_loaded_rows(monkeypatch):
    import src.ai.copilot.workspace_briefing as wb

    monkeypatch.setattr(
        wb,
        "_load_connectors",
        lambda _ws: [
            {"name": "A", "last_test_ok": True},
            {"name": "B", "last_test_ok": False},
        ],
    )
    monkeypatch.setattr(
        wb,
        "_load_jobs",
        lambda _ws: [
            {"id": "j1", "status": "failed", "source": "orders", "destination": "dest"},
            {"id": "j2", "status": "completed"},
        ],
    )
    monkeypatch.setattr(
        wb,
        "_load_schedules",
        lambda _ws: [{"name": "Nightly", "enabled": True, "needs_approval": True, "approval_finding": "x"}],
    )
    monkeypatch.setattr(wb, "_load_contracts", lambda _ws: [])
    facts = collect_workspace_briefing()
    assert facts["connector_count"] == 2
    assert facts["connectors_failed"] == 1
    assert facts["job_count"] == 2
    assert facts["jobs_failed"] == 1
    assert facts["schedules_parked"] == 1
    assert "failed transfer" in " ".join(facts["attention"])
    assert "approval" in " ".join(facts["attention"])
    # Names come from the rows we loaded — not a catalog tile count.
    assert facts["connector_names"] == ["A", "B"]


def test_greeting_and_history_through_agent():
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    hi = agent.chat("hi", history=[], data_context=None)
    assert hi.method == "greeting"
    assert "Datawrap Pilot" in hi.answer
    assert "semantic type:" not in hi.answer.lower()

    hist = [
        {"role": "user", "content": "show my jobs"},
        {"role": "assistant", "content": "You have **2** recent jobs. None failed."},
    ]
    recap = agent.chat("summarize that", history=hist, data_context=None)
    assert recap.method == "pilot_conversation"
    assert "Short version" in recap.answer
    assert "2" in recap.answer

    rice = agent.chat("how do I cook rice tonight please", history=[], data_context=None)
    assert rice.method == "pilot_conversation"
    assert rice.confidence == 0.2
    assert "will not answer it from guesswork" in rice.answer
    assert "Settings → AI" in rice.answer
    assert "boil water" not in rice.answer.lower()


def test_briefing_through_agent_uses_tool_not_faq(monkeypatch):
    from src.ai.copilot.pilot_agent import DataPilotAgent
    import src.ai.copilot.workspace_briefing as wb

    monkeypatch.setattr(
        wb,
        "_load_connectors",
        lambda _ws: [{"name": "Sales PG", "last_test_ok": True}],
    )
    monkeypatch.setattr(
        wb,
        "_load_jobs",
        lambda _ws: [{"id": "job_deadbeef", "status": "failed", "source": "orders", "destination": "dest"}],
    )
    monkeypatch.setattr(wb, "_load_schedules", lambda _ws: [])
    monkeypatch.setattr(wb, "_load_contracts", lambda _ws: [])

    agent = DataPilotAgent()
    resp = agent.chat("give me a workspace briefing", history=[], data_context=None)
    used = [t.get("name") for t in (resp.tools_used or [])]
    assert "brief_workspace" in used
    assert "Sales PG" in resp.answer
    assert "job_deadbee" in resp.answer or "failed" in resp.answer.lower()
    assert "650" not in resp.answer
    assert "99%" not in resp.answer
