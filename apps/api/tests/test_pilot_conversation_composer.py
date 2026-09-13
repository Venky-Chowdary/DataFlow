"""Conversational Pilot — dialogue acts, briefing facts, no invented counts.

Grounded in the operator bar: RAG does not invent a second confidence, and
the composer never invents a job or connector count the stores did not return.
"""

from __future__ import annotations

from src.ai.copilot.conversation_composer import (
    compose_briefing,
    compose_calendar,
    compose_create_connection_capability,
    compose_general,
    compose_greeting,
    compose_history_turn,
    compose_next_action,
    compose_route_plan_capability,
    explain_simpler,
    summarize_text,
)
from src.ai.copilot.dialogue_acts import (
    classify_dialogue_act,
    is_calendar_question,
    is_create_connection_capability_ask,
    is_route_plan_capability_paste,
    is_schedule_health_question,
)
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
    assert classify_dialogue_act("what's the status") == "briefing"
    assert classify_dialogue_act("what is the status of my workspace") == "briefing"
    # "what is the status of my last transfer" matched the sitrep pattern on
    # "what is the status" and opened with the workspace briefing instead of
    # the last job.
    assert classify_dialogue_act("what is the status of my last transfer") == "workspace"


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


def test_calendar_and_schedule_health_are_not_help_articles():
    assert is_calendar_question("date today")
    assert is_calendar_question("what is todays date")
    assert is_calendar_question("what's today's date")
    assert is_calendar_question("what is the date today ?")
    assert is_calendar_question("what is the date today")
    assert is_calendar_question("what's the date")
    assert is_calendar_question("current date")
    assert not is_calendar_question("what is a date column")
    assert not is_calendar_question("date format in postgres")
    assert not is_calendar_question("Snowflake DATE type")
    assert is_schedule_health_question("is schedules working")
    assert is_schedule_health_question("why schedules are not working")
    assert is_schedule_health_question("are my pipelines working?")
    assert not is_schedule_health_question("what happens if I delete a CDC schedule")
    assert not is_schedule_health_question("how do I schedule a transfer")
    assert is_create_connection_capability_ask("can you create connection")
    assert is_create_connection_capability_ask("can you create a connector")
    assert not is_create_connection_capability_ask(
        "create a postgres connector at db.acme.com"
    )
    assert is_route_plan_capability_paste(
        "Plan source→destination routes and sync modes"
    )


def test_compose_calendar_speaks_utc_date_not_column_types():
    text = compose_calendar({"connectors": [{"name": "A"}], "recent_jobs": []})
    assert "UTC" in text
    assert "DATE" not in text
    assert "transform" not in text.lower()
    assert "1" in text or "connector" in text.lower()


def test_schedule_setup_is_a_capability_not_a_yaml_export():
    from src.ai.copilot.dialogue_acts import is_schedule_setup_capability_ask
    from src.ai.copilot.conversation_composer import compose_schedule_setup_capability
    from src.ai.copilot.tools import infer_tools_from_message

    assert is_schedule_setup_capability_ask("can you setup schedule")
    assert is_schedule_setup_capability_ask("can you set up a pipeline")
    assert is_schedule_setup_capability_ask("could you create a schedule")
    assert not is_schedule_setup_capability_ask("how do I export a schedule as YAML")
    assert not is_schedule_setup_capability_ask("show my pipelines")

    text = compose_schedule_setup_capability({"pipeline_count": 2})
    assert "Pipelines" in text
    assert "YAML" not in text
    assert "Export YAML" not in text
    assert "2" in text
    # No RAG plan, so GitOps export cannot be retrieved for this turn.
    assert infer_tools_from_message("can you setup schedule") == []


def test_connector_health_filter_reads_the_last_test():
    from src.ai.copilot.tools import connector_health_filter

    assert connector_health_filter("get me the passed connectors") == "passed"
    assert connector_health_filter("which connectors are failing") == "failed"
    assert connector_health_filter("untested connectors") == "untested"
    assert connector_health_filter("show my connectors") == "any"
    # Not a connector ask at all.
    assert connector_health_filter("did the job pass") == "any"


def test_pasted_connector_row_is_a_named_connector():
    from src.ai.copilot.tools import split_pasted_connector_row

    name, rest = split_pasted_connector_row(
        "Snowflake_venky (snowflake) → EMPLOYEE_DB how many tables there"
    )
    assert name == "Snowflake_venky"
    assert rest == "how many tables there"

    bullet, ask = split_pasted_connector_row(
        "• MySQL (mysql) → railway list tables"
    )
    assert bullet == "MySQL"
    assert ask == "list tables"

    # A plain sentence is not an inventory row.
    assert split_pasted_connector_row("how many tables in orders") == ("", "")


def test_compose_create_connection_is_confirm_gated():
    text = compose_create_connection_capability({
        "connectors": [{"name": "SnowFlake"}, {"name": "MySQL"}],
    })
    assert "Confirm" in text
    assert "SnowFlake" in text
    assert "Click New connection" not in text


def test_compose_route_plan_asks_for_named_connectors():
    text = compose_route_plan_capability()
    assert "source" in text.lower()
    assert "Confirm" in text


def test_compose_greeting_empty_workspace_does_not_invent_counts():
    text = compose_greeting({"connectors": [], "recent_jobs": []})
    assert "Datawrap Pilot" in text
    assert "not a general chatbot" in text.lower()
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


def test_summarize_help_dump_stays_grammatical():
    dump = (
        "**Procedure: inspect quarantine** — Quarantine means bad values were "
        "isolated with column, value, and reason — never silently dropped.\n"
        "Open the job\n"
        "Look for Quarantine status or a non-zero rejected count in the stats strip.\n"
        "Where: Operations → Jobs → select run\n"
        "Source: Job Theater & reconciliation → Procedure: inspect quarantine (Help)\n\n"
        "— Optional narration polish is off (no Ollama/cloud provider ready)."
    )
    short = summarize_text(dump)
    assert "never silently dropped" in short
    assert "Open the job Look for" not in short
    assert "Where:" not in short
    assert "Optional narration" not in short
    plain = explain_simpler(dump)
    assert "documented meaning" in plain
    assert "I only report" not in plain


def test_summarize_preflight_keeps_named_gates():
    src = (
        "**Core gates (before write)** — These checks run in Transfer Studio Validate. "
        "Open any failing card for the rule text and remediation. "
        "G1 Source readable — source connects and rows can be read. "
        "G2 Destination write access — destination is reachable with write/create privilege. "
        "G3 Schema contract — source and target schemas are compatible."
    )
    short = summarize_text(src)
    assert "G1 " in short
    assert "These checks run" in short


def test_next_action_ignores_help_confirm_wording():
    idle = compose_next_action(
        last_answer="**Append** adds rows. Full refresh overwrite replaces the destination (Confirm required)."
    )
    assert "Confirm card" not in idle
    assert "briefing" in idle.lower()


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

    names = [n for n, _ in infer_tools_from_message("date today")]
    assert "explain_product" not in names
    assert names == []

    names = [n for n, _ in infer_tools_from_message("what is todays date")]
    assert "explain_product" not in names

    names = [n for n, _ in infer_tools_from_message("is schedules working")]
    assert "list_schedules" in names
    assert "explain_product" not in names

    names = [n for n, _ in infer_tools_from_message("why schedules are not working")]
    assert "list_schedules" in names
    assert "explain_product" not in names

    names = [n for n, _ in infer_tools_from_message("can you create connection")]
    assert "explain_product" not in names
    assert "create_connector" not in names

    names = [n for n, _ in infer_tools_from_message("what happens if I delete a CDC schedule")]
    assert "explain_product" in names
    assert "list_schedules" not in names

    # Definitional FAQ — Help, not hashed-upload inventory.
    names = [n for n, _ in infer_tools_from_message("what does quarantine mean")]
    assert "explain_product" in names
    assert "list_datasets" not in names

    names = [n for n, _ in infer_tools_from_message("what are the preflight gates")]
    assert "explain_product" in names or "profile_quality_rules" in names
    assert "list_datasets" not in names

    names = [n for n, _ in infer_tools_from_message("who is allowed to start a transfer")]
    assert "explain_product" in names
    assert "start_transfer_studio" not in names

    names = [
        n
        for n, _ in infer_tools_from_message(
            "if I run the same CDC change twice is it safe"
        )
    ]
    assert "explain_product" in names
    assert "list_jobs" not in names

    names = [n for n, _ in infer_tools_from_message("can a viewer start a transfer")]
    assert "explain_product" in names
    assert "start_transfer_studio" not in names

    names = [n for n, _ in infer_tools_from_message("what plugin does postgres CDC use")]
    assert "recommend_sync_mode" not in names
    assert "explain_product" in names or "search_knowledge" in names

    names = [n for n, _ in infer_tools_from_message("does a green connector test skip validate")]
    assert "explain_product" in names
    assert "list_connectors" not in names

    names = [n for n, _ in infer_tools_from_message("can a viewer export YAML")]
    assert "explain_product" in names or names == []
    from src.ai.copilot.tools import _looks_like_unsupported_mutation

    assert not _looks_like_unsupported_mutation("can a viewer export yaml")
    assert not _looks_like_unsupported_mutation(
        "what happens if I delete a CDC schedule"
    )
    assert not _looks_like_unsupported_mutation("who can export audit logs")
    assert not _looks_like_unsupported_mutation("can I export audit logs as CSV")
    names = [n for n, _ in infer_tools_from_message("who can export audit logs")]
    assert "search_knowledge" in names or "explain_product" in names

    names = [
        n
        for n, _ in infer_tools_from_message(
            "do I need REPLICA IDENTITY FULL for postgres CDC"
        )
    ]
    assert "recommend_sync_mode" not in names
    names = [
        n
        for n, _ in infer_tools_from_message(
            "do I need binlog_format ROW for mysql CDC"
        )
    ]
    assert "recommend_sync_mode" not in names

    names = [
        n
        for n, _ in infer_tools_from_message(
            "gotta have logical wal for pg cdc right?"
        )
    ]
    assert "explain_product" in names
    assert "recommend_sync_mode" not in names

    from src.ai.copilot.followup import resolve_knowledge_engine_followup

    assert (
        resolve_knowledge_engine_followup(
            "and for mongo?",
            [
                {
                    "role": "assistant",
                    "content": "Postgres CDC uses the pgoutput plugin.",
                }
            ],
        )
        == "does Mongo CDC need change-stream pre-images"
    )
    assert (
        resolve_knowledge_engine_followup(
            "same question but for mysql",
            [
                {
                    "role": "assistant",
                    "content": "Yes — Postgres CDC needs wal_level=logical.",
                }
            ],
        )
        == "do I need binlog_format ROW for mysql CDC"
    )
    assert (
        resolve_knowledge_engine_followup(
            "what about deletes?",
            [
                {
                    "role": "assistant",
                    "content": "Yes — Postgres CDC needs wal_level=logical.",
                }
            ],
        )
        == "what happens to a delete in CDC"
    )
    assert (
        resolve_knowledge_engine_followup(
            "do I need that?",
            [
                {
                    "role": "assistant",
                    "content": "Yes — Postgres CDC needs wal_level=logical.",
                }
            ],
        )
        == "do I need wal_level logical for postgres CDC"
    )


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
    assert facts["connector_names"] == ["A", "B"]


def test_collect_workspace_briefing_uses_whole_history_job_counts(monkeypatch):
    import src.ai.copilot.workspace_briefing as wb

    monkeypatch.setattr(
        wb,
        "_load_connectors",
        lambda _ws: [{"name": "A", "last_test_ok": True}],
    )
    monkeypatch.setattr(
        wb,
        "_load_jobs",
        lambda _ws: (
            [{"id": "j1", "status": "failed", "source": "orders", "destination": "dest"}],
            {
                "total": 84,
                "by_status": {"failed": 12, "completed": 70, "running": 2},
            },
        ),
    )
    monkeypatch.setattr(wb, "_load_schedules", lambda _ws: [])
    monkeypatch.setattr(wb, "_load_contracts", lambda _ws: [])
    facts = collect_workspace_briefing(workspace_id="ws-1")
    assert facts["job_count"] == 84
    assert facts["jobs_failed"] == 12
    assert facts["jobs_ok"] == 70
    assert facts["jobs_running"] == 2


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


def test_hybrid_openai_cannot_replace_clock_with_date_types(monkeypatch):
    """The live Hybrid failure: OpenAI said it cannot give the date, then dumped DATE types."""
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot import pilot_agent as pa

    monkeypatch.setattr(pa, "_resolve_pilot_engine", lambda: "hybrid")
    raced = []

    def _openai_lie(message, history, system, data_context):
        raced.append(message)
        return CopilotResponse(
            answer=(
                "I can’t provide the current date, but I can help with data-related "
                "questions. In Datawrap, a date column can be created in PostgreSQL: DATE."
            ),
            intent="product_help",
            confidence=0.95,
            method="openai_agent",
            tools_used=[{"name": "explain_product", "success": True, "summary": "DATE types"}],
            sources=[{"title": "Type fidelity & coercion"}],
        )

    agent = pa.DataPilotAgent()
    monkeypatch.setattr(agent, "_first_available_native_agent", lambda: _openai_lie)
    resp = agent.chat(
        "what is the date today ?",
        history=[],
        data_context={"pilot_session_id": "clock-hybrid"},
    )
    assert raced == []
    assert "UTC" in resp.answer
    assert "PostgreSQL" not in resp.answer
    assert "can't provide" not in resp.answer.lower()
    assert "cannot provide" not in resp.answer.lower()
    assert "DATE" not in resp.answer


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


def test_an_inventory_read_is_not_prefixed_with_filler():
    """The tool prose already opens with the finding.

    "Here's what I found." cost "how many connectors do I have" and "list my
    connectors" their first line: the operator read four words of filler
    before "You have **2 saved connector(s)**".
    """
    from src.ai.copilot.conversation_composer import weave_tool_answer

    body = "You have **2 saved connector(s)**.\n\n• **Demo Orders** (sqlite)"
    woven = weave_tool_answer("list my connectors", [body], act="workspace")
    assert woven.startswith("You have **2 saved connector(s)**")
    assert "Here's what I found" not in woven


def test_a_count_of_my_own_inventory_reads_the_inventory_tool():
    """"How many X do I have" is a workspace read for every inventory noun.

    Only jobs and connectors had hand-written branches, so "how many schedules
    do i have" fell through to the documentation and answered a count with the
    CDC-delete-drops-the-replication-slot passage.
    """
    from src.ai.copilot.tools import infer_tools_from_message

    expected = {
        "how many schedules do i have": "list_schedules",
        "how many pipelines do i have": "list_schedules",
        "how many connectors do i have": "list_connectors",
        "how many jobs do i have": "list_jobs",
        "how many datasets do i have": "list_datasets",
        "count of my connectors": "list_connectors",
    }
    for message, tool in expected.items():
        planned = [n for n, _ in infer_tools_from_message(message)]
        assert planned == [tool], f"{message!r} planned {planned}"


def test_a_catalog_count_stays_with_the_documentation():
    """"How many connectors do you support" is about the catalog, not my workspace."""
    from src.ai.copilot.tools import infer_tools_from_message

    for message in (
        "how many connectors do you support",
        "how many connectors are live",
        "how many engines are transfer ready",
    ):
        planned = [n for n, _ in infer_tools_from_message(message)]
        assert "explain_product" in planned, f"{message!r} planned {planned}"


def test_rows_moved_is_job_telemetry_not_a_table_to_aggregate():
    """The rows a transfer wrote live on the job, not in a table called yesterday."""
    from src.ai.copilot.tools import infer_tools_from_message

    planned = [n for n, _ in infer_tools_from_message("how many rows did we move yesterday")]
    assert planned == ["list_jobs"]


def test_passive_schedule_wording_asks_for_the_procedure():
    """"How are pipelines scheduled" wants the steps, not the live cadence list."""
    from src.ai.copilot.dialogue_acts import is_schedule_health_question
    from src.ai.copilot.tools import infer_tools_from_message
    from src.ai.rag.query_analysis import classify_ask

    assert classify_ask("how are pipelines scheduled") == "procedure"
    assert not is_schedule_health_question("how are pipelines scheduled")
    assert [n for n, _ in infer_tools_from_message("how are pipelines scheduled")] == [
        "explain_product"
    ]
    # Health wording still reaches the live list.
    assert is_schedule_health_question("are my pipelines running")
    assert "list_schedules" in [
        n for n, _ in infer_tools_from_message("are my pipelines running")
    ]


def test_a_stative_participle_is_not_a_procedure():
    """"How are arrays supported" wants type fidelity, not a wizard step."""
    from src.ai.rag.query_analysis import classify_ask

    assert classify_ask("how are arrays supported on mysql") != "procedure"
    assert classify_ask("how are jobs different from pipelines") != "procedure"


def test_a_health_word_in_a_how_to_is_not_a_bucket_filter():
    """A bucket is a subset of the saved list, not documentation vocabulary."""
    from src.ai.copilot.tools import connector_health_filter, infer_tools_from_message

    assert connector_health_filter("which of my connectors are broken") == "failed"
    assert connector_health_filter("list failed connectors") == "failed"
    assert connector_health_filter("which connectors are healthy") == "passed"
    # Singular, and a diagnosis: the documented preflight answer must survive.
    assert connector_health_filter(
        "my connector test passed but the transfer failed, why"
    ) == "any"
    assert connector_health_filter("how do I fix a broken connector") == "any"
    assert connector_health_filter("what happens when a connector test fails") == "any"
    assert connector_health_filter(
        "is a green test on connectors enough to skip validation"
    ) == "any"
    # The bucket read answers on its own — no Connectors page tour in front.
    assert [
        n for n, _ in infer_tools_from_message("which of my connectors are broken")
    ] == ["list_connectors"]


def test_a_named_dataset_outranks_the_documentation():
    """"Tell me about the employees dataset" is a profile, not the Lineage card."""
    from src.ai.copilot.tools import dataset_subject, infer_tools_from_message

    assert dataset_subject("tell me about the employees dataset") == "employees"
    assert dataset_subject("what is in the orders csv") == "orders"
    assert dataset_subject("analyze my HR upload") == "HR"
    # Vocabulary overlap is not a subject.
    assert dataset_subject("how does Datawrap mask employee PII") is None
    assert dataset_subject("analyze that") is None
    assert dataset_subject("what are the preflight gates") is None

    planned = [n for n, _ in infer_tools_from_message("tell me about the employees dataset")]
    assert planned == ["analyze_dataset"]


def test_a_pasted_connector_row_routes_against_the_named_connector():
    """Operators paste our own bullet back, bold and arrow included.

    ``Snowflake_venky (snowflake) → EMPLOYEE_DB how many tables there`` was
    answered with "that is outside what the Datawrap documentation covers",
    which hid the real reason: no connector by that name is saved.
    """
    from src.ai.copilot.tools import infer_tools_from_message, split_pasted_connector_row

    assert split_pasted_connector_row(
        "Snowflake_venky (snowflake) → EMPLOYEE_DB how many tables there"
    ) == ("Snowflake_venky", "how many tables there")
    assert split_pasted_connector_row(
        "• **Demo Orders** (sqlite) → /data/demo.db list the tables"
    ) == ("Demo Orders", "list the tables")
    # A row with no trailing question is just a row.
    assert split_pasted_connector_row("Snowflake_venky (snowflake) → EMPLOYEE_DB") == ("", "")
    # The parenthetical has to be a real driver, or the shape is a coincidence.
    assert split_pasted_connector_row(
        "Ghost Thing (notadriver) → X what is upsert"
    ) == ("", "")

    planned = [
        n
        for n, _ in infer_tools_from_message(
            "• **Snowflake_venky** (snowflake) → EMPLOYEE_DB how many tables there"
        )
    ]
    assert "list_connector_objects" in planned
    assert "explain_product" not in planned


def test_hybrid_never_replaces_a_grounded_workspace_answer(monkeypatch):
    """A saved key rewords; it does not get to answer.

    The whole point of Hybrid is third-party wording over first-party evidence.
    A native tool loop that scores well on fluency must never win the turn away
    from a local tool that actually read the workspace, or the operator reads
    "you may have around 5 connectors" about a workspace holding two.
    """
    import src.ai.copilot.pilot_agent as pa
    from src.ai.copilot.agent import CopilotResponse

    monkeypatch.setattr(pa, "_resolve_pilot_engine", lambda: "hybrid")
    raced: list[str] = []

    def _confident_stranger(message, history, system, data_context):
        raced.append(message)
        return CopilotResponse(
            answer=(
                "I don't have access to your workspace. In general a pipeline is "
                "scheduled with cron. You may have around 5 connectors."
            ),
            intent="product_help",
            confidence=0.99,
            method="openai_agent",
            tools_used=[{"name": "explain_product", "success": True, "summary": "generic"}],
            sources=[{"title": "General knowledge"}],
        )

    agent = pa.DataPilotAgent()
    monkeypatch.setattr(
        agent, "_first_available_native_agent", lambda: _confident_stranger
    )

    for i, ask in enumerate([
        "what is the date today ?",
        "how many schedules do i have",
        "how many connectors do i have",
        "which of my connectors are broken",
        "how many rows did we move yesterday",
        "can you setup schedule",
    ]):
        resp = agent.chat(
            ask, history=[], data_context={"pilot_session_id": f"hybrid-guard-{i}"}
        )
        answer = resp.answer or ""
        assert "around 5 connectors" not in answer, ask
        assert "don't have access to your workspace" not in answer, ask

    assert raced == []


def test_a_generic_route_sketch_leads_with_the_next_action_and_real_gates():
    """"Move data from mysql to postgres" names engine families, not connectors.

    ``preflight.gates`` has never existed in this repo, so the gate list was
    always empty and the sketch rendered as a bare "**Standard gate sequence**:"
    heading in front of the only sentence the operator could act on.
    """
    from services.preflight_rules import PREFLIGHT_GATE_RULES
    from src.ai.copilot.tools import get_pilot_tools

    result = get_pilot_tools().execute(
        "plan_transfer_route", {"source": "mysql", "destination": "postgres"}
    )
    assert result.success
    gates = result.output.get("required_gates") or []
    assert gates, "the sketch must name the gates Validate actually enforces"
    assert set(gates) == set(PREFLIGHT_GATE_RULES)
    assert result.output["note"].startswith("Name two saved connectors")


def test_a_passive_mechanism_question_is_not_a_procedure():
    """The passive procedure frame is scoped to objects an operator acts on.

    Read wider, it made "how are bad rows quarantined" and "how is a schema
    mapped" lead on whichever passage had the strongest imperative — the
    type_locked card in both cases — instead of on quarantine and on semantic
    mapping.
    """
    from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
    from src.ai.rag.query_analysis import classify_ask

    def lead(question: str) -> str:
        body = compose_product_answer(retrieve_product_answer(question, limit=4)) or ""
        return " ".join(body.split()).lower()

    assert classify_ask("how are pipelines scheduled") == "procedure"
    assert classify_ask("how are bad rows quarantined") != "procedure"
    assert classify_ask("how is a schema mapped") != "procedure"
    assert classify_ask("how is cdc resumed after a restart") != "procedure"

    assert "quarantine" in lead("how are bad rows quarantined")
    assert "semantic column mapping" in lead("how is a schema mapped")
    assert "type_locked" not in lead("how are bad rows quarantined").split(".")[0]


def test_parked_and_pending_work_reaches_the_briefing():
    """The briefing already reports parked pipelines and unsigned contracts."""
    from src.ai.copilot.dialogue_acts import classify_dialogue_act
    from src.ai.copilot.tools import infer_tools_from_message

    for ask in (
        "is anything parked",
        "what is parked",
        "is anything waiting on approval",
        "is anything blocked",
    ):
        assert classify_dialogue_act(ask) == "briefing", ask
        assert [n for n, _ in infer_tools_from_message(ask)] == ["brief_workspace"], ask


def test_a_spoken_column_phrase_resolves_to_the_real_column():
    """"sum the amount column" answered "Column 'amount column' is not in orders".

    The resolver only ever compared the whole spoken phrase, so every noun an
    operator says *around* a column name broke the lookup. Content words are
    tried individually and the answer is accepted only when they agree, so a
    genuinely ambiguous phrase still fails closed instead of picking a column.
    """
    from src.ai.copilot.aggregate_tools import resolve_name

    orders = ["id", "region", "amount", "placed_at"]
    assert resolve_name("amount column", orders) == "amount"
    assert resolve_name("the amount column", orders) == "amount"
    assert resolve_name("order amount", orders) == "amount"
    assert resolve_name("distinct region", orders) == "region"
    assert resolve_name("the placed at column", orders) == "placed_at"

    # Unknown columns stay unknown — the honest error is the right answer.
    assert resolve_name("customers", orders) == ""
    assert resolve_name("second", orders) == ""

    # A noise word that is itself a column keeps its vote, so the phrase is
    # ambiguous rather than silently resolved to one of the two.
    assert resolve_name("total", ["total", "amount"]) == "total"
    assert resolve_name("total column", ["total", "amount"]) == "total"
    assert resolve_name("total amount", ["total", "amount"]) == ""
    assert resolve_name("order amount", ["order_id", "amount"]) == ""


def test_throughput_and_limit_questions_are_not_row_counts():
    """"how many rows can you move per second" ran COUNT(*) on the last table.

    It answered "Column 'second' is not in orders" — a product capability
    question reported as a schema error against the operator's data. Real
    temporal grains ("orders per day") must keep grouping.
    """
    from src.ai.copilot.tools import asks_about_product_capacity, infer_tools_from_message

    for ask in (
        "how many rows can you move per second",
        "what is your throughput",
        "how much data can you handle",
    ):
        assert asks_about_product_capacity(ask), ask
        assert "aggregate_data" not in [n for n, _ in infer_tools_from_message(ask)], ask

    assert not asks_about_product_capacity("count orders per day on Demo Orders")
    grouped = dict(infer_tools_from_message("count orders per day on Demo Orders"))
    assert grouped["aggregate_data"]["group_by"] == "day"
    assert "aggregate_data" in dict(infer_tools_from_message("count rows in orders on Demo Orders"))


def test_a_schema_property_of_a_named_table_is_read_live():
    """A primary key and a nullability are facts only the source can answer.

    Both used to retrieve Help: the primary-key ask returned the migration
    certificate aspect list, and the nulls ask returned the coerced-null ledger
    passage. Neither can know the operator's table.
    """
    from src.ai.copilot.tools import infer_tools_from_message

    for ask in (
        "what's the primary key of orders on Demo Orders",
        "are there nulls in orders on Demo Orders",
        "what are the indexes on orders on Demo Orders",
        "what data types does orders have on Demo Orders",
    ):
        plan = dict(infer_tools_from_message(ask))
        assert "introspect_connector_schema" in plan, ask
        assert plan["introspect_connector_schema"]["table"] == "orders", ask

    # The same words without a table are still documentation.
    assert [n for n, _ in infer_tools_from_message("what is a primary key")] == ["explain_product"]


def test_opening_a_transfer_without_endpoints_answers_with_the_next_step():
    """"plan a transfer" retrieved Azure Test Plans; "i want to copy a table"
    retrieved Iceberg merge-on-read; "can you migrate my database" answered
    "Datawrap does not ship Azure Migrate". None of the three names an endpoint,
    so the one useful reply is the sketch that asks for two connectors and a
    table and lists the gates the route will run.
    """
    from src.ai.copilot.dialogue_acts import is_transfer_capability_ask
    from src.ai.copilot.tools import infer_tools_from_message

    for ask in (
        "plan a transfer",
        "i want to copy a table",
        "can you migrate my database",
        "help me move my data",
    ):
        assert is_transfer_capability_ask(ask), ask
        plan = dict(infer_tools_from_message(ask))
        assert plan.get("plan_transfer_route") == {"source": "", "destination": ""}, ask

    # Naming endpoints is past the opening ask — the real planner runs.
    assert not is_transfer_capability_ask("move data from mysql to postgres")
    named = dict(infer_tools_from_message("move data from mysql to postgres"))
    assert named["plan_transfer_route"]["source"] == "mysql"
    assert named["plan_transfer_route"]["destination"] == "postgres"

    # An unparsed route must not echo the whole question back as an endpoint.
    sketch = dict(infer_tools_from_message("how fast can you move data"))
    assert sketch["plan_transfer_route"]["source"] == ""


def test_quarantine_counts_and_run_history_are_job_telemetry():
    """Both were answered from the documentation, which holds neither number.

    "how many rows got quarantined" also hunted for a saved connector named
    "quarantined" and stacked a clarification, a Help passage and the connector
    list into one reply.
    """
    from src.ai.copilot.tools import infer_tools_from_message

    for ask in (
        "how many rows got quarantined",
        "how many rows were rejected",
        "did anything run last night",
        "did anything run",
    ):
        plan = [n for n, _ in infer_tools_from_message(ask)]
        assert "list_jobs" in plan, ask
        assert "aggregate_data" not in plan, ask

    # The nightly *procedure* is still documentation.
    assert [n for n, _ in infer_tools_from_message("how do i schedule a nightly run")] == [
        "explain_product"
    ]


def test_a_misspelled_workspace_noun_still_reaches_the_workspace():
    """"conenctors?" and "shcedules" were refused as undocumented.

    The retry may only upgrade a plan that read nothing live, or one that was
    about to look up the misspelling itself, so a real table name one edit from
    a workspace noun is never rewritten.
    """
    from src.ai.copilot.spelling import correct_workspace_typos
    from src.ai.copilot.tools import plan_tools_tolerant

    assert correct_workspace_typos("conenctors?") == "connectors?"
    assert correct_workspace_typos("shcedules") == "schedules"
    assert correct_workspace_typos("list tabels on Demo Orders") == "list tables on Demo Orders"
    assert correct_workspace_typos("my pipelins") == "my pipelines"

    assert [n for n, _ in plan_tools_tolerant("conenctors?")] == ["list_connectors"]
    assert [n for n, _ in plan_tools_tolerant("how many conenctors do i have")] == [
        "list_connectors"
    ]
    assert [n for n, _ in plan_tools_tolerant("my pipelins")] == ["list_schedules"]
    assert [n for n, _ in plan_tools_tolerant("list tabels on Demo Orders")] == [
        "list_connector_objects"
    ]
    # A misspelling that became the lookup subject is corrected too — COUNT(*)
    # over a table called `pipelins` was never going to resolve.
    assert [n for n, _ in plan_tools_tolerant("how many pipelins do i have")] == ["list_schedules"]

    # `contacts` is one edit from `contracts` and is a real table everywhere.
    assert correct_workspace_typos("count rows in contacts on Demo Orders") == (
        "count rows in contacts on Demo Orders"
    )
    live = dict(plan_tools_tolerant("count rows in contacts on Demo Orders"))
    assert live["aggregate_data"]["table"] == "contacts"


def test_an_open_clarification_does_not_swallow_the_next_question():
    """A failed connector match left a slot open, and the next unrelated
    question replayed "No connector matched “quarantined”" with "I didn't match
    that reply" appended. A self-contained act is never a slot fill.
    """
    from src.ai.copilot.followup import looks_like_fresh_intent

    for ask in (
        "is anything waiting on me",
        "is anything parked",
        "what needs my attention",
        "hi",
        "thanks",
    ):
        assert looks_like_fresh_intent(ask), ask

    # A bare name is still the answer to "which connector did you mean?".
    assert not looks_like_fresh_intent("Demo Orders")


def test_an_uncovered_subject_is_quoted_in_the_operators_own_words():
    """The caveat quoted the *stem*, so "can you migrate my database" was told
    the documentation does not cover “databas”.
    """
    from src.ai.rag.product_docs import retrieve_product_answer

    answer = retrieve_product_answer("can you migrate my database", limit=4)
    assert answer.verdict.uncovered_subjects == ("databas",), "stem is still the index term"
    assert "“database”" in answer.caveat
    assert "“databas”" not in answer.caveat


def test_a_connector_without_a_target_renders_no_dangling_arrow():
    from src.ai.copilot.pilot_agent import get_pilot_agent

    resp = get_pilot_agent().chat("list my connectors")
    for line in resp.answer.splitlines():
        assert not line.rstrip().endswith("→"), line


def test_a_repair_turn_redoes_the_previous_question_with_the_correction():
    """"no i meant the failed ones" was refused as undocumented.

    A repair keeps the subject the operator already named and replaces one
    constraint on it, so re-planning the corrected question is what recovers the
    real ask. A correction that names its own subject replaces the old one
    instead of stacking on it — appending "pipelines" to a connector count
    planned both lists and answered with two that contradicted each other.
    """
    from src.ai.copilot.followup import repair_correction, resolve_repair
    from src.ai.copilot.tools import infer_tools_from_message

    assert repair_correction("no i meant the failed ones") == "failed ones"
    assert repair_correction("i meant pipelines") == "pipelines"
    assert repair_correction("wait, that's not what i meant") == ""
    assert repair_correction("how many connectors do i have") is None

    hist = [
        {"role": "user", "content": "how many connectors do i have"},
        {"role": "assistant", "content": "You have **2 saved connector(s)**."},
    ]
    constrained = resolve_repair("no i meant the failed ones", hist)
    assert constrained == "how many connectors do i have failed ones"
    assert dict(infer_tools_from_message(constrained))["list_connectors"] == {"health": "failed"}

    swapped = resolve_repair("i meant pipelines", hist)
    assert swapped == "how many pipelines do i have"
    assert [n for n, _ in infer_tools_from_message(swapped)] == ["list_schedules"]

    # A bare objection carries nothing to re-plan.
    assert resolve_repair("that's not what i meant", hist) is None


def test_a_repair_skips_over_an_earlier_bare_objection():
    """"that's not what i meant" is itself a user turn.

    Correcting it on the next turn built "that's not what i meant pipelines" and
    answered with the job list. The question being corrected is the last one that
    carried a subject.
    """
    from src.ai.copilot.followup import resolve_repair

    hist = [
        {"role": "user", "content": "how many connectors do i have"},
        {"role": "assistant", "content": "You have **2 saved connector(s)**."},
        {"role": "user", "content": "wait, that's not what i meant"},
        {"role": "assistant", "content": "Understood — I read that wrong."},
    ]
    assert resolve_repair("i meant pipelines", hist) == "how many pipelines do i have"


def test_a_bare_objection_asks_what_was_misread():
    """It used to replay the very answer it was objecting to."""
    from src.ai.copilot.conversation_composer import compose_repair_prompt
    from src.ai.copilot.dialogue_acts import classify_dialogue_act

    hist = [
        {"role": "user", "content": "how many connectors do i have"},
        {"role": "assistant", "content": "You have **2 saved connector(s)**."},
    ]
    for ask in ("wait, that's not what i meant", "no, not what i asked", "that's not it"):
        assert classify_dialogue_act(ask, history=hist) == "repair_unclear", ask

    prompt = compose_repair_prompt(hist)
    assert "I read that wrong" in prompt
    assert "how many connectors do i have" in prompt
    assert "2 saved connector" not in prompt, "must not replay the rejected answer"


def test_what_did_i_just_ask_is_answered_from_the_transcript():
    """It was answered with the three closest Help headings and a refusal."""
    from src.ai.copilot.conversation_composer import compose_recall_ask
    from src.ai.copilot.dialogue_acts import classify_dialogue_act

    hist = [
        {"role": "user", "content": "how many connectors do i have"},
        {"role": "assistant", "content": "You have **2 saved connector(s)**."},
        {"role": "user", "content": "which ones failed"},
        {"role": "assistant", "content": "None failed."},
    ]
    for ask in ("what did i just ask you", "what was my last question", "repeat my question"):
        assert classify_dialogue_act(ask, history=hist) == "recall_ask", ask

    recalled = compose_recall_ask(hist)
    assert "which ones failed" in recalled
    assert "how many connectors do i have" in recalled
    assert "does not cover" not in recalled

    assert "first thing you've asked" in compose_recall_ask([])


def test_the_other_one_points_at_the_previous_list():
    """It carries no pronoun, so it reached no tool and was refused."""
    from src.ai.copilot.followup import resolve_platform_coreference

    hist = [{"role": "assistant", "content": "You have **2 saved connector(s)**."}]
    assert resolve_platform_coreference("ok and the other one?", hist) == [("list_connectors", {})]


def test_an_underspecified_duplicate_call_is_collapsed():
    """The failed bucket was answered and then contradicted by the full list."""
    from src.ai.copilot.tools import infer_tools_from_message

    plan = infer_tools_from_message("how many connectors do i have failed ones")
    assert plan == [("list_connectors", {"health": "failed"})]
