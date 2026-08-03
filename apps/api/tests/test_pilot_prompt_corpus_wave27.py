"""Enterprise Data Pilot prompt corpus — ≥1000 real NL prompts.

Validates local NL routing (infer_tools_from_message) against the parametric
corpus. This is not a mocked chatbot: every case is a real operator phrase and
must route to the declared tool family (or honestly refuse).
"""

from __future__ import annotations

from src.ai.copilot.prompt_corpus import corpus_stats, iter_prompt_corpus
from src.ai.copilot.tools import TOOL_DEFINITIONS, get_tool_registry, infer_tools_from_message


def test_local_engine_is_default_without_cloud():
    """Forced local engine: deterministic tools, no third-party dependency."""
    import os
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.llm.provider import get_model_capabilities

    os.environ["DATAFLOW_PILOT_ENGINE"] = "local"
    caps = get_model_capabilities()
    assert caps["agent_mode"] == "local_tools"
    assert caps.get("pilot_engine") == "local"
    assert caps["active_provider"] == "local"

    agent = DataPilotAgent()
    resp = agent.chat("what can you do?")
    assert resp.method == "pilot_local_engine"
    assert "confirm" in (resp.answer or "").lower() or "transfer" in (resp.answer or "").lower()
    stats = corpus_stats()
    assert stats["total"] >= 1000, stats
    assert stats["by_family"].get("aggregate", 0) >= 100
    assert stats["by_family"].get("transfer_start", 0) >= 100
    assert stats["by_family"].get("navigate", 0) >= 50


def test_tool_registry_is_honest_no_marketing_inflation():
    registry = get_tool_registry()
    assert registry["tool_count"] == len(TOOL_DEFINITIONS)
    assert registry["generated_action_count"] == 0
    assert registry["total_routable_actions"] == len(TOOL_DEFINITIONS)
    assert registry["total_routable_actions"] < 200
    move = next(f for f in registry["families"] if f["id"] == "move")
    assert "plan_transfer" in move["tools"]
    assert "start_transfer" in move["tools"]


def test_describe_pilot_admits_staged_transfers():
    from src.ai.copilot.tools import get_pilot_tools

    out = get_pilot_tools().execute("describe_pilot", {}).output or {}
    can = " ".join(out.get("can") or []).lower()
    cannot = " ".join(out.get("cannot_yet") or []).lower()
    assert "confirm" in can and "transfer" in can
    assert "start a transfer" not in cannot
    assert "9" in " ".join(out.get("transfers") or [])


def test_unmapped_transfer_reply_admits_confirm_start():
    from src.ai.copilot.pilot_agent import _unmapped_intent_reply

    reply = _unmapped_intent_reply("please sync my warehouse tables", {"connectors": []})
    lower = reply.lower()
    assert "can't start a sync myself yet" not in lower
    assert "confirm" in lower


def test_how_many_rows_binds_connector():
    planned = infer_tools_from_message("how many rows in orders on Local Postgres?")
    assert planned
    name, args = planned[0]
    assert name == "aggregate_data"
    assert args.get("table") == "orders"
    assert "Local Postgres" in str(args.get("connector_name") or "")


def test_full_prompt_corpus_routing():
    """Run every corpus prompt in one test — pytest param overhead is too high at 3k."""
    failures: list[str] = []
    cases = iter_prompt_corpus()
    assert len(cases) >= 1000

    for case in cases:
        planned = infer_tools_from_message(case.prompt)
        names = {n for n, _ in planned}

        for banned in case.must_not:
            if banned in names:
                failures.append(f"MUST_NOT {case.prompt!r} → {banned} in {planned}")
                break
        else:
            if case.family in {"greeting", "refuse"} and not case.expected_tools:
                continue
            if not (names & case.expected_tools):
                failures.append(
                    f"MISS {case.family}:{case.prompt!r} expected "
                    f"{sorted(case.expected_tools)} got {planned}"
                )

        if len(failures) >= 40:
            break

    assert not failures, (
        f"{len(failures)} routing failures (showing up to 40):\n"
        + "\n".join(failures)
    )
