"""The provider-backed (native) tool loop is a bounded *read* loop.

It runs only after the deterministic planner found nothing grounded, so a
model-chosen mutation is refused instead of staged, the tool budget is enforced
server-side, oversized payloads are clipped to valid JSON, and a provider
failure falls back to the local answer without inventing one. Both provider
loops (Anthropic and OpenAI shapes) go through the same chokepoint.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot import pilot_agent as pa  # noqa: E402
from src.ai.copilot.pilot_agent import DataPilotAgent, PilotTurn  # noqa: E402
from src.ai.copilot.tool_permissions import MUTATE, TOOL_PERMISSIONS  # noqa: E402
from src.ai.copilot.tools import ToolResult  # noqa: E402

MUTATING = sorted(name for name, (_perm, effect) in TOOL_PERMISSIONS.items() if effect == MUTATE)
READS = sorted(name for name, (_perm, effect) in TOOL_PERMISSIONS.items() if effect != MUTATE)


@pytest.fixture
def agent(monkeypatch):
    agent = DataPilotAgent()
    executed: list[tuple[str, dict]] = []

    def fake_execute(name, args):
        executed.append((name, dict(args or {})))
        return ToolResult(name=name, success=True, output={"ok": True, "name": name})

    monkeypatch.setattr(agent.tools, "execute", fake_execute)
    monkeypatch.setattr(agent, "_with_result_context", lambda name, args, ctx: dict(args or {}))
    agent._executed = executed  # type: ignore[attr-defined]
    return agent


@pytest.mark.parametrize("name", MUTATING)
def test_model_chosen_mutation_is_refused_not_staged(agent, name):
    turn = PilotTurn()
    tr, payload = agent._native_tool_call(turn, name, {"name": "x"}, None)
    assert tr.success is False
    assert name in (tr.error or "")
    assert "Confirm" in (tr.error or "")
    assert agent._executed == [], "the tool registry must never see the call"
    assert turn.pending_actions == [], "nothing may be staged from a model-chosen mutation"
    assert json.loads(payload)["error"] == tr.error


def test_reads_pass_through_the_registry(agent):
    turn = PilotTurn()
    tr, payload = agent._native_tool_call(turn, "list_connectors", {}, None)
    assert tr.success is True
    assert agent._executed == [("list_connectors", {})]
    assert json.loads(payload) == {"ok": True, "name": "list_connectors"}


def test_every_registered_tool_is_classified():
    assert set(MUTATING) | set(READS) == set(TOOL_PERMISSIONS)
    assert {"delete_connector", "start_transfer", "cancel_job", "delete_schedule"} <= set(MUTATING)


def test_per_turn_tool_budget_is_enforced_server_side(agent):
    turn = PilotTurn()
    for _ in range(pa._NATIVE_MAX_TOOL_CALLS):
        tr, _payload = agent._native_tool_call(turn, "list_connectors", {}, None)
        assert tr.success
    tr, payload = agent._native_tool_call(turn, "list_connectors", {}, None)
    assert tr.success is False
    assert str(pa._NATIVE_MAX_TOOL_CALLS) in (tr.error or "")
    assert len(agent._executed) == pa._NATIVE_MAX_TOOL_CALLS
    assert "error" in json.loads(payload)


def test_oversized_tool_output_is_clipped_to_valid_json(agent, monkeypatch):
    big = {"rows": [{"i": i, "pad": "x" * 200} for i in range(400)]}
    monkeypatch.setattr(
        agent.tools,
        "execute",
        lambda name, args: ToolResult(name=name, success=True, output=big),
    )
    turn = PilotTurn()
    tr, payload = agent._native_tool_call(turn, "sample_connector_object", {}, None)
    assert tr.success and tr.output is big, "the operator-facing result is not clipped"
    clipped = json.loads(payload)
    assert clipped["truncated"] is True
    assert clipped["chars"] > pa._NATIVE_TOOL_OUTPUT_CHARS
    assert len(clipped["preview"]) == pa._NATIVE_TOOL_OUTPUT_CHARS


def _scripted_provider(rounds: list[dict]):
    calls = {"n": 0, "messages": []}

    class _Provider:
        name = "scripted"

        def is_available(self):
            return True

        def generate_agent(self, **kwargs):
            calls["messages"].append(kwargs.get("messages"))
            idx = min(calls["n"], len(rounds) - 1)
            calls["n"] += 1
            return rounds[idx]

    return _Provider(), calls


def test_anthropic_loop_refuses_mutation_and_still_answers(agent, monkeypatch):
    provider, calls = _scripted_provider([
        {
            "success": True,
            "content": "",
            "tool_calls": [
                {"id": "t1", "name": "delete_connector", "input": {"name": "Warehouse"}},
                {"id": "t2", "name": "list_connectors", "input": {}},
            ],
        },
        {"success": True, "content": "I cannot delete from here; say “delete connector Warehouse”.", "tool_calls": []},
    ])
    monkeypatch.setattr(agent, "_anthropic", provider)
    monkeypatch.setattr(agent, "_commit_memory", lambda *a, **k: None)
    monkeypatch.setattr(agent, "_run_local_recovery", lambda *a, **k: None)

    resp = agent._anthropic_agent_loop("get rid of Warehouse", [], "system", {"pilot_session_id": "s1"})
    assert resp is not None
    assert resp.pending_actions == []
    assert [n for n, _ in agent._executed] == ["list_connectors"]
    used = {t["name"]: t for t in resp.tools_used}
    assert used["delete_connector"]["success"] is False
    assert used["list_connectors"]["success"] is True
    # Every tool_use block the model emitted got a tool_result back, so the
    # provider does not reject the next round.
    second_round = calls["messages"][1]
    uses = [b["id"] for b in second_round[-2]["content"] if b.get("type") == "tool_use"]
    results = [b["tool_use_id"] for b in second_round[-1]["content"]]
    assert uses == results == ["t1", "t2"]


def test_openai_loop_caps_calls_per_round_and_answers_every_call(agent, monkeypatch):
    many = [
        {"id": f"c{i}", "name": "list_connectors", "input": {}}
        for i in range(pa._NATIVE_MAX_CALLS_PER_ROUND + 3)
    ]
    provider, calls = _scripted_provider([
        {"success": True, "content": "", "tool_calls": many},
        {"success": True, "content": "You have connectors.", "tool_calls": []},
    ])
    monkeypatch.setattr("src.ai.llm.provider.DataTransferOpenAIProvider", lambda: provider)
    monkeypatch.setattr(agent, "_commit_memory", lambda *a, **k: None)
    monkeypatch.setattr(agent, "_run_local_recovery", lambda *a, **k: None)

    resp = agent._openai_agent("what do i have", [], "system", {"pilot_session_id": "s2"})
    assert resp is not None and resp.answer == "You have connectors."
    assert len(agent._executed) == pa._NATIVE_MAX_CALLS_PER_ROUND
    second_round = calls["messages"][1]
    assistant = next(m for m in second_round if m.get("role") == "assistant" and m.get("tool_calls"))
    tool_msgs = [m for m in second_round if m.get("role") == "tool"]
    assert [tc["id"] for tc in assistant["tool_calls"]] == [m["tool_call_id"] for m in tool_msgs]


def test_provider_failure_yields_no_answer_not_an_invented_one(agent, monkeypatch):
    provider, _calls = _scripted_provider([{"success": False, "error": "timed out"}])
    monkeypatch.setattr(agent, "_anthropic", provider)
    assert agent._anthropic_agent_loop("anything", [], "system", None) is None
    assert agent._executed == []


def test_native_answer_without_evidence_loses_to_a_local_refusal():
    from src.ai.copilot.agent import CopilotResponse

    fluent = CopilotResponse(
        answer="Sure! Snowflake supports everything you need and I have set it up for you." * 2,
        intent="knowledge",
        confidence=0.94,
        method="anthropic_agent",
        tools_used=[],
    )
    refusal = CopilotResponse(
        answer="I will not answer that from general knowledge; it is not in the product docs.",
        intent="knowledge",
        confidence=0.4,
        method="local_agent",
        tools_used=[],
    )
    assert pa._score_response(refusal) > pa._score_response(fluent)


def test_provider_sdk_clients_are_bounded():
    from src.ai.llm import provider

    assert provider.REQUEST_TIMEOUT_SECONDS <= 60
    assert provider.REQUEST_MAX_RETRIES <= 1
    src = Path(provider.__file__).read_text(encoding="utf-8")
    assert src.count("timeout=REQUEST_TIMEOUT_SECONDS") == 2
    assert src.count("max_retries=REQUEST_MAX_RETRIES") == 2
