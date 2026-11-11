"""Pilot ground truth — cell values never leave under "metadata-only" (audit C6).

The egress gate recognised only the mapper's prompt formats, so a narrated
sample ("show me 3 rows from customers") sent e-mail addresses and SSNs to the
cloud narrator while the manifest recorded ``cell_values_included: false``.
These cases capture what a stub provider is handed — no real provider is
called — and read the manifest the gate wrote.
"""

from __future__ import annotations

import json

import pytest

from tests.helpers import pilot_truth as pt

CELLS = [pt.pii_email(1), pt.pii_ssn(1), pt.pii_email(3), pt.pii_ssn(3)]
ROWS_PAYLOAD = json.dumps(
    {"rows": [{"id": 1, "email": pt.pii_email(1), "ssn": pt.pii_ssn(1)}], "row_count": 1, "columns": ["id", "email", "ssn"]}
)


class _StubNarrator:
    name = "openai"

    def __init__(self):
        self.prompts: list[str] = []

    def is_available(self) -> bool:
        return True

    def generate(self, prompt, system="", max_tokens=1024):
        from src.ai.llm.provider import LLMResponse

        self.prompts.append(f"{system}\n{prompt}")
        draft = prompt.split("Draft answer:\n", 1)[-1].strip()
        return LLMResponse(content=draft, success=True, provider="openai")


@pytest.fixture
def hybrid(monkeypatch, tmp_path):
    """Hybrid engine with a narrator that records every prompt it is handed."""
    from src.ai.llm import provider

    pt.reset_workspace(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")
    monkeypatch.delenv("DATAFLOW_AI_METADATA_ONLY", raising=False)
    monkeypatch.delenv("DATAWRAP_AI_METADATA_ONLY", raising=False)
    narrator = _StubNarrator()
    monkeypatch.setattr(provider, "pick_narration_provider", lambda: (narrator, "openai_polish"))
    pt.seed_sqlite_connector(tmp_path)
    return narrator


def _leaked(texts: list[str]) -> list[str]:
    return [cell for cell in CELLS for text in texts if cell in text]


def test_narrator_is_never_handed_sampled_cells(hybrid):
    from services.ai_egress import metadata_only_enabled

    assert metadata_only_enabled()
    response = pt.ask(pt.agent(), "show me 3 rows from customers on CRM SQLite")

    # The operator still gets the rows; they are simply not sent to a model.
    assert pt.pii_email(1) in response.answer and pt.pii_ssn(1) in response.answer
    assert _leaked(hybrid.prompts) == [], "cell values reached the narrator under metadata-only"


def test_rows_in_the_transcript_are_withheld_from_a_later_narration(hybrid):
    agent = pt.agent()
    first = pt.ask(agent, "show me 3 rows from customers on CRM SQLite", pilot_session_id="egress")
    history = [
        {"role": "user", "content": "show me 3 rows from customers on CRM SQLite"},
        {"role": "assistant", "content": first.answer},
    ]
    hybrid.prompts.clear()
    second = pt.ask(agent, "how many connectors do I have", history=history, pilot_session_id="egress")

    assert second.answer
    assert hybrid.prompts, "the connectors answer carries no cells, so it is narrated"
    assert _leaked(hybrid.prompts) == []


def test_gate_withholds_a_row_payload_and_says_so(hybrid):
    from services.ai_egress import gate_outbound_prompt

    outbound = gate_outbound_prompt(ROWS_PAYLOAD, "", provider="openai")
    assert _leaked([outbound.prompt, outbound.system]) == []
    assert outbound.cells_withheld is True
    assert outbound.cell_values_included is False


def test_gate_withholds_declared_cells_in_free_text(hybrid):
    from services.ai_egress import declared_cells, gate_outbound_prompt

    draft = f"Live sample — 1 row.\n| 1 | {pt.pii_email(1)} | {pt.pii_ssn(1)} |"
    with declared_cells([pt.pii_email(1), pt.pii_ssn(1)]):
        outbound = gate_outbound_prompt(f"Draft answer:\n{draft}", "", provider="openai")
    assert _leaked([outbound.prompt]) == []
    assert outbound.cells_withheld is True


def test_manifest_is_truthful_when_cells_are_allowed(hybrid, monkeypatch):
    from services.ai_egress import declared_cells, last_manifest, prepare_generate

    monkeypatch.setenv("DATAFLOW_AI_METADATA_ONLY", "false")
    prompt, _system = prepare_generate(ROWS_PAYLOAD, "", provider="openai")
    assert pt.pii_ssn(1) in prompt  # the operator opted out: cells are sent...
    manifest = last_manifest()
    assert manifest["cell_values_included"] is True  # ...and the manifest says so
    assert manifest["policy"] == "cells_allowed"

    with declared_cells([pt.pii_email(3)]):
        prepare_generate(f"Draft answer:\n| 3 | {pt.pii_email(3)} |", "", provider="openai")
    assert last_manifest()["cell_values_included"] is True


def test_manifest_records_withheld_under_metadata_only(hybrid):
    from services.ai_egress import last_manifest, prepare_generate

    prompt, _system = prepare_generate(ROWS_PAYLOAD, "", provider="openai")
    manifest = last_manifest()
    assert _leaked([prompt]) == []
    assert manifest["cells_withheld"] is True and manifest["cell_values_included"] is False
    assert manifest["policy"] == "metadata_only"


def test_agent_messages_with_block_content_are_gated(hybrid):
    from services.ai_egress import last_manifest, prepare_messages

    messages = [
        {"role": "user", "content": "show me rows"},
        {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "t1", "content": ROWS_PAYLOAD}]},
        {"role": "tool", "content": ROWS_PAYLOAD},
    ]
    gated, _system = prepare_messages(messages, "", provider="anthropic")
    assert _leaked([json.dumps(gated)]) == []
    assert last_manifest()["cells_withheld"] is True


def test_native_loop_tool_payload_carries_metadata_only(hybrid):
    from src.ai.copilot.pilot_agent import PilotTurn

    agent = pt.agent()
    turn = PilotTurn()
    result, payload = agent._native_tool_call(
        turn, "sample_connector_object",
        {"connector_name": "CRM SQLite", "table": "customers", "limit": 3}, {"pilot_session_id": "native"},
    )
    assert result.success and result.output["rows"], "the tool itself still returns rows to the app"
    assert _leaked([payload]) == [], "tool rows were serialised for the model under metadata-only"
    shown = json.loads(payload)
    assert shown["row_count"] == 3 and "email" in shown["columns"]
    assert "withheld" in shown


def test_native_loop_tool_payload_keeps_rows_when_cells_are_allowed(hybrid, monkeypatch):
    from src.ai.copilot.pilot_agent import PilotTurn

    monkeypatch.setenv("DATAFLOW_AI_METADATA_ONLY", "false")
    _result, payload = pt.agent()._native_tool_call(
        PilotTurn(), "sample_connector_object",
        {"connector_name": "CRM SQLite", "table": "customers", "limit": 3}, {"pilot_session_id": "native2"},
    )
    assert pt.pii_email(1) in payload
