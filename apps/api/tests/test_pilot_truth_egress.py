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


#: A turn that is still narrated: count and status answers are not (review H5).
NARRATED = "what columns are in customers on CRM SQLite"


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
    # A schema answer is narrated; count answers are not narrated at all (review H5).
    second = pt.ask(agent, NARRATED, history=history, pilot_session_id="egress")

    assert second.answer
    assert hybrid.prompts, "the schema answer carries no cells, so it is narrated"
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


# ---- free text that carries values (review C1) -------------------------------
#
# A failed job's error text is written by a database driver and quotes the row
# it choked on. Under metadata-only that is cell data: it must not reach a model
# by any channel — the narrated draft, the transcript, the system prompt, a
# native tool payload or the fallback prompt — and the manifest must say what
# was withheld (metadata-only) or included (cells allowed).

FREE_TEXT = ["jane.roe@corp.example", "123-45-6003", "4111-1111-1111-1111"]
JOB_ERROR = (
    'duplicate key value violates unique constraint "customers_email_key" DETAIL: Key '
    '(email)=(jane.roe@corp.example) already exists. Row: {"ssn": "123-45-6003", '
    '"card": "4111-1111-1111-1111"}'
)
VALUE_HISTORY = [
    {"role": "user", "content": "why did it fail"},
    {"role": "assistant", "content": f"Your last transfer job is **FAILED**.\nError: {JOB_ERROR}"},
    {"role": "assistant", "content": (
        f"Rows:\n1. id=1, email={pt.pii_email(2)}, ssn={pt.pii_ssn(1)}\n- Alice Roe / {pt.pii_email(3)}"
    )},
]
VALUE_HISTORY_CELLS = FREE_TEXT + [pt.pii_email(2), pt.pii_ssn(1), pt.pii_email(3), "Alice Roe"]


def _free_leaked(blob, cells=FREE_TEXT) -> list[str]:
    text = blob if isinstance(blob, str) else json.dumps(blob, default=str)
    return [cell for cell in cells if cell in text]


@pytest.fixture
def wire(monkeypatch, tmp_path):
    """Hybrid engine with the real OpenAI provider class on a fake HTTP client.

    What the fake client records is what would have left the box: the bytes
    after the egress gate, not the prompt Pilot built.
    """
    import types

    from src.ai.llm import provider

    pt.reset_workspace(monkeypatch)
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")
    monkeypatch.delenv("DATAFLOW_AI_METADATA_ONLY", raising=False)
    monkeypatch.delenv("DATAWRAP_AI_METADATA_ONLY", raising=False)
    sent: list[dict] = []

    class _Completions:
        def create(self, **kw):
            sent.append(kw)
            if kw.get("tools"):
                raise RuntimeError("tool loop unavailable")  # forces the fallback prompt
            msg = types.SimpleNamespace(content="(model reply that is long enough)", tool_calls=None)
            return types.SimpleNamespace(
                choices=[types.SimpleNamespace(message=msg, finish_reason="stop")], usage=None
            )

    class _Wire(provider.DataTransferOpenAIProvider):
        def _init_client(self):
            self._client = types.SimpleNamespace(chat=types.SimpleNamespace(completions=_Completions()))

    monkeypatch.setattr(provider, "_provider_auth_failed", lambda name: False)
    monkeypatch.setattr(provider, "pick_narration_provider", lambda: (_Wire(), "openai_polish"))
    pt.seed_sqlite_connector(tmp_path)
    return types.SimpleNamespace(sent=sent, cls=_Wire)


@pytest.mark.parametrize("question", [
    "what is the status of my last transfer",
    "why did my last job fail",
    "status of job {jid}",
])
def test_job_error_text_never_reaches_a_model(wire, question):
    jid = pt.seed_job("failed", minutes_ago=1, error=JOB_ERROR, source_name="customers")
    for i in range(3):
        pt.seed_job("completed", minutes_ago=10 + i)
    response = pt.ask(pt.agent(), question.format(jid=jid))
    # The operator still reads the full error locally...
    assert "123-45-6003" in response.answer, response.answer
    # ...and none of it was sent to the model.
    assert _free_leaked(wire.sent) == [], "job error text reached the provider under metadata-only"


def test_value_lines_in_the_transcript_are_withheld_and_the_manifest_says_so(wire):
    from services.ai_egress import last_manifest

    for i in range(3):
        pt.seed_job("completed", minutes_ago=10 + i)
    pt.ask(pt.agent(), NARRATED, history=VALUE_HISTORY)
    assert wire.sent, "the schema answer carries no cells, so it is narrated"
    assert _free_leaked(wire.sent, VALUE_HISTORY_CELLS) == []
    manifest = last_manifest()
    assert manifest["cells_withheld"] is True and manifest["cell_values_included"] is False


def test_client_blockers_are_withheld_from_the_system_prompt(wire):
    from services.ai_egress import last_manifest

    for i in range(3):
        pt.seed_job("completed", minutes_ago=10 + i)
    blockers = [f"Row 12: email '{pt.pii_email(1)}' is not unique", f"ssn {pt.pii_ssn(1)} fails pattern"]
    pt.ask(pt.agent(), NARRATED, blockers=blockers, columns=["id", "email"],
           name="customers", row_count=7)
    assert wire.sent
    assert _free_leaked(wire.sent, [pt.pii_email(1), pt.pii_ssn(1)]) == []
    assert last_manifest()["cells_withheld"] is True


@pytest.mark.parametrize("tool,args", [("get_job", "job"), ("list_jobs", {"limit": 5, "status": "failed"})])
@pytest.mark.parametrize("shape", ["openai", "anthropic"])
def test_native_job_payloads_withhold_error_text(wire, tool, args, shape):
    from services.ai_egress import last_manifest, prepare_messages
    from src.ai.copilot.pilot_agent import PilotTurn

    jid = pt.seed_job("failed", minutes_ago=1, error=JOB_ERROR, source_name="customers")
    arguments = {"job_id": jid} if args == "job" else args
    result, payload = pt.agent()._native_tool_call(PilotTurn(), tool, arguments, {"pilot_session_id": "nat"})
    assert result.success and _free_leaked(result.output), "the app still gets the full error"
    assert _free_leaked(payload) == []
    if shape == "openai":
        messages = [{"role": "user", "content": "q"}, {"role": "tool", "tool_call_id": "c1", "content": payload}]
    else:
        messages = [{"role": "user", "content": "q"},
                    {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "c1", "content": payload}]}]
    gated, _system = prepare_messages(messages, "sys", provider=shape)
    assert _free_leaked(gated) == []
    assert last_manifest()["cells_withheld"] is True


def test_rows_removed_before_the_gate_are_recorded_as_withheld(wire):
    from services.ai_egress import last_manifest, prepare_messages
    from src.ai.copilot.pilot_agent import PilotTurn

    _result, payload = pt.agent()._native_tool_call(
        PilotTurn(), "sample_connector_object",
        {"connector_name": "CRM SQLite", "table": "customers", "limit": 3}, {"pilot_session_id": "rm"},
    )
    prepare_messages([{"role": "tool", "tool_call_id": "c1", "content": payload}], "", provider="openai")
    assert last_manifest()["cells_withheld"] is True, "Pilot withheld rows but the manifest says nothing was"


def test_fallback_prompt_withholds_job_error_text(wire, monkeypatch):
    """The native tool loop fails, so tool results go to the model in one prompt."""
    from services.ai_egress import last_manifest
    from src.ai.llm import provider

    monkeypatch.setattr(provider, "DataTransferOpenAIProvider", wire.cls)
    pt.seed_job("failed", minutes_ago=1, error=JOB_ERROR, source_name="customers")
    response = pt.agent()._openai_agent("why did my last job fail", [], "system", {"pilot_session_id": "fb"})
    fallback = [kw for kw in wire.sent if not kw.get("tools")]
    assert response is not None and fallback, "the fallback prompt was not sent"
    assert _free_leaked(fallback) == []
    assert last_manifest()["cells_withheld"] is True


def test_manifest_records_free_text_as_included_when_cells_are_allowed(wire, monkeypatch):
    from services.ai_egress import last_manifest

    monkeypatch.setenv("DATAFLOW_AI_METADATA_ONLY", "false")
    # Job answers are not narrated (review H5): the error text rides in the blockers.
    pt.ask(pt.agent(), NARRATED, blockers=[JOB_ERROR], columns=["id", "email"], name="customers", row_count=7)
    assert _free_leaked(wire.sent), "cells allowed: the blocker text is sent"
    assert last_manifest()["cell_values_included"] is True

    wire.sent.clear()
    pt.ask(pt.agent(), NARRATED, history=VALUE_HISTORY)
    assert _free_leaked(wire.sent, VALUE_HISTORY_CELLS)
    assert last_manifest()["cell_values_included"] is True
