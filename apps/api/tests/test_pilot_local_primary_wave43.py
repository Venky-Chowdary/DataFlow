"""Wave 43 — local Datawrap engine is primary; cloud/Ollama are opt-in only."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_auto_engine_is_local_even_if_ollama_up(monkeypatch):
    from src.ai.llm import provider as prov

    class _Up:
        def is_available(self):
            return True

    monkeypatch.delenv("DATAFLOW_PILOT_ENGINE", raising=False)
    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Up())
    monkeypatch.setattr(prov, "DataTransferAnthropicProvider", lambda: _Up())
    monkeypatch.setattr(prov, "DataTransferOpenAIProvider", lambda: _Up())
    assert prov.resolve_pilot_engine() == "local"


def test_hybrid_is_explicit_opt_in(monkeypatch):
    from src.ai.llm import provider as prov

    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")
    assert prov.resolve_pilot_engine() == "hybrid"


def test_product_howto_uses_explain_product(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    resp = agent.chat(
        "How do I transfer data in Datawrap?",
        data_context={"pilot_session_id": "wave43-howto"},
    )
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "explain_product" in names
    assert "search_knowledge" not in names
    assert "transfer" in (resp.answer or "").lower() or "confirm" in (resp.answer or "").lower()


def test_what_is_dataflow_local(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    resp = agent.chat(
        "What is Datawrap?",
        data_context={"pilot_session_id": "wave43-what"},
    )
    names = [t["name"] for t in (resp.tools_used or [])]
    assert "explain_product" in names
    assert "synonym group" not in (resp.answer or "").lower()


def test_capabilities_list_local_first():
    from src.ai.llm.provider import get_model_capabilities

    caps = get_model_capabilities()
    assert caps["fallback_order"][0] == "local"
    assert caps["pilot_engine"] == "local"
    assert "optional" in " ".join(caps["guarantees"]).lower() or "add-on" in " ".join(caps["guarantees"]).lower()
