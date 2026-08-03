"""Wave 41 — live key verify, invalid_key status, Ollama polish, honesty footnote."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_capabilities_marks_invalid_key(monkeypatch):
    from src.ai.llm import provider as prov

    prov.clear_auth_failures()
    prov._AUTH_FAILED_PROVIDERS.add("openai")
    caps = prov.get_model_capabilities()
    openai = next(p for p in caps["providers"] if p["provider"] == "openai")
    assert openai["available"] is False
    assert openai["status"] == "invalid_key"
    prov.clear_auth_failures()


def test_verify_rejects_masked_key():
    from src.ai.llm.provider import verify_cloud_api_key

    ok, err = verify_cloud_api_key("openai", "••••••••")
    assert ok is False
    assert "masked" in err.lower() or "empty" in err.lower()


def test_polish_falls_back_to_ollama(monkeypatch):
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.llm import provider as prov

    class _Down:
        def is_available(self):
            return False

    class _Ollama:
        def is_available(self):
            return True

        def generate(self, prompt, system="", max_tokens=900):
            return prov.LLMResponse(
                content="You have three recent transfer jobs ready to review.",
                success=True,
                provider="ollama",
                model="llama3.2",
            )

    monkeypatch.setattr(prov, "DataTransferOpenAIProvider", lambda: _Down())
    monkeypatch.setattr(prov, "DataTransferAnthropicProvider", lambda: _Down())
    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Ollama())

    agent = DataPilotAgent()
    local = CopilotResponse(
        answer="3 jobs listed.",
        intent="jobs",
        confidence=0.9,
        method="pilot_local_engine",
        tools_used=[{"name": "list_jobs", "success": True, "summary": "3 jobs"}],
    )
    out = agent._polish_with_llm("show jobs", [], local, "system")
    assert out.method == "ollama_polish"
    assert "three" in out.answer.lower() or "jobs" in out.answer.lower()


def test_hybrid_footnote_on_auth_failure(monkeypatch):
    from src.ai.copilot import pilot_agent as pa
    from src.ai.copilot.agent import CopilotResponse
    from src.ai.llm import provider as prov

    prov.clear_auth_failures()
    prov._AUTH_FAILED_PROVIDERS.add("openai")
    monkeypatch.setattr(pa, "_resolve_pilot_engine", lambda: "hybrid")

    agent = pa.DataPilotAgent()
    local = CopilotResponse(
        answer="Here are your jobs.",
        intent="jobs",
        confidence=0.95,
        method="pilot_local_engine",
        tools_used=[{"name": "list_jobs", "success": True, "summary": "ok"}],
    )
    monkeypatch.setattr(agent, "_local_agent", lambda *a, **k: local)
    monkeypatch.setattr(agent, "_polish_with_llm", lambda *a, **k: local)
    monkeypatch.setattr(agent, "context_builder", MagicMock(build=lambda *a, **k: {}))

    out = agent.chat("show my jobs", data_context={"pilot_session_id": "wave41"})
    assert "Optional" in (out.answer or "") or "optional" in (out.answer or "") or "Local Data Pilot" in (out.answer or "")
    prov.clear_auth_failures()
