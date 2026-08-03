"""Wave 42 — Ollama-first engine SSOT + soft-pending typo keep."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_resolve_engine_prefers_ollama(monkeypatch):
    from src.ai.llm import provider as prov

    class _Up:
        def is_available(self):
            return True

    class _Down:
        def is_available(self):
            return False

    monkeypatch.delenv("DATAFLOW_PILOT_ENGINE", raising=False)
    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Up())
    monkeypatch.setattr(prov, "DataTransferAnthropicProvider", lambda: _Down())
    monkeypatch.setattr(prov, "DataTransferOpenAIProvider", lambda: _Down())
    assert prov.resolve_pilot_engine() == "hybrid"


def test_resolve_engine_local_env(monkeypatch):
    from src.ai.llm import provider as prov

    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    assert prov.resolve_pilot_engine() == "local"


def test_pick_narration_prefers_ollama(monkeypatch):
    from src.ai.llm import provider as prov

    class _Ollama:
        def is_available(self):
            return True

    class _Cloud:
        def is_available(self):
            return True

    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Ollama())
    monkeypatch.setattr(prov, "DataTransferAnthropicProvider", lambda: _Cloud())
    monkeypatch.setattr(prov, "DataTransferOpenAIProvider", lambda: _Cloud())
    provider, method = prov.pick_narration_provider()
    assert method == "ollama_polish"
    assert provider is not None


def test_soft_pending_typo_promotes_to_memory(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.working_memory import get_working_memory

    agent = DataPilotAgent()
    sid = "wave42-soft"
    history = [
        {
            "role": "assistant",
            "content": "Which connector has users?\n\nAvailable: **Local Postgres**, **Warehouse**.",
        }
    ]
    planned = agent._plan_with_memory(
        "Loacl Postgress",
        {"pilot_session_id": sid},
        history,
    )
    assert planned == []
    pending = get_working_memory().get_pending(sid)
    assert pending is not None
    assert pending.missing == "connector_name"


def test_pilot_agent_uses_provider_ssot(monkeypatch):
    from src.ai.copilot import pilot_agent as pa
    from src.ai.llm import provider as prov

    monkeypatch.setattr(prov, "resolve_pilot_engine", lambda: "hybrid")
    assert pa._resolve_pilot_engine() == "hybrid"
