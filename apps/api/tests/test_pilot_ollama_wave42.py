"""Wave 42 leftovers — engine SSOT helpers (updated for local-primary wave 43)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest


@pytest.fixture(autouse=True)
def _no_configured_provider(tmp_path, monkeypatch):
    """Engine choice is about operator configuration, so start from none."""
    from services import integrations_store

    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


def test_resolve_engine_auto_is_local(monkeypatch):
    """A reachable Ollama is not an operator decision — auto stays local."""
    from src.ai.llm import provider as prov

    class _Up:
        def is_available(self):
            return True

    monkeypatch.delenv("DATAFLOW_PILOT_ENGINE", raising=False)
    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Up())
    assert prov.resolve_pilot_engine() == "local"


def test_resolve_engine_local_env(monkeypatch):
    from src.ai.llm import provider as prov

    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    assert prov.resolve_pilot_engine() == "local"


def test_pick_narration_prefers_ollama_when_no_cloud_key_is_saved(monkeypatch):
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

    monkeypatch.setattr(prov, "resolve_pilot_engine", lambda: "local")
    assert pa._resolve_pilot_engine() == "local"
