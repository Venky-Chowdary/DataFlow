"""Unit tests for LLM provider robustness and Data Pilot fallbacks."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.llm.provider import (  # noqa: E402
    DataTransferAnthropicProvider,
    DataTransferOpenAIProvider,
)


def test_openai_provider_survives_bad_integrations_store(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OPENAI_API_KEY", "")
    with patch("services.integrations_store.resolve_provider_api_key", side_effect=RuntimeError("bad store")):
        provider = DataTransferOpenAIProvider()
    assert not provider.is_available()


def test_anthropic_provider_survives_bad_integrations_store(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    with patch("services.integrations_store.resolve_provider_api_key", side_effect=RuntimeError("bad store")):
        provider = DataTransferAnthropicProvider()
    assert not provider.is_available()


def test_pilot_agent_anthropic_property_does_not_crash_on_bad_config():
    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    with patch("src.ai.llm.provider.DataTransferAnthropicProvider", side_effect=RuntimeError("bad config")):
        anthropic = agent.anthropic
    assert not anthropic.is_available()
