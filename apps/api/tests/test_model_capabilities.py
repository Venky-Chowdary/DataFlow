"""Model capability registry should be explicit and fail closed."""

from __future__ import annotations

from src.ai.llm.provider import get_model_capabilities


def test_model_capabilities_expose_cloud_and_local_fallbacks():
    status = get_model_capabilities()
    providers = {p["provider"]: p for p in status["providers"]}

    assert status["fallback_order"] == ["anthropic", "openai", "ollama", "rag", "local"]
    assert {"anthropic", "openai", "ollama", "local"}.issubset(providers)
    assert providers["local"]["available"] is True
    assert "preflight" in " ".join(status["guarantees"]).lower()
    assert status["active_provider"] in providers

