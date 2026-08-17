"""Bring-your-own AI provider keys: a saved key must actually reach Pilot.

The contract these tests pin down:

* a key saved in Settings selects the provider even with a clean process env;
* the process env wins over the encrypted store;
* an operator who disables a provider disables it, env key or not;
* with nothing configured Pilot answers on the local engine;
* a provider outage changes the wording, never the grounded result;
* nothing on the status surface returns credential material.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from services import integrations_store  # noqa: E402


@pytest.fixture
def store(tmp_path, monkeypatch):
    """An empty integrations store, isolated from the operator's real one."""
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("DATAFLOW_PILOT_ENGINE", raising=False)
    return integrations_store


def _save_key(provider: str, key: str) -> None:
    integrations_store.update_ai_provider(provider, {"api_key": key})


# ── key resolution ───────────────────────────────────────────────────────────


def test_persisted_openai_key_is_resolved_without_process_env(store, monkeypatch):
    _save_key("openai", "sk-persisted-key")
    # update_ai_provider hydrates process env; resolution must not depend on it.
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    assert store.resolve_provider_api_key("openai") == "sk-persisted-key"
    assert store.configured_ai_providers() == ("openai",)


def test_environment_openai_key_takes_precedence(store, monkeypatch):
    _save_key("openai", "sk-persisted-key")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env-key")

    assert store.resolve_provider_api_key("openai") == "sk-env-key"


def test_disabled_openai_provider_is_not_available(store, monkeypatch):
    _save_key("openai", "sk-persisted-key")
    integrations_store.update_ai_provider("openai", {"enabled": False})
    # A disable is a decision: an env key must not resurrect the provider.
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env-key")

    assert store.resolve_provider_api_key("openai") == ""
    assert "openai" not in store.configured_ai_providers()


def test_masked_and_sentinel_values_are_not_keys(store):
    _save_key("openai", "sk-real-key")
    # Re-saving the masked value the UI displays must keep the real key, and a
    # corrupted record must never count as configured.
    _save_key("openai", "••••••••")
    assert store.resolve_provider_api_key("openai") == "sk-real-key"

    integrations_store.update_ai_provider("anthropic", {"api_key": ""})
    raw = integrations_store._load_raw()
    raw["ai_providers"]["anthropic"]["api_key"] = "[decryption-failed]"
    integrations_store._save(raw)
    assert store.resolve_provider_api_key("anthropic") == ""
    assert "anthropic" not in store.configured_ai_providers()


def test_provider_configs_never_return_the_key(store):
    _save_key("openai", "sk-secret-value")
    row = integrations_store.get_ai_provider_configs()["openai"]

    assert row["configured"] is True
    assert "sk-secret-value" not in str(row)
    assert set(row["api_key"]) <= {"•"}


# ── engine decision ──────────────────────────────────────────────────────────


def test_no_provider_uses_local_engine(store):
    from src.ai.llm.provider import pilot_engine_decision

    decision = pilot_engine_decision()
    assert decision["engine"] == "local"
    assert decision["configured_providers"] == []
    assert "no ai provider key" in decision["reason"].lower()


def test_saved_key_turns_auto_into_hybrid(store):
    from src.ai.llm.provider import pilot_engine_decision, resolve_pilot_engine

    _save_key("openai", "sk-persisted-key")

    decision = pilot_engine_decision()
    assert resolve_pilot_engine() == "hybrid"
    assert decision["source"] == "configured_provider"
    assert decision["configured_providers"] == ["openai"]
    assert "sk-persisted-key" not in decision["reason"]


def test_saved_workspace_preference_pins_local_despite_key(store):
    from src.ai.llm.provider import pilot_engine_decision

    _save_key("openai", "sk-persisted-key")
    assert integrations_store.set_pilot_engine_preference("local") == "local"

    decision = pilot_engine_decision()
    assert decision["engine"] == "local"
    assert decision["source"] == "workspace_setting"


def test_environment_engine_overrides_workspace_preference(store, monkeypatch):
    from src.ai.llm.provider import pilot_engine_decision

    integrations_store.set_pilot_engine_preference("local")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "hybrid")

    decision = pilot_engine_decision()
    assert decision["engine"] == "hybrid"
    assert decision["source"] == "environment"


def test_selected_cloud_without_key_still_answers_locally(store):
    """Choosing cloud with no key must not brick Pilot — it says why instead."""
    from src.ai.llm.provider import get_model_capabilities

    integrations_store.set_pilot_engine_preference("cloud")
    caps = get_model_capabilities()

    assert caps["pilot_engine"] == "cloud"
    assert caps["configured_providers"] == []
    assert caps["active_provider"] == "local"
    assert "local engine" in caps["pilot_engine_reason"].lower()


def test_invalid_engine_choice_is_rejected(store):
    with pytest.raises(ValueError):
        integrations_store.set_pilot_engine_preference("gpt-9")
    assert integrations_store.get_pilot_engine_preference() == "auto"


# ── capabilities surface ─────────────────────────────────────────────────────


def test_provider_status_masks_credentials_and_explains_blockers(store):
    from src.ai.llm.provider import get_model_capabilities

    _save_key("openai", "sk-secret-value")
    caps = get_model_capabilities()
    rows = {row["provider"]: row for row in caps["providers"]}

    assert "sk-secret-value" not in str(caps)
    assert rows["openai"]["configured"] is True
    assert rows["anthropic"]["configured"] is False
    assert rows["anthropic"]["blocked_reason"]
    assert rows["local"]["available"] is True


def test_disabled_provider_status_says_disabled(store):
    from src.ai.llm.provider import get_model_capabilities

    _save_key("openai", "sk-secret-value")
    integrations_store.update_ai_provider("openai", {"enabled": False})

    row = next(r for r in get_model_capabilities()["providers"] if r["provider"] == "openai")
    assert row["configured"] is False
    assert row["available"] is False
    assert "disabled" in row["blocked_reason"].lower()


# ── narration fallback ───────────────────────────────────────────────────────


def test_configured_provider_is_preferred_for_narration(store, monkeypatch):
    from src.ai.llm import provider as prov

    class _Ready:
        def __init__(self, name):
            self.name = name

        def is_available(self):
            return True

    _save_key("anthropic", "sk-ant-key")
    monkeypatch.setattr(prov, "DataTransferOllamaProvider", lambda: _Ready("ollama"))
    monkeypatch.setattr(prov, "DataTransferAnthropicProvider", lambda: _Ready("anthropic"))
    monkeypatch.setattr(prov, "DataTransferOpenAIProvider", lambda: _Ready("openai"))

    picked, method = prov.pick_narration_provider()
    assert method == "anthropic_polish"
    assert picked.name == "anthropic"


def test_cloud_provider_failure_falls_back_to_local(store, monkeypatch):
    """A provider outage may change wording; it may never lose the answer."""
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.agent import CopilotResponse

    class _Exploding:
        def generate(self, *_args, **_kwargs):
            raise RuntimeError("openai: connection reset")

    monkeypatch.setattr(
        "src.ai.llm.provider.pick_narration_provider",
        lambda: (_Exploding(), "openai_polish"),
    )

    local = CopilotResponse(
        answer="airports has 12,455 rows on Local Postgres.",
        intent="row_count",
        confidence=1.0,
        method="pilot_local_engine",
        tools_used=[{"name": "count_rows", "success": True, "summary": "12,455 rows"}],
    )
    agent = DataPilotAgent()
    out = agent._polish_with_llm("how many rows in airports", [], local, "system")

    assert out.answer == local.answer
    assert out.method == "pilot_local_engine"


# ── HTTP surface ─────────────────────────────────────────────────────────────


@pytest.fixture
def client(store):
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_pilot_engine_routes_round_trip(client):
    got = client.get("/api/v1/workspace/pilot-engine")
    assert got.status_code == 200, got.text
    assert got.json()["preference"] == "auto"

    saved = client.patch("/api/v1/workspace/pilot-engine", json={"engine": "local"})
    assert saved.status_code == 200, saved.text
    body = saved.json()
    assert body["preference"] == "local"
    assert body["engine"] == "local"
    assert body["reason"]

    assert client.get("/api/v1/workspace/pilot-engine").json()["preference"] == "local"

    rejected = client.patch("/api/v1/workspace/pilot-engine", json={"engine": "gpt-9"})
    assert rejected.status_code == 400
    assert "gpt-9" in rejected.json()["detail"]


def test_test_key_endpoint_without_a_saved_key(client):
    res = client.post("/api/v1/workspace/ai-providers/openai/test")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is False
    assert "no api key" in body["error"].lower()
    assert body["capabilities"]["pilot_engine"] == "local"


def test_test_key_endpoint_reports_rejection_without_echoing_the_key(client, monkeypatch):
    _save_key("openai", "sk-bad-key")
    monkeypatch.setattr(
        "src.ai.llm.provider.verify_cloud_api_key",
        lambda provider, key: (False, "API key rejected (401)"),
    )

    res = client.post("/api/v1/workspace/ai-providers/openai/test")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["ok"] is False
    assert "401" in body["error"]
    assert "sk-bad-key" not in res.text


def test_test_key_endpoint_accepts_a_working_key(client, monkeypatch):
    _save_key("openai", "sk-good-key")
    monkeypatch.setattr(
        "src.ai.llm.provider.verify_cloud_api_key",
        lambda provider, key: (True, ""),
    )

    body = client.post("/api/v1/workspace/ai-providers/openai/test").json()
    assert body["ok"] is True
    assert body["error"] == ""


def test_test_key_endpoint_refuses_providers_without_cloud_keys(client):
    res = client.post("/api/v1/workspace/ai-providers/ollama/test")
    assert res.status_code == 400


def test_rejected_key_stops_the_engine_promising_that_provider(store):
    """A 401 means Pilot cannot use the provider, so the engine must not claim it."""
    from src.ai.llm import provider as provider_mod

    _save_key("openai", "sk-rejected-key")
    provider_mod.clear_auth_failures()
    assert provider_mod.pilot_engine_decision()["engine"] == "hybrid"

    provider_mod._mark_provider_auth_failed("openai", "Error code: 401 - invalid_api_key")
    try:
        decision = provider_mod.pilot_engine_decision()
        assert decision["engine"] == "local"
        assert decision["configured_providers"] == []
        assert "rejected" in decision["reason"].lower()
        assert provider_mod.usable_cloud_providers() == []

        caps = provider_mod.get_model_capabilities()
        row = next(p for p in caps["providers"] if p["provider"] == "openai")
        # The key is still saved — the card says so — but nothing promises it works.
        assert row["configured"] is True
        assert row["available"] is False
        assert row["status"] == "invalid_key"
        assert caps["pilot_engine"] == "local"
        assert caps["configured_providers"] == []
    finally:
        provider_mod.clear_auth_failures()


def test_disabling_a_provider_outranks_a_cached_rejection(store):
    """Turning a provider off must explain itself, not blame its withheld key."""
    from src.ai.llm import provider as provider_mod

    _save_key("openai", "sk-rejected-key")
    integrations_store.update_ai_provider("openai", {"enabled": False})
    provider_mod._mark_provider_auth_failed("openai", "Error code: 401 - invalid_api_key")
    try:
        row = next(
            p
            for p in provider_mod.get_model_capabilities()["providers"]
            if p["provider"] == "openai"
        )
        assert row["available"] is False
        assert row["blocked_reason"] == "Disabled in Settings — enable it to let Pilot use it."
    finally:
        provider_mod.clear_auth_failures()


def test_key_verification_is_bounded_and_reports_a_timeout(store, monkeypatch):
    """Someone is waiting on the button, so a hung provider must not hang it."""
    from src.ai.llm import provider as provider_mod

    class _Timeout(Exception):
        def __str__(self) -> str:
            return "Request timed out."

    class _Models:
        def list(self):
            raise _Timeout

    class _FakeOpenAI:
        seen: dict[str, object] = {}

        def __init__(self, **kwargs):
            _FakeOpenAI.seen = kwargs
            self.models = _Models()

    import openai

    monkeypatch.setattr(openai, "OpenAI", _FakeOpenAI)
    provider_mod.clear_auth_failures()
    try:
        ok, err = provider_mod.verify_cloud_api_key("openai", "sk-slow-key")
        assert ok is False
        assert "did not answer within" in err
        assert _FakeOpenAI.seen["max_retries"] == 0
        assert _FakeOpenAI.seen["timeout"] == provider_mod.VERIFY_TIMEOUT_SECONDS
        # A timeout is not proof of a bad key, so the provider stays usable.
        assert provider_mod._provider_auth_failed("openai") is False
    finally:
        provider_mod.clear_auth_failures()


def test_credential_and_engine_routes_need_workspace_administration():
    """A viewer or editor cannot change whose model answers, or with which key."""
    from services.rbac import Permission, _required_permission, role_permissions

    for method, path in (
        ("PATCH", "/api/v1/workspace/ai-providers/openai"),
        ("POST", "/api/v1/workspace/ai-providers/openai/test"),
        ("PATCH", "/api/v1/workspace/pilot-engine"),
        ("GET", "/api/v1/workspace/pilot-engine"),
    ):
        assert _required_permission(method, path) == Permission.WORKSPACE_MANAGE

    assert Permission.WORKSPACE_MANAGE not in role_permissions("editor")
    assert Permission.WORKSPACE_MANAGE not in role_permissions("viewer")
    assert Permission.WORKSPACE_MANAGE in role_permissions("admin")
