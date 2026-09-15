"""Provider key resolution must reject masked or corrupted sentinel values."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


from services import integrations_store


def test_resolve_provider_api_key_prefers_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-env-key")
    # Point the store to a temp file so no stored key interferes.
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    assert integrations_store.resolve_provider_api_key("openai") == "sk-env-key"


def test_resolve_provider_api_key_rejects_masked_stored_value(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "")
    store = tmp_path / "integrations.json"
    store.write_text(
        '{"sso": {}, "ai_providers": {"openai": {"enabled": true, "api_key": "••••••••"}}}'
    )
    monkeypatch.setattr(integrations_store, "STORE_PATH", store)
    assert integrations_store.resolve_provider_api_key("openai") == ""


def test_resolve_provider_api_key_rejects_decryption_failed_sentinel(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "")
    store = tmp_path / "integrations.json"
    store.write_text(
        '{"sso": {}, "ai_providers": {"openai": {"enabled": true, "api_key": "[decryption-failed]"}}}'
    )
    monkeypatch.setattr(integrations_store, "STORE_PATH", store)
    assert integrations_store.resolve_provider_api_key("openai") == ""


def test_apply_integrations_to_env_does_not_set_sentinel(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "")
    store = tmp_path / "integrations.json"
    store.write_text(
        '{"sso": {}, "ai_providers": {"openai": {"enabled": true, "api_key": "[encrypted-secret-unavailable]"}}}'
    )
    monkeypatch.setattr(integrations_store, "STORE_PATH", store)
    integrations_store.apply_integrations_to_env()
    assert os.environ.get("OPENAI_API_KEY") != "[encrypted-secret-unavailable]"
    assert not os.environ.get("OPENAI_API_KEY")


def test_resolve_provider_api_key_survives_secrets_key_rotation(tmp_path, monkeypatch):
    """A ciphertext the current key cannot open must read as 'no key', not a 500."""
    monkeypatch.setenv("OPENAI_API_KEY", "")
    monkeypatch.setenv("DATAFLOW_ENV", "development")
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "integrations.json")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "first-secrets-key-first-secrets-key")
    integrations_store.update_ai_provider("openai", {"api_key": "sk-live-1234567890"})
    assert integrations_store.provider_key_state("openai") == "ready"

    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "rotated-secrets-key-rotated-secrets")
    monkeypatch.setenv("OPENAI_API_KEY", "")  # saving hydrated the process env; a restart would not
    assert integrations_store.resolve_provider_api_key("openai") == ""
    assert integrations_store.provider_key_state("openai") == "undecryptable"
    row = integrations_store.get_ai_provider_configs()["openai"]
    assert row["configured"] is False
    assert row["key_state"] == "undecryptable"


def test_storage_status_flags_unmounted_railway_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(integrations_store, "STORE_PATH", tmp_path / "data" / "integrations.json")
    monkeypatch.delenv("RAILWAY_ENVIRONMENT", raising=False)
    assert integrations_store.storage_status()["persistent"] is True

    monkeypatch.setenv("RAILWAY_ENVIRONMENT", "production")
    status = integrations_store.storage_status()
    assert status["persistent"] is False
    assert "volume" in status["reason"]
