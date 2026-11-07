"""Active BYOK wraps newly saved connector secrets — platform Fernet stays for leftovers."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services import byok_key_manager, connector_store, tenant_store
from services.secret_vault import decrypt_secret, encrypt_secret


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(connector_store, "STORE_PATH", tmp_path / "connectors.json")
    monkeypatch.setattr(connector_store, "_backend_choice", "file")
    monkeypatch.setattr(tenant_store, "STORE_PATH", tmp_path / "tenants.json")
    monkeypatch.setattr(byok_key_manager, "STORE_PATH", tmp_path / "byok_keys.json")
    return tmp_path


def test_without_byok_connector_uses_platform_fernet(isolated):
    conn = connector_store.create_connector(
        {
            "name": "No BYOK",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "plain-secret",
            "workspace_id": "ws-none",
        }
    )
    raw = json.loads((isolated / "connectors.json").read_text(encoding="utf-8"))
    stored = raw["connectors"][0]["password"]
    assert stored.startswith("enc:v1:")
    loaded = connector_store.get_connector(conn.id)
    assert loaded.password == "plain-secret"


def test_active_byok_wraps_new_connector_secret(isolated):
    tenant = tenant_store.create_tenant("ws-byok", "Acme")
    key = byok_key_manager.create_key(tenant.id, label="Production", provider="local")
    tenant_store.update_tenant(tenant.id, byok_key_id=key.id)

    conn = connector_store.create_connector(
        {
            "name": "BYOK Postgres",
            "type": "postgresql",
            "host": "127.0.0.1",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "customer-secret",
            "workspace_id": "ws-byok",
        }
    )
    raw = json.loads((isolated / "connectors.json").read_text(encoding="utf-8"))
    stored = raw["connectors"][0]["password"]
    assert stored.startswith(f"byok:{key.id}:")
    assert "customer-secret" not in stored
    loaded = connector_store.get_connector(conn.id)
    assert loaded.password == "customer-secret"


def test_rotated_byok_still_decrypts_old_ciphertext(isolated):
    tenant = tenant_store.create_tenant("ws-rot", "Rotate")
    first = byok_key_manager.create_key(tenant.id, label="v1", provider="local")
    wrapped = encrypt_secret("keep-me", tenant_id=tenant.id)
    assert wrapped.startswith(f"byok:{first.id}:")
    byok_key_manager.rotate_key(tenant.id, label="v2", provider="local")
    assert decrypt_secret(wrapped, tenant_id=tenant.id) == "keep-me"


def test_byok_key_material_is_not_wrapped_with_itself(isolated):
    tenant = tenant_store.create_tenant("ws-loop", "Loop")
    key = byok_key_manager.create_key(tenant.id, label="loop", provider="local")
    assert key.key_reference.startswith("enc:v1:") or key.key_reference.startswith("enc:v0:")
    public = byok_key_manager.public_key_dict(key)
    assert public["key_reference"] == "[wrapped]"
    assert "enc:v1:" not in public["key_reference"]


def test_byok_ciphertext_refuses_decrypt_without_tenant(isolated):
    from services.secret_vault import SecretVaultError

    tenant = tenant_store.create_tenant("ws-need-tid", "Need")
    byok_key_manager.create_key(tenant.id, label="need", provider="local")
    wrapped = encrypt_secret("hidden", tenant_id=tenant.id)
    assert wrapped.startswith("byok:")
    with pytest.raises(SecretVaultError):
        decrypt_secret(wrapped)
