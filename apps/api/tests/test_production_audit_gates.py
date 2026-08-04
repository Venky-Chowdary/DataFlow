"""Security + throughput regression tests for production audit gates."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_mcp_tools_call_requires_auth_when_enforced(monkeypatch):
    monkeypatch.setattr("src.services.auth_service.auth_required", lambda: True)
    monkeypatch.setattr("src.middleware.auth_middleware.auth_required", lambda: True)
    monkeypatch.setattr("services.rbac.auth_required", lambda: True)
    monkeypatch.setattr(
        "src.routers.mcp_router._mcp_authenticated",
        lambda _req: False,
    )

    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/mcp/tools/call",
            json={"name": "get_transfer_capabilities", "arguments": {}},
        )
    assert response.status_code == 401, response.text


def test_production_bans_allow_dev_user(monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAWRAP_ALLOW_DEV_USER", "1")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "1")
    monkeypatch.setenv("DATAWRAP_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DATAWRAP_AUTH_SECRET", "x" * 40)
    monkeypatch.setenv("DATAWRAP_SECRETS_KEY", "y" * 40)
    monkeypatch.setenv("DATAWRAP_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAWRAP_ADMIN_PASSWORD", "admin-password-long")

    from services import platform_config

    assert platform_config.is_production() is True
    errors = platform_config.validate_production_config()
    assert any("ALLOW_DEV_USER" in e for e in errors)


def test_dev_user_never_loaded_in_production(monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAWRAP_ALLOW_DEV_USER", "1")
    monkeypatch.setenv("DATAWRAP_ADMIN_EMAIL", "")
    monkeypatch.setenv("DATAWRAP_ADMIN_PASSWORD", "")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "")
    monkeypatch.setenv("DATAWRAP_AUTH_USERS", "")
    monkeypatch.setenv("DATAFLOW_AUTH_USERS", "")

    import importlib

    import src.services.auth_service as auth_svc

    importlib.reload(auth_svc)
    users = auth_svc._load_users()
    assert users == []
    assert all(u.get("email") != "test@gmail.com" for u in users)


def test_vault_refuses_plaintext_passthrough_in_production(monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "production")
    monkeypatch.setenv("DATAFLOW_ENV", "production")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "k" * 32)
    monkeypatch.setenv("DATAWRAP_SECRETS_KEY", "k" * 32)

    from services import secret_vault

    secret_vault._vault_instance = None
    with pytest.raises(secret_vault.SecretVaultError):
        secret_vault.decrypt_secret("literally-plaintext-password")


def test_transfer_request_encrypts_secrets_at_rest(monkeypatch):
    monkeypatch.setenv("DATAWRAP_ENV", "dev")
    monkeypatch.setenv("DATAFLOW_ENV", "dev")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "k" * 32)
    monkeypatch.setenv("DATAWRAP_SECRETS_KEY", "k" * 32)

    from services import secret_vault
    from src.transfer.models import (
        EndpointConfig,
        TransferRequest,
        transfer_request_from_dict,
        transfer_request_to_dict,
    )

    secret_vault._vault_instance = None
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="db.example.com",
            database="app",
            username="u",
            password="super-secret-db-password",
        ),
        mappings=[{"source": "a", "target": "a", "confidence": 1.0}],
    )
    stored = transfer_request_to_dict(req)
    assert stored["destination"]["password"].startswith("enc:")
    assert "super-secret-db-password" not in stored["destination"]["password"]

    restored = transfer_request_from_dict(stored)
    assert restored.destination.password == "super-secret-db-password"


def test_assert_mappings_executable_blocks_review_rows():
    from services.mapping_pipeline import assert_mappings_executable

    assert_mappings_executable(
        [{"source": "a", "target": "a", "requires_review": False}]
    )
    with pytest.raises(ValueError, match="require review"):
        assert_mappings_executable(
            [{"source": "a", "target": "b", "requires_review": True}]
        )
    # approved review row with no fidelity risk still executable
    assert_mappings_executable(
        [{"source": "a", "target": "b", "requires_review": True, "approved": True}]
    )


def test_exact_name_calibrated_confidence_caps_on_review():
    from services.semantic_mapper import _calibrated_confidence

    conf = _calibrated_confidence(0.99, score_gap=0.02, requires_review=True)
    assert conf <= 0.9


def test_tenant_custom_origin_allowed(monkeypatch):
    from services import cors_policy

    fake = MagicMock()
    fake.custom_domain = "data.acme.example"
    monkeypatch.setattr(
        "services.tenant_store.get_tenant_by_domain",
        lambda host: fake if host == "data.acme.example" else None,
    )
    assert cors_policy.tenant_custom_origin_allowed("https://data.acme.example") is True
    assert cors_policy.tenant_custom_origin_allowed("https://evil.example") is False


def test_snowflake_warehouse_rejects_injection():
    from connectors.snowflake import test_snowflake

    result = test_snowflake(
        host="xy12345",
        port=443,
        database="DB",
        username="u",
        password="p",
        schema="PUBLIC",
        connection_string="",
        ssl=True,
        warehouse='WH"; DROP TABLE users; --',
    )
    assert result.ok is False
    assert "Invalid" in (result.error or "")
