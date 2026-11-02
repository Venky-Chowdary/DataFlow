"""Phase D2–D6 security hardening — tenant bind, sessions, secrets, roles, SQL guard."""

from __future__ import annotations

import base64
import hashlib
import hmac
import sys
import time
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


@pytest.fixture
def isolated_data(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAWRAP_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "unit-test-secret-value-phase-d")
    monkeypatch.setenv("DATAFLOW_ADMIN_EMAIL", "admin@example.com")
    monkeypatch.setenv("DATAFLOW_ADMIN_PASSWORD", "strong-password-123")
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_AUTH_LEGACY_TOKENS", "0")
    monkeypatch.delenv("DATAFLOW_AUTH_USERS", raising=False)
    monkeypatch.delenv("DATAWRAP_AUTH_USERS", raising=False)


def test_d2_tenant_bind_strict_refuses_cross_tenant(isolated_data, monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_TENANT_BIND", "strict")
    from services.tenant_bind import principal_allowed_for_tenant

    user = {"email": "a@b.c", "tenant_ids": ["tenant-a"]}
    assert principal_allowed_for_tenant(user, "tenant-a") is True
    assert principal_allowed_for_tenant(user, "tenant-b") is False
    assert principal_allowed_for_tenant({"email": "x"}, "tenant-a") is False


def test_d2_soft_allows_unclaimed_identity(isolated_data, monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_TENANT_BIND", "soft")
    from services.tenant_bind import principal_allowed_for_tenant

    assert principal_allowed_for_tenant({"email": "x"}, "tenant-a") is True
    assert principal_allowed_for_tenant({"email": "x", "tenant_id": "t1"}, "t2") is False


def test_d3_logout_revokes_session(isolated_data):
    from src.services.auth_service import create_token, revoke_token, verify_token

    token, _ = create_token("admin@example.com")
    assert verify_token(token) == "admin@example.com"
    assert revoke_token(token) is True
    assert verify_token(token) is None


def test_d3_password_rotate_revokes_all(isolated_data):
    from src.services.auth_service import create_token, revoke_sessions_for_email, verify_token

    t1, _ = create_token("admin@example.com")
    t2, _ = create_token("admin@example.com")
    assert revoke_sessions_for_email("admin@example.com") >= 2
    assert verify_token(t1) is None
    assert verify_token(t2) is None


def test_d3_legacy_token_rejected_when_disabled(isolated_data, monkeypatch):
    monkeypatch.setenv("DATAFLOW_AUTH_LEGACY_TOKENS", "0")
    from src.services import auth_service as auth_mod

    email = "admin@example.com"
    expires = int(time.time()) + 3600
    payload = f"{email}:{expires}"
    sig = hmac.new(
        auth_mod._token_secret().encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    legacy = f"{payload}:{sig}"
    assert auth_mod.verify_token(legacy) is None


def test_d4_decrypt_raises_not_sentinel(isolated_data, monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "phase-d-secrets-key-32bytes!!")
    from services import secret_vault

    secret_vault._vault_instance = None
    enc = secret_vault.encrypt_secret("super-secret")
    assert enc.startswith("enc:v1:")
    assert secret_vault.decrypt_secret(enc) == "super-secret"

    with pytest.raises(secret_vault.SecretVaultError):
        secret_vault.decrypt_secret("enc:v1:" + base64.urlsafe_b64encode(b"not-a-token").decode())


def test_d4_legacy_sha256_auth_key_still_decrypts(isolated_data, monkeypatch):
    """Secrets encrypted with pre-HKDF SHA256(AUTH) still open via legacy candidates."""
    monkeypatch.setenv("DATAFLOW_ENV", "dev")
    monkeypatch.delenv("DATAFLOW_SECRETS_KEY", raising=False)
    monkeypatch.delenv("DATAWRAP_SECRETS_KEY", raising=False)
    monkeypatch.setenv("DATAFLOW_AUTH_SECRET", "legacy-auth-secret-material")

    from cryptography.fernet import Fernet
    from services import secret_vault

    secret_vault._vault_instance = None
    legacy_key = base64.urlsafe_b64encode(
        hashlib.sha256(b"legacy-auth-secret-material").digest()
    )
    token = Fernet(legacy_key).encrypt(b"old-password").decode("ascii")
    stored = f"enc:v1:{token}"
    assert secret_vault.decrypt_secret(stored) == "old-password"


def test_d5_dev_user_requires_opt_in(isolated_data, monkeypatch):
    monkeypatch.delenv("DATAFLOW_ADMIN_EMAIL", raising=False)
    monkeypatch.delenv("DATAFLOW_ADMIN_PASSWORD", raising=False)
    monkeypatch.delenv("DATAWRAP_ADMIN_EMAIL", raising=False)
    monkeypatch.delenv("DATAWRAP_ADMIN_PASSWORD", raising=False)
    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "0")
    monkeypatch.setenv("DATAFLOW_ENV", "development")

    from src.services.auth_service import _load_users

    assert _load_users() == []

    monkeypatch.setenv("DATAFLOW_ALLOW_DEV_USER", "1")
    users = _load_users()
    assert len(users) == 1
    assert users[0]["email"] == "test@gmail.com"


def test_d5_unknown_role_is_viewer_not_editor():
    from services.rbac import normalize_role

    assert normalize_role("Workspace tester") == "viewer"
    assert normalize_role("admin") == "admin"
    assert normalize_role("totally-made-up") == "viewer"


def test_d6_copilot_sql_guard_blocks_unknown_columns():
    from services.copilot_sql_guard import assert_identifiers_allowed, schema_allowlist

    allowed = schema_allowlist(
        [{"name": "id"}, {"name": "amount"}, {"name": "status"}],
        [{"name": "orders"}],
    )
    assert_identifiers_allowed("SELECT id, amount FROM orders", allowed=allowed)
    # Result aliases are not schema identifiers (Pilot aggregate SQL).
    assert_identifiers_allowed(
        "SELECT status, COUNT(*) AS n FROM orders GROUP BY status",
        allowed=allowed,
    )
    with pytest.raises(ValueError, match="not in introspected schema"):
        assert_identifiers_allowed("SELECT ssn FROM orders", allowed=allowed)
