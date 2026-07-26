"""Secret vault backend tests — Fernet and AWS Secrets Manager references."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from services import secret_vault


def test_fernet_round_trip(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "fernet")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "x" * 32)
    monkeypatch.setenv("DATAFLOW_ENV", "dev")
    # reset singleton
    secret_vault._vault_instance = None
    plain = "super-secret-value"
    encrypted = secret_vault.encrypt_secret(plain, label="test")
    assert encrypted.startswith("enc:v1:")
    assert secret_vault.decrypt_secret(encrypted) == plain


def test_fernet_returns_existing_reference_unchanged(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "fernet")
    monkeypatch.setenv("DATAFLOW_SECRETS_KEY", "x" * 32)
    monkeypatch.setenv("DATAFLOW_ENV", "dev")
    secret_vault._vault_instance = None
    token = "enc:v1:abc"
    assert secret_vault.encrypt_secret(token) == token
    assert secret_vault.encrypt_secret("enc:v0:abc") == "enc:v0:abc"


def test_aws_secrets_manager_round_trip(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "aws_secretsmanager")
    monkeypatch.setenv("DATAFLOW_TENANT_ID", "acme-corp")
    monkeypatch.setenv("DATAFLOW_SECRETS_MANAGER_PREFIX", "dataflow/prod")
    secret_vault._vault_instance = None

    mock_client = MagicMock()
    mock_client.create_secret.return_value = {
        "ARN": "arn:aws:secretsmanager:us-east-1:123456789012:secret:dataflow/prod/acme-corp/test-abc123",
        "Name": "dataflow/prod/acme-corp/test-abc123",
        "VersionId": "v1",
    }
    mock_client.get_secret_value.return_value = {"SecretString": "plain-aws-secret"}

    with patch("boto3.client", return_value=mock_client):
        encrypted = secret_vault.encrypt_secret("plain-aws-secret", label="test")
        assert encrypted.startswith("sm:")
        assert "acme-corp" in encrypted
        decrypted = secret_vault.decrypt_secret(encrypted)
        assert decrypted == "plain-aws-secret"

    call = mock_client.create_secret.call_args.kwargs
    assert call["Name"].startswith("dataflow/prod/acme-corp/test-")
    assert call["SecretString"] == "plain-aws-secret"
    mock_client.get_secret_value.assert_called_once_with(
        SecretId="dataflow/prod/acme-corp/test-abc123",
        VersionId="v1",
    )


def test_aws_secrets_manager_decrypt_without_version_id(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "aws_secretsmanager")
    monkeypatch.setenv("DATAFLOW_TENANT_ID", "global")
    secret_vault._vault_instance = None

    mock_client = MagicMock()
    mock_client.get_secret_value.return_value = {"SecretString": "fallback-secret"}

    with patch("boto3.client", return_value=mock_client):
        decrypted = secret_vault.decrypt_secret("sm:dataflow/prod/global/conn-1234")
        assert decrypted == "fallback-secret"

    mock_client.get_secret_value.assert_called_once_with(SecretId="dataflow/prod/global/conn-1234")


def test_aws_secrets_manager_respects_kms_key_id(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "aws_secretsmanager")
    monkeypatch.setenv("DATAFLOW_SECRETS_KMS_KEY_ID", "arn:aws:kms:us-east-1:123:key/abc")
    secret_vault._vault_instance = None

    mock_client = MagicMock()
    mock_client.create_secret.return_value = {"Name": "secret-name", "VersionId": "v1"}

    with patch("boto3.client", return_value=mock_client):
        secret_vault.encrypt_secret("x", label="x")

    assert mock_client.create_secret.call_args.kwargs["KmsKeyId"] == "arn:aws:kms:us-east-1:123:key/abc"


def test_fernet_decrypt_legacy_base64_in_dev(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SECRETS_BACKEND", "fernet")
    monkeypatch.setenv("DATAFLOW_ENV", "dev")
    secret_vault._vault_instance = None
    import base64

    encoded = "enc:v0:" + base64.urlsafe_b64encode(b"legacy").decode("ascii")
    assert secret_vault.decrypt_secret(encoded) == "legacy"
