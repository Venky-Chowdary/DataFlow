"""Encrypt connector credentials at rest.

Three backends are supported:

* ``fernet`` (default) — Fernet symmetric encryption using ``DATAFLOW_SECRETS_KEY``.
* ``aws_secretsmanager`` — AWS Secrets Manager per-tenant secret storage.
* ``env`` (development only) — base64 ``enc:v0:`` fallback.

Per-tenant isolation in AWS is achieved by including ``DATAFLOW_TENANT_ID`` in
the secret name and requiring a unique secret for every value. In production,
the fail-closed policy is the same: no cryptography / no vault access means no
secret reads/writes.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os
from services.brand_env import getenv_brand
import uuid
from abc import ABC, abstractmethod
from typing import Any

_logger = logging.getLogger(__name__)

_PREFIX_V1 = "enc:v1:"
_PREFIX_V0 = "enc:v0:"
_PREFIX_SM = "sm:"


class SecretVaultError(RuntimeError):
    """Raised when production secret policy is violated."""


def _is_production() -> bool:
    try:
        from services.platform_config import is_production

        return bool(is_production())
    except Exception:
        env = getenv_brand("ENV", os.getenv("ENVIRONMENT", "")).lower()
        return env in ("production", "prod")


def _tenant_id() -> str:
    return (getenv_brand("TENANT_ID") or "global").strip() or "global"


def _secrets_manager_prefix() -> str:
    return (getenv_brand("SECRETS_MANAGER_PREFIX") or "dataflow").strip("/")


class SecretVault(ABC):
    @abstractmethod
    def secrets_encryption_ready(self) -> bool:
        ...

    @abstractmethod
    def encrypt(self, plain: str, *, tenant_id: str | None = None, label: str = "") -> str:
        ...

    @abstractmethod
    def decrypt(self, stored: str, *, tenant_id: str | None = None) -> str:
        ...


class FernetVault(SecretVault):
    """Default symmetric encryption using Fernet."""

    def secrets_encryption_ready(self) -> bool:
        try:
            import cryptography.fernet  # noqa: F401
        except Exception:
            return False
        if _is_production() and not _has_dedicated_secrets_key():
            auth = getenv_brand("AUTH_SECRET", "")
            return bool(auth and auth != "dev-change-me-before-production")
        return True

    def encrypt(self, plain: str, *, tenant_id: str | None = None, label: str = "") -> str:
        return _fernet_encrypt(plain)

    def decrypt(self, stored: str, *, tenant_id: str | None = None) -> str:
        return _fernet_decrypt(stored)


class AwsSecretsManagerVault(SecretVault):
    """Per-tenant secret storage backed by AWS Secrets Manager.

    Each call to ``encrypt`` creates a new secret version under a tenant-scoped
    name. The returned reference has the form ``sm:<secret-arn>:<version-id>``
    and is opaque to callers.
    """

    def __init__(self) -> None:
        self._client: Any | None = None
        self._region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1").strip()

    def _get_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            import boto3
        except Exception as exc:
            raise SecretVaultError("boto3 is not installed; cannot use AWS Secrets Manager backend") from exc
        try:
            self._client = boto3.client("secretsmanager", region_name=self._region)
        except Exception as exc:
            raise SecretVaultError(f"Failed to create AWS Secrets Manager client: {exc}") from exc
        return self._client

    def secrets_encryption_ready(self) -> bool:
        if _is_production():
            try:
                self._get_client()
            except Exception:
                return False
        return True

    def encrypt(self, plain: str, *, tenant_id: str | None = None, label: str = "") -> str:
        if not plain or plain == "****" or plain.startswith("["):
            return plain
        if plain.startswith(_PREFIX_V1) or plain.startswith(_PREFIX_V0) or plain.startswith(_PREFIX_SM):
            return plain

        client = self._get_client()
        tenant = tenant_id or _tenant_id()
        prefix = _secrets_manager_prefix()
        secret_name = f"{prefix}/{tenant}/{label or 'secret'}-{uuid.uuid4().hex[:16]}"
        try:
            create_kwargs = {
                "Name": secret_name,
                "Description": "Datawrap connector credential",
                "SecretString": plain,
            }
            kms_key_id = getenv_brand("SECRETS_KMS_KEY_ID", "").strip()
            if kms_key_id:
                create_kwargs["KmsKeyId"] = kms_key_id
            resp = client.create_secret(**create_kwargs)
            version_id = resp.get("VersionId", "")
            secret_id = resp.get("Name", secret_name)
            return f"{_PREFIX_SM}{secret_id}:{version_id}"
        except Exception as exc:
            raise SecretVaultError(f"AWS Secrets Manager create_secret failed: {exc}") from exc

    def decrypt(self, stored: str, *, tenant_id: str | None = None) -> str:
        if not stored:
            return stored
        if not stored.startswith(_PREFIX_SM):
            return _fernet_decrypt(stored)

        client = self._get_client()
        ref = stored[len(_PREFIX_SM) :]
        parts = ref.split(":", 1)
        arn = parts[0]
        version_id = parts[1] if len(parts) > 1 else ""
        try:
            kwargs = {"SecretId": arn}
            if version_id:
                kwargs["VersionId"] = version_id
            resp = client.get_secret_value(**kwargs)
            return resp.get("SecretString", "") or resp.get("SecretBinary", b"").decode("utf-8")
        except Exception as exc:
            raise SecretVaultError(f"AWS Secrets Manager get_secret_value failed: {exc}") from exc


_vault_instance: SecretVault | None = None


def _get_vault() -> SecretVault:
    global _vault_instance
    if _vault_instance is None:
        backend = (getenv_brand("SECRETS_BACKEND") or "fernet").lower().strip()
        if backend == "aws_secretsmanager":
            _vault_instance = AwsSecretsManagerVault()
        else:
            _vault_instance = FernetVault()
    return _vault_instance


def _has_dedicated_secrets_key() -> bool:
    return bool(getenv_brand("SECRETS_KEY", "").strip())


def _fernet_key() -> bytes:
    raw = getenv_brand("SECRETS_KEY", "").strip()
    if raw:
        try:
            decoded = base64.urlsafe_b64decode(raw + "==")
            if len(decoded) == 32:
                return base64.urlsafe_b64encode(decoded)
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
        if len(raw.encode()) >= 32:
            return base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    auth = getenv_brand("AUTH_SECRET", "dev-change-me-before-production")
    return base64.urlsafe_b64encode(hashlib.sha256(auth.encode()).digest())


def _get_fernet() -> Any:
    from cryptography.fernet import Fernet

    return Fernet(_fernet_key())


def _cryptography_available() -> bool:
    try:
        import cryptography.fernet  # noqa: F401
        return True
    except Exception:
        return False


def _warn_once() -> None:
    if not getattr(_warn_once, "done", False):
        _logger.warning(
            "cryptography is not installed in this Python environment. "
            "Connector secrets are being stored with a base64 fallback, "
            "which is NOT secure. Run `pip install -r requirements.txt` "
            "to enable real Fernet encryption."
        )
        _warn_once.done = True  # type: ignore[attr-defined]


def secrets_encryption_ready() -> bool:
    """True when the active vault can encrypt under current policy."""
    return _get_vault().secrets_encryption_ready()


def encrypt_secret(plain: str, *, tenant_id: str | None = None, label: str = "") -> str:
    """Encrypt/store a secret and return an opaque reference."""
    if not plain or plain == "****" or plain.startswith("["):
        return plain
    if plain.startswith(_PREFIX_V1) or plain.startswith(_PREFIX_V0) or plain.startswith(_PREFIX_SM):
        return plain
    return _get_vault().encrypt(plain, tenant_id=tenant_id, label=label)


def decrypt_secret(stored: str, *, tenant_id: str | None = None) -> str:
    """Retrieve/decrypt a stored secret reference."""
    if not stored:
        return stored
    return _get_vault().decrypt(stored, tenant_id=tenant_id)


def _fernet_encrypt(plain: str) -> str:
    if not _cryptography_available():
        if _is_production():
            raise SecretVaultError(
                "Production refuses insecure secret storage. Install cryptography "
                "and set DATAFLOW_SECRETS_KEY before saving connector credentials."
            )
        _warn_once()
        return f"{_PREFIX_V0}{base64.urlsafe_b64encode(plain.encode('utf-8')).decode('ascii')}"

    if _is_production() and not _has_dedicated_secrets_key():
        auth = getenv_brand("AUTH_SECRET", "")
        if not auth or auth == "dev-change-me-before-production":
            raise SecretVaultError(
                "Production requires DATAFLOW_SECRETS_KEY (or a strong DATAFLOW_AUTH_SECRET) "
                "for Fernet encryption of connector credentials."
            )

    token = _get_fernet().encrypt(plain.encode("utf-8")).decode("ascii")
    return f"{_PREFIX_V1}{token}"


def _fernet_decrypt(stored: str) -> str:
    if stored.startswith(_PREFIX_V1):
        if not _cryptography_available():
            if _is_production():
                raise SecretVaultError(
                    "Cannot decrypt Fernet secrets: cryptography is not installed in production."
                )
            _warn_once()
            return "[encrypted-secret-unavailable]"
        token = stored[len(_PREFIX_V1) :]
        try:
            return _get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
        except Exception:
            return "[decryption-failed]"

    if stored.startswith(_PREFIX_V0):
        if _is_production():
            raise SecretVaultError(
                "Production refuses to read legacy base64 (enc:v0) secrets. "
                "Re-save connectors after enabling Fernet encryption."
            )
        token = stored[len(_PREFIX_V0) :]
        try:
            return base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8")
        except Exception:
            return stored

    return stored


def encrypt_connection_string(conn_str: str, *, tenant_id: str | None = None) -> str:
    return encrypt_secret(conn_str, tenant_id=tenant_id, label="connection-string")


def decrypt_connection_string(stored: str, *, tenant_id: str | None = None) -> str:
    return decrypt_secret(stored, tenant_id=tenant_id)
