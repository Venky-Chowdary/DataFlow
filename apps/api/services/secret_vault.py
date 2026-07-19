"""Encrypt connector credentials at rest (Fernet).

Fernet encryption is used when the `cryptography` package is installed. In
development, a base64 ``enc:v0:`` fallback keeps local setups working when
cryptography is missing. In production, encryption fail-closes: cryptography
must be installed and ``DATAFLOW_SECRETS_KEY`` (or a strong auth secret) must
be set — base64 writes and reads are rejected.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os

_logger = logging.getLogger(__name__)

_PREFIX_V1 = "enc:v1:"
_PREFIX_V0 = "enc:v0:"


class SecretVaultError(RuntimeError):
    """Raised when production secret policy is violated."""


def _is_production() -> bool:
    try:
        from services.platform_config import is_production

        return bool(is_production())
    except Exception:
        env = os.getenv("DATAFLOW_ENV", os.getenv("ENVIRONMENT", "")).lower()
        return env in ("production", "prod")


def _has_dedicated_secrets_key() -> bool:
    return bool(os.getenv("DATAFLOW_SECRETS_KEY", "").strip())


def _fernet_key() -> bytes:
    raw = os.getenv("DATAFLOW_SECRETS_KEY", "").strip()
    if raw:
        try:
            decoded = base64.urlsafe_b64decode(raw + "==")
            if len(decoded) == 32:
                return base64.urlsafe_b64encode(decoded)
        except Exception:
            pass
        if len(raw.encode()) >= 32:
            return base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest())
    auth = os.getenv("DATAFLOW_AUTH_SECRET", "dev-change-me-before-production")
    return base64.urlsafe_b64encode(hashlib.sha256(auth.encode()).digest())


def _get_fernet():
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
    """True when the vault can encrypt with Fernet under current policy."""
    if not _cryptography_available():
        return False
    if _is_production() and not _has_dedicated_secrets_key():
        # Production may still derive from AUTH_SECRET if validate_production_config allows it,
        # but dedicated key is preferred. Ready if cryptography works and auth secret is strong.
        auth = os.getenv("DATAFLOW_AUTH_SECRET", "")
        return bool(auth and auth != "dev-change-me-before-production")
    return True


def encrypt_secret(plain: str) -> str:
    if not plain or plain == "****" or plain.startswith("["):
        return plain
    if plain.startswith(_PREFIX_V1) or plain.startswith(_PREFIX_V0):
        return plain

    if not _cryptography_available():
        if _is_production():
            raise SecretVaultError(
                "Production refuses insecure secret storage. Install cryptography "
                "and set DATAFLOW_SECRETS_KEY before saving connector credentials."
            )
        _warn_once()
        return f"{_PREFIX_V0}{base64.urlsafe_b64encode(plain.encode('utf-8')).decode('ascii')}"

    if _is_production() and not _has_dedicated_secrets_key():
        auth = os.getenv("DATAFLOW_AUTH_SECRET", "")
        if not auth or auth == "dev-change-me-before-production":
            raise SecretVaultError(
                "Production requires DATAFLOW_SECRETS_KEY (or a strong DATAFLOW_AUTH_SECRET) "
                "for Fernet encryption of connector credentials."
            )

    token = _get_fernet().encrypt(plain.encode("utf-8")).decode("ascii")
    return f"{_PREFIX_V1}{token}"


def decrypt_secret(stored: str) -> str:
    if not stored:
        return stored

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


def encrypt_connection_string(conn_str: str) -> str:
    return encrypt_secret(conn_str)


def decrypt_connection_string(stored: str) -> str:
    return decrypt_secret(stored)
