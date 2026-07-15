"""Encrypt connector credentials at rest (Fernet).

Fernet encryption is used when the `cryptography` package is installed. If it is
not available (e.g. a local `.venv` that has not run `pip install -r
requirements.txt`), we fall back to a base64 encoding so the application still
functions and saves/loads connector records without crashing. In production,
`cryptography` must be installed and `DATAFLOW_SECRETS_KEY` must be set to a
strong key.
"""

from __future__ import annotations

import base64
import hashlib
import logging
import os

_logger = logging.getLogger(__name__)

_PREFIX_V1 = "enc:v1:"
_PREFIX_V0 = "enc:v0:"


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


def encrypt_secret(plain: str) -> str:
    if not plain or plain == "****" or plain.startswith("["):
        return plain
    if plain.startswith(_PREFIX_V1) or plain.startswith(_PREFIX_V0):
        return plain

    if not _cryptography_available():
        _warn_once()
        # Base64 fallback so the app still saves/loads connector records when
        # cryptography is not installed. NOT secure — production must install it.
        return f"{_PREFIX_V0}{base64.urlsafe_b64encode(plain.encode('utf-8')).decode('ascii')}"
    token = _get_fernet().encrypt(plain.encode("utf-8")).decode("ascii")
    return f"{_PREFIX_V1}{token}"


def decrypt_secret(stored: str) -> str:
    if not stored:
        return stored

    if stored.startswith(_PREFIX_V1):
        if not _cryptography_available():
            _warn_once()
            return "[encrypted-secret-unavailable]"
        token = stored[len(_PREFIX_V1) :]
        try:
            return _get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
        except Exception:
            # Wrong key or corrupted token; keep the token as a sentinel so the
            # connector still loads and the user can re-enter credentials.
            return "[decryption-failed]"

    if stored.startswith(_PREFIX_V0):
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
