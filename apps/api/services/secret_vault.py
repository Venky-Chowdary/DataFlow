"""Encrypt connector credentials at rest (Fernet)."""

from __future__ import annotations

import base64
import hashlib
import os

_PREFIX = "enc:v1:"


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


def encrypt_secret(plain: str) -> str:
    if not plain or plain == "****":
        return plain
    if plain.startswith(_PREFIX):
        return plain
    token = _get_fernet().encrypt(plain.encode("utf-8")).decode("ascii")
    return f"{_PREFIX}{token}"


def decrypt_secret(stored: str) -> str:
    if not stored:
        return stored
    if stored.startswith(_PREFIX):
        token = stored[len(_PREFIX) :]
        return _get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
    return stored


def encrypt_connection_string(conn_str: str) -> str:
    return encrypt_secret(conn_str)


def decrypt_connection_string(stored: str) -> str:
    return decrypt_secret(stored)
