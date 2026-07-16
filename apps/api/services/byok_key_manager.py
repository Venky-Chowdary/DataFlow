"""Bring-your-own-key (BYOK) key manager for tenant-scoped encryption.

Each tenant may opt to supply a customer-managed encryption key.  The key
material is encrypted at rest with the platform master key (Fernet) so the
application can use it, but it is logically separate from the global platform
key.  Data for the tenant is encrypted with a derived key produced by HKDF
from the tenant key plus a context string (tenant id + purpose).

Providers:
- ``local``    : DataFlow generates a 256-bit key for the tenant.  Useful for
                 testing and for a "managed customer key" mode.
- ``wrapped``  : The customer pastes a base64-encoded 256-bit AES key.
                 The plaintext is encrypted with the platform key and stored.
- ``aws_kms``  : The key reference is an AWS KMS key ARN.  Envelope encryption
                 is performed by KMS; DataFlow only stores the encrypted data
                 key reference (not yet active until KMS integration is wired).
- ``azure_keyvault`` / ``gcp_kms`` : references to cloud KMS keys (roadmap).
"""

from __future__ import annotations

import base64
import json
import logging
import os
import secrets
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.secret_vault import decrypt_secret, encrypt_secret

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "byok_keys.json"

_PREFIX_V1 = "enc:v1:"
_PREFIX_V0 = "enc:v0:"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_raw() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"keys": []}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"keys": []}
        return raw
    except Exception:
        return {"keys": []}


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(STORE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(STORE_PATH)


@dataclass
class BYOKKey:
    id: str
    tenant_id: str
    label: str
    provider: str  # local, wrapped, aws_kms, azure_keyvault, gcp_kms
    key_reference: str = ""  # wrapped key material or cloud ARN
    status: str = "active"  # active, rotated, revoked
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BYOKKey":
        return cls(
            id=data.get("id", ""),
            tenant_id=data.get("tenant_id", ""),
            label=data.get("label", ""),
            provider=data.get("provider", "local"),
            key_reference=data.get("key_reference", ""),
            status=data.get("status", "active"),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
        )


def _is_platform_encrypted(value: str) -> bool:
    return bool(value and (value.startswith(_PREFIX_V1) or value.startswith(_PREFIX_V0)))


def _wrap_key_material(raw_bytes: bytes) -> str:
    """Encrypt raw key bytes with the platform master key."""
    encoded = base64.urlsafe_b64encode(raw_bytes).decode("ascii")
    return encrypt_secret(encoded)


def _unwrap_key_material(wrapped: str) -> bytes:
    """Return the raw key bytes from a platform-encrypted reference."""
    if _is_platform_encrypted(wrapped):
        plain = decrypt_secret(wrapped)
        if not plain or plain.startswith("["):
            raise RuntimeError("Unable to decrypt BYOK key material")
        return base64.urlsafe_b64decode(plain.encode("ascii") + "==")
    # Plain base64 (legacy/dev fallback)
    return base64.urlsafe_b64decode(wrapped.encode("ascii") + "==")


def _cryptography_available() -> bool:
    try:
        import cryptography.fernet  # noqa: F401
        import cryptography.hazmat.primitives.kdf.hkdf  # noqa: F401
        return True
    except Exception:
        return False


def _derive_key(raw_key: bytes, context: str) -> bytes:
    """Derive a 32-byte tenant+purpose-specific key using HKDF-SHA256."""
    if _cryptography_available():
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.kdf.hkdf import HKDF

        return HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=None,
            info=context.encode("utf-8"),
        ).derive(raw_key)
    # Fallback for environments without cryptography — not for production.
    import hashlib

    return hashlib.pbkdf2_hmac("sha256", raw_key, context.encode("utf-8"), 100000, dklen=32)


def _fernet_from_bytes(key_bytes: bytes):
    """Build a Fernet instance from 32 raw bytes."""
    from cryptography.fernet import Fernet

    b64_key = base64.urlsafe_b64encode(key_bytes).decode("ascii")
    return Fernet(b64_key)


def _generate_32_bytes() -> bytes:
    return secrets.token_bytes(32)


def _validate_key_material(key_material: str | bytes | None, provider: str) -> bytes:
    """Normalize customer-supplied key material to 32 bytes."""
    if key_material is None:
        if provider == "local":
            return _generate_32_bytes()
        raise ValueError(f"Key material required for provider '{provider}'")

    if isinstance(key_material, bytes):
        raw = key_material
    else:
        key_material = key_material.strip()
        try:
            raw = base64.urlsafe_b64decode(key_material.encode("ascii") + "==")
        except Exception:
            raise ValueError("Key material must be a base64-encoded 256-bit value") from None

    if len(raw) not in (16, 24, 32):
        raise ValueError(f"Key material must be 16, 24, or 32 bytes; got {len(raw)}")
    if len(raw) != 32:
        # Expand smaller AES keys to 32 bytes with HKDF for Fernet compatibility.
        raw = _derive_key(raw, f"byok-key-stretch-{uuid.uuid4().hex[:8]}")
    return raw


def create_key(
    tenant_id: str,
    label: str,
    provider: str = "local",
    key_material: str | bytes | None = None,
) -> BYOKKey:
    """Create a BYOK key for a tenant.

    For ``local`` a random 256-bit key is generated.  For ``wrapped`` the caller
    supplies the raw key as base64 text or bytes.  For cloud KMS providers only
    the key reference (ARN/URL) is stored; data-key encryption is delegated.
    """
    provider = (provider or "local").lower().strip()
    allowed = {"local", "wrapped", "aws_kms", "azure_keyvault", "gcp_kms"}
    if provider not in allowed:
        raise ValueError(f"Unsupported BYOK provider: {provider}")

    if provider in ("aws_kms", "azure_keyvault", "gcp_kms"):
        if not key_material:
            raise ValueError(f"Cloud KMS key reference required for provider '{provider}'")
        reference = str(key_material).strip()
    else:
        raw = _validate_key_material(key_material, provider)
        reference = _wrap_key_material(raw)

    key = BYOKKey(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        label=(label or f"{provider} key").strip()[:128],
        provider=provider,
        key_reference=reference,
        status="active",
    )
    data = _load_raw()
    data["keys"].append(key.to_dict())
    _save(data)
    logger.info("Created BYOK key %s for tenant %s (provider=%s)", key.id, tenant_id, provider)
    return key


def list_keys(tenant_id: str | None = None) -> list[BYOKKey]:
    data = _load_raw()
    keys = [BYOKKey.from_dict(k) for k in data.get("keys", []) if isinstance(k, dict)]
    if tenant_id is not None:
        keys = [k for k in keys if k.tenant_id == tenant_id]
    return sorted(keys, key=lambda k: k.created_at, reverse=True)


def get_key(key_id: str) -> BYOKKey | None:
    for k in list_keys():
        if k.id == key_id:
            return k
    return None


def get_active_key_for_tenant(tenant_id: str) -> BYOKKey | None:
    for k in list_keys(tenant_id):
        if k.status == "active":
            return k
    return None


def delete_key(key_id: str) -> bool:
    data = _load_raw()
    before = len(data.get("keys", []))
    data["keys"] = [k for k in data.get("keys", []) if isinstance(k, dict) and k.get("id") != key_id]
    if len(data["keys"]) == before:
        return False
    _save(data)
    return True


def rotate_key(tenant_id: str, label: str | None = None, provider: str = "local") -> BYOKKey:
    """Mark existing active keys as rotated and create a new active key."""
    data = _load_raw()
    changed = False
    for raw in data.get("keys", []):
        if isinstance(raw, dict) and raw.get("tenant_id") == tenant_id and raw.get("status") == "active":
            raw["status"] = "rotated"
            raw["updated_at"] = _now()
            changed = True
    if changed:
        _save(data)
    new_label = label or "Rotated key"
    return create_key(tenant_id=tenant_id, label=new_label, provider=provider)


def _resolve_key(tenant_id: str, key_id: str | None = None) -> BYOKKey:
    if key_id:
        key = get_key(key_id)
    else:
        key = get_active_key_for_tenant(tenant_id)
    if not key:
        raise ValueError(f"No BYOK key configured for tenant {tenant_id}")
    if key.status != "active":
        raise ValueError(f"BYOK key {key.id} is {key.status}")
    if key.tenant_id != tenant_id:
        raise ValueError("BYOK key does not belong to tenant")
    return key


def _data_key_for_key(key: BYOKKey, context: str) -> bytes:
    """Return 32 raw bytes used for Fernet tenant encryption."""
    if key.provider in ("local", "wrapped"):
        raw = _unwrap_key_material(key.key_reference)
        return _derive_key(raw, context)
    raise NotImplementedError(f"Data key derivation not implemented for provider '{key.provider}'")


def tenant_encrypt(tenant_id: str, plaintext: str, key_id: str | None = None, purpose: str = "data") -> str:
    """Encrypt ``plaintext`` with the tenant's BYOK-derived key."""
    if not _cryptography_available():
        raise RuntimeError("BYOK encryption requires the cryptography package")
    key = _resolve_key(tenant_id, key_id)
    context = f"dataflow:{tenant_id}:{purpose}"
    data_key = _data_key_for_key(key, context)
    token = _fernet_from_bytes(data_key).encrypt(plaintext.encode("utf-8")).decode("ascii")
    return f"byok:{key.id}:{token}"


def tenant_decrypt(tenant_id: str, token: str, key_id: str | None = None, purpose: str = "data") -> str:
    """Decrypt a value produced by ``tenant_encrypt``."""
    if not _cryptography_available():
        raise RuntimeError("BYOK encryption requires the cryptography package")
    if token.startswith("byok:"):
        _, stored_key_id, ciphertext = token.split(":", 2)
        key_id = key_id or stored_key_id
    key = _resolve_key(tenant_id, key_id)
    context = f"dataflow:{tenant_id}:{purpose}"
    data_key = _data_key_for_key(key, context)
    return _fernet_from_bytes(data_key).decrypt(ciphertext.encode("ascii")).decode("utf-8")


def key_status_summary(tenant_id: str) -> dict[str, Any]:
    keys = list_keys(tenant_id)
    active = [k for k in keys if k.status == "active"]
    return {
        "configured": bool(keys),
        "active_count": len(active),
        "total_count": len(keys),
        "providers": sorted({k.provider for k in keys}),
        "rotated": any(k.status == "rotated" for k in keys),
    }


def is_ip_allowed(ip_str: str, allowlist: list[str]) -> bool:
    """Return True if ``ip_str`` is in any of the CIDR/network entries."""
    if not allowlist:
        return True
    if not ip_str:
        return False
    try:
        addr = ipaddress.ip_address(ip_str.split(",")[0].strip())
    except ValueError:
        return False
    for entry in allowlist:
        entry = entry.strip()
        if not entry:
            continue
        try:
            network = ipaddress.ip_network(entry, strict=False)
            if addr in network:
                return True
        except ValueError:
            continue
    return False
