"""Evidence signing keys: one keyring per workspace, separate from login (P0-1).

Proof packs, Migration Certificates, field-reduction ledgers, signed Trust
Reports and Exit Evidence Packs used to be HMAC-signed with the login token
secret (``auth_service._token_secret``), under a fixed ``key_id``
``platform_auth_secret``. The product called that "the workspace proof key".
It was neither a workspace key nor a proof key: anyone holding the login
secret could forge a pack, and rotating the login secret silently broke every
pack ever issued.

Now each workspace has its own evidence signing keyring:

* A key is 32 random bytes, generated on first use and stored encrypted
  (``services.secret_vault``, ``enc:v1`` — the deployment's secrets key, never
  the login secret) in the control-plane database, the same durable home the
  ``hash_pii`` keys use (:mod:`services.pii_hash_keys`); the JSON file under
  the data directory is the store when no database is configured.
* Work with no workspace (auth off, system jobs) uses the deployment keyring
  (scope ``__deployment__``).
* Every key has a ``key_id`` derived from the key (``ev1:`` + 16 hex of a
  domain-separated SHA-256); the id names the key, it does not reveal it.
  Every signature records ``key_id``, ``key_scope`` and ``workspace_id``.
* Rotation adds a new active version and keeps every earlier one (marked
  retired), so packs signed before a rotation keep verifying.
* Packs signed before this keyring existed carry ``key_id``
  ``platform_auth_secret`` (or none); they still verify against the login
  secret, reported as the legacy key. Nothing new is signed with it.

Key material never leaves this module except as the bytes an HMAC is computed
with; status and verify answers carry key ids only, and nothing here logs a key.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "evidence_signing_keys"
#: Keyring for work outside any workspace (auth off, system jobs).
DEPLOYMENT_SCOPE = "__deployment__"
#: The ``key_id`` packs carried when they were signed with the login secret.
LEGACY_KEY_ID = "platform_auth_secret"
_VAULT_LABEL = "evidence_signing_key"
_KEY_ID_LABEL = b"datawrap-evidence-signing-key-id\x00"
_LOCK = threading.Lock()

KEY_UNAVAILABLE_MESSAGE = (
    "The evidence signing key could not be loaded, so nothing was signed. "
    "An operator must make the secrets store reachable; the login secret is "
    "never used to sign evidence."
)


class EvidenceKeyUnavailable(RuntimeError):
    """The keyring could not be read or written. Message is user-safe."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def key_id_for(key: bytes) -> str:
    """A stable public name for ``key`` (it does not reveal the key)."""
    return "ev1:" + hashlib.sha256(_KEY_ID_LABEL + key).hexdigest()[:16]


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001 - no database = file store
        return None


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "evidence_signing_keys.json"


def scope_of(workspace_id: str | None) -> str:
    return str(workspace_id or "").strip() or DEPLOYMENT_SCOPE


def _load(scope: str) -> dict[str, Any] | None:
    coll = _collection()
    if coll is not None:
        doc = coll.find_one({"_id": scope})
        return dict(doc) if isinstance(doc, dict) else None
    from services.metadata_backend import load_json_doc

    data = load_json_doc(_file_path(), {"keys": {}})
    doc = (data.get("keys") or {}).get(scope)
    return dict(doc) if isinstance(doc, dict) else None


def _insert_if_absent(scope: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Store ``doc`` unless another writer got there first; return the winner."""
    coll = _collection()
    if coll is not None:
        from pymongo.errors import DuplicateKeyError

        try:
            coll.insert_one({"_id": scope, **doc})
        except DuplicateKeyError:
            pass
        return dict(coll.find_one({"_id": scope}) or doc)
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"keys": {}}) as data:
        keys = data.setdefault("keys", {})
        if scope not in keys:
            keys[scope] = doc
        return dict(keys[scope])


def _replace(scope: str, doc: dict[str, Any], *, expected_version: int) -> bool:
    coll = _collection()
    if coll is not None:
        result = coll.replace_one(
            {"_id": scope, "active_version": expected_version}, {"_id": scope, **doc}
        )
        return result.matched_count == 1
    from services.metadata_backend import json_doc_transaction

    with json_doc_transaction(_file_path(), {"keys": {}}) as data:
        keys = data.setdefault("keys", {})
        current = keys.get(scope) or {}
        if int(current.get("active_version") or 0) != expected_version:
            return False
        keys[scope] = doc
        return True


def _new_version(version: int, actor: str) -> dict[str, Any]:
    from services.secret_vault import encrypt_secret

    key = secrets.token_bytes(32)
    return {
        "version": version,
        "key_id": key_id_for(key),
        "key_enc": encrypt_secret(key.hex(), label=_VAULT_LABEL),
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _decrypt(entry: dict[str, Any]) -> bytes | None:
    from services.secret_vault import decrypt_secret

    try:
        key = bytes.fromhex(decrypt_secret(str(entry.get("key_enc") or "")))
    except Exception as exc:  # noqa: BLE001 - reported by id; never the key
        logger.error(
            "evidence signing key %s could not be decrypted: %s",
            entry.get("key_id"), type(exc).__name__,
        )
        return None
    if len(key) < 32 or key_id_for(key) != entry.get("key_id"):
        logger.error("evidence signing key %s does not match its id", entry.get("key_id"))
        return None
    return key


def _active_doc(scope: str) -> dict[str, Any]:
    doc = _load(scope)
    if doc is None:
        with _LOCK:
            doc = _load(scope) or _insert_if_absent(
                scope,
                {
                    "workspace_id": "" if scope == DEPLOYMENT_SCOPE else scope,
                    "active_version": 1,
                    "versions": [_new_version(1, "system")],
                    "created_at": _now(),
                },
            )
    return doc


def signing_key(workspace_id: str | None) -> tuple[str, bytes, int, str]:
    """``(key_id, key, version, scope)`` to sign with now for ``workspace_id``.

    Created on first use. Raises :class:`EvidenceKeyUnavailable` when the
    keyring cannot be read, created or decrypted — never a fallback key.
    """
    scope = scope_of(workspace_id)
    try:
        doc = _active_doc(scope)
        active = int(doc.get("active_version") or 0)
        entry = next(
            (v for v in doc.get("versions") or [] if int(v.get("version") or 0) == active),
            None,
        )
        key = _decrypt(entry) if entry else None
    except EvidenceKeyUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - every failure is the same answer
        logger.error("evidence signing keyring unavailable for %s: %s", scope, type(exc).__name__)
        raise EvidenceKeyUnavailable(KEY_UNAVAILABLE_MESSAGE) from exc
    if not key or not entry:
        raise EvidenceKeyUnavailable(KEY_UNAVAILABLE_MESSAGE)
    return str(entry["key_id"]), key, active, scope


def key_for(workspace_id: str | None, key_id: str) -> bytes | None:
    """The key ``key_id`` of ``workspace_id``'s keyring (retired keys included), or None."""
    if not key_id or key_id == LEGACY_KEY_ID:
        return None
    scope = scope_of(workspace_id)
    try:
        doc = _load(scope) or {}
    except Exception as exc:  # noqa: BLE001 - unreadable keyring: cannot verify
        logger.error("evidence signing keyring unreadable for %s: %s", scope, type(exc).__name__)
        return None
    for entry in doc.get("versions") or []:
        if entry.get("key_id") == key_id:
            return _decrypt(entry)
    return None


def rotate(workspace_id: str | None, *, actor: str) -> dict[str, Any]:
    """Start a new key version; earlier versions stay (retired) and keep verifying."""
    scope = scope_of(workspace_id)
    signing_key(workspace_id)  # make sure version 1 exists
    for _attempt in range(3):
        doc = _load(scope) or {}
        current = int(doc.get("active_version") or 1)
        versions = list(doc.get("versions") or [])
        for version in versions:
            if int(version.get("version") or 0) == current and not version.get("retired_at"):
                version["retired_at"] = _now()
        try:
            versions.append(_new_version(current + 1, actor))
        except Exception as exc:  # noqa: BLE001 - the vault refused
            logger.error("evidence signing key rotation failed: %s", type(exc).__name__)
            raise EvidenceKeyUnavailable(KEY_UNAVAILABLE_MESSAGE) from exc
        updated = {k: v for k, v in doc.items() if k != "_id"}
        updated.update({"active_version": current + 1, "versions": versions, "rotated_at": _now()})
        if _replace(scope, updated, expected_version=current):
            return key_status(workspace_id)
    raise EvidenceKeyUnavailable(KEY_UNAVAILABLE_MESSAGE)


def key_status(workspace_id: str | None) -> dict[str, Any]:
    """Public facts about the keyring: ids, versions and dates, never key material."""
    scope = scope_of(workspace_id)
    doc = _load(scope) or {}
    active = int(doc.get("active_version") or 0)
    return {
        "workspace_id": "" if scope == DEPLOYMENT_SCOPE else scope,
        "scope": "deployment" if scope == DEPLOYMENT_SCOPE else "workspace",
        "algorithm": "HMAC-SHA256",
        "configured": bool(doc),
        "active_version": active,
        "active_key_id": next(
            (v.get("key_id") for v in doc.get("versions") or []
             if int(v.get("version") or 0) == active),
            None,
        ),
        "versions": [
            {k: v.get(k) for k in ("version", "key_id", "created_at", "created_by", "retired_at")}
            for v in doc.get("versions") or []
        ],
        "separate_from_login": True,
        "legacy_key_id": LEGACY_KEY_ID,
    }
