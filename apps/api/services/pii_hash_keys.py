"""Per-workspace HMAC keys for ``hash_pii`` (QA enterprise #19).

``hash_pii`` used to need ``DATAFLOW_PII_HASH_KEY`` on the API service and
refused with an error that named that variable and Railway — on production no
one had set it, so the transform could not be used at all, and the message
exposed infrastructure to a tenant.

Each workspace now has its own key, generated on first use and stored
encrypted (``services.secret_vault``, ``enc:v1``) in the control-plane database,
the same durable home round 7 gave AI keys (``integrations_persistence``); the
JSON file under the data directory is the store when no database is configured.
The same workspace always gets the same digest for the same value; two
workspaces get different digests, so hashed columns cannot be joined across
tenants.

Rotation adds a new active version and keeps the old ones (marked retired), so
an operator can tell which key produced historic digests. Rotating changes
every future digest, which is why it is an explicit admin action. A workspace
admin may also set the key (``set_workspace_key``) — to keep the digests
another system already produced, or to recover a key that can no longer be
read — from Settings → Security or ``PUT /api/v1/workspace/pii-hash-key``.
The key material goes through the vault and is never returned.

An operator who pins ``DATAFLOW_PII_HASH_KEY`` keeps exactly the digests they
had: the pinned key wins (``transform_engine.pii_hash_key``).
"""

from __future__ import annotations

import logging
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "workspace_pii_hash_keys"
#: Key id for work that runs outside any workspace (system jobs, auth-off).
DEPLOYMENT_SCOPE = "__deployment__"
_LOCK = threading.Lock()


class PiiHashKeyUnavailable(RuntimeError):
    """The workspace key could not be read or created. Message is user-safe."""


class PiiHashKeyRejected(ValueError):
    """A key an admin tried to set is not acceptable. Message is user-safe."""


#: Shortest key an admin may set: 32 characters (the generated key is 64 hex).
MIN_KEY_LENGTH = 32
MAX_KEY_LENGTH = 512

#: What a tenant is told when ``hash_pii`` cannot get its key. Validate raises
#: it as a blocker and the write path refuses with it, so both say the same
#: thing (D8). It names no environment variable, path, host or vendor — only
#: the action an operator can take (QA browser 2 / security 13).
KEY_UNAVAILABLE_MESSAGE = (
    "hash_pii could not load this workspace's hashing key, so no row was hashed. "
    "A workspace admin can set or rotate the hashing key in Settings → Security, "
    "then re-run Validate. Use mask_pii or redact if a keyed digest is not required."
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001 - no database = file store
        return None


def _file_path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "pii_hash_keys.json"


def _scope_id(workspace_id: str) -> str:
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


def _new_version(version: int, actor: str, secret: str | None = None) -> dict[str, Any]:
    from services.secret_vault import encrypt_secret

    return {
        "version": version,
        "key_enc": encrypt_secret(secret or secrets.token_hex(32), label="pii_hash_key"),
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _active_secret(doc: dict[str, Any]) -> str:
    from services.secret_vault import decrypt_secret

    active = int(doc.get("active_version") or 0)
    for version in doc.get("versions") or []:
        if int(version.get("version") or 0) == active:
            return decrypt_secret(str(version.get("key_enc") or ""))
    return ""


_UNAVAILABLE = KEY_UNAVAILABLE_MESSAGE


def workspace_key(workspace_id: str) -> str:
    """The active key for ``workspace_id``, created on first use."""
    scope = _scope_id(workspace_id)
    try:
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
        secret = _active_secret(doc)
    except Exception as exc:  # noqa: BLE001 - every failure is the same answer
        logger.error("pii hash key unavailable for %s: %s", scope, type(exc).__name__)
        raise PiiHashKeyUnavailable(_UNAVAILABLE) from exc
    if not secret:
        raise PiiHashKeyUnavailable(_UNAVAILABLE)
    return secret


def _add_version(workspace_id: str, *, actor: str, secret: str | None) -> dict[str, Any]:
    """Make a new version active (generated, or the admin's ``secret``)."""
    scope = _scope_id(workspace_id)
    if _load(scope) is None:
        if secret is not None:
            # First key for this workspace is the admin's own: no generated v1.
            doc = _insert_if_absent(scope, {
                "workspace_id": "" if scope == DEPLOYMENT_SCOPE else scope,
                "active_version": 1,
                "versions": [_new_version(1, actor, secret)],
                "created_at": _now(),
                "set_by_admin": True,
            })
            if _active_secret(doc) == secret:
                return key_status(workspace_id)
        else:
            workspace_key(workspace_id)  # make sure version 1 exists
    for _attempt in range(3):
        doc = _load(scope) or {}
        current = int(doc.get("active_version") or 1)
        versions = list(doc.get("versions") or [])
        for version in versions:
            if int(version.get("version") or 0) == current:
                version["retired_at"] = _now()
        versions.append(_new_version(current + 1, actor, secret))
        updated = {k: v for k, v in doc.items() if k != "_id"}
        updated.update({"active_version": current + 1, "versions": versions, "rotated_at": _now()})
        if secret is not None:
            updated["set_by_admin"] = True
        if _replace(scope, updated, expected_version=current):
            return key_status(workspace_id)
    raise PiiHashKeyUnavailable(_UNAVAILABLE)


def rotate_workspace_key(workspace_id: str, *, actor: str) -> dict[str, Any]:
    """Start a new generated key version; earlier versions stay, marked retired."""
    return _add_version(workspace_id, actor=actor, secret=None)


def set_workspace_key(workspace_id: str, secret: str, *, actor: str) -> dict[str, Any]:
    """Make ``secret`` the workspace's active hash_pii key (a new version).

    For an admin who must keep digests another system produced, or whose
    stored key can no longer be read. Earlier versions stay, marked retired.
    The key is stored through the vault and never returned or logged.
    """
    text = str(secret or "")
    if text != text.strip() or not (MIN_KEY_LENGTH <= len(text) <= MAX_KEY_LENGTH):
        raise PiiHashKeyRejected(
            f"A hashing key must be {MIN_KEY_LENGTH}–{MAX_KEY_LENGTH} characters "
            "with no leading or trailing spaces. Leave it to Datawrap (Rotate) "
            "unless you must match digests produced elsewhere."
        )
    if len(set(text)) < 8:
        raise PiiHashKeyRejected(
            "That hashing key repeats too few characters to be secret. Use a "
            "random value (for example 64 hex characters), or Rotate to have "
            "Datawrap generate one."
        )
    try:
        return _add_version(workspace_id, actor=actor, secret=text)
    except PiiHashKeyUnavailable:
        raise
    except Exception as exc:  # noqa: BLE001 - the vault refused: same answer
        logger.error("pii hash key set failed: %s", type(exc).__name__)
        raise PiiHashKeyUnavailable(_UNAVAILABLE) from exc


def key_status(workspace_id: str) -> dict[str, Any]:
    """Public facts about the key: versions and dates, never key material."""
    from services.brand_env import getenv_brand

    doc = _load(_scope_id(workspace_id)) or {}
    return {
        "workspace_id": str(workspace_id or ""),
        "configured": bool(doc),
        # The deployment pins one key for every workspace: setting or rotating
        # the workspace key would not change any digest, so the UI says so.
        "managed_by_deployment": bool(getenv_brand("PII_HASH_KEY") or getenv_brand("SECRET")),
        "set_by_admin": bool(doc.get("set_by_admin")),
        "active_version": int(doc.get("active_version") or 0),
        "versions": [
            {k: v.get(k) for k in ("version", "created_at", "created_by", "retired_at")}
            for v in doc.get("versions") or []
        ],
    }
