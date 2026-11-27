"""Durable home for the Settings integrations document (R7).

``services.integrations_store`` kept SSO, AI provider keys, the Pilot engine
and the MCP policy in ``integrations.json`` on the container disk. A container
without a mounted volume (QA's hosted build) lost every saved model key on each
deploy or restart. The document now lives in the control-plane database; the
file is a local cache and the fallback when no database is configured. Secret
fields are already ``enc:v1`` ciphertext (``services.secret_vault``) before
they reach either store, so neither holds a plaintext key.

API keys are not part of this document: they have their own collection.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "workspace_integrations"
DOC_ID = "integrations"
#: Sections of the integrations document that live here.
SECTIONS = ("sso", "ai_providers", "pilot", "mcp_policy", "updated_at")
#: Set once a node's local file has been copied up in full. The copy is a
#: one-time migration, not a sync: once set, no file is ever copied again, so
#: a setting deleted on one node is not brought back by another node's stale
#: file (review round 5).
HEAL_MARKER = "file_heal_done"


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001
        logger.debug("integrations collection unavailable", exc_info=True)
        return None


def durable() -> bool:
    """True when saved settings survive a restart that wipes the local disk."""
    return _collection() is not None


def read_document() -> tuple[str, dict[str, Any] | None]:
    """``("ok", sections)``, ``("absent", None)``, ``("nodb", None)`` or ``("error", None)``.

    ``error`` covers a configured control plane that is unreachable or a read
    that failed: the document may well exist, so nothing may be healed over it
    and no policy may be written against a guess (review round 3).
    """
    coll = _collection()
    if coll is None:
        return ("error" if _control_plane_configured() else "nodb"), None
    try:
        doc = coll.find_one({"_id": DOC_ID})
    except Exception as exc:  # noqa: BLE001
        logger.warning("integrations settings read failed; using the local file: %s", exc)
        return "error", None
    if not isinstance(doc, dict):
        return "absent", None
    return "ok", {k: doc[k] for k in (*SECTIONS, HEAL_MARKER) if k in doc}


def _control_plane_configured() -> bool:
    try:
        from services.control_plane_store import control_plane_configured

        return control_plane_configured()
    except Exception:  # noqa: BLE001
        logger.debug("control-plane configuration unknown", exc_info=True)
        return False


def load_sections() -> dict[str, Any] | None:
    """The stored sections, or ``None`` when there is no database, no doc, or no read."""
    return read_document()[1]


#: Sections whose entries are written one by one (``sso.oidc``,
#: ``ai_providers.openai``): healed entry by entry too.
_KEYED_SECTIONS = ("sso", "ai_providers")


def _heal_paths(data: dict[str, Any], doc: dict[str, Any] | None) -> dict[str, Any]:
    """The file's sections (keyed sections: entries) that ``doc`` does not have."""
    doc = doc or {}
    paths: dict[str, Any] = {}
    for section in SECTIONS:
        if section not in data or section == "updated_at":
            continue
        value, have = data[section], doc.get(section)
        if section in _KEYED_SECTIONS and isinstance(value, dict):
            have = have if isinstance(have, dict) else {}
            paths.update({f"{section}.{k}": v for k, v in value.items() if k not in have})
        elif section not in doc:
            paths[section] = value
    return paths


def save_sections(data: dict[str, Any], doc: dict[str, Any] | None = None) -> bool:
    """Copy up what the local file has and the database document lacks — once.

    Missing-only: each section (each entry of a keyed section) is set only
    where the document does not have it, so a database value (a concurrent
    writer's upsert, before or after this) is never overwritten. ``doc`` is
    the document as just read (``None``: there was none). Runs while the
    document has no :data:`HEAL_MARKER`, which is set only after every path
    of this copy succeeded: a heal cut short, or a document another node
    created first, is finished by a later read, and after the marker no file
    is copied again, so a deletion is never undone (review rounds 4-5).
    """
    if doc is not None and doc.get(HEAL_MARKER):
        return True
    paths = _heal_paths(data, doc)
    coll = _collection()
    if coll is None:
        return False
    try:
        if doc is None:
            coll.update_one(
                {"_id": DOC_ID},
                {"$setOnInsert": {"updated_at": data.get("updated_at") or ""}},
                upsert=True,
            )
        for path, value in paths.items():
            coll.update_one({"_id": DOC_ID, path: {"$exists": False}}, {"$set": {path: value}})
        coll.update_one({"_id": DOC_ID}, {"$set": {HEAL_MARKER: True}})
    except Exception as exc:  # noqa: BLE001 - no marker: the next read tries again
        logger.warning("integrations settings heal not finished: %s", exc)
        return False
    return True


def save_section(path: str, value: Any, updated_at: str) -> bool | None:
    """``$set`` one section (dotted ``path``) and ``updated_at``; nothing else."""
    return save_paths({path: value}, updated_at)


def save_paths(paths: dict[str, Any], updated_at: str) -> bool | None:
    """``$set`` these dotted paths and ``updated_at`` in one update; nothing else.

    Each writer owns its section, so a writer holding a stale copy of another
    section (the MCP policy and its ``rev``) cannot put it back (review round 3).
    ``None`` when no path lives in the database or there is none.
    """
    paths = {k: v for k, v in paths.items() if k.split(".", 1)[0] in SECTIONS}
    if not paths:
        return None
    coll = _collection()
    if coll is None:
        return None
    try:
        coll.update_one({"_id": DOC_ID}, {"$set": {**paths, "updated_at": updated_at}}, upsert=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("integrations settings write failed; local file kept: %s", exc)
        return False
    return True


def unset_paths(paths: list[str], updated_at: str) -> bool | None:
    """``$unset`` these dotted paths (a deletion: no tombstone) and set ``updated_at``."""
    paths = [k for k in paths if k.split(".", 1)[0] in SECTIONS]
    if not paths:
        return None
    coll = _collection()
    if coll is None:
        return None
    try:
        coll.update_one(
            {"_id": DOC_ID},
            {"$unset": {k: "" for k in paths}, "$set": {"updated_at": updated_at}},
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("integrations settings delete failed; local file updated: %s", exc)
        return False
    return True


def save_mcp_policy_if_revision(policy: dict[str, Any], expected_rev: int, updated_at: str) -> str:
    """Compare-and-swap ``mcp_policy`` on its ``rev``; other sections are not touched.

    ``saved``; ``conflict`` (another writer got there first); ``nodb`` (no
    database: the caller's locks are the only writer guard); ``unavailable``
    (a configured database cannot be written: the caller answers 503).
    """
    coll = _collection()
    if coll is None:
        return "unavailable" if _control_plane_configured() else "nodb"
    match: dict[str, Any] = {"_id": DOC_ID}
    if expected_rev:
        match["mcp_policy.rev"] = expected_rev
    else:
        match["$or"] = [{"mcp_policy.rev": {"$exists": False}}, {"mcp_policy.rev": 0}]
    update = {"$set": {"mcp_policy": policy, "updated_at": updated_at}}
    try:
        if getattr(coll.update_one(match, update), "matched_count", 0):
            return "saved"
        if expected_rev or coll.find_one({"_id": DOC_ID}) is not None:
            return "conflict"
        coll.insert_one({"_id": DOC_ID, "mcp_policy": policy, "updated_at": updated_at})
        return "saved"
    except Exception as exc:  # noqa: BLE001
        if type(exc).__name__ == "DuplicateKeyError":
            return "conflict"  # lost the race to create the document
        logger.warning("integrations settings compare-and-swap failed: %s", exc)
        return "unavailable"
