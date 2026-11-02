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


def load_sections() -> dict[str, Any] | None:
    """The stored sections, or ``None`` when there is no database (or no doc yet)."""
    coll = _collection()
    if coll is None:
        return None
    try:
        doc = coll.find_one({"_id": DOC_ID})
    except Exception as exc:  # noqa: BLE001
        logger.warning("integrations settings read failed; using the local file: %s", exc)
        return None
    if not isinstance(doc, dict):
        return None
    return {k: doc[k] for k in SECTIONS if k in doc}


def save_sections(data: dict[str, Any]) -> bool:
    coll = _collection()
    if coll is None:
        return False
    doc = {k: data[k] for k in SECTIONS if k in data}
    try:
        coll.replace_one({"_id": DOC_ID}, {"_id": DOC_ID, **doc}, upsert=True)
    except Exception as exc:  # noqa: BLE001
        logger.warning("integrations settings write failed; local file kept: %s", exc)
        return False
    return True
