"""External audit tip anchoring — WORM / Object-Lock style receipts.

Engineering diligence: seals the current HMAC chain tip into an append-only
anchor store. The only provider in this build is a local stub (not S3 Object
Lock / TSA). ``AUDIT_ANCHOR_PROVIDER`` records what the operator asked for; a
receipt always names the store that really holds it, and says so when the
requested provider is not implemented.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo.errors import PyMongoError

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.shared_doc_store import SharedLog
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

ANCHOR_STORE = data_dir() / "audit_tip_anchors.jsonl"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("AUDIT_ANCHOR_STORE", "") or "").strip()
    return Path(env) if env else ANCHOR_STORE


# Shared across replicas when a control-plane store is configured, so
# "latest anchor" is the deployment's, not this pod's (H-5).
_LOG = SharedLog("audit_tip_anchors", file_path=_store_path, key_field="id")

#: Where a receipt is really written. The only provider this build implements
#: is the built-in append-only store (the local file, or the shared
#: control-plane log when one is configured); ``stub`` is its long-standing label.
LOCAL_PROVIDER = "stub"
_LOCAL_ALIASES = frozenset({"", "stub", "local"})


def requested_anchor_provider() -> str:
    """What ``AUDIT_ANCHOR_PROVIDER`` asks for (``s3`` …), whether or not it exists yet."""
    return (getenv_brand("AUDIT_ANCHOR_PROVIDER", LOCAL_PROVIDER) or LOCAL_PROVIDER).strip().lower()


def anchor_provider() -> str:
    """The provider that actually stores receipts: the local store, whatever was requested.

    A receipt must not carry the name of a provider that did not store it
    (audit M-16): ``AUDIT_ANCHOR_PROVIDER=s3`` used to label a local write "s3".
    """
    return LOCAL_PROVIDER


def _say_requested_provider_not_implemented(receipt: dict[str, Any], requested: str, *, shared: bool) -> None:
    """Say what was asked for and that it did not happen (audit M-16).

    Never label the stub write with the requested provider's name, and name
    the store that really holds the receipt: this node's file, or the
    deployment's shared control-plane log.
    """
    where = "the stub store in this deployment's shared control-plane database" if shared else (
        "the local stub store on this node"
    )
    receipt["requested_provider"] = requested
    receipt["honesty"] = (
        f"Requested anchor provider '{requested}' is not implemented in this build: "
        f"this receipt was written to {where}. "
        "It is not S3 Object Lock / RFC-3161 TSA and is not an external anchor."
    )
    logger.warning(
        "AUDIT_ANCHOR_PROVIDER=%s is not implemented; audit tip anchored to the stub store", requested
    )


def anchor_tip(tip_hash: str, *, event_time: str = "") -> dict[str, Any] | None:
    """Seal ``tip_hash`` into the anchor store. Returns the receipt or None if empty tip."""
    tip = (tip_hash or "").strip()
    if not tip:
        return None
    requested = requested_anchor_provider()
    receipt: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "tip_hash": tip,
        "anchored_at": _now(),
        "event_time": event_time or None,
        "provider": anchor_provider(),
        "externally_anchored": False,
        "honesty": "Local stub anchor — not S3 Object Lock / RFC-3161 TSA.",
    }
    try:
        coll = _LOG.backend()
        if requested not in _LOCAL_ALIASES:
            _say_requested_provider_not_implemented(receipt, requested, shared=coll is not None)
        if coll is not None:
            _LOG.append(coll, receipt)
            return receipt
        path = _store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(receipt, default=json_default) + "\n")
    except Exception as exc:
        logger.warning("Audit tip anchor write failed (chain still intact): %s", exc)
        return None
    return receipt


def latest_anchor() -> dict[str, Any] | None:
    try:
        coll = _LOG.backend()
        if coll is not None:
            newest = _LOG.records(coll, limit=1)
            return newest[0] if newest else None
    except (PyMongoError, RuntimeError, OSError, ValueError) as exc:
        # Anchors are advisory: an unreachable shared store reads as "no anchor".
        logger.warning("Audit tip anchor store unavailable: %s", exc)
        return None
    path = _store_path()
    if not path.exists():
        return None
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return None
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            continue
    return None


def list_anchors(*, limit: int = 20) -> list[dict[str, Any]]:
    try:
        coll = _LOG.backend()
        if coll is not None:
            return list(reversed(_LOG.records(coll, limit=limit)))
    except (PyMongoError, RuntimeError, OSError, ValueError) as exc:
        logger.warning("Audit tip anchor store unavailable: %s", exc)
        return []
    path = _store_path()
    if not path.exists():
        return []
    out: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
        if len(out) >= limit:
            break
    return out
