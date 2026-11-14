"""External anchoring of digests — the audit chain tip and signed evidence packs.

``DATAFLOW_AUDIT_ANCHOR_PROVIDER`` picks the backend:

* ``stub`` (default; ``local`` is an alias) — a receipt in the built-in
  append-only store only. It is **not** write-once, so every receipt says
  ``status: not_anchored``.
* ``s3`` — S3 Object Lock in compliance mode (:mod:`services.audit_anchor_s3`).
  A receipt reads ``anchored`` only after the object, its retention and its
  digest were read back; any failure reads ``failed`` with the reason. It
  never falls back to the stub.

A receipt always names the store that really holds it (audit M-16). Any other
requested provider (``tsa`` …) is not implemented: the receipt is written to
the stub store, is labelled ``stub``, carries ``requested_provider`` and says
plainly that the requested provider is not implemented and which stub store
holds it (this node's file, or the deployment's shared control-plane log); its
status is ``failed`` because the anchor the operator asked for did not happen.

Every receipt is also appended to the anchor index so the audit page can list
them: the local ``audit_tip_anchors.jsonl`` on a single node, or the shared
control-plane log (:class:`services.shared_doc_store.SharedLog`) when one is
configured, so "latest anchor" is the deployment's, not this pod's (H-5). For
S3 the bucket, not the index, is the authority, and :func:`verify_receipt`
reads the anchor back on every check.

The chain tip is anchored from the audit append path; with the S3 provider at
most once per ``AUDIT_ANCHOR_MIN_INTERVAL_SECONDS`` per process (default 300),
because a PUT and a read-back on every audit event would stall that path.
A forced anchor (``POST /audit/tip/anchor``) and evidence packs always anchor.
"""

from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo.errors import PyMongoError

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.shared_doc_store import SharedLog, control_store_backend
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

ANCHOR_STORE = data_dir() / "audit_tip_anchors.jsonl"
KIND_TIP = "audit_tip"
STUB_HONESTY = (
    "Local stub anchor — not S3 Object Lock / RFC-3161 TSA, so not write-once: "
    "not anchored. Configure DATAFLOW_AUDIT_ANCHOR_PROVIDER=s3 with an Object Lock bucket."
)
S3_HONESTY = "S3 Object Lock, compliance mode: confirmed by reading the object, its retention and its digest back."
_last_tip_anchor = 0.0
_tip_lock = threading.Lock()
#: A shared index that cannot be reached, or a file that cannot be written.
_STORE_ERRORS = (PyMongoError, RuntimeError, OSError, TypeError, ValueError)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("AUDIT_ANCHOR_STORE", "") or "").strip()
    return Path(env) if env else ANCHOR_STORE


# Shared across replicas when a control-plane store is configured, so
# "latest anchor" is the deployment's, not this pod's (H-5).
_LOG = SharedLog("audit_tip_anchors", file_path=_store_path, key_field="id")

#: Where a stub receipt is really written: the built-in append-only store (the
#: local file, or the shared control-plane log when one is configured);
#: ``stub`` is its long-standing label.
LOCAL_PROVIDER = "stub"
S3_PROVIDER = "s3"
_LOCAL_ALIASES = frozenset({"", "stub", "local"})


def requested_anchor_provider() -> str:
    """What ``AUDIT_ANCHOR_PROVIDER`` asks for (``s3``, ``tsa`` …), whether or not it is implemented."""
    return (getenv_brand("AUDIT_ANCHOR_PROVIDER", LOCAL_PROVIDER) or LOCAL_PROVIDER).strip().lower()


def anchor_provider() -> str:
    """The provider that actually stores receipts: ``s3`` when requested, else the local store.

    A receipt must not carry the name of a provider that did not store it
    (audit M-16): a requested provider this build does not implement is never
    used as the label of a local write.
    """
    return S3_PROVIDER if requested_anchor_provider() == S3_PROVIDER else LOCAL_PROVIDER


def _index_is_shared() -> bool:
    """Whether the anchor index the receipt goes to is the shared control-plane log.

    Asks the log itself (as :func:`_append` does), so the receipt names the
    store that really takes the write; an unreachable shared store still
    counts as shared (the receipt is then not written to this node either).
    """
    try:
        return _LOG.backend() is not None
    except _STORE_ERRORS:
        return control_store_backend() != "file"


def _min_interval() -> float:
    raw = str(getenv_brand("AUDIT_ANCHOR_MIN_INTERVAL_SECONDS", "") or "").strip()
    try:
        return max(0.0, float(raw)) if raw else 300.0
    except ValueError:
        return 300.0


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
        "AUDIT_ANCHOR_PROVIDER=%s is not implemented; %s anchored to the stub store",
        requested, receipt.get("kind") or KIND_TIP,
    )


def _append(receipt: dict[str, Any]) -> bool:
    """Add ``receipt`` to the anchor index (shared log or local file); False if it could not be written."""
    try:
        coll = _LOG.backend()
        if coll is not None:
            _LOG.append(coll, receipt)
            return True
        path = _store_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(receipt, default=json_default) + "\n")
    except _STORE_ERRORS as exc:
        logger.warning("Anchor receipt index write failed (chain still intact): %s", exc)
        return False
    return True


def anchor_digest(digest: str, *, kind: str, subject: str = "", event_time: str = "") -> dict[str, Any] | None:
    """Anchor ``digest`` with the configured provider and record the receipt.

    Returns the AnchorReceipt (contract 2), or ``None`` for an empty digest. With
    the stub the receipt is ``not_anchored``; with S3 it is ``anchored`` or
    ``failed`` (never assumed); with a provider this build does not implement it
    is ``failed``, labelled with the stub store that holds it. A receipt is
    returned even when the index could not be written, so the caller still
    knows what happened.
    """
    value = (digest or "").strip()
    if not value:
        return None
    provider = anchor_provider()
    receipt: dict[str, Any] = {
        "id": str(uuid.uuid4()), "provider": provider, "kind": kind, "subject": subject or kind,
        "digest": value, "tip_hash": value, "anchored_at": _now(), "event_time": event_time or None,
        "location": None, "retention": None,
    }
    if provider == S3_PROVIDER:
        from services.audit_anchor_s3 import put_anchor

        result = put_anchor(value, kind=kind, subject=receipt["subject"], event_time=event_time)
        receipt.update(status=result["status"], location=result["location"], retention=result["retention"],
                       reason=result["reason"], honesty=S3_HONESTY)
        if result["status"] != "anchored":
            logger.error("Audit anchor not confirmed (%s): %s", kind, result["reason"])
    elif (requested := requested_anchor_provider()) in _LOCAL_ALIASES:
        receipt.update(status="not_anchored", reason="the local stub is not a write-once store",
                       honesty=STUB_HONESTY)
    else:
        receipt.update(status="failed",
                       reason=f"requested anchor provider {requested!r} is not implemented in this build")
        _say_requested_provider_not_implemented(receipt, requested, shared=_index_is_shared())
    receipt["anchored"] = receipt["status"] == "anchored"
    receipt["externally_anchored"] = receipt["anchored"]
    _append(receipt)
    return receipt


def anchor_tip(tip_hash: str, *, event_time: str = "", force: bool = False) -> dict[str, Any] | None:
    """Seal the audit chain tip. ``None`` for an empty tip, or a rate-limited S3 tick."""
    global _last_tip_anchor
    if not (tip_hash or "").strip():
        return None
    if anchor_provider() == S3_PROVIDER and not force:
        with _tip_lock:
            now = time.monotonic()
            if _last_tip_anchor and now - _last_tip_anchor < _min_interval():
                return None
            _last_tip_anchor = now
    return anchor_digest(tip_hash, kind=KIND_TIP, subject="audit_chain_tip", event_time=event_time)


def verify_receipt(receipt: dict[str, Any] | None, *, digest: str = "") -> dict[str, Any]:
    """AnchorVerification (contract 2): read the anchor back now; never trust the receipt alone."""
    checked_at = _now()
    if not isinstance(receipt, dict):
        return {"status": "not_anchored", "digest": digest or None, "provider": anchor_provider(),
                "retention": None, "checked_at": checked_at, "reason": "no anchor receipt exists for this digest"}
    value = str(receipt.get("digest") or receipt.get("tip_hash") or "")
    base = {"digest": value or None, "provider": receipt.get("provider"), "checked_at": checked_at}
    if digest and value != digest:
        return {**base, "status": "failed", "retention": None,
                "reason": "the receipt names a different digest"}
    if receipt.get("status") == "failed" and not isinstance(receipt.get("location"), dict):
        # The anchor was attempted and not confirmed: that is a failure, never "not anchored".
        return {**base, "status": "failed", "retention": None,
                "reason": str(receipt.get("reason") or "the anchor could not be confirmed when it was written")}
    if receipt.get("provider") != S3_PROVIDER or not isinstance(receipt.get("location"), dict):
        return {**base, "status": "not_anchored", "retention": None,
                "reason": str(receipt.get("reason") or "the receipt is not a write-once anchor")}
    from services.audit_anchor_s3 import verify_location

    checked = verify_location(receipt["location"], value)
    return {**base, **checked}


def _receipts(*, newest: int | None = None) -> list[dict[str, Any]]:
    """Index receipts, newest first; ``newest`` reads only that many of the latest.

    Anchors are advisory: an unreachable shared store or an unreadable file
    reads as "no receipts".
    """
    try:
        coll = _LOG.backend()
        if coll is not None:
            return list(reversed(_LOG.records(coll, limit=newest)))
    except (PyMongoError, RuntimeError, OSError, ValueError) as exc:
        logger.warning("Audit tip anchor store unavailable: %s", exc)
        return []
    path = _store_path()
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except (OSError, UnicodeDecodeError):
        return []
    out: list[dict[str, Any]] = []
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
        if newest and len(out) >= newest:
            break
    return out


def _tip_receipts(limit: int) -> list[dict[str, Any]]:
    """The newest ``limit`` chain-tip receipts; reads a window of the index, all of it only if needed."""
    window = max(1, limit) + 64
    rows = _receipts(newest=window)
    tips = [r for r in rows if r.get("kind", KIND_TIP) == KIND_TIP]
    if len(tips) < limit and len(rows) >= window:
        tips = [r for r in _receipts() if r.get("kind", KIND_TIP) == KIND_TIP]
    return tips[:limit]


def find_receipt(digest: str, *, kind: str = "") -> dict[str, Any] | None:
    """The newest receipt for ``digest`` (an ``anchored`` one first), or ``None``."""
    matches = [r for r in _receipts()
               if str(r.get("digest") or r.get("tip_hash") or "") == digest and (not kind or r.get("kind") == kind)]
    return next((r for r in matches if r.get("status") == "anchored"), matches[0] if matches else None)


def find_bucket_receipt(digest: str, *, kind: str, around: Any = None) -> dict[str, Any] | None:
    """A receipt rebuilt from the S3 anchor bucket itself (the authority), for a replica
    whose index does not hold one. ``around`` is when the digest was anchored
    (the key is date-partitioned); ``None`` when the provider is not S3 or nothing is there."""
    if anchor_provider() != S3_PROVIDER or not (digest or "").strip():
        return None
    from services.audit_anchor_s3 import find_location

    location = find_location(digest, kind=kind, around=around)
    if location is None:
        return None
    return {"id": None, "provider": S3_PROVIDER, "kind": kind, "subject": None, "digest": digest, "tip_hash": digest,
            "anchored_at": None, "event_time": None, "status": "anchored", "anchored": True,
            "externally_anchored": True,
            "location": location, "retention": None, "reason": None, "honesty": S3_HONESTY}


def latest_anchor() -> dict[str, Any] | None:
    receipts = _tip_receipts(1)
    return receipts[0] if receipts else None


def list_anchors(*, limit: int = 20) -> list[dict[str, Any]]:
    return _tip_receipts(limit)
