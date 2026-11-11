"""Anchoring of Evidence Ledger checkpoints in the write-once store.

Every checkpoint's digest (``checkpoint_sha256``: the SHA-256 of the signed
checkpoint body) is handed to the existing anchor
(:func:`services.audit_anchor.anchor_digest`) when the checkpoint is signed:
S3 Object Lock in compliance mode when that provider is configured, otherwise
the local stub, whose receipts read ``not_anchored``. The receipt is kept with
the checkpoint record; it is not part of the signed checkpoint.

Nothing here decides that a checkpoint is anchored. "anchored" is what
:func:`services.audit_anchor.verify_receipt` answers after reading the object,
its retention and its digest back, on every verification. A provider that is
configured and cannot confirm reads ``failed`` (and fails the check); no
write-once store reads "Not anchored" (the check is not made, so a verdict is
never ``verified`` on it).
"""

from __future__ import annotations

import logging
from typing import Any

from services import ledger_store
from tools import datawrap_verify as verifier

logger = logging.getLogger(__name__)

KIND = "ledger_checkpoint"


def _subject(checkpoint: dict[str, Any]) -> str:
    return f"{checkpoint.get('origin')}:{checkpoint.get('tree_size')}"


def _audit(receipt: dict[str, Any], record: dict[str, Any]) -> None:
    from services.audit_log import append_audit_event

    try:
        append_audit_event(
            action="evidence.checkpoint.anchored", resource=f"evidence_checkpoint:{record.get('tree_size')}",
            actor="system", level="info",
            outcome="succeeded" if receipt.get("status") == "anchored" else "failed",
            details={"tree_size": record.get("tree_size"), "checkpoint_sha256": record.get("checkpoint_sha256"),
                     "provider": receipt.get("provider"), "anchor_status": receipt.get("status"),
                     "anchor_reason": receipt.get("reason")})
    except Exception as exc:  # noqa: BLE001 - the anchor stands; a failed append is reported
        logger.error("evidence audit evidence.checkpoint.anchored failed (%s)", type(exc).__name__, exc_info=exc)


def anchor_checkpoint(record: dict[str, Any]) -> dict[str, Any]:
    """Anchor a checkpoint record's digest and keep the receipt with it. Returns the record."""
    from services.audit_anchor import anchor_digest, anchor_provider

    checkpoint = record.get("checkpoint") or {}
    digest = str(record.get("checkpoint_sha256") or "")
    try:
        receipt = anchor_digest(digest, kind=KIND, subject=_subject(checkpoint),
                                event_time=str(checkpoint.get("timestamp") or ""))
    except Exception as exc:  # noqa: BLE001 - an anchor that raised is a failed anchor, said so
        logger.error("evidence checkpoint anchor raised (%s)", type(exc).__name__, exc_info=exc)
        receipt = {"id": None, "provider": anchor_provider(), "kind": KIND, "subject": _subject(checkpoint),
                   "digest": digest, "status": "failed", "anchored": False, "location": None, "retention": None,
                   "reason": f"the anchor could not be written ({type(exc).__name__})"}
    if not receipt:
        return record
    ledger_store.set_fields(ledger_store.CHECKPOINTS, int(record["tree_size"]), {"anchor": receipt})
    if receipt.get("status") != "not_anchored":
        _audit(receipt, record)  # an anchor was attempted: confirmed or failed, on the record
    return {**record, "anchor": receipt}


def anchor_latest() -> dict[str, Any]:
    """Anchor the checkpoint of the whole log now (signing it first if needed)."""
    from services import ledger_log

    record = ledger_log.checkpoint()
    if record is None:
        return {"ok": False, "anchored": False, "checkpoint_sha256": None, "anchor": None}
    if (record.get("anchor") or {}).get("status") != "anchored":
        record = anchor_checkpoint(record)
    receipt = record.get("anchor")
    return {"ok": bool(receipt), "anchored": bool(receipt and receipt.get("status") == "anchored"),
            "checkpoint_sha256": record.get("checkpoint_sha256"), "anchor": receipt}


def _receipt_for(checkpoint: dict[str, Any], digest: str) -> dict[str, Any] | None:
    """The anchor receipt of a checkpoint, wherever this replica can find it.

    The checkpoint record (shared store) keeps the receipt made when it was
    signed; the local index is this process's own; the bucket is the authority
    and is listed when neither holds an anchored one. Whatever is found is read
    back by the caller.
    """
    from services.audit_anchor import find_bucket_receipt, find_receipt

    try:
        size = int(checkpoint.get("tree_size"))
    except (TypeError, ValueError):
        return None
    record = ledger_store.get(ledger_store.CHECKPOINTS, size) or {}
    stored = record.get("anchor") if record.get("checkpoint_sha256") == digest else None
    if isinstance(stored, dict) and stored.get("status") == "anchored":
        return stored
    local = find_receipt(digest, kind=KIND)
    if local and local.get("status") == "anchored":
        return local
    return find_bucket_receipt(digest, kind=KIND, around=checkpoint.get("timestamp")) or stored or local


def verify_checkpoint(checkpoint: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """The ``anchor`` check for a checkpoint: (check, AnchorVerification). Reads the anchor back now."""
    from services.audit_anchor import verify_receipt

    try:
        digest = verifier.checkpoint_sha256(checkpoint)
        result = verify_receipt(_receipt_for(checkpoint, digest), digest=digest)
    except Exception as exc:  # noqa: BLE001 - an anchor that cannot be checked is "not checked", never passed
        logger.error("evidence anchor read-back could not run (%s)", type(exc).__name__, exc_info=exc)
        return {"status": verifier.NOT_CHECKED,
                "detail": f"the anchor could not be read back now ({type(exc).__name__})"}, None
    if result["status"] == "anchored":
        until = (result.get("retention") or {}).get("retain_until")
        return {"status": verifier.PASSED,
                "detail": f"anchored in {result.get('provider')}, read back now"
                          + (f"; write-once until {until}" if until else "")}, result
    if result["status"] == "failed":
        return {"status": verifier.FAILED,
                "detail": f"the anchor could not be confirmed: {result.get('reason')}"}, result
    return {"status": verifier.NOT_CHECKED, "detail": f"Not anchored: {result.get('reason')}"}, result
