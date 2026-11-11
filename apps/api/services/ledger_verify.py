"""Verification of evidence packs by the service: the offline checks plus what only it can read.

The checks themselves are the standalone verifier's
(:func:`tools.datawrap_verify.verify_evidence`), run with this deployment's
own key registry as the trusted set. The service adds the three checks an
offline tool cannot make:

* ``consistency`` — the live log still has this leaf at this index and this
  root at the receipt's size (a forged or stale receipt does not);
* ``withdrawal`` — the log holds no withdrawal of the pack, or a valid one
  (then the verdict is "issued, later withdrawn");
* ``anchor`` — the checkpoint's write-once object was read back now.

A verdict is ``verified`` only when all eight checks passed. A check that
could not run is ``not_checked`` and the verdict is ``incomplete``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from services import ledger_keys, ledger_log
from tools import datawrap_verify as verifier

logger = logging.getLogger(__name__)

PASSED, FAILED, NOT_CHECKED = verifier.PASSED, verifier.FAILED, verifier.NOT_CHECKED


def _check(status: str, detail: str, **more: Any) -> dict[str, Any]:
    return {"status": status, "detail": detail, **more}


def _consistency(receipt: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
    try:
        size, index = int(checkpoint.get("tree_size")), int(receipt.get("leaf_index"))
    except (TypeError, ValueError):
        return _check(FAILED, "the receipt does not name a leaf index and a tree size")
    if checkpoint.get("origin") != ledger_log.origin():
        return _check(FAILED, "the receipt is from a different log than this deployment's")
    if size > ledger_log.tree_size():
        return _check(FAILED, f"the receipt's checkpoint (size {size}) is larger than the log")
    if ledger_log.root_hash(size) != checkpoint.get("root_hash"):
        return _check(FAILED, f"the log's root at size {size} is not the receipt's: the log was rewritten, "
                              "or the receipt is not from this log")
    entry = ledger_log.get_entry(index)
    if not entry or entry.get("leaf_hash") != receipt.get("leaf_hash"):
        return _check(FAILED, f"the log's entry {index} is not this statement")
    return _check(PASSED, f"the log still holds this leaf at index {index} and this root at size {size}")


def _signatures_ok(entry: dict[str, Any]) -> bool:
    statement = entry.get("statement") or {}
    status, _detail, _rows = verifier.check_signature_set(
        statement, entry.get("signatures"),
        message=verifier.pae(verifier.STATEMENT_PAYLOAD, verifier.statement_bytes(statement)),
        keys=ledger_keys.trusted_keys(list(statement.get("signer_key_ids") or [])),
        role=verifier.ROLE_STATEMENT)
    return (status == PASSED
            and entry.get("leaf_hash") == verifier.leaf_hash(verifier.statement_bytes(statement)).hex())


def _withdrawal(statement: dict[str, Any]) -> dict[str, Any]:
    if statement.get("kind") == verifier.WITHDRAWAL:
        return _check(PASSED, "this entry is itself a withdrawal")
    found = ledger_log.withdrawal_of(str(statement.get("content_sha256") or ""))
    if not found:
        return _check(PASSED, "the log holds no withdrawal of this pack")
    if not _signatures_ok(found):
        return _check(FAILED, "the log holds a withdrawal of this pack that does not verify")
    detail = (found.get("statement") or {}).get("withdrawal") or {}
    info = {"leaf_index": found.get("index"), "withdrawn_at": found.get("issued_at"),
            "reason": detail.get("reason"), "superseded_by": detail.get("superseded_by")}
    return _check(PASSED, f"the pack was withdrawn at {info['withdrawn_at']}", withdrawal=info)


def _guarded(name: str, run: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    try:
        return run()
    except Exception as exc:  # noqa: BLE001 - a check that cannot run is "not checked", never "passed"
        logger.error("evidence check %s could not run (%s)", name, type(exc).__name__, exc_info=exc)
        return _check(NOT_CHECKED, f"the service could not run this check now ({type(exc).__name__})")


def stored_receipt(pack: dict[str, Any], *, allows: Callable[[Any], bool] | None = None) -> dict[str, Any] | None:
    """The receipt on record for a pack that carries none, within what the caller may see."""
    entry = ledger_log.find_entry(str(pack.get("content_sha256") or ""), verifier.pack_kind(pack))
    if not entry or (allows is not None and not allows(entry.get("workspace_id") or None)):
        return None
    return ledger_log.receipt(entry)


def verify_online(pack: dict[str, Any] | None, receipt: dict[str, Any] | None = None, *,
                  search: bool = False, allows: Callable[[Any], bool] | None = None) -> dict[str, Any]:
    """The EvidenceVerdict for a pack (and receipt), with the log and the anchor consulted.

    ``search`` looks the receipt up by the pack's digest when the pack carries
    none (never done for a caller without a session).
    """
    if receipt is None and isinstance(pack, dict) and isinstance(pack.get("ledger"), dict):
        receipt = pack["ledger"]
    if receipt is None and search and isinstance(pack, dict):
        try:
            receipt = stored_receipt(pack, allows=allows)
        except Exception as exc:  # noqa: BLE001 - the ledger could not be read: nothing is claimed
            logger.error("evidence lookup could not run (%s)", type(exc).__name__, exc_info=exc)
            return verifier.hmac_only_verdict("online", detail="the ledger could not be read now")
    if not isinstance(receipt, dict):
        return verifier.hmac_only_verdict("online")
    statement = receipt.get("statement") if isinstance(receipt.get("statement"), dict) else {}
    checkpoint = receipt.get("checkpoint") if isinstance(receipt.get("checkpoint"), dict) else {}
    named = [*(statement.get("signer_key_ids") or []), *(checkpoint.get("signer_key_ids") or [])]
    try:
        keys = ledger_keys.trusted_keys([k for k in named if isinstance(k, str)])
    except Exception as exc:  # noqa: BLE001 - no registry: the signatures read "not checked"
        logger.error("evidence key registry could not be read (%s)", type(exc).__name__, exc_info=exc)
        keys = verifier.KeySet()
    from services.ledger_anchor import verify_checkpoint

    anchor_check, anchor = verify_checkpoint(checkpoint)
    overrides = {
        "consistency": _guarded("consistency", lambda: _consistency(receipt, checkpoint)),
        "withdrawal": _guarded("withdrawal", lambda: _withdrawal(statement)),
        "anchor": anchor_check,
    }
    return verifier.verify_evidence(pack, receipt, keys, overrides=overrides, mode="online", anchor=anchor)


def evidence_for_pack(pack: dict[str, Any]) -> dict[str, Any]:
    """The verdict the pack-verify routes add: the embedded receipt, or "HMAC only"."""
    try:
        return verify_online(pack)
    except Exception as exc:  # noqa: BLE001 - the HMAC answer must still be given
        logger.error("evidence verification could not run (%s)", type(exc).__name__, exc_info=exc)
        if isinstance(pack.get("ledger"), dict):
            return verifier.verify_evidence(pack, None, verifier.KeySet(), mode="online")
        return verifier.hmac_only_verdict("online")
