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


def _consistency(receipt: dict[str, Any], statement: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
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
    if not entry or entry.get("leaf_hash") != verifier.leaf_hash(verifier.statement_bytes(statement)).hex():
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


def _withdrawal(statement: dict[str, Any], *, public: bool = False) -> dict[str, Any]:
    if statement.get("kind") == verifier.WITHDRAWAL:
        return _check(PASSED, "this entry is itself a withdrawal")
    found = ledger_log.withdrawal_of(str(statement.get("content_sha256") or ""))
    if not found:
        return _check(PASSED, "the log holds no withdrawal of this pack")
    if not _signatures_ok(found):
        return _check(FAILED, "the log holds a withdrawal of this pack that does not verify")
    detail = (found.get("statement") or {}).get("withdrawal") or {}
    # The reason is the workspace's own words: a caller without a session is told when, not why.
    info = {"leaf_index": found.get("index"), "withdrawn_at": found.get("issued_at"),
            "reason": None if public else detail.get("reason"), "superseded_by": detail.get("superseded_by")}
    return _check(PASSED, f"the pack was withdrawn at {info['withdrawn_at']}", withdrawal=info)


_NOT_THIS_LOG = "not consulted: the receipt is not from this deployment's log"


def _public_anchor(check: dict[str, Any], anchor: dict[str, Any] | None) -> tuple[dict[str, Any],
                                                                                dict[str, Any] | None]:
    """The anchor answer without the store's own details (bucket, object, error text)."""
    generic = {PASSED: "anchored in a write-once store, read back now",
               FAILED: "the anchor could not be confirmed", NOT_CHECKED: "Not anchored."}
    return _check(check["status"], generic[check["status"]]), ({"status": anchor.get("status")} if anchor else None)


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
                  search: bool = False, allows: Callable[[Any], bool] | None = None,
                  public: bool = False) -> dict[str, Any]:
    """The EvidenceVerdict for a pack (and receipt), with the log and the anchor consulted.

    ``search`` looks the receipt up by the pack's digest when the pack carries
    none (never done for a caller without a session). ``public`` is that
    caller: the answer then holds verdicts only — no withdrawal reason, no
    anchor location or store error text.
    """
    if receipt is None and isinstance(pack, dict) and isinstance(pack.get("ledger"), dict):
        receipt = pack["ledger"]
    if receipt is None and search and isinstance(pack, dict):
        try:
            receipt = stored_receipt(pack, allows=allows)
        except Exception as exc:  # noqa: BLE001 - the ledger could not be read: nothing is claimed
            logger.error("evidence lookup could not run (%s)", type(exc).__name__, exc_info=exc)
            return verifier.hmac_only_verdict("online", detail="the ledger could not be read now", pack=pack)
    if not isinstance(receipt, dict):
        # "HMAC only" for a pack that never had signatures; a failure for one that says it had.
        return verifier.no_receipt_verdict(pack, "online")
    statement = receipt.get("statement") if isinstance(receipt.get("statement"), dict) else {}
    checkpoint = receipt.get("checkpoint") if isinstance(receipt.get("checkpoint"), dict) else {}
    named = [*(statement.get("signer_key_ids") or []), *(checkpoint.get("signer_key_ids") or [])]
    try:
        keys = ledger_keys.trusted_keys([k for k in named if isinstance(k, str)])
    except Exception as exc:  # noqa: BLE001 - no registry: the signatures read "not checked"
        logger.error("evidence key registry could not be read (%s)", type(exc).__name__, exc_info=exc)
        keys = verifier.KeySet()
    consistency = _guarded("consistency", lambda: _consistency(receipt, statement, checkpoint))
    anchor: dict[str, Any] | None = None
    if consistency["status"] == FAILED:
        # A receipt that is not this log's says nothing about this log's withdrawals or anchors.
        withdrawal = anchor_check = _check(NOT_CHECKED, _NOT_THIS_LOG)
    else:
        from services.ledger_anchor import verify_checkpoint

        withdrawal = _guarded("withdrawal", lambda: _withdrawal(statement, public=public))
        anchor_check, anchor = verify_checkpoint(checkpoint)
        if public:
            anchor_check, anchor = _public_anchor(anchor_check, anchor)
    overrides = {"consistency": consistency, "withdrawal": withdrawal, "anchor": anchor_check}
    return verifier.verify_evidence(pack, receipt, keys, overrides=overrides, mode="online", anchor=anchor)


def evidence_for_pack(pack: dict[str, Any]) -> dict[str, Any]:
    """The verdict the pack-verify routes add: the pack's receipt, or "HMAC only".

    A pack that says it was issued with public-key signatures
    (``ledger_commitment``) and carries no receipt — the stored copy of a
    signed pack, for one — is looked up in the ledger by its digest; if it is
    not there either, that is a failed verification, not "HMAC only".
    """
    try:
        return verify_online(pack, search=verifier.commitment_of(pack) is not None)
    except Exception as exc:  # noqa: BLE001 - the HMAC answer must still be given
        logger.error("evidence verification could not run (%s)", type(exc).__name__, exc_info=exc)
        if isinstance(pack.get("ledger"), dict):
            return verifier.verify_evidence(pack, None, verifier.KeySet(), mode="online")
        return verifier.hmac_only_verdict("online", detail="the ledger could not be read now", pack=pack)
