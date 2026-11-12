"""Evidence packs in the ledger: issue a statement for a signed pack, attach its receipt, withdraw.

The three signing paths (the proof pack export, Parity Studio's sign-off, the
Exit Evidence sign-off) keep signing exactly as before
(:func:`services.signed_proof_pack.sign_body`: content hash + HMAC). After a
pack is signed and stored, :func:`issue` adds what an outsider can check: a
public-key signature over a statement about the pack, appended to the log.
:func:`with_ledger` returns the pack with its receipt under ``ledger`` (outside
the content hash, like ``signature``).

Issuance never blocks or fails a signing path: when the ledger cannot be
written (no active key, vault or store down) the pack stays as it was — it
verifies by HMAC and is reported as "HMAC only" — and the failure is audited.
A statement is only ever issued for a pack this deployment signed.

Withdrawal appends a new entry naming the withdrawn pack; nothing is erased.
"""

from __future__ import annotations

import logging
from typing import Any

from services import ledger_keys, ledger_log
from tools import datawrap_verify as verifier

logger = logging.getLogger(__name__)

LEDGER_KEY = "ledger"
MAX_REASON = 500


def _audit(action: str, *, workspace_id: str, actor: str, details: dict[str, Any],
           outcome: str = "succeeded") -> None:
    from services.audit_log import append_audit_event

    try:
        append_audit_event(action=action, resource=f"evidence:{details.get('content_sha256') or ''}",
                           actor=actor or "system", level="info", details=details,
                           workspace_id=workspace_id or None, outcome=outcome)
    except Exception as exc:  # noqa: BLE001 - the ledger write stands; a failed append is reported
        logger.error("evidence audit %s failed (%s)", action, type(exc).__name__, exc_info=exc)


def _signed_here(pack: dict[str, Any]) -> bool:
    """The pack's content hash and HMAC are this deployment's (never issue for anything else)."""
    from services.signed_proof_pack import pack_subject, verify_body

    try:
        _actual, errors = verify_body(pack, subject=pack_subject(pack))
    except Exception:  # noqa: BLE001 - no signing secret: nothing can be confirmed
        return False
    return not errors


def commitment(workspace_id: str) -> dict[str, Any] | None:
    """What a pack about to be signed states about its public-key signatures, or None.

    It goes inside the pack's hashed body (``ledger_commitment``), so a pack
    that was issued with signatures and later arrives without them, or with
    fewer, is a failed verification and can never pass as "HMAC only". ``None``
    when the workspace cannot sign now: such a pack makes no promise.
    """
    try:
        signer = ledger_keys.signer(ledger_keys.scope_for(workspace_id), actor="system")
    except Exception as exc:  # noqa: BLE001 - signing a pack must never wait on the ledger
        logger.error("evidence ledger commitment not made (%s)", type(exc).__name__, exc_info=exc)
        return None
    if signer is None:
        return None
    return {"type": verifier.COMMITMENT_TYPE, "signature_algs": sorted(signer.algs)}


def issue(pack: dict[str, Any], *, workspace_id: str, actor: str) -> dict[str, Any] | None:
    """The ledger entry of a signed pack, appending one if none exists. ``None`` when it cannot be issued."""
    digest = str((pack or {}).get("content_sha256") or "")
    kind = verifier.pack_kind(pack) if isinstance(pack, dict) else ""
    try:
        existing = ledger_log.find_entry(digest, kind) if digest else None
        if existing:
            return existing
        if not digest or not _signed_here(pack):
            raise ledger_log.LedgerInvalid("the pack is not one this deployment signed")
        signer = ledger_keys.signer(ledger_keys.scope_for(workspace_id), actor="system")
        if signer is None:
            raise ledger_log.LedgerUnavailable("the workspace has no active signing key")
        ledger_keys.ensure_published(signer)
        entry, created = ledger_log.append(
            kind=kind, subject=verifier.pack_subject(pack), content_sha256=digest, workspace_id=workspace_id,
            actor=actor, signer=signer)
        ledger_log.checkpoint()  # a signed (and anchored) tree head that covers the new leaf
    except Exception as exc:  # noqa: BLE001 - issuance must never break a signing or export path
        reason = str(exc) if isinstance(exc, (ledger_log.LedgerError, ledger_keys.LedgerKeyError)) \
            else type(exc).__name__
        logger.error("evidence ledger issuance failed for %s %s (%s)", kind, digest[:16], reason, exc_info=exc)
        _audit("evidence.issue.failed", workspace_id=workspace_id, actor=actor, outcome="failed",
               details={"kind": kind, "content_sha256": digest, "reason": reason})
        return None
    if created:
        _audit("evidence.entry.issued", workspace_id=workspace_id, actor=actor,
               details={"kind": kind, "content_sha256": digest, "subject": entry["subject"],
                        "leaf_index": entry["index"], "signature_scheme": entry["signature_scheme"],
                        "signer_key_ids": entry["signer_key_ids"]})
    return entry


def with_ledger(pack: dict[str, Any] | None, *, workspace_id: str, actor: str) -> dict[str, Any] | None:
    """``pack`` with its receipt under ``ledger``; the pack unchanged when no receipt can be had."""
    if not isinstance(pack, dict):
        return pack
    entry = issue(pack, workspace_id=workspace_id, actor=actor)
    if not entry:
        return pack
    try:
        return {**pack, LEDGER_KEY: ledger_log.receipt(entry)}
    except Exception as exc:  # noqa: BLE001 - the pack is still exported, HMAC only
        logger.error("evidence receipt could not be built (%s)", type(exc).__name__, exc_info=exc)
        return pack


def withdraw(entry: dict[str, Any], *, reason: str, superseded_by: str | None, actor: str) -> dict[str, Any]:
    """Withdraw (or supersede) the pack of ``entry`` by appending a withdrawal entry."""
    reason = str(reason or "").strip()
    if not 1 <= len(reason) <= MAX_REASON:
        raise ledger_log.LedgerInvalid(f"Give a reason of 1 to {MAX_REASON} characters.")
    if entry.get("kind") == verifier.WITHDRAWAL:
        raise ledger_log.LedgerInvalid("A withdrawal cannot itself be withdrawn.")
    if entry.get("kind") not in verifier.PACK_KINDS:
        raise ledger_log.LedgerInvalid("A key event cannot be withdrawn; retire the key instead.")
    digest, workspace_id = str(entry["content_sha256"]), str(entry.get("workspace_id") or "")
    if ledger_log.withdrawal_of(digest):
        raise ledger_log.LedgerConflict("This pack is already withdrawn.")
    superseded_by = str(superseded_by or "").strip().lower() or None
    if superseded_by:
        replacement = ledger_log.find_entry(superseded_by)
        if (not replacement or superseded_by == digest
                or str(replacement.get("workspace_id") or "") != workspace_id
                or ledger_log.withdrawal_of(superseded_by)):
            raise ledger_log.LedgerInvalid(
                "superseded_by must be the content_sha256 of another issued pack of this workspace.")
    signer = ledger_keys.signer(ledger_keys.scope_for(workspace_id), actor="system")
    if signer is None:
        raise ledger_log.LedgerUnavailable("The workspace has no active signing key; create one first.")
    withdrawal, created = ledger_log.append(
        kind=verifier.WITHDRAWAL, subject=str(entry["subject"]), content_sha256=digest,
        workspace_id=workspace_id, actor=actor, signer=signer,
        detail={"withdrawal": {"withdrawn_kind": entry["kind"], "reason": reason,
                               "superseded_by": superseded_by}})
    if not created:
        raise ledger_log.LedgerConflict("This pack is already withdrawn.")
    ledger_log.checkpoint()
    _audit("evidence.entry.withdrawn", workspace_id=workspace_id, actor=actor,
           details={"kind": entry["kind"], "content_sha256": digest, "subject": entry["subject"],
                    "leaf_index": withdrawal["index"], "withdrawn_leaf_index": entry["index"],
                    "reason": reason, "superseded_by": superseded_by})
    return {"entry": ledger_log.public_entry(withdrawal),
            "withdrawn": ledger_log.public_entry(entry, withdrawal),
            "receipt": ledger_log.receipt(withdrawal)}
