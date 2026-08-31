"""Verification of the append-only evidence chain, and anchoring of proof packs.

``services.audit_log`` already HMAC-chains every workspace event (``prev_hash``
+ ``event_hash``). Nothing re-walked that chain, so an altered or deleted record
was undetectable in practice: a chain nobody verifies is a chain nobody can rely
on. This module is the verifier, plus the two things a verifier needs to be
honest about a real store:

* **Retention is not tampering.** Trimming the JSONL store deletes the oldest
  records, which leaves the first surviving record pointing at a hash that no
  longer exists — indistinguishable from a deletion attack. ``audit_log`` now
  writes a signed truncation checkpoint when it trims, and verification consults
  it to say "retention removed N records" instead of "the chain is broken".
* **A pointer is not an anchor.** A signed proof pack stamps ``prev_audit_hash``,
  but nothing was ever appended to the chain for it, so the pointer was dangling:
  the pack claimed a chain position it did not occupy. ``anchor_evidence`` files
  the pack's content hash *into* the chain and the pack carries the resulting
  record, so a pack and the chain can be checked against each other.

What this proves: the records that remain have not been edited or removed since
they were written by a process holding the platform HMAC secret, and an exported
pack matches the record filed for it. What it does not prove: that the recorded
facts were true, that the writer was who it claimed, or that the store's owner
could not have discarded records *and* their checkpoint. Only an external WORM /
timestamping anchor (``services.audit_anchor``, a stub by default) narrows that.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

#: Chain records carrying migration evidence, distinct from operator actions.
EVIDENCE_ACTION = "migration.evidence_sealed"
RUN_EVIDENCE_ACTION = "migration.run_evidence"

#: Ceiling on a single verification walk. A verifier that silently stops early
#: would report "verified" for a suffix while calling it the whole chain, so the
#: report always states how far it actually walked.
MAX_VERIFY_EVENTS = 20000

TRUNCATION_STORE = "audit_truncations.jsonl"


class ChainFinding:
    """One defect in the chain, at a known position.

    ``kind`` is the machine-readable verdict; ``detail`` is what an auditor
    reads. A finding never guesses intent — a hash mismatch is reported as
    "altered, or written under a different secret", because from the store
    alone those two are the same observation.
    """

    __slots__ = ("kind", "index", "event_id", "detail")

    def __init__(self, kind: str, *, index: int, event_id: str, detail: str) -> None:
        self.kind = kind
        self.index = index
        self.event_id = event_id
        self.detail = detail

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "index": self.index,
            "event_id": self.event_id,
            "detail": self.detail,
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncation_store_path() -> Path:
    return data_dir() / TRUNCATION_STORE


def _canonical(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=json_default)


def _sha256_hex(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def record_truncation(
    *,
    removed_count: int,
    last_removed_event_hash: str | None,
    first_kept_event_hash: str | None,
) -> dict[str, Any] | None:
    """Sign and file the fact that retention deleted the oldest ``removed_count`` records.

    Without this the verifier cannot tell retention from deletion. The checkpoint
    is itself HMAC-signed, so forging "retention removed the record you are
    looking for" requires the platform secret.
    """
    if removed_count <= 0:
        return None
    from services.audit_log import _hmac_event_hash, _platform_hmac_secret

    checkpoint = {
        "kind": "chain_truncation",
        "at": _now(),
        "removed_count": int(removed_count),
        "last_removed_event_hash": last_removed_event_hash or None,
        "first_kept_event_hash": first_kept_event_hash or None,
        "hash_alg": "HMAC-SHA256",
    }
    checkpoint["checkpoint_hmac"] = _hmac_event_hash(
        {k: v for k, v in checkpoint.items() if k != "checkpoint_hmac"},
        _platform_hmac_secret(),
    )
    path = truncation_store_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(checkpoint, default=json_default) + "\n")
    except Exception as exc:
        # A failed checkpoint write must not fail the audit append, but it does
        # mean the deletion is now unexplainable — say so in the log.
        logger.warning(
            "Retention removed %s audit record(s) but the truncation checkpoint "
            "could not be written; chain verification will report an unexplained "
            "prefix: %s",
            removed_count,
            exc,
        )
        return None
    return checkpoint


def list_truncations() -> list[dict[str, Any]]:
    """Signed retention checkpoints, oldest first. Unsigned entries are dropped."""
    from services.audit_log import _hmac_event_hash, _platform_hmac_secret

    path = truncation_store_path()
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return []
    secret = _platform_hmac_secret()
    out: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        claimed = str(entry.get("checkpoint_hmac") or "")
        expected = _hmac_event_hash(
            {k: v for k, v in entry.items() if k != "checkpoint_hmac"}, secret
        )
        # An unsigned or edited checkpoint is not evidence of retention — ignore
        # it so it cannot be used to excuse a deletion.
        if claimed and claimed == expected:
            out.append(entry)
    return out


def read_chain(*, limit: int = MAX_VERIFY_EVENTS) -> list[dict[str, Any]]:
    """The chain in write order (oldest first), across the whole store.

    Deliberately unscoped by workspace: the chain links every event, so a
    workspace-filtered read would show gaps that are only filtering.
    """
    from services import audit_log

    coll = audit_log._mongo_collection()
    if coll is not None:
        try:
            cursor = coll.find({}).sort("time", 1).limit(limit)
            return [{k: v for k, v in doc.items() if k != "_id"} for doc in cursor]
        except Exception as exc:
            logger.warning("Mongo chain read failed; falling back to file: %s", exc)

    path = audit_log.STORE_PATH
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return []
    events: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def verify_chain(*, limit: int = MAX_VERIFY_EVENTS) -> dict[str, Any]:
    """Re-walk the stored chain and name every record that does not hold up.

    Findings, in the order they are checked per record:

    ``event_hash_missing``
        The record carries no hash, so nothing about it can be verified.
    ``event_hash_mismatch``
        Recomputing the HMAC over the record's own content disagrees with the
        stored hash: the record was altered after it was written, or written
        under a different platform secret.
    ``broken_link``
        ``prev_hash`` does not name the previous record: a record between them
        was deleted, or the two were reordered.
    ``fork``
        Two records claim the same predecessor — a concurrent writer or a
        replayed segment. The chain is no longer a single line of history.
    ``unexplained_prefix``
        The oldest record points at a predecessor that is not in the store and
        no signed retention checkpoint accounts for it.
    """
    events = read_chain(limit=limit)
    findings: list[ChainFinding] = []
    truncations = list_truncations()
    if not events:
        return {
            "verified": True,
            "checked": 0,
            "chain_head": None,
            "findings": [],
            "retention_checkpoints": truncations,
            "walked_limit": limit,
            "honesty": (
                "No audit records to verify. An empty store proves nothing about "
                "history."
            ),
        }

    from services.audit_log import _hmac_event_hash, _platform_hmac_secret

    secret = _platform_hmac_secret()
    seen_prev: dict[str, int] = {}
    prev_hash: str | None = None
    for index, event in enumerate(events):
        event_id = str(event.get("id") or event.get("event_id") or "")
        stored_hash = str(event.get("event_hash") or "")
        if not stored_hash:
            findings.append(
                ChainFinding(
                    "event_hash_missing",
                    index=index,
                    event_id=event_id,
                    detail="Record carries no event_hash, so it cannot be verified.",
                )
            )
        else:
            expected = _hmac_event_hash(event, secret)
            if expected != stored_hash:
                findings.append(
                    ChainFinding(
                        "event_hash_mismatch",
                        index=index,
                        event_id=event_id,
                        detail=(
                            "Recomputed HMAC does not match the stored event_hash: "
                            "this record was altered after it was written, or it "
                            "was written under a different platform secret."
                        ),
                    )
                )

        claimed_prev = event.get("prev_hash") or None
        if index == 0:
            if claimed_prev and not _prefix_explained(str(claimed_prev), truncations):
                findings.append(
                    ChainFinding(
                        "unexplained_prefix",
                        index=index,
                        event_id=event_id,
                        detail=(
                            "The oldest stored record points at a predecessor that "
                            "is absent, and no signed retention checkpoint accounts "
                            "for its removal."
                        ),
                    )
                )
        elif str(claimed_prev or "") != str(prev_hash or ""):
            findings.append(
                ChainFinding(
                    "broken_link",
                    index=index,
                    event_id=event_id,
                    detail=(
                        f"prev_hash={_short(claimed_prev)} does not name the "
                        f"preceding record ({_short(prev_hash)}): a record between "
                        "them was removed, or the two were reordered."
                    ),
                )
            )

        if claimed_prev:
            first = seen_prev.get(str(claimed_prev))
            if first is not None:
                findings.append(
                    ChainFinding(
                        "fork",
                        index=index,
                        event_id=event_id,
                        detail=(
                            f"Record also claims the predecessor already claimed at "
                            f"index {first}: history forks here and is no longer a "
                            "single line."
                        ),
                    )
                )
            else:
                seen_prev[str(claimed_prev)] = index
        prev_hash = stored_hash or prev_hash

    return {
        "verified": not findings,
        "checked": len(events),
        "chain_head": str(events[-1].get("event_hash") or "") or None,
        "findings": [f.as_dict() for f in findings],
        "retention_checkpoints": truncations,
        "walked_limit": limit,
        "honesty": (
            "Verification covers the records still in the store: it proves they "
            "were not edited or removed since a holder of the platform secret "
            "wrote them. It does not prove the recorded facts are true, nor that "
            "records were never discarded together with their checkpoint. Only an "
            "external WORM / timestamp anchor narrows that."
        ),
    }


def _prefix_explained(claimed_prev: str, truncations: list[dict[str, Any]]) -> bool:
    """True when a signed checkpoint says retention removed the missing prefix."""
    return any(
        str(t.get("last_removed_event_hash") or "") == claimed_prev
        or str(t.get("first_kept_event_hash") or "") == claimed_prev
        for t in truncations
    )


def _short(value: Any) -> str:
    text = str(value or "")
    return (text[:12] + "…") if len(text) > 12 else (text or "none")


def anchor_evidence(
    *,
    evidence_kind: str,
    evidence_sha256: str,
    job_id: str = "",
    actor: str = "system",
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """File an evidence digest into the chain and return the record it occupies.

    The chain record commits to the digest, not to the artifact: the pack itself
    stays with whoever exported it, and the chain says "a pack with this content
    hash was sealed at this position". A pack whose bytes changed afterwards no
    longer matches the record filed for it.

    Never raises — an export must not fail because the audit store is down. The
    returned ``anchored`` flag says which happened, so a pack cannot silently
    claim a chain position it never got.
    """
    digest = str(evidence_sha256 or "").strip()
    if not digest:
        return {
            "anchored": False,
            "reason": "no evidence digest to anchor",
            "hash_alg": "HMAC-SHA256",
        }
    try:
        from services.audit_log import append_audit_event

        event = append_audit_event(
            action=EVIDENCE_ACTION,
            resource=f"job:{job_id}" if job_id else "job:unknown",
            actor=actor,
            level="success",
            details={
                "evidence_kind": evidence_kind,
                "evidence_sha256": digest,
                "job_id": job_id,
                **(summary or {}),
            },
        )
    except Exception as exc:
        logger.warning("Evidence anchor failed; evidence stays unanchored: %s", exc)
        return {
            "anchored": False,
            "reason": f"audit store unavailable: {exc}",
            "hash_alg": "HMAC-SHA256",
        }
    return {
        "anchored": True,
        "evidence_kind": evidence_kind,
        "evidence_sha256": digest,
        "event_id": event.get("id"),
        "event_hash": event.get("event_hash"),
        "prev_hash": event.get("prev_hash"),
        "sealed_at": event.get("time"),
        "hash_alg": "HMAC-SHA256",
    }


def find_anchor(evidence_sha256: str, *, limit: int = MAX_VERIFY_EVENTS) -> dict[str, Any] | None:
    """The chain record filed for ``evidence_sha256``, if the store still holds it."""
    digest = str(evidence_sha256 or "").strip()
    if not digest:
        return None
    for event in reversed(read_chain(limit=limit)):
        if str(event.get("action") or "") != EVIDENCE_ACTION:
            continue
        details = event.get("details") if isinstance(event.get("details"), dict) else {}
        if str(details.get("evidence_sha256") or "") == digest:
            return event
    return None


def seal_run_evidence(
    *,
    run_id: str,
    job_id: str,
    records_transferred: int = 0,
    reconciliation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Chain a durable record of a finished run, independent of a pack export.

    The in-memory lineage ring is bounded and process-local, and the job
    document is mutable, so without this a run that nobody exported a pack for
    left no tamper-evident trace at all.
    """
    recon = reconciliation if isinstance(reconciliation, dict) else {}
    summary = {
        "run_id": run_id,
        "records_transferred": int(records_transferred or 0),
        "gate8_phase": str(recon.get("phase") or "") or None,
        "gate8_passed": recon.get("passed"),
        "gate8_coverage": str(recon.get("coverage") or "") or None,
        "source_checksum": str(recon.get("source_checksum") or "") or None,
        "target_checksum": str(recon.get("target_checksum") or "") or None,
    }
    digest = _sha256_hex(_canonical({"job_id": job_id, **summary}))
    try:
        from services.audit_log import append_audit_event

        event = append_audit_event(
            action=RUN_EVIDENCE_ACTION,
            resource=f"job:{job_id}" if job_id else "job:unknown",
            actor="system",
            level="success",
            details={"evidence_sha256": digest, **summary},
        )
    except Exception as exc:
        logger.warning("Run evidence seal failed for job %s: %s", job_id, exc)
        return {"anchored": False, "reason": str(exc)}
    return {
        "anchored": True,
        "evidence_sha256": digest,
        "event_hash": event.get("event_hash"),
        "prev_hash": event.get("prev_hash"),
    }
