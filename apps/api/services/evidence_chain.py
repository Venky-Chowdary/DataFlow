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
they were written by a process holding the chain key they name, and an exported
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
    looking for" requires the chain key.
    """
    if removed_count <= 0:
        return None
    from services.audit_chain_keys import sign

    checkpoint = {
        "kind": "chain_truncation",
        "at": _now(),
        "removed_count": int(removed_count),
        "last_removed_event_hash": last_removed_event_hash or None,
        "first_kept_event_hash": first_kept_event_hash or None,
        "hash_alg": "HMAC-SHA256",
    }
    # Signed with the chain key and naming it (D221), like every chain record.
    checkpoint = sign(checkpoint, field="checkpoint_hmac")
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
    from services.audit_chain_keys import verify_side_record

    path = truncation_store_path()
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").strip().splitlines()
    except Exception:
        return []
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
        # An unsigned or edited checkpoint is not evidence of retention — ignore
        # it so it cannot be used to excuse a deletion.
        if verify_side_record(entry, field="checkpoint_hmac") is not None:
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
            # Write order, not clock order: records sharing a timestamp would
            # otherwise come back in an arbitrary order and be reported as
            # broken links on a chain nobody had touched.
            cursor = coll.find({}).sort(list(audit_log.CHAIN_ORDER)).limit(limit)
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


def verify_chain(*, limit: int = MAX_VERIFY_EVENTS, workspace_id: str = "") -> dict[str, Any]:
    """Re-walk the stored chain and name every record that does not hold up.

    With no ``workspace_id`` this is the platform walk: every record, linked by
    the global ``prev_hash`` line. It is the platform admin's report, and each
    finding names the workspace of the record it is about.

    With ``workspace_id`` this is that workspace's own chain (R7): its records'
    HMACs, and the ``ws_prev_hash`` line that links each record to the previous
    record of the same workspace. Other tenants' records are neither walked
    nor counted, so their findings cannot fail this verdict or leak into it.
    Records written before per-workspace links existed are hash-checked only;
    their links are part of the platform walk.

    Findings, in the order they are checked per record:

    ``event_hash_missing``
        The record carries no hash, so nothing about it can be verified.
    ``event_hash_mismatch``
        Recomputing the HMAC, under the key the record names, disagrees with
        the stored hash: the record was altered after it was written, or (for a
        record with no ``key_id``, written before D221) it was signed under a
        login secret this deployment no longer lists, or (``cause`` =
        ``legacy_datetime_roundtrip``) it predates canonical hashing and holds a
        value the store rewrote.
    ``key_mismatch``
        The record names a signing key (``key_id``) this deployment does not
        hold, so its HMAC cannot be checked. Not evidence of tampering; the
        record is unverified until that key is listed again (D221).
    ``broken_link``
        ``prev_hash`` does not name the previous record, nor any earlier one: a
        record between them was deleted, or the records were reordered.
    ``fork``
        Two records claim the same predecessor, or a record links to an earlier
        position while a sibling branch sits between them — concurrent writers
        or a replayed segment. The chain is no longer a single line of history,
        but no record was removed.

    Each finding has a ``category`` (``tampering``, ``fork``, ``key_mismatch``,
    ``legacy_hash_input``) and the report totals them in ``categories``, so a
    fork or a key rotation is never counted as tampering.
    ``unexplained_prefix``
        The oldest record points at a predecessor that is not in the store and
        no signed retention checkpoint accounts for it.

    A finding a platform admin migrated with :func:`reanchor_chain` (and that
    still matches the content sealed then) moves to ``reanchored``.
    """
    from services.audit_chain_reanchor import split_reanchored

    events = read_chain(limit=limit)
    truncations = list_truncations()
    ws = (workspace_id or "").strip()
    if not events:
        return present_verify_report(
            {
                "verified": True,
                "checked": 0,
                "chain_head": None,
                "findings": [],
                "categories": {name: 0 for name in CATEGORIES},
                "tampering_detected": False,
                "reanchored": [],
                "retention_checkpoints": truncations,
                "walked_limit": limit,
                "honesty": (
                    "No audit records to verify. An empty store proves nothing about "
                    "history."
                ),
            },
            [],
            ws,
        )

    from services.audit_log import _hmac_event_hash

    if ws:
        positions = [i for i, ev in enumerate(events) if _event_workspace(ev) == ws]
        findings = _walk(
            events, positions, _hmac_event_hash, truncations,
            link_field="ws_prev_hash",
            prefix_explained=_retention_explains_first(events, truncations),
        )
        head = events[positions[-1]] if positions else None
    else:
        positions = list(range(len(events)))
        findings = _walk(
            events, positions, _hmac_event_hash, truncations,
            link_field="prev_hash", prefix_explained=None,
        )
        head = events[-1]
    found = [f.as_dict() for f in findings]
    for f in found:
        ev = events[f["index"]]
        f["cause"] = _cause(f["kind"], ev)
        f["category"] = finding_category(f["kind"], f["cause"])
        if not ws:
            f["workspace_id"] = _event_workspace(ev) or None
    found, reanchored = split_reanchored(found, events)
    categories = {name: 0 for name in CATEGORIES}
    for f in found:
        categories[f["category"]] += 1
    from services.audit_chain_keys import describe

    return present_verify_report(
        {
            "verified": not found,
            "categories": categories,
            "tampering_detected": categories["tampering"] > 0,
            "signing": describe(),
            "legacy_records": _legacy_summary(events, positions, found),
            "checked": len(positions),
            "chain_head": str((head or {}).get("event_hash") or "") or None,
            "findings": found,
            "reanchored": reanchored,
            "retention_checkpoints": truncations,
            "walked_limit": limit,
            "honesty": (
                "Verification covers the records still in the store: it proves they "
                "were not edited or removed since a holder of the chain key they "
                "name wrote them. It does not prove the recorded facts are true, nor that "
                "records were never discarded together with their checkpoint. Only an "
                "external WORM / timestamp anchor narrows that."
            ),
        },
        events,
        ws,
    )


def _event_workspace(event: dict[str, Any]) -> str:
    return str(event.get("workspace_id") or "").strip()


def _walk(
    events: list[dict[str, Any]],
    positions: list[int],
    hmac_of: Any,
    truncations: list[dict[str, Any]],
    *,
    link_field: str,
    prefix_explained: bool | None,
) -> list[ChainFinding]:
    """Check each record at ``positions`` and the ``link_field`` line between them.

    For the workspace line only records that carry ``link_field`` are
    link-checked (older records predate it).
    """
    from services.audit_chain_keys import check_signature

    findings: list[ChainFinding] = []
    seen_prev: dict[str, int] = {}
    walked: dict[str, int] = {}
    prev_hash: str | None = None
    prev_seq: int | None = None
    for n, index in enumerate(positions):
        event = events[index]
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
            verdict, key_id = check_signature(event, hmac_of)
            if verdict == "unknown_key":
                findings.append(
                    ChainFinding(
                        "key_mismatch",
                        index=index,
                        event_id=event_id,
                        detail=(
                            f"Signed under key {key_id}, which this deployment does "
                            "not hold, so its HMAC cannot be checked. This is a key "
                            "configuration gap, not evidence of tampering: list that "
                            "key in DATAFLOW_AUDIT_HMAC_RETIRED_KEYS to verify it."
                        ),
                    )
                )
            elif verdict == "mismatch":
                findings.append(
                    ChainFinding(
                        "event_hash_mismatch",
                        index=index,
                        event_id=event_id,
                        detail=(
                            "Recomputed HMAC does not match the stored event_hash: "
                            "this record was altered after it was written."
                            if key_id
                            else "Recomputed HMAC does not match the stored event_hash "
                            "under the login secret or any retired key: this record "
                            "(written before the chain had its own key) was altered, "
                            "or was signed under a login secret since rotated — list "
                            "that secret in DATAFLOW_AUDIT_HMAC_RETIRED_KEYS."
                        ),
                    )
                )

        linked = link_field in event
        claimed_prev = event.get(link_field) or None
        seq = _position(event)
        branched = False
        if linked and n == 0:
            explained = (
                prefix_explained
                if prefix_explained is not None
                else _prefix_explained(str(claimed_prev or ""), truncations)
            )
            if claimed_prev and not explained:
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
        elif linked and str(claimed_prev or "") != str(prev_hash or ""):
            parent = walked.get(str(claimed_prev or ""))
            if (
                parent is not None
                and seq > 0
                and _position(events[parent]) == seq - 1
                and (prev_seq is None or prev_seq <= seq)
            ):
                # It names the record one position back, which is still here:
                # a sibling branch was written in between. Nothing was removed.
                branched = True
                first = seen_prev.get(str(claimed_prev))
                findings.append(
                    ChainFinding(
                        "fork",
                        index=index,
                        event_id=event_id,
                        detail=(
                            f"Record claims the predecessor already claimed at index "
                            f"{first}: history forks here and is no longer a single line."
                            if first is not None
                            else f"Record links to {_short(claimed_prev)} (index "
                            f"{parent}), not to the record before it: a concurrent "
                            "writer's branch sits in between. No record is missing."
                        ),
                    )
                )
            else:
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

        if linked and claimed_prev:
            first = seen_prev.get(str(claimed_prev))
            if first is not None and not branched:
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
            elif first is None:
                seen_prev[str(claimed_prev)] = index
        if stored_hash:
            walked[stored_hash] = index
        prev_hash = stored_hash or prev_hash
        prev_seq = seq
    return findings


def _position(event: dict[str, Any]) -> int:
    try:
        return int(event.get("chain_seq") or 0)
    except (TypeError, ValueError):
        return 0


#: Report categories. Only ``tampering`` means content or history was changed.
CATEGORIES = ("tampering", "fork", "key_mismatch", "legacy_hash_input")


def finding_category(kind: str, cause: str) -> str:
    if kind == "fork":
        return "fork"
    if kind == "key_mismatch":
        return "key_mismatch"
    if kind == "event_hash_mismatch" and cause == "legacy_datetime_roundtrip":
        return "legacy_hash_input"
    return "tampering"


def _legacy_summary(
    events: list[dict[str, Any]], positions: list[int], findings: list[dict[str, Any]]
) -> dict[str, Any]:
    """How the records written before the chain had its own key fare.

    When none of them verifies, the likeliest cause is a rotated login secret,
    not wholesale alteration — but the store alone cannot tell the two apart,
    so this says so instead of guessing.
    """
    legacy = [i for i in positions if events[i].get("event_hash") and not events[i].get("key_id")]
    failed = {
        f["index"] for f in findings
        if f["kind"] == "event_hash_mismatch" and f["index"] in set(legacy)
    }
    out: dict[str, Any] = {
        "count": len(legacy),
        "hmac_verified": len(legacy) - len(failed),
        "hmac_failed": len(failed),
    }
    if legacy and len(failed) == len(legacy):
        out["note"] = (
            "No record written before the chain had its own key verifies under the "
            "current login secret or any retired key. That is what a rotated login "
            "secret looks like; it is also what altering all of them would look like. "
            "List the previous login secret in DATAFLOW_AUDIT_HMAC_RETIRED_KEYS to tell "
            "them apart."
        )
    return out


def _retention_explains_first(
    events: list[dict[str, Any]], truncations: list[dict[str, Any]]
) -> bool:
    """A workspace line may start mid-history only if retention trimmed the store."""
    first_prev = str(events[0].get("prev_hash") or "") if events else ""
    return bool(first_prev) and _prefix_explained(first_prev, truncations)


def _holds_datetime(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, dict):
        return any(_holds_datetime(v) for v in value.values())
    if isinstance(value, (list, tuple)):
        return any(_holds_datetime(v) for v in value)
    return False


def _cause(kind: str, event: dict[str, Any]) -> str:
    """What the store alone can say about why a record does not hold up.

    ``legacy_datetime_roundtrip``: written before canonical hashing, and it
    holds a value BSON rewrites (a datetime) — the old HMAC covered bytes the
    store no longer has. ``legacy_unserialized_writer``: a link defect on a
    record written before cross-process write positions (the old race could
    produce it; so could a deletion). Anything else is ``unexplained``.
    """
    legacy = "chain_v" not in event
    if kind == "event_hash_mismatch" and legacy and _holds_datetime(event):
        return "legacy_datetime_roundtrip"
    if kind in {"broken_link", "fork"} and legacy:
        return "legacy_unserialized_writer"
    return "unexplained"


def present_verify_report(
    report: dict[str, Any],
    events: list[dict[str, Any]],
    workspace_id: str = "",
) -> dict[str, Any]:
    """Label the report's scope. A workspace report holds only its own records."""
    ws = (workspace_id or "").strip()
    out = dict(report)
    out.setdefault("reanchored", [])
    out["workspace_id"] = ws or None
    out["scope"] = "workspace" if ws else "platform"
    # Kept for API clients that read it. A workspace walk never sees another
    # tenant's records, so there is nothing to withhold.
    out["withheld_findings"] = 0
    if ws:
        out["honesty"] = str(out.get("honesty") or "") + (
            f" This verdict covers this workspace's own chain: {out.get('checked') or 0} "
            "record(s), their HMACs, and the link from each to the previous record "
            "of this workspace. The platform-wide chain is verified by a platform "
            "administrator."
        )
    return out


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
    record = event if isinstance(event, dict) else {}
    return {
        "anchored": True,
        "evidence_kind": evidence_kind,
        "evidence_sha256": digest,
        "event_id": _record_text(record.get("id")),
        "event_hash": _record_text(record.get("event_hash")),
        "prev_hash": _record_text(record.get("prev_hash")),
        "sealed_at": _record_text(record.get("time")),
        "hash_alg": "HMAC-SHA256",
    }


def _record_text(value: Any) -> str | None:
    """A chain coordinate as the text a signed pack ships it as.

    The store decides what an event id or timestamp is (``ObjectId``,
    ``datetime``, ``str``), and this record is embedded in a document that gets
    hashed and signed. Fixing the carrier here means the anchor a verifier reads
    is the anchor the signer hashed, whatever backed the store.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value) or None


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
