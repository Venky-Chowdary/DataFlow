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
from datetime import datetime, timedelta, timezone
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
    """The newest ``limit`` records of the whole store, in write order (oldest first).

    Unscoped by workspace: this is the platform line, and the indices of a
    platform :func:`verify_chain` report point into exactly this list. A store
    that cannot be read returns nothing — never another store's records.
    """
    from services.audit_chain_window import read_window

    return read_window(limit=limit).events


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
    ``key_mismatch`` (category ``unverifiable_key_lost``)
        The record names a signing key (``key_id``) this deployment does not
        hold — not configured, not retired, not in the persisted keyring — so
        its HMAC cannot be checked. Not evidence of tampering, and not
        verified either: the record stays unverifiable until that key is
        supplied again (D221, QA r12 #3). A key is "lost" only if the
        sealed key history (vault-sealed ids and last-signed times:
        ``audit_chain_keystore.key_history``) says the deployment signed with
        it and the record falls within its use, or the record sits before the
        chain position where the history began (an earlier deploy).
    ``unknown_key_after_keyring`` (category ``tampering``)
        The record names a key the deployment never signed with although it
        sits after the chain position where the history began (judged by
        position, never by the time the record claims), or a key that had
        stopped signing long before the trusted clock (the last record before
        it that verified, or the history's own anchor record), or its own time
        runs backwards against the verified records around it (C4 reviews).
        The key id was changed, typically to hide an edit. A forged history
        row fails its MAC and does not count. Every verify walk adds the keys
        it verified to the history, so an env key dropped later reads as
        lost, not as tampering.
    ``key_history_tampered`` (report field; counted as tampering)
        The history begins at a signed ``audit.key_history.began`` chain
        record its MAC'd epoch names. The epoch row failing its MAC, going
        missing, or naming a later anchor while an earlier one is still in
        the chain (history, copy and epoch wiped and recreated) is tampering.
        After a ``DATAFLOW_SECRETS_KEY`` change a platform admin re-anchors it
        (``POST /api/v1/audit/chain/key-history/reanchor``), which writes a
        signed ``audit.key_history.reanchored`` record.
    ``broken_link``
        ``prev_hash`` does not name the previous record, nor any earlier one: a
        record between them was deleted, or the records were reordered.
    ``fork``
        Two records claim the same predecessor, or a record links to an earlier
        position while a sibling branch sits between them — concurrent writers
        or a replayed segment. The chain is no longer a single line of history,
        but no record was removed.

    Each finding has a ``category`` (``tampering``, ``fork``,
    ``unverifiable_key_lost``, ``legacy_hash_input``) and the report totals
    them in ``categories``, so a fork or a lost key is never counted as
    tampering.
    ``unexplained_prefix``
        The oldest record points at a predecessor that is not in the store and
        no signed retention checkpoint accounts for it.

    A finding a platform admin migrated with :func:`reanchor_chain` (and that
    still matches the content sealed then) moves to ``reanchored``.

    What was walked (A1): the newest ``limit`` records of the scope, read from
    the store by workspace (``services.audit_chain_window``). The report names
    the ``store``, the ``records_in_scope`` and whether the limit left older
    ones out (``truncated_by_limit``). ``status`` is ``verified``,
    ``verified_with_exceptions`` (every record that could be checked holds up,
    and the only findings are records signed under a lost key: ``verified`` is
    false, ``tampering_detected`` is false, and ``unverifiable_key_lost`` gives
    the count per key id), ``findings``, ``nothing_checked (no record in scope — ``verified`` is
    null: nothing walked is never verified) or ``store_unavailable`` (the
    store could not be read; reported, never answered from another store).
    """
    from services.audit_chain_reanchor import split_reanchored
    from services.audit_chain_window import read_window

    ws = (workspace_id or "").strip()
    window = read_window(workspace_id=ws, limit=limit)
    events = window.events
    truncations = list_truncations()
    walk = {
        "store": window.store,
        "records_in_scope": window.total,
        "truncated_by_limit": window.truncated_by_limit,
        "walked_limit": limit,
        "retention_checkpoints": truncations,
    }
    if window.fallback_file_records:
        walk["unwalked_file_records"] = window.fallback_file_records
    if window.error or not events:
        return present_verify_report(_unverified_report(window, walk), [], ws)

    from services.audit_log import _hmac_event_hash

    positions = list(range(len(events)))
    if ws:
        first_claim = str(events[0].get("ws_prev_hash") or "")
        prefix_explained: bool | None = (
            window.truncated_by_limit
            or _retention_explains_first(
                [window.store_first] if window.store_first else [], truncations
            )
            or (bool(first_claim) and _prefix_explained(first_claim, truncations))
        )
        link_field = "ws_prev_hash"
    else:
        # The window's oldest record links to one the limit left out: that is
        # the limit, not a deletion. A whole-store walk asks the checkpoints.
        prefix_explained = True if window.truncated_by_limit else None
        link_field = "prev_hash"
    anchor = _key_history_state(_hmac_event_hash)
    findings = _walk(
        events, positions, _hmac_event_hash, truncations,
        link_field=link_field, prefix_explained=prefix_explained,
        trusted_before=_trusted_time_before(events[0], _hmac_event_hash),
        anchor=anchor,
    )
    history_tampered = anchor["tampered"]
    history_unreadable = anchor["unreadable"]
    # A workspace sees its own records' prior findings and history-level ones.
    prior = [
        p for p in anchor["prior_tampering"]
        if not ws or p.get("workspace_id") == ws
        or (not p.get("workspace_id") and not p.get("event_id"))
    ]
    head = events[-1]
    found = [f.as_dict() for f in findings]
    for f in found:
        ev = events[f["index"]]
        f["cause"] = _cause(f["kind"], ev)
        f["category"] = finding_category(f["kind"], f["cause"])
        f["chain_seq"] = _position(ev) or None
        if not ws:
            f["workspace_id"] = _event_workspace(ev) or None
    found, reanchored = split_reanchored(found, events)
    categories = {name: 0 for name in CATEGORIES}
    for f in found:
        categories[f["category"]] += 1
    lost = _lost_key_summary(found, events)
    if history_tampered:
        # The key history's anchor was removed or altered: the lost-key /
        # relabel verdicts rest on it, so that alone is tampering.
        categories["tampering"] += 1
    # Tampering an admin re-anchor acknowledged stays tampering (review 4, HIGH 3).
    categories["tampering"] += len(prior)
    if history_tampered or categories["tampering"]:
        status = "findings"
    elif categories["inconclusive"]:
        status = "inconclusive"
    elif not found:
        status = "verified"
    elif categories["unverifiable_key_lost"] == len(found):
        status = "verified_with_exceptions"
    else:
        status = "findings"
    from services.audit_chain_keys import describe

    honesty = (
        "Verification covers the records still in the store: it proves they "
        "were not edited or removed since a holder of the chain key they "
        "name wrote them. It does not prove the recorded facts are true, nor that "
        "records were never discarded together with their checkpoint. Only an "
        "external WORM / timestamp anchor narrows that."
    )
    if lost["count"]:
        honesty += (
            f" {lost['count']} record(s) name a signing key this deployment no longer "
            "holds, so they could not be checked; they are counted under "
            "unverifiable_key_lost, not as tampering, and the chain is not reported "
            "as verified over them."
        )
    if window.truncated_by_limit:
        honesty += (
            f" This walk covers the newest {len(events)} of {window.total} record(s); "
            "the older ones were not walked, and the first walked record's link "
            f"to them is not checked. Raise the limit (up to {MAX_VERIFY_EVENTS}) "
            "to cover more."
        )
    return present_verify_report(
        {
            **walk,
            "verified": not found and not history_tampered and not prior,
            "status": status,
            "key_history_tampered": history_tampered or None,
            "key_history_unreadable": history_unreadable or None,
            "prior_tampering": prior,
            "key_history_resolved": anchor["resolved"],
            "key_history_seq": anchor["history_seq"] or None,
            "key_history_signed_positions": anchor["signed_kh"],
            "categories": categories,
            "unverifiable_key_lost": lost,
            "tampering_detected": categories["tampering"] > 0,
            "signing": describe(),
            "legacy_records": _legacy_summary(events, positions, found),
            "checked": len(positions),
            "chain_head": str((head or {}).get("event_hash") or "") or None,
            "findings": found,
            "reanchored": reanchored,
            "honesty": honesty,
        },
        events,
        ws,
    )


def _unverified_report(window: Any, walk: dict[str, Any]) -> dict[str, Any]:
    """Nothing was walked, so nothing is verified — and the report says why.

    ``verified`` is null, not true: an empty walk proves nothing, and calling
    it verified is what QA read as "verified: true, checked: 0" (A1).
    """
    if window.error:
        status = "store_unavailable"
        honesty = (
            "The audit store could not be read, so no record was checked. This is "
            "not a verdict on the chain; try again, and check the audit store's "
            "health if it persists. The local fallback file is not substituted for it."
        )
    else:
        status = "nothing_checked"
        honesty = (
            "No audit records were found to verify, so nothing was checked. An empty "
            "walk proves nothing about history."
        )
    report: dict[str, Any] = {
        **walk,
        "verified": None,
        "status": status,
        "checked": 0,
        "chain_head": None,
        "findings": [],
        "categories": {name: 0 for name in CATEGORIES},
        "unverifiable_key_lost": {"count": 0, "by_key_id": {}},
        "tampering_detected": False,
        "reanchored": [],
        "honesty": honesty,
    }
    if window.error:
        report["store_error"] = window.error
    return report


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
    trusted_before: datetime | None = None,
    anchor: dict[str, Any] | None = None,
) -> list[ChainFinding]:
    """Check each record at ``positions`` and the ``link_field`` line between them.

    For the workspace line only records that carry ``link_field`` are
    link-checked (older records predate it).
    """
    from services import audit_chain_keystore as keystore
    from services.audit_chain_keys import check_signature, keyring

    if anchor is None:
        anchor = _key_history_state(hmac_of)
    history = anchor["history"]
    held = keyring()
    signatures = [check_signature(events[i], hmac_of) if events[i].get("event_hash") else ("", None)
                  for i in positions]
    # The time of the next record that verifies, per walked position: the
    # upper neighbour an unverifiable record's own time is checked against.
    next_trusted: list[datetime | None] = [None] * len(positions)
    upcoming: datetime | None = None
    for n in range(len(positions) - 1, -1, -1):
        next_trusted[n] = upcoming
        if signatures[n][0] == "ok":
            upcoming = _as_time(events[positions[n]].get("time")) or upcoming
    # The time of the last record that verified under a held key: a trusted
    # clock for the records after it (an unverifiable record's own time is
    # whatever its editor wrote).
    # A window (limit, workspace line) starts on the trusted clock of the
    # verified record just before it, never on nothing (third review).
    trusted_at: datetime | None = trusted_before
    verified_by_key: dict[str, datetime] = {}
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
            verdict, key_id = signatures[n]
            judged, relabel = (
                _relabel_reason(
                    key_id, history, trusted_at, anchor, seq=_position(event),
                    own_time=_as_time(event.get("time")), next_trusted=next_trusted[n],
                    proven_from=_relabelled_from(event, held, hmac_of),
                )
                if verdict == "unknown_key"
                else ("", "")
            )
            if verdict == "ok":
                stamp = _as_time(event.get("time"))
                if stamp is not None:
                    trusted_at = stamp
                    if key_id and (key_id not in verified_by_key or stamp > verified_by_key[key_id]):
                        verified_by_key[key_id] = stamp
            if judged == "inconclusive":
                findings.append(
                    ChainFinding("unknown_key_unjudged", index=index, event_id=event_id, detail=relabel)
                )
            elif relabel:
                findings.append(
                    ChainFinding(
                        "unknown_key_after_keyring",
                        index=index,
                        event_id=event_id,
                        detail=relabel,
                    )
                )
            elif verdict == "unknown_key":
                findings.append(
                    ChainFinding(
                        "key_mismatch",
                        index=index,
                        event_id=event_id,
                        detail=(
                            f"Signed under key {key_id}, which this deployment no "
                            "longer holds, so its HMAC cannot be checked. This is a "
                            "lost signing key, not evidence of tampering; the record "
                            "verifies again once a platform operator supplies that "
                            "key as a retired audit key."
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
                            "or was signed under a login secret since rotated — add "
                            "that secret to this deployment's retired audit keys."
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
    # Backfill: every key that verified here is now on the sealed history with
    # the newest record it signed, so dropping it later reads as a lost key.
    for kid, last in verified_by_key.items():
        keystore.note_key_id(kid, origin="verified", seen_at=last.isoformat())
    return findings


def _clock_skew() -> timedelta:
    """How far an unverifiable record's own time may run against its verified
    neighbours before it reads as edited (``DATAFLOW_AUDIT_CLOCK_SKEW_S``, 1 h)."""
    from services.brand_env import getenv_brand

    try:
        return timedelta(seconds=max(0.0, float(getenv_brand("AUDIT_CLOCK_SKEW_S", "") or 3600)))
    except ValueError:
        return timedelta(hours=1)


_KH_INDEXED: set[str] = set()


def _relabelled_from(event: dict[str, Any], held: dict[str, bytes], hmac_of: Any) -> str:
    """The held key the record verifies under once its ``key_id`` is set back.

    Proof of a relabel that needs none of the record's own unverifiable
    fields: only a holder of that key could have produced its hash.
    """
    stored = str(event.get("event_hash") or "")
    for kid, key in held.items():
        if stored and hmac_of({**event, "key_id": kid}, key) == stored:
            return kid
    return ""


def _key_history_state(hmac_of: Any) -> dict[str, Any]:
    """Where the sealed key history begins, judged from VERIFIED records only.

    Every signed record carries the history's position in its signed fields
    (``kh_seq``: the chain position of the "began" or current re-anchor
    record; ``kh_epoch``: its epoch id). ``history_seq`` is the EARLIEST
    ``kh_seq`` any verified record signs (or the epoch's anchor). A verified
    ``kh_seq`` the current epoch cannot reach through verified re-anchor
    records (whose signed details name the positions they supersede) means
    the history was reset or rolled back: ``tampered``. Nothing here reads an
    unverifiable record's action, resource, time or key id. Records written
    before ``kh_seq`` existed carry none and never move the anchor.

    ``unreadable``: the epoch or the entries could not be read — unknown keys
    are then not judged at all (inconclusive), never excused as lost.
    ``prior_tampering``: findings verified re-anchor records signed.
    """
    from services import audit_chain_keystore as keystore
    from services.audit_chain_keys import check_signature

    out: dict[str, Any] = {
        "epoch": None, "history_seq": 0, "tampered": "", "unreadable": "", "history": {},
        "prior_tampering": [], "resolved": [], "configured": True, "signed_kh": [],
    }
    rec: dict[str, str] | None = None
    try:
        rec = keystore.epoch_record(fresh=True)
    except keystore.HistoryTampered as exc:
        out["tampered"] = str(exc)
        if "MAC" in str(exc) or "disagrees" in str(exc):
            # The MAC key changed (a secrets-key change) or the row was edited:
            # no entry can be trusted, so no unknown key can be judged.
            out["unreadable"] = f"the key history cannot be trusted ({exc})"
    except Exception as exc:  # noqa: BLE001 - unreadable: inconclusive, never "judged without"
        out["unreadable"] = f"the key history epoch could not be read ({type(exc).__name__})"
    try:
        out["history"] = keystore.key_history(strict=True)
    except Exception as exc:  # noqa: BLE001
        out["unreadable"] = out["unreadable"] or (
            f"the key history entries could not be read ({type(exc).__name__})"
        )
    if rec is None and not out["tampered"] and not out["unreadable"]:
        out["configured"] = False  # no control-plane database: no history kept
        return out
    head_seq = _position({"chain_seq": (rec or {}).get("head_seq")}) if rec else 0
    if rec:
        out["epoch"] = _as_time(rec.get("at"))
    try:
        from services.control_plane_store import mongo_collection

        coll = mongo_collection("audit_events")
        if coll is None:
            out["history_seq"] = head_seq
            return out
        _ensure_kh_index(coll)

        def verified(doc: dict[str, Any]) -> bool:
            return bool(doc.get("event_hash")) and check_signature(doc, hmac_of)[0] == "ok"

        signed_kh: set[int] = set()
        for value in coll.distinct("kh_seq"):
            v = _position({"chain_seq": value})
            if v and any(verified(d) for d in coll.find({"kh_seq": value}).limit(20)):
                signed_kh.add(v)
        reanchors = [
            d for d in coll.find({"action": keystore.REANCHOR_ACTION}).sort("chain_seq", 1).limit(1000)
            if verified(d) and _position({"chain_seq": d.get("kh_seq")}) == _position(d)
        ]
        if head_seq:
            anchor = next((d for d in coll.find({"chain_seq": head_seq}).limit(5)
                           if str(d.get("event_hash") or "") == str(rec.get("head_hash") or "")), None)
            ok = (anchor is not None and verified(anchor)
                  and _position({"chain_seq": anchor.get("kh_seq")}) == head_seq
                  and str(anchor.get("kh_epoch") or "") == str(rec.get("epoch_id") or ""))
            purged = coll.find_one({"chain_seq": {"$lte": head_seq}}, {"_id": 1}) is None
            if not ok and not purged:
                out["tampered"] = out["tampered"] or (
                    "the chain record that anchors the key history (position "
                    f"{head_seq}) is missing or no longer verifies"
                )
            # Positions the current epoch legitimately supersedes, through
            # verified re-anchor records reachable from it.
            legit = {head_seq}
            changed = True
            while changed:
                changed = False
                for r in reanchors:
                    if _position(r) in legit:
                        d = r.get("details") or {}
                        for v in [d.get("prev_history_seq"), *(d.get("superseded_kh_seq") or [])]:
                            v = _position({"chain_seq": v})
                            if v and v not in legit:
                                legit.add(v)
                                changed = True
            stray = sorted(v for v in signed_kh if v not in legit)
            if stray:
                earlier = [v for v in stray if v < head_seq]
                out["tampered"] = out["tampered"] or (
                    f"the key history was reset: verified records sign the history begun at "
                    f"chain position {earlier[0]}, but its epoch now names position {head_seq} "
                    "with no verified re-anchor between"
                    if earlier
                    else f"the key history epoch was rolled back: verified records sign position "
                    f"{stray[0]}, after the epoch's {head_seq}"
                )
            for r in reanchors:
                if _position(r) not in legit:
                    continue
                d = r.get("details") or {}
                out["prior_tampering"].extend(
                    {**f, "reanchored_at": _position(r)} for f in (d.get("findings") or [])
                    if isinstance(f, dict)
                )
                problem = str(d.get("history_problem") or "")
                if problem:
                    item = {"reanchored_at": _position(r), "problem": problem}
                    if "fails its MAC" in problem:
                        out["resolved"].append(item)  # a secrets-key change reads like this
                    else:
                        out["prior_tampering"].append({"kind": "key_history_tampered", **item})
        out["signed_kh"] = sorted(signed_kh)
        starts = sorted(signed_kh | ({head_seq} if head_seq else set()))
        out["history_seq"] = starts[0] if starts else 0
    except Exception as exc:  # noqa: BLE001 - the anchor could not be checked: inconclusive
        out["unreadable"] = out["unreadable"] or (
            f"the key history anchor could not be checked ({type(exc).__name__})"
        )
        logger.warning("key history anchor not checked: %s", type(exc).__name__)
    return out


def _ensure_kh_index(coll: Any) -> None:
    ns = str(getattr(coll, "full_name", "") or id(coll))
    if ns in _KH_INDEXED:
        return
    _KH_INDEXED.add(ns)
    try:
        coll.create_index([("kh_seq", 1)], name="key_history_position", sparse=True)
    except Exception as exc:  # noqa: BLE001 - the lookup works without it, slower
        logger.warning("key history position index not created: %s", str(exc)[:200])


def _trusted_time_before(first: dict[str, Any], hmac_of: Any) -> datetime | None:
    """The time of the nearest record before ``first`` that verifies (Mongo store)."""
    try:
        from services.audit_chain_keys import check_signature
        from services.control_plane_store import mongo_collection

        coll = mongo_collection("audit_events")
        seq = _position(first)
        if coll is None or not seq:
            return None
        for doc in coll.find({"chain_seq": {"$lt": seq}}).sort("chain_seq", -1).limit(50):
            if doc.get("event_hash") and check_signature(doc, hmac_of)[0] == "ok":
                return _as_time(doc.get("time"))
    except Exception:  # noqa: BLE001 - no anchor: the record's own time is used
        return None
    return None


#: A lost key may have signed records this long after its sealed "last seen".
RELABEL_SLACK = timedelta(hours=2)


def _as_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _relabel_reason(
    key_id: str | None,
    history: dict[str, dict[str, Any]],
    trusted_at: datetime | None,
    anchor: dict[str, Any],
    *,
    seq: int = 0,
    own_time: datetime | None = None,
    next_trusted: datetime | None = None,
    proven_from: str = "",
) -> tuple[str, str]:
    """``(judgement, detail)`` for a record naming a key this deployment does not hold.

    ``("relabel", why)`` is tampering; ``("inconclusive", why)`` means it
    cannot be judged (history unreadable); ``("", "")`` may be a lost key.
    In order:
    * its hash verifies under a held key once ``key_id`` is set back: relabel;
    * the history is unreadable: inconclusive, never lost;
    * its own time runs backwards/forwards against its verified neighbours by
      more than :func:`_clock_skew` — unless its key is on the sealed history
      and the time is within that key's use: an edit;
    * a key on the history that stopped signing well before the trusted clock: relabel;
    * a key never on the history, at a chain position after the history began
      (``history_seq``, from verified records' signed ``kh_seq``): relabel.
    Before that position (pre-upgrade deploys) an unknown key is a lost key.
    """
    if proven_from:
        return "relabel", (
            f"Names key {key_id}, but its hash verifies under held key {proven_from}: "
            "the record's key id was changed. Counted as tampering."
        )
    if anchor.get("unreadable"):
        # A tampered history is still judged by position (failing toward
        # tampering); only one that could not be read is not judged at all.
        why = anchor.get("unreadable")
        return "inconclusive", (
            f"Names key {key_id}, which this deployment does not hold, and {why}: whether "
            "it is a lost key or a relabel cannot be judged, so it is neither excused nor verified."
        )
    epoch = anchor.get("epoch")
    history_seq = int(anchor.get("history_seq") or 0)
    after_history = bool(history_seq and seq and seq > history_seq)
    if after_history and epoch is not None and (trusted_at is None or trusted_at < epoch):
        trusted_at = epoch  # the anchor record verified, and this one follows it
    entry = history.get(str(key_id or ""))
    skew = _clock_skew()
    in_use = False
    if entry is not None and own_time is not None:
        first = _as_time(entry.get("first_seen")) if entry.get("first_seen") else None
        last = _as_time(entry.get("last_seen"))
        in_use = (first is None or own_time >= first - RELABEL_SLACK) and (
            last is None or own_time <= last + RELABEL_SLACK
        )
    if not in_use and own_time is not None:
        if trusted_at is not None and own_time < trusted_at - skew:
            return "relabel", (
                f"Names key {key_id} and claims {own_time.isoformat()}, earlier than the "
                f"verified record before it ({trusted_at.isoformat()}): the record was "
                "back-dated. Counted as tampering."
            )
        if next_trusted is not None and own_time > next_trusted + skew:
            return "relabel", (
                f"Names key {key_id} and claims {own_time.isoformat()}, later than the "
                f"verified record after it ({next_trusted.isoformat()}): the record's time "
                "was edited. Counted as tampering."
            )
    if entry is not None:
        last = _as_time(entry.get("last_seen"))
        if last is not None and trusted_at is not None and trusted_at > last + RELABEL_SLACK:
            return "relabel", (
                f"Names key {key_id}, which last signed at {last.isoformat()}, on a record "
                f"written after {trusted_at.isoformat()}: the key id was changed. "
                "Counted as tampering."
            )
        return "", ""
    if after_history or (
        not seq and epoch is not None and trusted_at is not None and trusted_at >= epoch
    ):
        return "relabel", (
            f"Names key {key_id}, which this deployment never signed with, on a record "
            "after the key history began: every writer since signs with a key on that "
            "history, so the record's key id was changed. Counted as tampering."
        )
    return "", ""


def _position(event: dict[str, Any]) -> int:
    try:
        return int(event.get("chain_seq") or 0)
    except (TypeError, ValueError):
        return 0


#: Report categories. Only ``tampering`` means content or history was changed.
CATEGORIES = ("tampering", "fork", "unverifiable_key_lost", "legacy_hash_input", "inconclusive")


def finding_category(kind: str, cause: str) -> str:
    if kind == "fork":
        return "fork"
    if kind == "key_mismatch":
        return "unverifiable_key_lost"
    if kind == "unknown_key_unjudged":
        return "inconclusive"
    if kind == "event_hash_mismatch" and cause == "legacy_datetime_roundtrip":
        return "legacy_hash_input"
    return "tampering"


def _lost_key_summary(
    findings: list[dict[str, Any]], events: list[dict[str, Any]]
) -> dict[str, Any]:
    """Records that could not be checked because their signing key is gone.

    Ids only, never key material, and no server variable names: workspace
    admins read this in Settings.
    """
    by_key: dict[str, int] = {}
    for f in findings:
        if f.get("category") != "unverifiable_key_lost":
            continue
        kid = str(events[f["index"]].get("key_id") or "")
        f["key_id"] = kid
        by_key[kid] = by_key.get(kid, 0) + 1
    out: dict[str, Any] = {"count": sum(by_key.values()), "by_key_id": by_key}
    if by_key:
        out["owner_action"] = (
            "These records were signed under a key this deployment no longer holds, "
            "so nothing can say whether they changed. If a copy of that key exists "
            "(an earlier data volume or audit key file), a platform operator can add "
            "it as a retired audit key and these records will verify again. If it "
            "is gone, they stay unverifiable; they are not counted as tampering."
        )
    return out


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
            "Add the previous login secret to this deployment's retired audit keys to "
            "tell them apart."
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
