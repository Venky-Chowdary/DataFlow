"""Audited re-anchor: migrate chain records the old hash input broke.

Records written before canonical hashing (``services.audit_chain_link``) that
held a ``datetime`` can never verify again: the HMAC covered microseconds and a
zone the store no longer has. Hiding those findings would make the verifier
lie; leaving them makes every verdict "failed" forever. A re-anchor is the
honest middle:

* a platform administrator names a reason and the causes to accept
  (by default only ``legacy_datetime_roundtrip``; a real tamper stays a
  finding unless they explicitly accept ``unexplained`` too);
* the decision is itself a chain record (``audit.chain.reanchored``) naming
  the actor, reason, counts and the manifest digest;
* the manifest seals each accepted record *as it stands now* (an HMAC over its
  current content). The verifier moves such a finding to ``reanchored`` only
  while the record still matches that seal — any later edit is a finding again.

What a re-anchor proves: the record has not changed since an identified admin
accepted it. What it does not prove: what the record said before the old hash
input broke it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)

ACTION = "audit.chain.reanchored"
CAUSES = ("legacy_datetime_roundtrip", "legacy_unserialized_writer", "unexplained")
DEFAULT_CAUSES = ("legacy_datetime_roundtrip",)
_STORE = "audit_reanchors.jsonl"
_COLLECTION = "audit_chain_reanchors"
_LINK_KINDS = {"broken_link", "fork", "unexplained_prefix"}


class ReanchorError(ValueError):
    """The re-anchor request cannot be carried out as asked."""


def _hmac(payload: dict[str, Any], key: bytes) -> str:
    from services.audit_chain_keys import hmac_hex

    return hmac_hex(payload, key)


def content_seal(event: dict[str, Any], key: bytes) -> str:
    """HMAC over the record's whole current content, its stored hash included.

    Sealed under the manifest's key: the chain key (and its ``key_id``) for
    manifests written since D221, the login secret for older ones.
    """
    body = {k: v for k, v in event.items() if k not in ("_id", "event_hash")}
    return _hmac({"record": body, "sealed_event_hash": event.get("event_hash")}, key)


def _collection() -> Any | None:
    from services import audit_log

    coll = audit_log._mongo_collection()
    if coll is None:
        return None
    try:
        return coll.database[_COLLECTION]
    except Exception:  # noqa: BLE001
        return None


def _file_path():
    from services.platform_config import data_dir

    return data_dir() / _STORE


def _signing_key_of(manifest: dict[str, Any]) -> bytes | None:
    from services.audit_chain_keys import verify_side_record

    return verify_side_record(manifest, field="manifest_hmac")


def list_manifests() -> list[dict[str, Any]]:
    """Signed re-anchor manifests, each with the key that verified it (``_key``).

    An unsigned or edited one, or one signed under a key no longer held, excuses
    nothing.
    """
    raw: list[dict[str, Any]] = []
    coll = _collection()
    if coll is not None:
        try:
            raw = [dict(doc) for doc in coll.find({})]
        except Exception as exc:  # noqa: BLE001
            logger.warning("re-anchor manifests unreadable from Mongo: %s", exc)
    if not raw:
        path = _file_path()
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(entry, dict):
                    raw.append(entry)
    out: list[dict[str, Any]] = []
    for manifest in raw:
        key = _signing_key_of(manifest)
        if key is not None:
            out.append({**manifest, "_key": key})
    return out


def _prev_of(events: list[dict[str, Any]], index: int) -> str | None:
    return str(events[index - 1].get("event_hash") or "") or None if index > 0 else None


def split_reanchored(
    findings: list[dict[str, Any]], events: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Move findings a still-valid re-anchor covers into their own list."""
    manifests = list_manifests() if findings else []
    if not manifests:
        return findings, []
    sealed: dict[tuple[str, str], tuple[dict[str, Any], dict[str, Any]]] = {}
    for manifest in manifests:
        for entry in manifest.get("entries") or []:
            sealed[(str(entry.get("event_id")), str(entry.get("kind")))] = (entry, manifest)
    kept: list[dict[str, Any]] = []
    moved: list[dict[str, Any]] = []
    for finding in findings:
        hit = sealed.get((str(finding.get("event_id")), str(finding.get("kind"))))
        index = int(finding.get("index") or 0)
        event = events[index] if 0 <= index < len(events) else None
        if hit and event is not None:
            entry, manifest = hit
            same = content_seal(event, manifest["_key"]) == entry.get("content_seal")
            if same and finding["kind"] in _LINK_KINDS:
                same = _prev_of(events, index) == entry.get("prev_seen")
            if same:
                moved.append(
                    {
                        "event_id": finding.get("event_id"),
                        "kind": finding.get("kind"),
                        "cause": entry.get("cause"),
                        "reanchored_at": manifest.get("at"),
                        "reanchored_by": manifest.get("actor"),
                        "reason": manifest.get("reason"),
                        "receipt_event_id": manifest.get("receipt_event_id"),
                    }
                )
                continue
        kept.append(finding)
    return kept, moved


def reanchor_chain(
    *,
    actor: str,
    reason: str,
    accept_causes: list[str] | tuple[str, ...] = DEFAULT_CAUSES,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Plan (``dry_run``) or apply a re-anchor of the current platform findings."""
    from services.evidence_chain import MAX_VERIFY_EVENTS, read_chain, verify_chain

    text = str(reason or "").strip()
    if len(text) < 10:
        raise ReanchorError("A re-anchor needs a reason of at least 10 characters.")
    causes = sorted({str(c) for c in accept_causes or ()})
    unknown = [c for c in causes if c not in CAUSES]
    if unknown or not causes:
        raise ReanchorError(f"accept_causes must be drawn from {', '.join(CAUSES)}")

    from services.audit_chain_keys import sign, signing_key

    report = verify_chain(limit=MAX_VERIFY_EVENTS)
    events = read_chain(limit=MAX_VERIFY_EVENTS)
    key_id, key, _source = signing_key()
    accepted = [f for f in report["findings"] if f.get("cause") in causes]
    skipped = len(report["findings"]) - len(accepted)
    entries = [
        {
            "event_id": f["event_id"],
            "kind": f["kind"],
            "cause": f["cause"],
            "workspace_id": f.get("workspace_id"),
            "content_seal": content_seal(events[f["index"]], key),
            "prev_seen": _prev_of(events, f["index"]),
        }
        for f in accepted
    ]
    plan = {
        "dry_run": bool(dry_run),
        "accepted_causes": causes,
        "accepted": [
            {k: e[k] for k in ("event_id", "kind", "cause", "workspace_id")} for e in entries
        ],
        "skipped_unexplained": skipped,
        "checked": report.get("checked", 0),
    }
    if dry_run or not entries:
        return {**plan, "receipt_event_id": None, "manifest_sha256": None}

    manifest: dict[str, Any] = {
        "kind": "chain_reanchor",
        "at": datetime.now(timezone.utc).isoformat(),
        "actor": actor,
        "reason": text,
        "accepted_causes": causes,
        "entries": entries,
    }
    digest = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, default=json_default).encode("utf-8")
    ).hexdigest()
    by_cause: dict[str, int] = {}
    for entry in entries:
        by_cause[entry["cause"]] = by_cause.get(entry["cause"], 0) + 1

    from services.audit_log import append_audit_event

    receipt = append_audit_event(
        action=ACTION,
        resource="/audit/chain",
        actor=actor,
        level="warn",
        details={
            "reason": text,
            "accepted_count": len(entries),
            "by_cause": by_cause,
            "accepted_causes": causes,
            "skipped_unexplained": skipped,
            "manifest_sha256": digest,
            "event_ids": [e["event_id"] for e in entries][:500],
        },
        reason_code="audit_chain_reanchor",
    )
    manifest["manifest_sha256"] = digest
    manifest["receipt_event_id"] = str(receipt.get("id") or "")
    manifest["key_id"] = key_id
    signed = sign(manifest, field="manifest_hmac")
    if signed["key_id"] != key_id:  # the key changed mid-call: seals would not match
        raise ReanchorError("The audit chain key changed during the re-anchor; retry.")
    _store(signed)
    return {**plan, "receipt_event_id": manifest["receipt_event_id"], "manifest_sha256": digest}


def _store(manifest: dict[str, Any]) -> None:
    coll = _collection()
    if coll is not None:
        coll.insert_one(dict(manifest))
        return
    path = _file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(manifest, default=json_default) + "\n")
