"""Durable quarantine dead-letter queue — Mongo primary, JSONL fallback.

Jobs already persist ``rejected_details`` on the job document. This module
adds a workspace-scoped, replay-auditable DLQ so remediations survive job GC
and multi-instance deploys share the same remediation trail.

Replay closure (Dual Run's sibling)
-----------------------------------
Airbyte/Fivetran land a DLQ stream and leave the original sync's metrics
open forever. Kafka/Uber DLQ practice is: stable event identity, idempotent
upsert, mark merged only after side effects commit, never bulk-drain poison
pills. Module 9 already named ``retry_status``; this module is the kernel
that *uses* it.

    open_quarantine = durable_rejects − Gate-8-promoted remediations

``evaluate_replay_closure`` is Dual Run's ``evaluate_campaign`` for hold-outs:
cutover analogue is ``closed`` (open_count == 0), never ``migration_proven``.
The parent Gate-8 report is historical — remediations get their own child
keyed-upsert proof. Replay stays at-least-once upsert.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

DLQ_PATH = data_dir() / "quarantine_dlq.jsonl"
_MONGO_COLL = "quarantine_dlq"
# Rotate JSONL before unbounded growth fills the disk (and then silently fails).
_DLQ_MAX_BYTES = 100 * 1024 * 1024  # 100 MiB


class QuarantineDlqLostError(RuntimeError):
    """Rejected rows exist but durable DLQ persist failed — fail closed.

    Migration Assurance forbids completing a transfer as success/quarantine-ok
    when rejected rows cannot be recovered from the control-plane DLQ.
    """


def is_contract_skip_detail(detail: Any) -> bool:
    """True for SKIP_ROW / audit-skip rows — not replay-DLQ quarantine.

    Intentional contract skips must not enter ``{table}_df_quarantine`` or the
    control-plane replay queue. They remain in ``rejected_details`` for audit.
    """
    if not isinstance(detail, dict):
        return False
    if detail.get("quarantine_required") is False:
        return True
    if str(detail.get("disposition") or "").strip().lower() == "skipped":
        return True
    if str(detail.get("execution_policy") or "").strip().upper() == "SKIP_ROW":
        return True
    return False


def replay_quarantine_details(
    rejected_details: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Filter to rows that must be durable in the replay DLQ."""
    return [
        d
        for d in (rejected_details or [])
        if isinstance(d, dict) and not is_contract_skip_detail(d)
    ]


def persist_job_quarantine_outcome(dest_summary: dict[str, Any] | None) -> dict[str, Any]:
    """Evaluate whether quarantine durability is acceptable for terminal status."""
    summary = dest_summary or {}
    details = list(summary.get("rejected_details") or [])
    replay = replay_quarantine_details(details)
    skip_count = max(0, len(details) - len(replay))
    if not replay:
        return {
            "ok": True,
            "fail_closed": False,
            "rejected_count": 0,
            "skipped_contract_count": skip_count,
            "quarantine_durable": True,
            "note": (
                "No replay-quarantine rows — DLQ durability not required."
                if not details
                else (
                    f"{skip_count} SKIP_ROW/audit-skip row(s) only — "
                    "not DLQ replay; durability not required."
                )
            ),
        }
    durable = summary.get("quarantine_durable")
    ok = durable is True
    return {
        "ok": ok,
        "fail_closed": not ok,
        "rejected_count": len(replay),
        "skipped_contract_count": skip_count,
        "quarantine_durable": durable,
        "error": summary.get("quarantine_dlq_error"),
        "note": (
            "Control-plane DLQ durable."
            if ok
            else (
                "Rejected rows exist but control-plane DLQ is not durable — "
                "fail closed (Module 5). Replay would find nothing."
            )
        ),
    }


def assert_quarantine_durable_or_raise(dest_summary: dict[str, Any] | None) -> None:
    """Raise when rejected rows would be lost from the durable DLQ."""
    outcome = persist_job_quarantine_outcome(dest_summary)
    if outcome["ok"]:
        return
    err = (dest_summary or {}).get("quarantine_dlq_error") or outcome["note"]
    raise QuarantineDlqLostError(
        f"Quarantine DLQ not durable for {outcome['rejected_count']} rejected row(s): {err}"
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dlq_coll():
    from services.control_plane_store import mongo_collection

    return mongo_collection(_MONGO_COLL)


def append_dlq_event(
    *,
    job_id: str,
    action: str,
    rows: int = 0,
    child_job_id: str = "",
    workspace_id: str = "",
    details: dict[str, Any] | None = None,
    path: Path | None = None,
) -> dict[str, Any]:
    """Append a DLQ event. Prefer Mongo; fall back to JSONL. Never silently drops."""
    event = {
        "id": str(uuid.uuid4()),
        "ts": _now(),
        "job_id": job_id,
        "child_job_id": child_job_id or None,
        "action": action,
        "rows": int(rows or 0),
        "workspace_id": workspace_id or "",
        "details": details or {},
    }
    coll = _dlq_coll()
    if coll is not None and path is None:
        try:
            doc = dict(event)
            doc["_id"] = event["id"]
            coll.insert_one(doc)
            return event
        except Exception as exc:
            logger.warning("DLQ Mongo append failed, falling back to JSONL: %s", exc)

    target: Path = path or DLQ_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        if target.exists() and target.stat().st_size >= _DLQ_MAX_BYTES:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            rotated = target.with_name(f"{target.stem}.{stamp}{target.suffix}")
            target.rename(rotated)
            logger.warning("DLQ JSONL rotated to %s (size cap %s bytes)", rotated, _DLQ_MAX_BYTES)
    except OSError as exc:
        logger.warning("DLQ rotation check failed: %s", exc)
    line = json.dumps(event, default=json_default) + "\n"
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            with target.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()
            return event
        except OSError as exc:
            last_exc = exc
            logger.warning("DLQ append failed (attempt %s): %s", attempt + 1, exc)
            time.sleep(0.05 * (attempt + 1))
    if last_exc is None:
        raise RuntimeError("DLQ append failed but no exception was captured")
    raise last_exc


_DLQ_CHUNK_SIZE = 200


def persist_rejected_rows(
    *,
    job_id: str,
    rejected_details: list[dict[str, Any]] | None,
    workspace_id: str = "",
    source: str = "transfer",
    connector: str = "",
) -> dict[str, Any] | None:
    """Persist rejected/quarantined rows to the DLQ. Returns summary event or None.

    Module 9 / GA: normalize to quarantine row contract and fail closed without
    job_id. Chunk appends so row bodies are never silently truncated to 500.
    """
    from services.quarantine_row_contract import (
        QuarantineRowContractError,
        assert_quarantine_rows_contract,
        normalize_quarantine_rows,
    )

    raw = list(rejected_details or [])
    if not raw:
        return None
    skip_n = sum(1 for d in raw if is_contract_skip_detail(d))
    replay = replay_quarantine_details(raw)
    skips = [d for d in raw if isinstance(d, dict) and is_contract_skip_detail(d)]
    jid = str(job_id or "").strip()
    if not jid:
        raise QuarantineRowContractError(
            "persist_rejected_rows requires job_id — refuse undurable quarantine"
        )

    # SKIP_ROW audit is durable but never enters replay (action=skip_audit).
    skip_chunks_written = 0
    if skips:
        skip_chunks = [
            skips[i : i + _DLQ_CHUNK_SIZE]
            for i in range(0, len(skips), _DLQ_CHUNK_SIZE)
        ]
        for idx, chunk in enumerate(skip_chunks):
            append_dlq_event(
                job_id=jid,
                action="skip_audit",
                rows=len(chunk),
                workspace_id=workspace_id,
                details={
                    "source": source,
                    "rejected_details": chunk,
                    "chunk_index": idx,
                    "chunk_count": len(skip_chunks),
                    "total_skipped": len(skips),
                    "audit_only": True,
                },
            )
            skip_chunks_written += 1

    if not replay:
        return {
            "rows": 0,
            "chunks": 0,
            "skip_audit_chunks": skip_chunks_written,
            "quarantine_durable": True,
            "total_rejected": 0,
            "skipped_contract": skip_n,
            "note": (
                "Contract skip rows excluded from replay DLQ"
                + (f"; {skip_n} skip_audit event(s) durable" if skip_n else "")
            ),
        }
    rows = normalize_quarantine_rows(replay, job_id=jid, connector=connector)
    assert_quarantine_rows_contract(rows, require_job_id=True)
    chunk_size = _DLQ_CHUNK_SIZE
    chunks = [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]
    last_event: dict[str, Any] | None = None
    for idx, chunk in enumerate(chunks):
        last_event = append_dlq_event(
            job_id=jid,
            action="quarantine" if idx == 0 else "quarantine_chunk",
            rows=len(chunk),
            workspace_id=workspace_id,
            details={
                "source": source,
                "rejected_details": chunk,
                "chunk_index": idx,
                "chunk_count": len(chunks),
                "total_rejected": len(rows),
                "skipped_contract": skip_n,
            },
        )
    return {
        **(last_event or {}),
        "rows": len(rows),
        "chunks": len(chunks),
        "skip_audit_chunks": skip_chunks_written,
        "quarantine_durable": True,
        "total_rejected": len(rows),
        "skipped_contract": skip_n,
    }


def quarantine_details_from_dlq(
    job_id: str,
    *,
    max_rows: int = 10000,
    include_skip_audit: bool = True,
) -> list[dict[str, Any]]:
    """Flatten durable DLQ quarantine (+ optional skip_audit) into Inspect rows.

    Job documents truncate ``rejected_details`` (~2000). Stream/engine persist
    full bodies here — Inspect must hydrate from DLQ when the job sample is
    incomplete, or quarantine overflow is invisible to operators.
    """
    jid = str(job_id or "").strip()
    if not jid:
        return []
    # Enough events for large chunked jobs (200 rows/chunk → 50 events = 10k).
    events = list_dlq_events(job_id=jid, limit=500)
    events.sort(
        key=lambda e: (
            str(e.get("ts") or ""),
            int((e.get("details") or {}).get("chunk_index") or 0),
        )
    )
    allowed = {"quarantine", "quarantine_chunk"}
    if include_skip_audit:
        allowed.add("skip_audit")
    out: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for ev in events:
        if str(ev.get("action") or "") not in allowed:
            continue
        chunk = (ev.get("details") or {}).get("rejected_details") or []
        for d in chunk:
            if not isinstance(d, dict):
                continue
            key = (
                d.get("row"),
                str(d.get("column") or ""),
                str(d.get("reason") or "")[:160],
            )
            if key in seen:
                continue
            seen.add(key)
            out.append(d)
            if len(out) >= max_rows:
                return out
    return out


def list_dlq_events(*, job_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    coll = _dlq_coll()
    if coll is not None:
        try:
            query: dict[str, Any] = {}
            if job_id:
                query["job_id"] = job_id
            docs = list(coll.find(query).sort("ts", -1).limit(max(1, int(limit))))
            out: list[dict[str, Any]] = []
            for d in docs:
                row = dict(d)
                row.pop("_id", None)
                out.append(row)
            return out
        except Exception:
            logger.debug("DLQ Mongo list failed", exc_info=True)

    path: Path = DLQ_PATH
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if job_id and ev.get("job_id") != job_id:
            continue
        events.append(ev)
        if len(events) >= limit:
            break
    return events


# ---------------------------------------------------------------------------
# Replay closure — Module 9 retry_status as a Dual Run sibling.
# Honesty: closed ≠ parent Gate-8 rewrite, ≠ migration_proven.
# ---------------------------------------------------------------------------

VERDICT_VACUOUS = "vacuous"
VERDICT_OPEN = "open"
VERDICT_IN_PROGRESS = "in_progress"
VERDICT_DIVERGING = "diverging"
VERDICT_CLOSED = "closed"

_CLOSURE_NOTE = (
    "Quarantine remediations close hold-outs via keyed upsert + child Gate-8. "
    "closed is not migration_proven and does not rewrite the parent transfer's "
    "Gate-8 report. Delivery remains at-least-once upsert."
)
_REPLAY_HISTORY_KEEP = 10


def replay_row_identity(detail: dict[str, Any] | None) -> str:
    """Stable row identity for retry_status — never a broker offset.

    Replay upserts a *row*, so every cell finding that shares this identity
    closes together. Prefer a proven source PK (Uber/Kafka DLQ: event id).
    Fall back to the 1-based source row, then a compact values digest.
    """
    from services.quarantine_row_contract import normalize_quarantine_row

    row = normalize_quarantine_row(detail if isinstance(detail, dict) else {})
    pk = row.get("source_pk")
    if row.get("source_pk_proven") and pk is not None and str(pk) != "":
        return f"pk:{pk}"
    src_row = row.get("row")
    try:
        n = int(src_row)
    except (TypeError, ValueError):
        n = 0
    if n:
        return f"row:{n}"
    bag = row.get("source_values") if isinstance(row.get("source_values"), dict) else None
    if not bag:
        bag = row.get("values") if isinstance(row.get("values"), dict) else {}
    if isinstance(bag, dict) and bag:
        keys = ",".join(f"{k}={bag[k]!s}" for k in sorted(bag)[:8])
        return f"values:{keys[:160]}"
    col = str(row.get("column") or row.get("target") or "")
    reason = str(row.get("failure_reason") or row.get("reason") or "")[:80]
    return f"cell:{col}|{reason}"


def is_open_retry_status(status: str | None) -> bool:
    from services.quarantine_row_contract import RETRY_OPEN_STATUSES

    return str(status or "open").strip().lower() in RETRY_OPEN_STATUSES


def open_quarantine_details(
    details: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Findings still in the replay set. SKIP_ROW never enters this list."""
    replay = replay_quarantine_details(details)
    return [d for d in replay if is_open_retry_status(d.get("retry_status"))]


def stamp_replay_attempt(
    findings: list[dict[str, Any]] | None,
    *,
    child_rejected: list[dict[str, Any]] | None,
    gate8_passed: bool,
    child_job_id: str = "",
    attempted: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Apply Module 9 retry_status after one replay child.

    Merges onto the *full* durable set so a partial promote cannot claim
    ``closed`` while earlier findings are still open. Gate-8 failure promotes
    nothing — dest may have upserted (at-least-once) but the ledger stays open
    so the operator can replay idempotently. When Gate-8 passed, identities
    still in the child's rejected_details stay ``replay_failed``; the rest of
    the attempt set become ``promoted``. Rows not in this attempt are unchanged.
    """
    from services.quarantine_row_contract import normalize_quarantine_row

    full = [
        normalize_quarantine_row(d)
        for d in replay_quarantine_details(findings)
    ]
    attempt_rows = replay_quarantine_details(
        attempted if attempted is not None else open_quarantine_details(full)
    )
    attempt_ids = {replay_row_identity(d) for d in attempt_rows}
    if not gate8_passed:
        failed_ids = set(attempt_ids)
    else:
        failed_ids = {
            replay_row_identity(d)
            for d in replay_quarantine_details(child_rejected)
        }
    now = _now()
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in full:
        ident = replay_row_identity(row)
        seen.add(ident)
        if ident not in attempt_ids:
            out.append(row)
            continue
        stamped = dict(row)
        if ident in failed_ids:
            stamped["retry_status"] = "replay_failed"
        else:
            stamped["retry_status"] = "promoted"
            stamped["promoted_at"] = now
        if child_job_id:
            stamped["replay_child_job_id"] = child_job_id
        out.append(stamped)
    for row in attempt_rows:
        ident = replay_row_identity(row)
        if ident in seen:
            continue
        stamped = normalize_quarantine_row(row)
        stamped["retry_status"] = "replay_failed" if ident in failed_ids else "promoted"
        if stamped["retry_status"] == "promoted":
            stamped["promoted_at"] = now
        if child_job_id:
            stamped["replay_child_job_id"] = child_job_id
        out.append(stamped)
    return out


def evaluate_replay_closure(
    findings: list[dict[str, Any]] | None,
    *,
    last_replay: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dual Run sibling: consecutive remediations until hold-outs hit zero.

    Verdicts
    --------
    vacuous     — no replay-quarantine rows (SKIP_ROW only / empty)
    open        — durable rejects, none promoted
    in_progress — some promoted, some still open
    diverging   — last replay Gate-8 failed or still rejected
    closed      — every durable reject is promoted (not migration_proven)
    """
    replay = replay_quarantine_details(findings)
    durable = len(replay)
    open_n = sum(1 for d in replay if is_open_retry_status(d.get("retry_status")))
    promoted_n = sum(
        1 for d in replay if str(d.get("retry_status") or "").lower() == "promoted"
    )
    failed_n = sum(
        1 for d in replay if str(d.get("retry_status") or "").lower() == "replay_failed"
    )
    last = last_replay if isinstance(last_replay, dict) else {}
    last_gate8 = last.get("gate8_passed")
    last_rejected = int(last.get("rejected") or 0) if last else 0

    if durable == 0:
        verdict = VERDICT_VACUOUS
        next_action = "No replay-quarantine rows — DLQ closure is not required."
    elif last and last_gate8 is False:
        verdict = VERDICT_DIVERGING
        next_action = (
            "Last Promote/Replay failed Gate-8 — fix the child proof, then "
            "replay remaining open rows (upsert is idempotent)."
        )
    elif last and last_rejected > 0 and open_n > 0:
        verdict = VERDICT_DIVERGING if promoted_n == 0 else VERDICT_IN_PROGRESS
        next_action = (
            f"{open_n} open finding(s) still rejected after replay — edit the "
            "bad cells, then Promote remaining (not a bulk drain)."
        )
    elif open_n == 0:
        verdict = VERDICT_CLOSED
        next_action = (
            "Quarantine ledger closed — remediations landed with child Gate-8. "
            "This is not a rewrite of the parent transfer's checksum."
        )
    elif promoted_n > 0:
        verdict = VERDICT_IN_PROGRESS
        next_action = (
            f"{promoted_n} promoted, {open_n} still open — Promote remaining "
            "open rows only."
        )
    else:
        verdict = VERDICT_OPEN
        next_action = (
            "Edit bad cells if needed, then Promote / Replay the stored "
            "quarantine payload (not a fresh source extract)."
        )

    return {
        "verdict": verdict,
        "open_count": open_n,
        "promoted_count": promoted_n,
        "failed_count": failed_n,
        "durable_count": durable,
        "next_action": next_action,
        "note": _CLOSURE_NOTE,
        "last_replay": last or None,
        "migration_proven": False,
    }


def record_replay(
    findings: list[dict[str, Any]] | None,
    *,
    child_rejected: list[dict[str, Any]] | None,
    gate8_passed: bool,
    child_job_id: str = "",
    rows_written: int = 0,
    rejected: int = 0,
    prior: dict[str, Any] | None = None,
    attempted: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Stamp retry_status, append one Dual-Run-style cycle, return compact closure."""
    stamped = stamp_replay_attempt(
        findings,
        child_rejected=child_rejected,
        gate8_passed=bool(gate8_passed),
        child_job_id=child_job_id,
        attempted=attempted,
    )
    compact = {
        "child_job_id": child_job_id,
        "checked_at": _now(),
        "gate8_passed": bool(gate8_passed),
        "rows_written": int(rows_written or 0),
        "rejected": int(rejected or 0),
        "promoted_identities": [
            replay_row_identity(d)
            for d in stamped
            if str(d.get("retry_status") or "") == "promoted"
        ],
        "failed_identities": [
            replay_row_identity(d)
            for d in stamped
            if str(d.get("retry_status") or "") == "replay_failed"
        ],
    }
    current = dict(prior or {})
    history = [e for e in list(current.get("history") or []) if isinstance(e, dict)]
    history.append({
        "child_job_id": compact["child_job_id"],
        "checked_at": compact["checked_at"],
        "gate8_passed": compact["gate8_passed"],
        "rows_written": compact["rows_written"],
        "rejected": compact["rejected"],
        "promoted_count": len(compact["promoted_identities"]),
        "failed_count": len(compact["failed_identities"]),
    })
    history = history[-_REPLAY_HISTORY_KEEP:]
    state = evaluate_replay_closure(stamped, last_replay=compact)
    return {
        **state,
        "history": history,
        "updated_at": compact["checked_at"],
        "findings": stamped,
        "promoted_identities": compact["promoted_identities"],
        "failed_identities": compact["failed_identities"],
    }


def apply_replay_overlay(
    details: list[dict[str, Any]] | None,
    *,
    job_id: str = "",
    closure: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Re-apply durable replay_closure events onto findings (append-only DLQ).

    JSONL/Mongo events are the log; job-document ``retry_status`` is a cache.
    Overlay is last-write-wins per identity.
    """
    from services.quarantine_row_contract import normalize_quarantine_row

    rows = [
        normalize_quarantine_row(d)
        for d in (details or [])
        if isinstance(d, dict)
    ]
    status_by_id: dict[str, str] = {}
    if job_id:
        events = list_dlq_events(job_id=job_id, limit=500)
        overlays = [
            e for e in events
            if str(e.get("action") or "") == "replay_closure"
        ]
        overlays.sort(key=lambda e: str(e.get("ts") or ""))
        for ev in overlays:
            payload = ev.get("details") if isinstance(ev.get("details"), dict) else {}
            for ident in payload.get("promoted_identities") or []:
                status_by_id[str(ident)] = "promoted"
            for ident in payload.get("failed_identities") or []:
                status_by_id[str(ident)] = "replay_failed"
    stored = closure if isinstance(closure, dict) else {}
    for ident in stored.get("promoted_identities") or []:
        status_by_id[str(ident)] = "promoted"
    for ident in stored.get("failed_identities") or []:
        status_by_id[str(ident)] = "replay_failed"
    if not status_by_id:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        ident = replay_row_identity(row)
        if ident in status_by_id:
            stamped = dict(row)
            stamped["retry_status"] = status_by_id[ident]
            out.append(stamped)
        else:
            out.append(row)
    return out


def compact_replay_closure(closure: dict[str, Any] | None) -> dict[str, Any]:
    """Job-document payload — identities stay in the DLQ event, not Mongo bloat."""
    c = dict(closure or {})
    c.pop("findings", None)
    # Keep identities so GET can overlay when DLQ rotation dropped events.
    return {
        "verdict": c.get("verdict") or VERDICT_OPEN,
        "open_count": int(c.get("open_count") or 0),
        "promoted_count": int(c.get("promoted_count") or 0),
        "failed_count": int(c.get("failed_count") or 0),
        "durable_count": int(c.get("durable_count") or 0),
        "next_action": c.get("next_action") or "",
        "note": c.get("note") or _CLOSURE_NOTE,
        "last_replay": c.get("last_replay"),
        "history": list(c.get("history") or [])[-_REPLAY_HISTORY_KEEP:],
        "updated_at": c.get("updated_at") or _now(),
        "migration_proven": False,
        "promoted_identities": list(c.get("promoted_identities") or [])[:500],
        "failed_identities": list(c.get("failed_identities") or [])[:500],
    }


def job_quarantine_closure(job: dict[str, Any] | None) -> dict[str, Any] | None:
    """Read the compact closure from the job document."""
    if not isinstance(job, dict):
        return None
    raw = job.get("quarantine_closure")
    if isinstance(raw, dict) and raw:
        return raw
    dest = job.get("destination_summary")
    if isinstance(dest, dict):
        stored = dest.get("quarantine_closure")
        if isinstance(stored, dict) and stored:
            return stored
    return None


def quarantine_sample_incomplete(job: dict[str, Any] | None, details: list[dict[str, Any]]) -> str | None:
    """Refuse replay when the hydrated sample cannot reconstruct every open row.

    After remediations, compare to *open* count — never to the historical
    ``rejected_rows`` (that is the Full Append dest-Δ class of bug).
    """
    j = job if isinstance(job, dict) else {}
    dest = j.get("destination_summary") if isinstance(j.get("destination_summary"), dict) else {}
    truncated = bool(
        j.get("rejected_details_truncated")
        or (dest or {}).get("rejected_details_truncated")
    )
    try:
        total = int(
            j.get("rejected_details_total")
            or (dest or {}).get("rejected_details_total")
            or 0
        )
    except (TypeError, ValueError):
        total = 0
    if truncated and total > len(details):
        return (
            f"Quarantine sample is incomplete ({len(details)} of {total} rejects). "
            "Export the destination DLQ / full findings, or re-run so all rejects "
            "are persisted — partial replay would leave remaining rejects behind."
        )
    return None
