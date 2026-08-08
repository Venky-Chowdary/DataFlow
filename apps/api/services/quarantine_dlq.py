"""Durable quarantine dead-letter queue — Mongo primary, JSONL fallback.

Jobs already persist ``rejected_details`` on the job document. This module
adds a workspace-scoped, replay-auditable DLQ so remediations survive job GC
and multi-instance deploys share the same remediation trail.
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
