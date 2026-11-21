"""Validate verdict vs Execute outcome, one record per run (Accuracy Track A item 7).

"Validate predicts Execute" was not measurable: nothing paired the verdict a
run was validated with and what its write then did. Every job that becomes
terminal is recorded here once (``services.job_terminal_effects``):

* ``validate_verdict``: the separate Validate run the request reused
  (``preflight_run_id``), when there was one;
* ``execute_preflight``: the verdict of the preflight Execute itself ran
  before writing (``job["preflight"]``), or ``skipped``;
* ``execute_status`` and ``failed_at`` (``preflight`` or ``write``);
* ``verification``: the post-write reconcile verdict, when one was recorded.

The record holds no table, column, host or user names and no messages: ids,
connector types, verdict words and counts only. :func:`aggregate` turns the
records of one workspace into counts and rates; ``GET /ops/validate-execute``
serves it and the process counters ``dataflow_validate_execute_*_total`` are
scraped at ``GET /metrics``.

Outcome classes (the verdict that predicted the run is the separate Validate
when linked, else Execute's own preflight):

``agreed_pass``   predicted pass, the write completed
``missed``        predicted pass, the write failed (a preflight miss)
``drift``         a linked Validate passed, Execute's own preflight refused
``agreed_block``  predicted block, the run was refused before writing
``over_block``    a linked Validate blocked, the run then completed
``cancelled``     the run was cancelled (not counted for prediction)
``unvalidated``   no verdict to compare (preflight skipped, nothing linked)
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from services.shared_doc_store import SharedLog
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter

logger = logging.getLogger(__name__)
MAX_RECORDS = 5000
_LOCK = threading.Lock()
OUTCOME_CLASSES = (
    "agreed_pass", "missed", "drift", "agreed_block", "over_block", "cancelled", "unvalidated",
)
_COMPLETED = frozenset({"completed", "completed_with_quarantine", "success", "succeeded"})
_CANCELLED = frozenset({"cancelled", "canceled"})


def _path() -> Path:
    from services.platform_config import data_dir

    return data_dir() / "validate_execute_runs.jsonl"


_LOG = SharedLog("validate_execute_runs", file_path=_path, key_field="job_id")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _verdict(passed: Any) -> str | None:
    if passed is None:
        return None
    return "passed" if bool(passed) else "blocked"


def _kind(endpoint: Any) -> str:
    if not isinstance(endpoint, dict):
        return ""
    raw = endpoint.get("format") or endpoint.get("type") or endpoint.get("kind") or ""
    text = str(raw).strip().lower()
    # A connector type is a short identifier; anything else is not recorded.
    return text if text.replace("_", "").replace("-", "").isalnum() and len(text) <= 40 else ""


def _verification(job: dict[str, Any]) -> str | None:
    rec = job.get("reconciliation") or job.get("reconcile")
    if not isinstance(rec, dict):
        return None
    for key in ("passed", "ok", "success"):
        if key in rec and rec[key] is not None:
            return "passed" if bool(rec[key]) else "failed"
    return None


def classify(validate_verdict: str | None, execute_preflight: str | None, status: str) -> tuple[str, str | None]:
    """``(outcome_class, failed_at)`` for one run."""
    st = (status or "").strip().lower()
    completed = st in _COMPLETED
    failed_at: str | None = None
    if not completed and st not in _CANCELLED:
        failed_at = "preflight" if execute_preflight == "blocked" else (
            "write" if execute_preflight == "passed" else "unknown"
        )
    if st in _CANCELLED:
        return "cancelled", None
    predicted = validate_verdict or (execute_preflight if execute_preflight in ("passed", "blocked") else None)
    if predicted is None:
        return "unvalidated", failed_at
    if predicted == "passed":
        if completed:
            return "agreed_pass", None
        if validate_verdict == "passed" and failed_at == "preflight":
            return "drift", failed_at
        if failed_at == "write":
            return "missed", failed_at
        return "unvalidated", failed_at
    # predicted == "blocked"
    if completed:
        return "over_block", None
    return "agreed_block", failed_at


def build_record(job_id: str, status: str, job: dict[str, Any]) -> dict[str, Any]:
    from services.preflight_run_store import get_preflight_run

    request = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    run_id = str((request or {}).get("preflight_run_id") or job.get("preflight_run_id") or "").strip()
    validate_verdict = None
    if run_id:
        try:
            linked = get_preflight_run(run_id)
        except Exception:  # noqa: BLE001 - a store outage leaves the link unknown
            linked = None
        validate_verdict = _verdict(linked.get("passed")) if linked else None
    pf = job.get("preflight")
    if isinstance(pf, dict) and "passed" in pf:
        execute_preflight = _verdict(pf.get("passed"))
    else:
        execute_preflight = "skipped"
    outcome, failed_at = classify(validate_verdict, execute_preflight, status)
    return {
        "job_id": str(job_id),
        "workspace_id": str(job.get("workspace_id") or ""),
        "recorded_at": _now(),
        "source_type": _kind((request or {}).get("source")),
        "destination_type": _kind((request or {}).get("destination")),
        "validation_mode": str(job.get("validation_mode") or (request or {}).get("validation_mode") or "")[:20],
        "validate_run_id": run_id or None,
        "validate_verdict": validate_verdict,
        "execute_preflight": execute_preflight,
        "execute_status": str(status or "").strip().lower()[:40],
        "failed_at": failed_at,
        "verification": _verification(job),
        "outcome": outcome,
    }


def record_run_outcome(job_id: str, status: str, job: dict[str, Any] | None = None) -> dict[str, Any] | None:
    """Record a terminal run once; a later terminal write for the same job is ignored."""
    if not job_id:
        return None
    if job is None:
        from services.mongodb_service import get_mongodb_service

        job = get_mongodb_service().get_job(job_id) or {}
    record = build_record(job_id, status, job)
    coll = _LOG.backend()
    if coll is not None:
        if _LOG.get(coll, record["job_id"]) is not None:
            return None
        _LOG.append(coll, json.loads(json.dumps(record, default=json_default)))
        _LOG.trim(coll, MAX_RECORDS)
    else:
        path = _path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with _LOCK:
            if any(r.get("job_id") == record["job_id"] for r in _read_file(path)):
                return None
            with path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, default=json_default) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            _trim_locked(path)
    try:
        from services.ops_metrics import record_validate_execute

        record_validate_execute(record["outcome"])
    except Exception:  # noqa: BLE001 - a counter must not fail the transfer
        logger.debug("validate/execute counter not updated", exc_info=True)
    return record


def _read_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def _trim_locked(path: Path) -> None:
    rows = path.read_text(encoding="utf-8").splitlines()
    if len(rows) > MAX_RECORDS:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text("\n".join(rows[-MAX_RECORDS:]) + "\n", encoding="utf-8")
        tmp.replace(path)


def list_records(workspace_filter: WorkspaceFilter | None = None) -> list[dict[str, Any]]:
    coll = _LOG.backend()
    if coll is not None:
        rows = _LOG.records(coll, limit=MAX_RECORDS)
    else:
        with _LOCK:
            rows = _read_file(_path())
    if workspace_filter is None:
        return rows
    return [r for r in rows if workspace_filter.matches(r.get("workspace_id"))]


def _wilson(k: int, n: int, z: float = 1.959964) -> tuple[float, float] | None:
    if n <= 0:
        return None
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5)
    return round((c - m) / d, 4), round((c + m) / d, 4)


def _summary(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    counts = {c: 0 for c in OUTCOME_CLASSES}
    for r in rows:
        counts[r.get("outcome") if r.get("outcome") in counts else "unvalidated"] += 1
    predicted_pass = counts["agreed_pass"] + counts["missed"] + counts["drift"]
    misses = counts["missed"] + counts["drift"]
    return {
        "runs": len(rows),
        "by_outcome": counts,
        "validate_passed_runs": predicted_pass,
        "validate_predicts_execute_rate": round(counts["agreed_pass"] / predicted_pass, 4) if predicted_pass else None,
        "miss_rate": round(misses / predicted_pass, 4) if predicted_pass else None,
        "miss_rate_95ci": _wilson(misses, predicted_pass),
        "linked_validate_runs": sum(1 for r in rows if r.get("validate_run_id")),
        "verified_after_write": sum(1 for r in rows if r.get("verification") == "passed"),
        "verification_failed_after_write": sum(1 for r in rows if r.get("verification") == "failed"),
    }


def aggregate(workspace_filter: WorkspaceFilter | None = None) -> dict[str, Any]:
    """Counts and rates for the runs a caller may see. No ids, names or messages."""
    rows = list_records(workspace_filter)
    routes: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for r in rows:
        routes.setdefault((r.get("source_type") or "", r.get("destination_type") or ""), []).append(r)
    by_route = [
        {"source_type": s or None, "destination_type": d or None, **_summary(v)}
        for (s, d), v in sorted(routes.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    ]
    return {
        **_summary(rows),
        "by_route": by_route[:50],
        "definitions": {
            "validate_predicts_execute_rate": "agreed_pass / (agreed_pass + missed + drift)",
            "miss_rate": "(missed + drift) / (agreed_pass + missed + drift); 95% Wilson interval",
            "predicting_verdict": "the linked Validate run when the request reused one, else "
                                  "the preflight Execute ran before writing",
        },
        "honesty": (
            "Counted from runs recorded since this ledger existed, per workspace. A run whose "
            "preflight was skipped and that reused no Validate run is 'unvalidated', never "
            "counted as agreement."
        ),
    }
