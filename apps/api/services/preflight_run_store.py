"""Persist preflight/validation runs so Data Pilot and Jobs can look them up by ID."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "preflight_runs.jsonl"
_LOCK = threading.Lock()
MAX_RUNS = 500


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_preflight_run(
    result: dict[str, Any],
    *,
    source_label: str = "",
    dest_label: str = "",
    validation_mode: str = "",
    route: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Attach a stable run_id to *result*, persist a summary, return the enriched result."""
    run_id = str(result.get("run_id") or f"pf_{uuid.uuid4().hex[:12]}")
    enriched = {**result, "run_id": run_id}
    blockers = enriched.get("blockers") or []
    record = {
        "run_id": run_id,
        "created_at": _now(),
        "passed": bool(enriched.get("passed")),
        "readiness_score": enriched.get("readiness_score"),
        "passed_count": enriched.get("passed_count"),
        "total_gates": enriched.get("total_gates"),
        "validation_mode": validation_mode or enriched.get("validation_mode"),
        "source_label": source_label,
        "dest_label": dest_label,
        "route": route or {},
        "blockers": [
            {
                "id": b.get("id"),
                "message": b.get("message"),
                "fix": (b.get("guidance") or {}).get("fix"),
            }
            for b in blockers[:12]
            if isinstance(b, dict)
        ],
        "gates": [
            {"id": g.get("id"), "status": g.get("status"), "message": g.get("message")}
            for g in (enriched.get("gates") or [])[:20]
            if isinstance(g, dict)
        ],
        "suggested_remediations": _suggest_remediations(blockers),
    }
    _append(record)
    return enriched


def get_preflight_run(run_id: str) -> dict[str, Any] | None:
    rid = (run_id or "").strip()
    if not rid:
        return None
    for row in reversed(_read_all()):
        if str(row.get("run_id")) == rid:
            return row
    return None


def list_preflight_runs(limit: int = 20) -> list[dict[str, Any]]:
    rows = _read_all()
    return list(reversed(rows[-max(1, min(limit, 100)) :]))


def _suggest_remediations(blockers: list[Any]) -> list[dict[str, str]]:
    text = " ".join(
        str(b.get("message") or "") for b in blockers if isinstance(b, dict)
    ).lower()
    out: list[dict[str, str]] = []
    if "format-control" in text or "replacement character" in text or "encoding" in text:
        out.append({
            "kind": "normalize_control_chars",
            "label": "Strip control characters and re-run validation",
        })
        out.append({
            "kind": "open_bad_data_fix",
            "label": "Open Fix bad data dialog",
        })
    if "mapping" in text or "confidence" in text:
        out.append({"kind": "review_mappings", "label": "Review column mappings"})
    return out


def _append(record: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, default=json_default)
    with _LOCK:
        with STORE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        _trim_locked()


def _read_all() -> list[dict[str, Any]]:
    if not STORE_PATH.exists():
        return []
    rows: list[dict[str, Any]] = []
    with _LOCK:
        try:
            text = STORE_PATH.read_text(encoding="utf-8")
        except OSError:
            return []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _trim_locked() -> None:
    path: Path = STORE_PATH
    if not path.exists():
        return
    try:
        lines = [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    except OSError:
        return
    if len(lines) <= MAX_RUNS:
        return
    path.write_text("\n".join(lines[-MAX_RUNS:]) + "\n", encoding="utf-8")
