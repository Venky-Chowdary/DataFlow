"""Persist preflight/validation runs so Datawrap Pilot and Jobs can look them up by ID."""

from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.ops_events import emit_event
from services.platform_config import data_dir
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter, bound_allows, current_workspace_stamp

STORE_PATH = data_dir() / "preflight_runs.jsonl"
_LOCK = threading.Lock()
MAX_RUNS = 500

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_preflight_run(
    result: dict[str, Any],
    *,
    source_label: str = "",
    dest_label: str = "",
    validation_mode: str = "",
    route: dict[str, Any] | str | None = None,
) -> dict[str, Any]:
    """Attach a stable run_id to *result*, persist a summary, return the enriched result."""
    run_id = str(result.get("run_id") or f"pf_{uuid.uuid4().hex[:12]}")
    enriched = {**result, "run_id": run_id}
    blockers = enriched.get("blockers") or []
    record = {
        "run_id": run_id,
        # The workspace this request acts in (D154); "" = unscoped legacy.
        "workspace_id": current_workspace_stamp(),
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
    _emit_preflight_verdict(record, route)
    return enriched


def _route_types(route: dict[str, Any] | str | None) -> tuple[str | None, str | None]:
    """Source/dest connector types from a router dict or Pilot's ``"src->dst"``."""
    if isinstance(route, dict):
        return (
            route.get("source_type") or route.get("source_kind") or None,
            route.get("dest_type") or route.get("dest_kind") or None,
        )
    if isinstance(route, str) and "->" in route:
        src, _, dst = route.partition("->")
        return src.strip() or None, dst.strip() or None
    return None, None


def _emit_preflight_verdict(record: dict[str, Any], route: dict[str, Any] | str | None) -> None:
    """``preflight.run.completed``: verdict, counts and gate ids — no object names.

    A block is an expected, operator-recoverable validation outcome, so it is
    ``held`` at INFO rather than an error.
    """
    passed = bool(record.get("passed"))
    gate_ids = sorted(
        {
            str(b["id"])
            for b in record.get("blockers") or []
            if isinstance(b, dict) and b.get("id")
        }
    )
    source_type, destination_type = _route_types(route)
    emit_event(
        logger,
        "preflight.run.completed",
        outcome="succeeded" if passed else "held",
        level=logging.INFO,
        run_id=record.get("run_id"),
        verdict="passed" if passed else "blocked",
        validation_mode=record.get("validation_mode") or None,
        passed_count=record.get("passed_count"),
        total_gates=record.get("total_gates"),
        blocker_count=len(record.get("blockers") or []),
        blocking_gate_ids=",".join(gate_ids) or None,
        source_type=source_type,
        destination_type=destination_type,
    )


def get_preflight_run(run_id: str) -> dict[str, Any] | None:
    rid = (run_id or "").strip()
    if not rid:
        return None
    for row in reversed(_read_all()):
        if str(row.get("run_id")) == rid:
            # A run of another workspace does not exist for this caller (D154).
            return row if bound_allows(row.get("workspace_id")) else None
    return None


def list_preflight_runs(limit: int = 20) -> list[dict[str, Any]]:
    rows = [r for r in _read_all() if bound_allows(r.get("workspace_id"))]
    return list(reversed(rows[-max(1, min(limit, 100)) :]))


def assign_unscoped_workspaces(workspace_for, *, apply: bool) -> dict[str, int]:
    """Stamp legacy unscoped runs with ``workspace_for(run)`` (D154 backfill).

    Runs carry no owner, so ``workspace_for`` decides (the platform owner's
    workspace). Scoped runs are never touched.
    """
    unscoped = WorkspaceFilter.only("")
    counts = {"assigned": 0, "skipped": 0}
    if not STORE_PATH.exists():
        return counts
    with _LOCK:
        lines = STORE_PATH.read_text(encoding="utf-8").splitlines()
        out: list[str] = []
        for line in lines:
            try:
                row = json.loads(line) if line.strip() else None
            except json.JSONDecodeError:
                row = None
            if isinstance(row, dict) and unscoped.matches(row.get("workspace_id")):
                ws = (workspace_for(row) or "").strip()
                counts["assigned" if ws else "skipped"] += 1
                if ws:
                    row["workspace_id"] = ws
                    line = json.dumps(row, default=json_default)
            out.append(line)
        if apply and counts["assigned"]:
            tmp = STORE_PATH.with_suffix(STORE_PATH.suffix + ".tmp")
            tmp.write_text("\n".join(out) + "\n", encoding="utf-8")
            tmp.replace(STORE_PATH)
    return counts


def _suggest_remediations(blockers: list[Any]) -> list[dict[str, str]]:
    text = " ".join(
        str(b.get("message") or "") for b in blockers if isinstance(b, dict)
    ).lower()
    out: list[dict[str, str]] = []
    # Do not match bare "encoding" — column names like encoding_id steal CTAs.
    if (
        "format-control" in text
        or "replacement character" in text
        or "encoding anomaly" in text
        or "character encoding" in text
        or "u+200b" in text
        or "zero-width" in text
    ):
        out.append({
            "kind": "open_bad_data_fix",
            "label": "Fix bad data…",
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
            fh.flush()
            os.fsync(fh.fileno())
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
