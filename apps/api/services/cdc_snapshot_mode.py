"""Debezium-compatible snapshot mode resolution.

Modes (aligned with Debezium PostgreSQL connector):
  - ``initial`` — snapshot when no watermark exists (default)
  - ``always`` — snapshot every job run, then stream
  - ``never`` — never snapshot; stream only (fails if no watermark)
  - ``initial_only`` — snapshot then stop (no stream poll)
  - ``when_needed`` — snapshot if slot/resume missing **or broken**

Retention gap (``when_needed``)
--------------------------------
Debezium ``snapshot.mode=when_needed`` snapshots when the slot/resume token is
missing *or* unusable. A present watermark whose LSN/SCN/binlog/Change Tracking
version has been purged is broken resume — not "already snapshotted, skip."
Production must pass ``resume_broken`` from the retention probe (``status=gap``),
otherwise ``when_needed`` silently skips snapshot and the job polls a purged
cursor (or, for SQL Server CT, ``CHANGETABLE`` with a stale last_sync_version
returns an **invalid** change set; for MongoDB, ``watch()`` without the expired
resume token starts at current clusterTime and skips the oplog window).

Honesty
-------
Events in the purged WAL / binlog / redo / oplog window are gone forever. Recovery
re-upserts **current** source keys (blocking ``cdc.snapshot()`` LSN handoff),
then streams from the new tip. That is at-least-once upsert of the live
population, not continuous CDC across the gap, and never ``migration_proven``.

``initial`` already spent its one snapshot; a later gap cannot invent another
without the operator changing mode. ``never`` forbids a snapshot.

When dest already has keys **and** the log reader can interleave DDD-3
chunks, ``when_needed`` + retention gap selects ``incremental_snapshot``
instead of a blocking table dump (healthcare / banking / logistics
cutover: do not lock a 50M-row ledger for hours). Lost-window events are
still gone. Recovery re-upserts **current** source keys in PK-ordered
chunks, stream-wins, at-least-once — never ``migration_proven``. Query-CDC
fallback and an unmeasured / empty dest stay on the blocking path. This
kernel never enqueues a signal in the same run as a blocking snapshot
(that would double-scan the table).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

KIND_SKIP = "skip"
KIND_BLOCKING = "blocking_snapshot"
KIND_REFUSE = "refuse"
KIND_INCREMENTAL = "incremental_snapshot"


class SnapshotMode(str, Enum):
    INITIAL = "initial"
    ALWAYS = "always"
    NEVER = "never"
    INITIAL_ONLY = "initial_only"
    WHEN_NEEDED = "when_needed"


def schedule_snapshot_mode(sync_mode: str, raw: Any = "") -> str:
    """Persist Debezium snapshot mode only for CDC. Empty on other sync modes.

    Studio Advanced can set ``when_needed`` / ``never``; a schedule that drops
    the field silently falls back to ``initial`` and will not snapshot a cursor
    gap. Recurring CDC must replay the same mode Validate saw.
    """
    if str(sync_mode or "").strip().lower() != "cdc":
        return ""
    return parse_snapshot_mode(raw or "initial").value


def parse_snapshot_mode(raw: Any) -> SnapshotMode:
    text = str(raw or "initial").strip().lower().replace("-", "_")
    aliases = {
        "": SnapshotMode.INITIAL,
        "initial": SnapshotMode.INITIAL,
        "always": SnapshotMode.ALWAYS,
        "never": SnapshotMode.NEVER,
        "no_data": SnapshotMode.NEVER,
        "initial_only": SnapshotMode.INITIAL_ONLY,
        "initial_only_table": SnapshotMode.INITIAL_ONLY,
        "when_needed": SnapshotMode.WHEN_NEEDED,
        "whenneeded": SnapshotMode.WHEN_NEEDED,
    }
    if text not in aliases:
        raise ValueError(
            f"Unknown snapshot_mode '{raw}'. "
            f"Expected one of: {', '.join(m.value for m in SnapshotMode)}"
        )
    return aliases[text]


def watermark_present(watermark: Any) -> bool:
    if watermark is None:
        return False
    text = str(watermark).strip()
    return bool(text) and text.lower() not in {"none", "null", "~"}


def should_run_snapshot(
    mode: SnapshotMode,
    *,
    watermark: str | None,
    resume_broken: bool = False,
) -> bool:
    if mode == SnapshotMode.ALWAYS:
        return True
    if mode == SnapshotMode.NEVER:
        if not watermark:
            raise ValueError(
                "snapshot_mode=never requires an existing CDC watermark/resume token"
            )
        return False
    if mode == SnapshotMode.INITIAL_ONLY:
        return True
    if mode == SnapshotMode.WHEN_NEEDED:
        return watermark is None or resume_broken
    # initial
    return watermark is None


def should_run_stream(mode: SnapshotMode) -> bool:
    return mode != SnapshotMode.INITIAL_ONLY


def snapshot_mode_recovers_gap(mode: SnapshotMode | str | None) -> bool:
    """True when this mode may blocking-snapshot a present-but-purged cursor."""
    try:
        parsed = mode if isinstance(mode, SnapshotMode) else parse_snapshot_mode(mode)
    except ValueError:
        return False
    return parsed in {
        SnapshotMode.WHEN_NEEDED,
        SnapshotMode.ALWAYS,
        SnapshotMode.INITIAL_ONLY,
    }


def build_snapshot_mode_preflight_gate(
    *,
    sync_mode: str,
    stream_contracts: list[dict[str, Any]] | None = None,
    watermark: Any = None,
    request_snapshot_mode: str = "",
) -> dict[str, Any] | None:
    """Validate≡Execute: ``never`` without a watermark must block before Execute.

    Uses ``should_run_snapshot`` — the same kernel CDC transfer calls. Non-CDC
    syncs emit nothing. ``initial`` / ``when_needed`` without a watermark stay
    pass (they will snapshot). CDC remains at-least-once upsert.
    """
    if str(sync_mode or "").strip().lower() != "cdc":
        return None
    try:
        mode = resolve_snapshot_mode(
            stream_contracts,
            request_snapshot_mode=request_snapshot_mode,
        )
    except ValueError as exc:
        return {
            "id": "g18_cdc_snapshot_mode",
            "status": "block",
            "message": str(exc),
            "duration_ms": 0,
            "details": {
                "snapshot_mode": str(request_snapshot_mode or ""),
                "watermark_present": watermark_present(watermark),
                "primary_action": "open_advanced",
                "honesty": (
                    "CDC remains at-least-once upsert. Not dest-owned exactly-once."
                ),
            },
        }
    present = watermark_present(watermark)
    wm = watermark if present else None
    try:
        run_snap = should_run_snapshot(mode, watermark=wm)
    except ValueError as exc:
        return {
            "id": "g18_cdc_snapshot_mode",
            "status": "block",
            "message": str(exc),
            "duration_ms": 0,
            "details": {
                "snapshot_mode": mode.value,
                "watermark_present": False,
                "primary_action": "open_advanced",
                "honesty": (
                    "CDC remains at-least-once upsert. Not dest-owned exactly-once. "
                    "Set snapshot_mode=when_needed (or initial) in Advanced, or "
                    "restore a resume token."
                ),
            },
        }
    if mode == SnapshotMode.NEVER:
        message = "CDC snapshot_mode=never — stream only (watermark present)"
    elif run_snap:
        message = (
            f"CDC snapshot_mode={mode.value} — blocking snapshot then stream "
            "(at-least-once upsert, not dest-owned exactly-once)"
        )
    else:
        message = f"CDC snapshot_mode={mode.value} — stream only (resume present)"
    return {
        "id": "g18_cdc_snapshot_mode",
        "status": "pass",
        "message": message,
        "duration_ms": 0,
        "details": {
            "snapshot_mode": mode.value,
            "watermark_present": present,
            "run_snapshot": bool(run_snap),
            "honesty": "at-least-once upsert. not dest-owned exactly-once.",
        },
    }


def resolve_snapshot_mode(
    stream_contracts: list[dict] | None,
    *,
    request_snapshot_mode: str = "",
    cfg_snapshot_mode: str = "",
) -> SnapshotMode:
    """Priority: stream contract → request → connector cfg → initial."""
    for raw in stream_contracts or []:
        if not raw.get("selected", True):
            continue
        if raw.get("snapshot_mode"):
            return parse_snapshot_mode(raw.get("snapshot_mode"))
    if request_snapshot_mode:
        return parse_snapshot_mode(request_snapshot_mode)
    if cfg_snapshot_mode:
        return parse_snapshot_mode(cfg_snapshot_mode)
    return SnapshotMode.INITIAL


def _retention_status(retention: Any) -> str:
    if retention is None:
        return ""
    if isinstance(retention, dict):
        return str(retention.get("status") or "").strip().lower()
    return str(getattr(retention, "status", "") or "").strip().lower()


def _retention_field(retention: Any, name: str) -> str:
    if retention is None:
        return ""
    if isinstance(retention, dict):
        return str(retention.get(name) or "")
    return str(getattr(retention, name, "") or "")


def classify_snapshot_plan(
    mode: SnapshotMode | str,
    *,
    watermark: Any = None,
    retention_status: str = "",
    dest_already_keyed: bool = False,
    incremental_capable: bool = False,
) -> dict[str, Any]:
    """Named snapshot plan for one CDC run.

    Verdicts
    --------
    ``skip`` — stream only (resume is usable, or mode forbids snapshot).
    ``blocking_snapshot`` — full-table dump + LSN/SCN/binlog handoff, then stream
    unless ``initial_only``.
    ``refuse`` — present-but-purged cursor under ``initial`` or ``never``. Fail
    closed *before* poll. Not a silent skip.

    ``lost_window`` is True whenever retention is ``gap``. Recovery never claims
    ``migration_proven``. ``incremental_snapshot`` is selected only when
    ``when_needed`` + dest already keyed + the log reader can interleave
    DDD-3 chunks. Otherwise gap recovery stays a blocking snapshot.
    """
    parsed = mode if isinstance(mode, SnapshotMode) else parse_snapshot_mode(mode)
    present = watermark_present(watermark)
    wm = watermark if present else None
    gap = str(retention_status or "").strip().lower() == "gap"
    resume_broken = gap and present
    run_snapshot = should_run_snapshot(
        parsed, watermark=wm, resume_broken=resume_broken
    )
    run_stream = should_run_stream(parsed)

    lost_note = (
        "Events in the purged WAL/binlog/redo window are gone. Recovery re-upserts "
        "current source keys, then streams from the new tip. At-least-once upsert "
        "— not continuous CDC, not migration_proven."
    )

    if resume_broken and parsed == SnapshotMode.NEVER:
        return {
            "kind": KIND_REFUSE,
            "snapshot_mode": parsed.value,
            "run_snapshot": False,
            "run_stream": False,
            "lost_window": True,
            "resume_broken": True,
            "migration_proven": False,
            "next_action": "set_when_needed",
            "reason": "never_forbids_snapshot",
            "message": (
                "CDC resume is before retained log history and snapshot_mode=never "
                "forbids a recovery snapshot. Set snapshot_mode=when_needed (or always) "
                f"and re-run. {lost_note}"
            ),
        }
    if resume_broken and parsed == SnapshotMode.INITIAL:
        return {
            "kind": KIND_REFUSE,
            "snapshot_mode": parsed.value,
            "run_snapshot": False,
            "run_stream": False,
            "lost_window": True,
            "resume_broken": True,
            "migration_proven": False,
            "next_action": "set_when_needed",
            "reason": "initial_already_snapshotted",
            "message": (
                "CDC resume is before retained log history. snapshot_mode=initial "
                "already ran its one-time snapshot and will not snapshot again. Set "
                "snapshot_mode=when_needed (or reset the watermark) and re-run. "
                f"{lost_note}"
            ),
        }

    if (
        resume_broken
        and parsed == SnapshotMode.WHEN_NEEDED
        and dest_already_keyed
        and incremental_capable
    ):
        return {
            "kind": KIND_INCREMENTAL,
            "snapshot_mode": parsed.value,
            "run_snapshot": False,
            "run_stream": True,
            "lost_window": True,
            "resume_broken": True,
            "migration_proven": False,
            "next_action": "incremental_snapshot_then_stream",
            "reason": "retention_gap_when_needed_dest_keyed",
            "message": (
                "Resume token is present but purged. Destination already has keys — "
                "DDD-3 incremental snapshot interleaved with streaming (stream-wins). "
                "Not a blocking table dump. "
                f"{lost_note}"
            ),
        }

    kind = KIND_BLOCKING if run_snapshot else KIND_SKIP
    if resume_broken and parsed == SnapshotMode.WHEN_NEEDED:
        reason = "retention_gap_when_needed"
        next_action = "snapshot_then_stream"
        message = (
            "Resume token is present but purged (Debezium when_needed). "
            f"Blocking snapshot of current source keys, then stream from the new tip. {lost_note}"
        )
    elif run_snapshot and parsed == SnapshotMode.INITIAL_ONLY:
        reason = "retention_gap" if gap else "initial_only"
        next_action = "snapshot_only"
        message = (
            f"snapshot_mode=initial_only dumps current source keys and does not stream. "
            f"{lost_note}" if gap else "snapshot_mode=initial_only — snapshot, no stream."
        )
    elif run_snapshot:
        reason = "retention_gap" if gap else ("watermark_missing" if not present else "always")
        next_action = "snapshot_then_stream" if run_stream else "snapshot_only"
        message = (
            f"Blocking snapshot then stream. {lost_note}" if gap
            else "Blocking snapshot, then stream (Debezium snapshot → LSN handoff)."
        )
    else:
        reason = "resume_ok"
        next_action = "stream"
        message = "Resume is within retention — stream only."

    return {
        "kind": kind,
        "snapshot_mode": parsed.value,
        "run_snapshot": bool(run_snapshot),
        "run_stream": bool(run_stream),
        "lost_window": bool(gap),
        "resume_broken": bool(resume_broken),
        "migration_proven": False if gap else None,
        "next_action": next_action,
        "reason": reason,
        "message": message,
    }


def snapshot_plan_stamp(plan: dict[str, Any] | None) -> dict[str, Any]:
    """Compact operator-visible stamp. Omits None so Gate-8 is not rewritten."""
    if not isinstance(plan, dict) or not plan:
        return {}
    out: dict[str, Any] = {}
    for key in (
        "kind",
        "snapshot_mode",
        "lost_window",
        "resume_broken",
        "run_snapshot",
        "run_stream",
        "next_action",
        "reason",
        "migration_proven",
    ):
        if key in plan and plan[key] is not None:
            out[key] = plan[key]
    return out


def measure_dest_already_keyed(
    dest_type: str,
    dest_cfg: dict[str, Any],
    tables: list[tuple[str, list[str]]],
    *,
    schema: str = "",
) -> bool:
    """True only when dest-engine COUNT > 0 and at least one PK is listed.

    Unmeasured COUNT / key list is False — fail toward a blocking snapshot,
    never invent ``dest already keyed``. Empty dest is False.
    """
    from services.dest_precount import destination_key_list, destination_row_count

    engine = str(dest_type or "").strip().lower()
    cfg = dest_cfg if isinstance(dest_cfg, dict) else {}
    for table, key_columns in tables:
        name = str(table or "").strip()
        cols = [str(c).strip() for c in (key_columns or []) if str(c).strip()]
        if not name or not cols:
            continue
        try:
            n = destination_row_count(engine, cfg, schema=schema or "", table_name=name)
        except Exception:
            continue
        if n is None or int(n) <= 0:
            continue
        try:
            keys = destination_key_list(
                engine, cfg, schema=schema or "", table_name=name, key_columns=cols
            )
        except Exception:
            continue
        if keys:
            return True
    return False


def adapter_supports_incremental_interleave(cdc: Any) -> bool:
    """Log-native readers interleave DDD-3 chunks. Query CDC does not."""
    if cdc is None:
        return False
    source_key = str(getattr(cdc, "source_key", "") or "").strip()
    if not source_key:
        return False
    cls = type(cdc).__name__
    return cls != "CdcEngine"


def resolve_cdc_snapshot_plan(
    mode: SnapshotMode | str,
    *,
    watermark: Any = None,
    retention: Any = None,
    dest_already_keyed: bool = False,
    incremental_capable: bool = False,
) -> dict[str, Any]:
    """Classify, then fail-closed on ``refuse`` before the job polls a purged cursor."""
    plan = classify_snapshot_plan(
        mode,
        watermark=watermark,
        retention_status=_retention_status(retention),
        dest_already_keyed=dest_already_keyed,
        incremental_capable=incremental_capable,
    )
    if plan.get("kind") != KIND_REFUSE:
        return plan
    from services.cdc_cursor_gap import CdcCursorGapError

    raise CdcCursorGapError(
        str(plan.get("message") or "CDC cursor gap"),
        dialect=_retention_field(retention, "dialect"),
        resume=_retention_field(retention, "resume"),
        retained=_retention_field(retention, "retained"),
        cursor_key=_retention_field(retention, "cursor_key"),
        snapshot_plan=snapshot_plan_stamp(plan),
    )
