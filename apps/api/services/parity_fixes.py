"""Parity Studio: the fix menu, and the approve -> apply -> re-check loop.

Fixes are chosen by deterministic rules from the shared remediation registry
(:mod:`services.remediation_actions`, the same menu Pilot's
``remediate_validation`` uses):

* ``widen_column`` for a scale cut or truncation, where the destination dialect
  has an in-place widen DDL helper (``connectors.schema_drift``);
* ``resync_keys`` for missing or wrong rows on a keyed table, through the
  engine's own upsert path with a ``source_filter`` on the keys (D328
  operators), at most :data:`MAX_RESYNC_KEYS` keys per fix;
* ``rerun`` (the existing job re-run) where keys cannot be named;
* ``accept`` for any unexplained difference, recorded with the approver's note.

Nothing is applied without a person's approval. An applied write is followed by
a re-check of the affected keys on both sides, recorded as a round; a job gets
at most :data:`services.parity_state.MAX_APPLIED_ROUNDS` applied write fixes.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from services.parity_classifier import NARROW_COLUMN_CAUSES
from services.parity_evidence import TableEvidence, job_is_terminal, request_of, target_to_source
from services.parity_state import (  # noqa: F401 - the error types and load_job are re-exported
    MAX_APPLIED_ROUNDS,
    ParityConflict,
    ParityError,
    ParityInvalid,
    ParityNotFound,
    ParityStale,
    load_job,
)
from services.remediation_actions import PARITY, action as remediation_action

logger = logging.getLogger(__name__)

MAX_RESYNC_KEYS = 1000
WRITE_ACTIONS = frozenset({"widen_column", "resync_keys", "rerun"})
_DONE = frozenset({"applied", "failed"})
#: A write fix a person approved and that is being applied right now.
APPLYING = "applying"
#: Write fixes that count against the round limit (claimed, applied or failed).
_USED = _DONE | {APPLYING}
_ORDER = {"widen_column": 0, "resync_keys": 1, "rerun": 2, "accept": 3}


def _fix_id(action: str, diff_ids: list[str]) -> str:
    raw = action + "|" + ",".join(sorted(diff_ids))
    return "f_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def _fix(action: str, diffs: list[dict[str, Any]], *, title: str, detail: str,
         preview: str | None = None, reason: str | None = None, **internal: Any) -> dict[str, Any]:
    chosen = remediation_action(action, PARITY)
    if chosen is None:  # pragma: no cover - the menu is fixed in this module
        raise ValueError(f"{action} is not a Parity Studio action")
    ids = [d["id"] for d in diffs]
    return {
        "id": _fix_id(action, ids), "difference_ids": ids, "action": action,
        "title": title, "detail": detail, "preview": preview,
        "applicable": reason is None, "not_applicable_reason": reason,
        "status": "proposed", "approved_by": None, "applied_at": None, "result": None,
        **{f"_{k}": v for k, v in internal.items()},
    }


def _keys_preview(table: str, key_col: str, keys: list[str]) -> str:
    shown = ", ".join(keys[:20]) + (f" … (+{len(keys) - 20})" if len(keys) > 20 else "")
    return f"upsert {table} for {key_col} IN ({shown}) — {len(keys)} key(s)"


def _route_reason(job: dict[str, Any]) -> str | None:
    req = request_of(job)
    src, dst = req.get("source") or {}, req.get("destination") or {}
    if not src or not dst:
        return "This job has no saved route to write through; re-run it from Transfer Studio."
    if str(src.get("kind") or "database") != "database" or str(dst.get("kind") or "database") != "database":
        return "Keys can be re-synced only between database tables."
    if not job_is_terminal(job):
        return "The job is still running."
    return None


def _resync(job: dict[str, Any], ev: TableEvidence, diffs: list[dict[str, Any]], widen_pending: bool) -> dict:
    keys = list(dict.fromkeys(k for d in diffs for k in d["_keys"]))
    key_col = ev.key_columns[0] if len(ev.key_columns) == 1 else ""
    reason = _route_reason(job)
    if reason is None and not key_col:
        reason = "The destination key is not one known column, so rows cannot be addressed by key."
    if reason is None and len(keys) > MAX_RESYNC_KEYS:
        reason = f"{len(keys):,} keys exceed the {MAX_RESYNC_KEYS:,}-key limit of one re-sync; re-run the job."
    detail = (
        f"Reads these {len(keys)} key(s) from the source again and writes them with an upsert on "
        f"{key_col or 'the key'}; no other row is touched."
    )
    if widen_pending:
        detail += " Widen the column first, or the same cut repeats."
    return _fix(
        "resync_keys", diffs, title=f"Re-sync {len(keys)} key(s) in {ev.name} by upsert",
        detail=detail, preview=_keys_preview(ev.name, key_col or "key", keys), reason=reason,
        keys=keys, table=ev.name, key_column=key_col,
    )


def _widen(job: dict[str, Any], ev: TableEvidence, diff: dict[str, Any], live: dict[str, Any]) -> dict:
    from connectors.schema_drift import _build_widen_ddl, is_wider_type

    col = str(diff["column"])
    dialect = str(live.get("dialect") or "")
    existing = str(live.get("dest", {}).get(col) or ev.dest_types.get(col) or "")
    new_type = str(live.get("source", {}).get(col) or ev.source_types.get(col) or "")
    if diff["cause"]["code"] == "numeric_scale":
        new_type = _decimal_cover(new_type, existing, dialect, diff)
    reason = _route_reason(job)
    preview = None
    if reason is None:
        try:
            preview = _build_widen_ddl(
                dialect, live.get("schema") or None, ev.name, col, new_type or "TEXT", existing or None
            )
        except NotImplementedError:
            reason = (
                f"{dialect or 'This destination'} cannot change a column type in place; "
                "recreate the table with the wider type or re-run with overwrite."
            )
            preview = None
    if reason is None and not new_type:
        reason = "The source column type is not known, so there is no type to widen to."
        preview = None
    if reason is None and existing and not is_wider_type(existing, new_type, dest_db=dialect):
        reason = f"The source type {new_type} is not wider than the destination type {existing}."
        preview = None
    return _fix(
        "widen_column", [diff], title=f"Widen {ev.name}.{col} to {new_type or 'the source type'}",
        detail=(
            f"Changes the destination column {col} from {existing} to {new_type} so values keep "
            "every digit or character."
            if existing and new_type and existing.lower() != new_type.lower() else
            f"Changes the destination column {col} to the source column's type so values keep "
            "every digit or character."
        ),
        preview=preview, reason=reason, table=ev.name, column=col, new_type=new_type,
        existing_type=existing,
    )


def _decimal_cover(source_type: str, existing: str, dialect: str, diff: dict[str, Any]) -> str:
    """A DECIMAL that holds both the source's scale and the destination's integer digits.

    ``NUMERIC(12,4)`` into ``NUMERIC(12,2)`` widens to ``NUMERIC(14,4)``: the
    source's scale must not cost the destination integer digits it has.
    """
    from connectors.writer_common import parse_decimal_precision_scale
    from services.decimal_observe import decimal_widen_carrier, decimal_widen_from_envelope

    declared = parse_decimal_precision_scale(source_type, dest_db=dialect) if source_type else None
    if declared:
        precision, scale = declared
        return decimal_widen_from_envelope(
            max_int_digits=max(precision - scale, 0), max_scale=scale, dest_db=dialect,
            current_type=existing,
        ) or source_type
    example = (diff.get("example") or {}).get("source")
    if example:
        return decimal_widen_carrier(example, dest_db=dialect, current_type=existing) or source_type
    return source_type


def _rerun(job: dict[str, Any], diffs: list[dict[str, Any]], job_id: str) -> dict:
    reason = None
    if not request_of(job).get("source"):
        reason = "This job has no saved configuration to re-run; start it again from Transfer Studio."
    elif not job_is_terminal(job):
        reason = "The job is still running."
    return _fix(
        "rerun", diffs, title="Re-run the job",
        detail="Starts the job again as a new run (preflight kept); that run gets its own parity "
        "report when it completes.",
        preview=f"POST /api/v1/connectors/jobs/{job_id}/rerun", reason=reason,
    )


def _accept(diff: dict[str, Any]) -> dict:
    where = f"{diff['table']}.{diff['column']}" if diff.get("column") else diff["table"]
    return _fix(
        "accept", [diff],
        title=f"Accept {diff['rows']:,} row(s) in {where} ({diff['cause']['label'].lower()})",
        detail="Marks this difference as accepted by you, with your note. No data is changed.",
    )


def _live_types(job: dict[str, Any], ev: TableEvidence, columns: list[str]) -> dict[str, Any]:
    """Catalog types of the narrow columns on both sides (best effort, metadata only).

    The source side is :func:`services.value_readback._live_source_types` — the
    one source introspection, with its source-read driver translation and its
    catalog guard.
    """
    from services.dialect_profiles import schema_from_cfg
    from services.parity_evidence import _endpoints
    from services.value_readback import _live_source_types
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.adapters_introspect import _introspect_table_schema
    from src.transfer.connector_capabilities import resolve_driver_type

    out: dict[str, Any] = {"source": {}, "dest": {}}
    try:
        _req, source, destination = _endpoints(job)
        t2s = target_to_source(job)
        types = _live_source_types(source, [t2s.get(c, c) for c in columns])
        out["source"] = {c: types.get(t2s.get(c, c)) for c in columns if types.get(t2s.get(c, c))}
        cfg = resolve_connector_config(destination)
        db_type = resolve_driver_type(str(cfg.get("type") or destination.format or "")).lower()
        types = _introspect_table_schema(db_type, cfg, str(destination.table or ""), columns) or {}
        out["dest"] = {c: types.get(c) for c in columns if types.get(c)}
        out["dialect"] = db_type
        out["schema"] = schema_from_cfg(db_type, cfg)
    except Exception as exc:  # noqa: BLE001 - types are advisory; the fix says what is unknown
        logger.info("parity: live column types unavailable (%s)", type(exc).__name__)
    if not out.get("dialect"):
        from services.parity_evidence import dest_engine_of

        out["dialect"] = dest_engine_of(job)
    return out


def propose_fixes(job: dict[str, Any], report: dict[str, Any], state: dict[str, Any], *, reread: bool) -> list[dict]:
    """Every fix to show: decisions already made, then proposals for what is left."""
    job_id = str(report.get("job_id") or "")
    proposals: list[dict[str, Any]] = []
    for ev in report.get("_tables_ev") or []:
        bad = [d for d in report["differences"] if d["table"] == ev.name and not d["cause"]["explained"]]
        if not bad:
            if ev.capped or ev.gate8_unaccounted:
                proposals.append(_rerun(job, [], job_id))
            continue
        narrow = [d for d in bad if d["cause"]["code"] in NARROW_COLUMN_CAUSES and d["column"] and d["kind"] == "value"]
        live = _live_types(job, ev, [d["column"] for d in narrow]) if narrow and reread else {}
        widens = [_widen(job, ev, d, live) for d in narrow]
        proposals += widens
        keyed = [d for d in bad if d["_keys"] and d["kind"] in {"value", "missing_in_destination"}]
        resync = _resync(job, ev, keyed, any(w["applicable"] for w in widens)) if keyed else None
        if resync:
            proposals.append(resync)
        rest = [d for d in bad if d["_keyless"] or d["kind"] in {"extra_in_destination", "duplicate_key"}]
        if rest or (resync is not None and not resync["applicable"]):
            proposals.append(_rerun(job, rest or keyed, job_id))
        proposals += [_accept(d) for d in bad]
    return _merge(proposals, state)


def _merge(proposals: list[dict[str, Any]], state: dict[str, Any]) -> list[dict[str, Any]]:
    stored: dict[str, dict[str, Any]] = dict(state.get("fixes") or {})
    writes_used = write_fixes_used(state)
    limit_reason = (
        f"This job has used its {MAX_APPLIED_ROUNDS} applied fix rounds; re-run the job to start again."
        if writes_used >= MAX_APPLIED_ROUNDS else None
    )
    out: list[dict[str, Any]] = []
    for fix in proposals:
        base, n = fix["id"], 1
        while fix["id"] in stored and stored[fix["id"]].get("status") in _DONE:
            n += 1
            fix["id"] = f"{base}-{n}"
        if fix["id"] in stored:  # dismissed: listed below as decided, not proposed again
            continue
        if limit_reason and fix["action"] in WRITE_ACTIONS and fix["applicable"]:
            fix["applicable"], fix["not_applicable_reason"] = False, limit_reason
        out.append(fix)
    order = {fid: i for i, fid in enumerate(state.get("fix_order") or [])}
    out.sort(key=lambda f: (order.get(f["id"], len(order)), _ORDER.get(f["action"], 9)))
    decided = sorted(stored.values(), key=lambda f: str(f.get("applied_at") or f.get("decided_at") or ""))
    return [dict(f) for f in decided] + out


def write_fixes_used(state: dict[str, Any]) -> int:
    """Write fixes (widen, re-sync, re-run) claimed, applied or failed on this job."""
    return sum(
        1 for f in (state.get("fixes") or {}).values()
        if f.get("action") in WRITE_ACTIONS and f.get("status") in _USED
    )


def public_fix(fix: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in fix.items() if not k.startswith("_")}


# ── applying ─────────────────────────────────────────────────────────────────


def _find(report: dict[str, Any], fix_id: str) -> dict[str, Any]:
    for fix in report["fixes"]:
        if fix["id"] == fix_id:
            return fix
    raise ParityNotFound(f"Fix {fix_id} is not on this job's parity report")


# The decision workflow (approve, apply, settle, dismiss) lives in
# services.parity_apply; these names stay importable from here.
_APPLY_NAMES = frozenset({
    "approve_fix", "dismiss_fix", "settle_fix", "settle_pending", "summarized", "not_signed", "SIGNED_IS_FINAL",
})


def __getattr__(name: str) -> Any:
    if name in _APPLY_NAMES:
        from services import parity_apply

        return getattr(parity_apply, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
