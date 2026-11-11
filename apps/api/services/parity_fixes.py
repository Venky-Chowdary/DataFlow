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
from typing import Any, Callable

from services.parity_classifier import NARROW_COLUMN_CAUSES
from services.parity_evidence import TableEvidence, job_is_terminal, request_of, target_to_source
from services.parity_state import (
    MAX_APPLIED_ROUNDS,
    audit,
    job_lock,
    job_store,
    now_iso,
    save_state,
    state_of,
)
from services.remediation_actions import PARITY, action as remediation_action

logger = logging.getLogger(__name__)

MAX_RESYNC_KEYS = 1000
WRITE_ACTIONS = frozenset({"widen_column", "resync_keys", "rerun"})
_DONE = frozenset({"applied", "failed"})
_ORDER = {"widen_column": 0, "resync_keys": 1, "rerun": 2, "accept": 3}


class ParityError(Exception):
    status_code = 400


class ParityNotFound(ParityError):
    status_code = 404


class ParityConflict(ParityError):
    status_code = 409


class ParityInvalid(ParityError):
    status_code = 422


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
    """Catalog types of the narrow columns on both sides (best effort, metadata only)."""
    from services.parity_evidence import _endpoints
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.adapters_introspect import _introspect_table_schema
    from src.transfer.connector_capabilities import resolve_driver_type
    from services.dialect_profiles import schema_from_cfg

    out: dict[str, Any] = {"source": {}, "dest": {}}
    try:
        _req, source, destination = _endpoints(job)
        t2s = target_to_source(job)
        for side, endpoint, cols in (
            ("source", source, [t2s.get(c, c) for c in columns]),
            ("dest", destination, columns),
        ):
            cfg = resolve_connector_config(endpoint)
            db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or "")).lower()
            types = _introspect_table_schema(db_type, cfg, str(endpoint.table or ""), cols) or {}
            if side == "source":
                out["source"] = {c: types.get(t2s.get(c, c)) for c in columns if types.get(t2s.get(c, c))}
            else:
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
            if ev.capped:
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
    writes_used = sum(1 for f in stored.values() if f.get("action") in WRITE_ACTIONS and f.get("status") in _DONE)
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


def public_fix(fix: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in fix.items() if not k.startswith("_")}


# ── applying ─────────────────────────────────────────────────────────────────


def load_job(job_id: str) -> dict[str, Any]:
    job = job_store().get_job(job_id)
    if not job:
        raise ParityNotFound("Job not found")
    return job


def _find(report: dict[str, Any], fix_id: str) -> dict[str, Any]:
    for fix in report["fixes"]:
        if fix["id"] == fix_id:
            return fix
    raise ParityNotFound(f"Fix {fix_id} is not on this job's parity report")


def _resync_apply(job: dict[str, Any], job_id: str, fix: dict[str, Any], actor: str) -> str:
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import transfer_request_from_dict

    keys = list(fix.get("_keys") or [])
    key_tgt = str(fix.get("_key_column") or "")
    key_src = target_to_source(job).get(key_tgt, key_tgt)
    req = transfer_request_from_dict(request_of(job))
    req.sync_mode = "upsert"
    req.source_filter = {"column": key_src, "op": "in", "value": keys}
    contracts = [dict(c) for c in req.stream_contracts or [] if isinstance(c, dict)] or [
        {"name": str(req.source.table or req.source.collection or ""), "selected": True}
    ]
    for c in contracts:
        c.pop("cursor_field", None)
        c.update({"sync_mode": "upsert", "primary_key": [key_src]})
    req.stream_contracts = contracts
    req.skip_preflight = True
    req.limit = 0
    req.parent_job_id = job_id
    req.workspace_id = str(job.get("workspace_id") or req.workspace_id or "")
    req.triggered_by = actor or "parity studio"
    req.acknowledgment_actor = actor
    req.acknowledgment_reason = (
        f"Parity Studio fix {fix['id']}: re-sync {len(keys)} key(s) approved by {actor or 'operator'}."
    )
    req.idempotency_key = f"parity-{job_id}-{fix['id']}"
    engine = get_transfer_engine()
    child_id = engine._create_pending_job(req)
    result = engine.execute_tracked(req, child_id)
    if not getattr(result, "success", False):
        raise RuntimeError(f"re-sync job {child_id} failed: {str(getattr(result, 'error', '') or '')[:300]}")
    return f"Re-synced {len(keys)} key(s) through job {child_id} (upsert on {key_tgt})."


def _widen_apply(job: dict[str, Any], fix: dict[str, Any]) -> str:
    from connectors.schema_drift import widen_existing_columns_native
    from services.dest_precount import warehouse_sql_engine
    from services.dialect_profiles import schema_from_cfg
    from services.parity_evidence import _endpoints
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    _req, _source, destination = _endpoints(job)
    cfg = resolve_connector_config(destination)
    dialect = resolve_driver_type(str(cfg.get("type") or destination.format or "")).lower()
    with warehouse_sql_engine(dialect, cfg) as eng:
        raw = eng.raw_connection()
        try:
            ran = widen_existing_columns_native(
                raw.cursor(), dialect, schema_from_cfg(dialect, cfg) or None, str(fix["_table"]),
                [str(fix["_column"])], [str(fix["_new_type"])], backfill=True,
            )
            raw.commit()
        finally:
            raw.close()
    if not ran:
        raise RuntimeError("the destination column was not changed: it is already at least as wide")
    return "Ran: " + "; ".join(ran)


def approve_fix(
    job_id: str, fix_id: str, *, actor: str, note: str = "",
    start_rerun: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Approve and apply one fix, re-check, record the round; returns the new report."""
    from services.parity_report import build_parity_report

    with job_lock(job_id):
        job = load_job(job_id)
        report = build_parity_report(job, reread=True, job_id=job_id)
        fix = _find(report, fix_id)
        if fix["status"] != "proposed":
            raise ParityConflict(f"Fix {fix_id} is already {fix['status']}.")
        if not fix["applicable"]:
            raise ParityConflict(str(fix.get("not_applicable_reason") or "This fix cannot be applied."))
        state = state_of(job)
        details = {"fix_id": fix_id, "action": fix["action"], "difference_ids": fix["difference_ids"],
                   "note": note[:500]}
        audit("job.parity.fix_approved", job=job, job_id=job_id, actor=actor, details=details)
        record = {**public_fix(fix), "approved_by": actor, "decided_at": now_iso(), "note": note[:500]}
        before = report["parity"]["unexplained"]
        try:
            if fix["action"] == "accept":
                for did in fix["difference_ids"]:
                    state["acceptances"][did] = {"by": actor, "at": now_iso(), "note": note[:500],
                                                 "fix_id": fix_id}
                result = f"Accepted {len(fix['difference_ids'])} difference(s)."
            elif fix["action"] == "resync_keys":
                result = _resync_apply(job, job_id, fix, actor)
            elif fix["action"] == "widen_column":
                result = _widen_apply(job, fix)
            else:
                if start_rerun is None:
                    raise ParityConflict("A re-run can only be started from the API.")
                result = f"Started re-run {start_rerun()}; it gets its own parity report when it completes."
        except ParityError:
            raise
        except Exception as exc:  # noqa: BLE001 - a failed write is recorded on the fix, never hidden
            record.update(status="failed", applied_at=now_iso(), result=str(exc)[:500])
            state["fixes"][record["id"]] = record
            audit("job.parity.fix_failed", job=job, job_id=job_id, actor=actor,
                  details={**details, "error": str(exc)[:500]}, outcome="failed")
            save_state(job_id, job, state)
            return build_parity_report(job_store().get_job(job_id) or job, reread=True, job_id=job_id)
        record.update(status="applied", applied_at=now_iso(), result=result)
        state["fixes"][record["id"]] = record
        after_report = build_parity_report({**job, "parity": state}, reread=True, job_id=job_id)
        if fix["action"] in {"resync_keys", "widen_column"}:
            still = {k for d in after_report["differences"] if not d["cause"]["explained"] for k in d["_keys"]}
            table = str(fix.get("_table") or "")
            if table and fix.get("_keys"):
                resolved = set(state["resolved"].get(table) or []) | (set(fix["_keys"]) - still)
                state["resolved"][table] = sorted(resolved)
            state["rounds"].append({
                "n": len(state["rounds"]) + 1, "at": now_iso(), "unexplained_before": before,
                "unexplained_after": after_report["parity"]["unexplained"], "fix_id": record["id"],
            })
        audit("job.parity.fix_applied", job=job, job_id=job_id, actor=actor,
              details={**details, "result": result[:500],
                       "unexplained_after": after_report["parity"]["unexplained"]})
        save_state(job_id, job, state)
        return _with_state(after_report, state)


def _with_state(report: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    report["version"] = int(state.get("version") or 0)
    report["rounds"] = list(state.get("rounds") or [])
    return report


def dismiss_fix(job_id: str, fix_id: str, *, actor: str, note: str = "") -> dict[str, Any]:
    from services.parity_report import build_parity_report

    with job_lock(job_id):
        job = load_job(job_id)
        report = build_parity_report(job, reread=True, job_id=job_id)
        fix = _find(report, fix_id)
        if fix["status"] != "proposed":
            raise ParityConflict(f"Fix {fix_id} is already {fix['status']}.")
        state = state_of(job)
        state["fixes"][fix_id] = {**public_fix(fix), "status": "dismissed", "approved_by": actor,
                                  "decided_at": now_iso(), "note": note[:500], "result": note[:500] or None}
        audit("job.parity.fix_dismissed", job=job, job_id=job_id, actor=actor,
              details={"fix_id": fix_id, "action": fix["action"], "note": note[:500]})
        save_state(job_id, job, state)
        return build_parity_report({**job, "parity": state}, reread=True, job_id=job_id)
