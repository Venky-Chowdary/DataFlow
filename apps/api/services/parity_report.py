"""Parity Studio: one answer a person can sign — does the destination match?

``build_parity_report(job)`` aggregates the evidence the run already stored
(Gate-8 reconciliation, the verification ladder, the transform and protection
proofs, the write witness, the quarantine, the trust score), re-reads the cells
of the keys that evidence names (bounded), classifies every difference with the
pure rules in :mod:`services.parity_classifier`, bounds what a sampled check
could have missed (:mod:`services.parity_stats`), proposes fixes from the fixed
menu (:mod:`services.parity_fixes`) and merges what people decided
(:mod:`services.parity_state`). Verdicts are deterministic: the same evidence
and the same decisions give the same report. The AI layer
(:mod:`services.parity_explain`) only words it.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from services.parity_classifier import (
    UNEXPLAINED,
    CellContext,
    accepted_by_operator,
    cause,
    classify_row,
    classify_value,
    concurrent_source_update,
)
from services.parity_evidence import (
    MAX_KEYS_SAMPLE,
    Observation,
    TableEvidence,
    dest_engine_of,
    extract_evidence,
    gate8_failed,
    job_is_terminal,
    localizes,
    mark_unaccounted,
    reread_cells,
    sync_mode_of,
)
from services.parity_state import now_iso, state_of
from services.parity_stats import CONFIDENCE_LEVEL, clopper_pearson_upper, format_rate_pct

logger = logging.getLogger(__name__)

_FULL_CHECKS = frozenset({"full_checksum", "key_diff"})
#: How a protected value (a key included) is shown.
PROTECTED = "protected"


def job_id_of(job: dict[str, Any]) -> str:
    return str(job.get("_id") or job.get("id") or job.get("job_id") or "")


def difference_id(table: str, column: str | None, kind: str, code: str) -> str:
    raw = "|".join([table, column or "", kind, code])
    return "d_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


# ── classification ───────────────────────────────────────────────────────────


def _ctx(ev: TableEvidence, column: str | None, engine: str) -> CellContext:
    col = column or ""
    return CellContext(
        source_type=ev.source_types.get(col, ""),
        dest_type=ev.dest_types.get(col, ""),
        dest_engine=engine,
        transforms=ev.transforms.get(col, ()),
    )


#: Origins whose cells are the values Gate-8 itself stored for a cell it found different.
_STORED_CELL_ORIGINS = frozenset({"sample_compare", "ladder_l5"})
#: ``reconciliation.py`` keeps at most this many characters of a stored sample value.
STORED_VALUE_LIMIT = 120


def _classify(ob: Observation, ev: TableEvidence, engine: str) -> dict[str, Any] | None:
    if ob.cause is not None:
        return ob.cause
    if ob.kind == "value":
        if ob.column is None:
            return concurrent_source_update(ob.key, ev.drifted_keys) or cause(
                UNEXPLAINED,
                "The key census found this row changed; its cells could not be re-read.",
                explained=False,
            )
        found = classify_value(
            expected=ob.expected, dest=ob.dest, ctx=_ctx(ev, ob.column, engine),
            source_raw=ob.raw, raw_known=ob.raw_known, key=ob.key,
            quarantined_keys=ev.quarantined_keys, drifted_keys=ev.drifted_keys,
        )
        if ob.origin in _STORED_CELL_ORIGINS:
            return _stored_cell_cause(ob, found)
        return found
    return classify_row(
        ob.kind, key=ob.key, quarantined_keys=ev.quarantined_keys, drifted_keys=ev.drifted_keys,
        outside_this_run=ev.outside_run,
    )


def _stored_cell_cause(ob: Observation, found: dict[str, Any] | None) -> dict[str, Any]:
    """A cell Gate-8 found different is never dropped as equal.

    Gate-8 stores its sample values cut at :data:`STORED_VALUE_LIMIT`
    characters (as fingerprints), so two stored cells can read equal — or
    differ only in a way a rule explains — while the full values differ
    elsewhere. Unless the cells were re-read, such a cell stays unexplained.
    A diagnosis that is itself unexplained (a cut, a changed digit) stands:
    no longer value can make it explained.
    """
    cut = ob.origin == "sample_compare" and any(
        len(v or "") >= STORED_VALUE_LIMIT for v in (ob.expected, ob.dest)
    )
    if found is not None and not (cut and found.get("explained")):
        return found
    if cut:
        detail = (
            f"Gate-8 found this cell different, but it keeps only the first {STORED_VALUE_LIMIT} characters "
            "of a value, so the stored values are too short to classify the difference. Re-read the row "
            "(a single-column key and a saved route) or re-run the job."
        )
    else:
        detail = (
            "Gate-8 found this cell different, but its stored values are too short to classify the "
            "difference (they read equal as stored). Re-read the row or re-run the job."
        )
    return cause(UNEXPLAINED, detail, explained=False)


def _apply_reread(job: dict[str, Any], ev: TableEvidence, engine: str, resolved: set[str]) -> None:
    """Replace the evidence for re-read keys with what both sides hold now.

    A key the evidence named that now reads equal on both sides is *not*
    resolved by that alone: the run's difference can no longer be re-checked
    (the source or the destination changed since), so it stays an unexplained
    difference saying so. Only keys a recorded fix re-checked (``resolved``)
    and an extra row that is gone from the destination are resolved.
    """
    keyed = [o for o in ev.observations if o.key and o.cause is None]
    keys = list(dict.fromkeys(o.key for o in keyed))
    if not keys:
        ev.reread = {"status": "not_needed", "keys": 0}
        return
    try:
        cells = _bounded_reread(job, ev, keys)
    except Exception as exc:  # noqa: BLE001 - an unreadable side keeps the stored evidence
        logger.info("parity re-read skipped for %s: %s", ev.name, type(exc).__name__)
        ev.reread = {"status": "failed", "keys": 0, "reason": _reread_reason(exc)}
        return
    aggregated = {o.column for o in ev.observations if o.origin in {"recipe_effect", "protection"}}
    fresh: list[Observation] = []
    for key, row in cells.items():
        raw, expected, dest = row.get("raw"), row.get("expected"), row.get("dest")
        named = {o.kind for o in keyed if o.key == key}
        sampled = any(o.sampled and o.key == key for o in keyed)
        if raw is None and dest is None:
            if named <= {"extra_in_destination"}:
                continue  # the extra row is gone from the destination: resolved
            kind = "missing_in_destination" if "missing_in_destination" in named else "value"
            fresh.append(Observation(
                table=ev.name, kind=kind, key=key, origin="reread", sampled=sampled,
                cause=cause(
                    UNEXPLAINED,
                    f"Neither side holds this key now: the source removed it after the run, so the row the "
                    f"evidence named ({kind.replace('_', ' ')}) cannot be re-checked. Accept it with a note "
                    "if that removal was intended.",
                    explained=False,
                ),
            ))
            continue
        if raw is not None and dest is None:
            if not (row.get("held_out") and key in ev.quarantined_keys):
                fresh.append(Observation(table=ev.name, kind="missing_in_destination", key=key,
                                         origin="reread", sampled=sampled))
            continue
        if raw is None:
            fresh.append(Observation(table=ev.name, kind="extra_in_destination", key=key,
                                     origin="reread", sampled=sampled))
            continue
        before = len(fresh)
        # A row the run's own Map stage would hold out has no written image: grade it raw.
        for column, want in (expected or raw).items():
            ctx = _ctx(ev, column, engine)
            found = classify_value(
                expected=want, dest=dest.get(column), ctx=ctx, source_raw=raw.get(column),
                raw_known=True, key=key, quarantined_keys=ev.quarantined_keys,
                drifted_keys=ev.drifted_keys,
            )
            if found is None or (found["code"] == "transform_applied" and column in aggregated):
                continue
            fresh.append(Observation(
                table=ev.name, kind="value", column=column, key=key, expected=want,
                dest=dest.get(column), raw=raw.get(column), raw_known=True, origin="reread",
                cause=found, sampled=sampled,
            ))
        if len(fresh) == before and key not in resolved:
            fresh.append(_cannot_recheck(ev, key, named, sampled))
    done = set(cells)
    ev.observations = [o for o in ev.observations if not (o.key in done and o.cause is None)] + fresh
    ev.reread = {"status": "ok", "keys": len(done)}
    still = {o.key for o in fresh}
    ev.resolved_now = sorted(done - still)
    if done & resolved - still:
        ev.resolved_by_fix = True


def _cannot_recheck(ev: TableEvidence, key: str, named: set[str], sampled: bool) -> Observation:
    """An evidence-named key both sides now hold equal: the run's difference cannot be re-checked."""
    kind = next((k for k in ("missing_in_destination", "extra_in_destination") if k in named), "value")
    return Observation(
        table=ev.name, kind=kind, key=key, origin="reread", sampled=sampled,
        cause=cause(
            UNEXPLAINED,
            f"Gate-8 named this row ({kind.replace('_', ' ')}), but both sides now hold equal values, so the "
            "difference it saw cannot be re-checked: the source or the destination changed after the run. "
            "Accept it with a note if that change is expected, or re-run the job.",
            explained=False,
        ),
    )


def _reread_seconds() -> float:
    from services.brand_env import getenv_brand

    try:
        return max(1.0, min(600.0, float(getenv_brand("PARITY_REREAD_TIMEOUT", "30") or 30)))
    except ValueError:
        return 30.0


class RereadTimeout(TimeoutError):
    """The bounded re-read did not finish in time."""


def _bounded_reread(job: dict[str, Any], ev: TableEvidence, keys: list[str]) -> dict[str, dict[str, Any]]:
    """The keyed re-read, under the connector executor's deadline and statement budget."""
    from src.ai.copilot.tool_deadline import run_with_deadline

    def timed_out(message: str) -> Any:
        raise RereadTimeout(message)

    return run_with_deadline(
        "parity_reread", lambda: reread_cells(job, ev, keys), timed_out, seconds=_reread_seconds(),
    )


def _reread_reason(exc: Exception) -> str:
    if isinstance(exc, RereadTimeout):
        return f"the re-read did not finish within {_reread_seconds():g}s; the stored evidence stands"
    if isinstance(exc, LookupError):
        return str(exc)
    return f"the source or destination could not be re-read ({type(exc).__name__})"


def _apply_resolved(ev: TableEvidence, resolved: set[str]) -> None:
    """Without a re-read, keys a recorded fix re-check proved equal stay resolved."""
    if resolved:
        before = len(ev.observations)
        ev.observations = [o for o in ev.observations if not (o.key in resolved and o.cause is None)]
        ev.resolved_by_fix = ev.resolved_by_fix or len(ev.observations) < before


# ── grouping ─────────────────────────────────────────────────────────────────


def _example(ob: Observation, protected: bool) -> dict[str, Any] | None:
    if protected or ob.kind != "value" or (ob.expected is None and ob.dest is None and not ob.key):
        return None
    if ob.origin in {"recipe_effect", "protection", "write_witness", "row_count", "quarantine"}:
        return None
    return {"source": ob.raw if ob.raw_known else ob.expected, "destination": ob.dest}


def _group(ev: TableEvidence, engine: str, acceptances: dict[str, Any]) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}
    for ob in ev.observations:
        found = _classify(ob, ev, engine)
        if found is None:
            continue
        did = difference_id(ev.name, ob.column, ob.kind, found["code"])
        diff = groups.get(did)
        if diff is None:
            protected = bool(ob.column and ob.column in ev.protected)
            diff = groups[did] = {
                "id": did, "table": ev.name, "column": ob.column, "kind": ob.kind,
                "rows": 0, "keys_sample": [], "cause": dict(found),
                "example": _example(ob, protected), "protected": protected,
                "_keys": [], "_keyless": 0, "_sampled_keys": [], "_localizing": False,
            }
        diff["_localizing"] = diff["_localizing"] or localizes(ob)
        if ob.key:
            if ob.key not in diff["_keys"]:
                diff["_keys"].append(ob.key)
            if ob.sampled and ob.key not in diff["_sampled_keys"]:
                diff["_sampled_keys"].append(ob.key)
        else:
            diff["_keyless"] += max(int(ob.rows or 0), 0)
            for k in ob.keys_sample:
                if k not in diff["keys_sample"]:
                    diff["keys_sample"].append(k)
    # A protected key column (hash_pii, mask_pii, redact, omit) never shows its values.
    key_protected = any(c in ev.protected for c in ev.key_columns)
    out = []
    for diff in groups.values():
        accepted = acceptances.get(diff["id"])
        parts = [diff]
        if accepted and not diff["cause"]["explained"]:
            parts = _split_accepted(diff, accepted)
        for part in parts:
            part["rows"] = len(part["_keys"]) + part["_keyless"]
            part["keys_sample"] = (part["_keys"] + part["keys_sample"])[:MAX_KEYS_SAMPLE]
            if key_protected and part["keys_sample"]:
                part["keys_sample"] = [PROTECTED]
            if part["rows"] > 0:
                out.append(part)
    order = {"value": 0, "missing_in_destination": 1, "extra_in_destination": 2, "duplicate_key": 3}
    out.sort(key=lambda d: (d["cause"]["explained"], order.get(d["kind"], 9), d["column"] or "", d["id"]))
    return out


def _split_accepted(diff: dict[str, Any], accepted: dict[str, Any]) -> list[dict[str, Any]]:
    """The accepted rows (explained, same id) and what lies beyond them (unexplained).

    An acceptance names the keys and the keyless row count a person saw; a
    later difference with the same signature on other keys, or more rows,
    is not covered. An acceptance recorded without its rows covers nothing.
    """
    keys = set(accepted.get("keys") or [])
    keyless = min(diff["_keyless"], max(int(accepted.get("keyless_rows") or 0), 0))
    inside = [k for k in diff["_keys"] if k in keys]
    beyond = [k for k in diff["_keys"] if k not in keys]
    was = diff["cause"]
    covered = {
        **diff, "_keys": inside, "_keyless": keyless,
        "_sampled_keys": [k for k in diff["_sampled_keys"] if k in keys],
        "keys_sample": list(diff["keys_sample"]) if keyless else [],
        "cause": accepted_by_operator(accepted),
    }
    covered["cause"]["detail"] += f" Diagnosed as: {was['label'].lower()} — {was['detail']}"
    rest = {
        **diff, "id": difference_id(diff["table"], diff["column"], diff["kind"], was["code"] + "|beyond_acceptance"),
        "_keys": beyond, "_keyless": diff["_keyless"] - keyless,
        "_sampled_keys": [k for k in diff["_sampled_keys"] if k not in keys],
        "keys_sample": [] if keyless else list(diff["keys_sample"]),
        "cause": {**was, "detail": was["detail"] + " These rows are beyond what was accepted."},
    }
    return [covered, rest]


# ── tables, confidence, verdict ──────────────────────────────────────────────


def _row_counts(diffs: list[dict[str, Any]], cap: int | None) -> tuple[int, int]:
    """(rows that differ, rows with an unexplained difference) — a row counts once."""
    keyed_all: set[str] = set()
    keyed_bad: set[str] = set()
    keyless_all = keyless_bad = 0
    for d in diffs:
        keyed_all.update(d["_keys"])
        keyless_all += d["_keyless"]
        if not d["cause"]["explained"]:
            keyed_bad.update(d["_keys"])
            keyless_bad += d["_keyless"]
    differ = len(keyed_all) + keyless_all
    bad = len(keyed_bad) + keyless_bad
    if cap is not None and cap > 0:
        differ, bad = min(differ, cap), min(bad, cap)
    return differ, bad


def _table(ev: TableEvidence, diffs: list[dict[str, Any]]) -> dict[str, Any]:
    cap = max([n for n in (ev.rows_source, ev.rows_destination) if n is not None], default=None)
    differ, bad = _row_counts(diffs, cap)
    k = len({key for d in diffs if not d["cause"]["explained"] for key in d["_sampled_keys"]})
    upper = None
    pct: float | None = None
    if ev.check in _FULL_CHECKS and ev.rows_source:
        pct = round(100.0 * max(ev.rows_source - bad, 0) / ev.rows_source, 4)
    elif ev.check == "sampled" and ev.sampled_rows:
        upper = clopper_pearson_upper(min(k, ev.sampled_rows), ev.sampled_rows)
        pct = round(100.0 * (1.0 - upper), 4) if upper is not None else None
    if ev.capped or ev.gate8_unaccounted:
        pct = None  # the census stopped naming keys, or Gate-8 failed on rows it did not name
    if bad:
        status = "unexplained"
    elif ev.check in {"none", "count_only"} or ev.capped or ev.gate8_unaccounted:
        status = "unverifiable"
    elif differ:
        status = "explained"
    else:
        status = "match"
    return {
        "name": ev.name, "check": ev.check, "rows_source": ev.rows_source,
        "rows_destination": ev.rows_destination, "proven_equal_pct": pct,
        "differences": differ, "explained": differ - bad, "unexplained": bad,
        "status": status, "upper_bound_rate": upper,
        "_sample": (ev.sampled_rows, k) if ev.check == "sampled" else None,
        "_note": ev.note,
    }


def _confidence(tables: list[dict[str, Any]]) -> dict[str, Any]:
    kinds = {
        "full" if t["check"] in _FULL_CHECKS else "sampled" if t["check"] == "sampled" else "none"
        for t in tables
    } or {"none"}
    method = next(iter(kinds)) if len(kinds) == 1 else "mixed"
    n = sum(t["_sample"][0] for t in tables if t["_sample"])
    k = sum(t["_sample"][1] for t in tables if t["_sample"])
    upper = clopper_pearson_upper(min(k, n), n) if n else None
    full_tables = [t for t in tables if t["check"] in _FULL_CHECKS]
    bad_full = sum(t["unexplained"] for t in full_tables)
    if method == "full":
        statement = (
            "Every row was compared and every difference is explained."
            if not bad_full else
            f"Every row was compared: {bad_full} row(s) hold a difference no rule explains."
        )
        if not any(t["differences"] for t in full_tables):
            statement = "Every row was compared and no row differs."
    elif method == "sampled":
        statement = (
            f"Values were compared on {n:,} sampled row(s). With {int(CONFIDENCE_LEVEL * 100)}% "
            f"confidence at most {format_rate_pct(upper)} of rows hold an unexplained difference."
        )
    elif method == "mixed":
        parts = []
        if full_tables:
            parts.append(f"{len(full_tables)} table(s) were compared in full")
        if n:
            parts.append(
                f"values were compared on {n:,} sampled row(s) elsewhere; with "
                f"{int(CONFIDENCE_LEVEL * 100)}% confidence at most {format_rate_pct(upper)} "
                "of those tables' rows hold an unexplained difference"
            )
        none = [t["name"] for t in tables if t["check"] in {"count_only", "none"}]
        if none:
            parts.append(f"values were not compared for {', '.join(none[:5])}")
        statement = "; ".join(parts).capitalize() + "."
    else:
        statement = "No value comparison ran for this job, so nothing bounds the difference rate."
    return {
        "method": method, "sampled_rows": n, "differences_in_sample": k,
        "level": CONFIDENCE_LEVEL, "upper_bound_rate": upper, "statement": statement,
    }


def _verdict(job: dict[str, Any], tables: list[dict[str, Any]], confidence: dict[str, Any]) -> tuple[str, str]:
    if not job_is_terminal(job):
        return "unverifiable", "The job has not finished; parity is decided once it completes."
    if not isinstance(job.get("reconciliation"), dict) or not job.get("reconciliation"):
        return "unverifiable", "Gate-8 recorded no reconciliation for this job, so nothing was compared."
    bad = sum(t["unexplained"] for t in tables)
    if bad:
        names = ", ".join(t["name"] for t in tables if t["unexplained"])
        capped = " ".join(t["_note"] for t in tables if t.get("_note") and t["check"] not in {"count_only", "none"})
        return "unexplained", f"{bad:,} row(s) in {names} hold a difference no rule explains." + (
            f" {capped}" if capped else ""
        )
    noted = [t["_note"] for t in tables if t["status"] == "unverifiable" and t.get("_note")]
    if noted:
        return "unverifiable", " ".join(noted)
    blind = [t["name"] for t in tables if t["status"] == "unverifiable"]
    if blind:
        return "unverifiable", (
            f"Values were not compared for {', '.join(blind)} (only row counts or nothing was "
            "recorded), so the evidence cannot decide."
        )
    explained = sum(t["explained"] for t in tables)
    if explained:
        return "explained", f"{explained:,} row(s) differ and every difference is explained."
    if all(t["check"] in _FULL_CHECKS for t in tables):
        return "proven", "No differences: the destination matches the source."
    # A sample that found nothing bounds the difference rate; it proves nothing.
    sampled = [t["name"] for t in tables if t["check"] not in _FULL_CHECKS]
    return "no_difference_found", (
        f"No difference was found, but the values of {', '.join(sampled[:5])} were compared on a "
        f"sample of {confidence['sampled_rows']:,} row(s): with {int(CONFIDENCE_LEVEL * 100)}% "
        f"confidence at most {format_rate_pct(confidence['upper_bound_rate'])} of rows differ."
    )


def _sum(values: list[int | None]) -> int | None:
    known = [v for v in values if v is not None]
    return sum(known) if known else None


def _parity_block(tables: list[dict[str, Any]]) -> dict[str, Any]:
    weighted = [(t["proven_equal_pct"], t["rows_source"] or 0) for t in tables if t["proven_equal_pct"] is not None]
    total = sum(w for _p, w in weighted)
    pct = None
    if weighted:
        pct = round(sum(p * w for p, w in weighted) / total, 4) if total else round(
            sum(p for p, _w in weighted) / len(weighted), 4
        )
    return {
        "rows_source": _sum([t["rows_source"] for t in tables]),
        "rows_destination": _sum([t["rows_destination"] for t in tables]),
        "rows_differ": sum(t["differences"] for t in tables),
        "explained": sum(t["explained"] for t in tables),
        "unexplained": sum(t["unexplained"] for t in tables),
        "proven_equal_pct": pct,
    }


def _evidence_block(job: dict[str, Any], tables_ev: list[TableEvidence]) -> dict[str, Any]:
    from services.job_trust import compute_job_trust

    recon = job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else {}
    trust = None
    try:
        trust = compute_job_trust(job).get("score")
    except (TypeError, ValueError, KeyError) as exc:
        logger.info("parity: trust score unavailable (%s)", type(exc).__name__)
    protection = recon.get("protection_proof") if isinstance(recon.get("protection_proof"), dict) else {}
    transform = recon.get("transform_applied") if isinstance(recon.get("transform_applied"), dict) else {}
    passed = recon.get("passed")
    return {
        "reconciliation_passed": passed if isinstance(passed, bool) else None,
        "assurance_level": str(recon.get("assurance_level") or "") or None,
        "trust_score": float(trust) if isinstance(trust, (int, float)) else None,
        "protection_proof": str(protection.get("status") or "") or None,
        "transform_proof": str(transform.get("status") or "") or None,
        "reread": {ev.name: ev.reread for ev in tables_ev if ev.reread},
    }


def _signoff(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    from services.parity_signoff import content_sha256, valid_approvals

    verdict, unexplained = report["verdict"], report["parity"]["unexplained"]
    roles = list(state.get("required_roles") or [])
    signed = state.get("signed") if isinstance(state.get("signed"), dict) else None
    if signed:
        sealed = ((signed.get("pack") or {}).get("parity") or {}).get("signoff") or {}
        approvals, invalidated = list(sealed.get("approvals") or []), []
    else:
        approvals, invalidated = valid_approvals(state, content_sha256(report))
    blocked_reason = None
    if signed:
        status = "signed"
    elif unexplained > 0:
        status = "blocked"
        blocked_reason = f"{unexplained:,} row(s) still hold an unexplained difference."
    elif verdict == "unverifiable":
        status = "blocked"
        blocked_reason = "The evidence cannot decide parity, so there is nothing to sign."
    elif any(f.get("status") == "applying" for f in report.get("fixes") or []):
        status = "blocked"
        blocked_reason = "A fix is being applied; sign once its re-check is recorded."
    else:
        status = "open"
    return {
        "required_roles": roles, "approvals": approvals, "invalidated_approvals": invalidated,
        "status": status, "blocked_reason": blocked_reason,
        "signature": (signed or {}).get("signature"), "signed_at": (signed or {}).get("signed_at"),
    }


def _public(item: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in item.items() if not str(k).startswith("_")}


def public_report(report: dict[str, Any]) -> dict[str, Any]:
    """The report without its internal bookkeeping (``_``-prefixed keys).

    Everything that leaves the API — the response, the signed pack, the
    exports — goes through here, so the fixes' key lists and DDL inputs
    (``_keys``, ``_key_column``, ``_new_type`` …) never do.
    """
    out = dict(report)
    for name in ("tables", "differences", "fixes"):
        out[name] = [_public(item) for item in report.get(name) or []]
    return _public(out)


def build_parity_report(
    job: dict[str, Any], *, reread: bool = True, job_id: str = "",
    recheck: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """The full report for one job, with internal fields (see :func:`public_report`).

    ``reread=False`` reads no database: the summary and the MCP tool use it.
    ``recheck`` names, per table, the keys a fix just wrote: a re-read that
    finds them equal resolves them (a fix's re-check round).
    """
    from services.parity_fixes import propose_fixes

    state = state_of(job)
    engine = dest_engine_of(job)
    recon = job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else {}
    tables_ev = extract_evidence(job) if recon else []
    terminal = job_is_terminal(job)
    for ev in tables_ev:
        resolved = set((state.get("resolved") or {}).get(ev.name) or [])
        checking = set((recheck or {}).get(ev.name) or [])
        if reread and terminal and ev is tables_ev[0]:
            _apply_reread(job, ev, engine, resolved | checking)
            if ev.reread.get("status") != "ok":
                _apply_resolved(ev, resolved)
        else:
            _apply_resolved(ev, resolved)
    differences: list[dict[str, Any]] = []
    tables: list[dict[str, Any]] = []
    for ev in tables_ev:
        diffs = _group(ev, engine, state.get("acceptances") or {})
        if (ev is tables_ev[0] and gate8_failed(recon) and not ev.gate8_unaccounted
                and not ev.resolved_by_fix and not any(d["_localizing"] for d in diffs)):
            # Gate-8 failed, and no difference that accounts for it survived classification.
            mark_unaccounted(ev, recon)
        differences += diffs
        tables.append(_table(ev, diffs))
    confidence = _confidence(tables)
    verdict, reason = _verdict(job, tables, confidence)
    parity = _parity_block(tables)
    report: dict[str, Any] = {
        "job_id": job_id or job_id_of(job),
        "generated_at": now_iso(),
        "version": int(state.get("version") or 0),
        "verdict": verdict,
        "verdict_reason": reason,
        "parity": parity,
        "confidence": confidence,
        "tables": tables,
        "differences": differences,
        "explanation": state.get("explanation"),
        "rounds": list(state.get("rounds") or []),
        "evidence": _evidence_block(job, tables_ev),
        "_tables_ev": tables_ev,
        "_sync_mode": sync_mode_of(job),
    }
    report["fixes"] = propose_fixes(job, report, state, reread=reread)
    report["signoff"] = _signoff(state, report)
    return report


def compact_summary(report: dict[str, Any]) -> dict[str, Any]:
    """What the dashboard needs from a report, stored on the job (no row values)."""
    return {
        "verdict": report["verdict"],
        "proven_equal_pct": report["parity"]["proven_equal_pct"],
        "unexplained": report["parity"]["unexplained"],
        "rows_source": report["parity"]["rows_source"],
        "tables": [
            {k: t.get(k) for k in ("name", "status", "check", "unexplained", "rows_source", "proven_equal_pct")}
            for t in report["tables"]
        ],
        "signed": report["signoff"]["status"] == "signed",
        "at": report["generated_at"],
    }
