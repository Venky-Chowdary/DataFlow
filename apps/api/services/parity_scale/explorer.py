"""The Trust Report difference explorer: Columns, Keys and Values of one table.

Built from a finished Parity at Scale scan (the evidence on the job) and, for
the Values tab and repair SQL, a bounded re-read of the named keys on both
sides through the same dialects the scan used. Protected columns (masked or
hashed by the run's transforms) never show a value and never get repair SQL.

Repair SQL is text for a reviewer to read and run themselves: nothing here
executes it, and every statement block says so.
"""

from __future__ import annotations

import datetime as _dt
import decimal
import uuid as _uuid
from typing import Any

from services.parity_scale.verification import block_of, table_block

PAGE_MAX = 100
REPAIR_KEYS_MAX = 200
PROTECTED = "protected"


class ExplorerUnavailable(LookupError):
    """There is no finished scan for the table (or the job has no such table)."""


def _evidence(job: dict[str, Any], table: str) -> dict[str, Any]:
    block = table_block(job, table)
    if not block:
        raise ExplorerUnavailable(f"{table} has no verification on this job")
    if block.get("status") != "done" or not block.get("evidence"):
        raise ExplorerUnavailable(f"verification of {table} has not finished")
    return block["evidence"]


def _protected(job: dict[str, Any]) -> set[str]:
    from services.parity_evidence import protected_targets

    return {c.lower() for c in protected_targets(job)}


# ── Columns ─────────────────────────────────────────────────────────────────


def columns(job: dict[str, Any], table: str) -> dict[str, Any]:
    ev = _evidence(job, table)
    diff = ev.get("differences") or {}
    rows = int((ev.get("population") or {}).get("rows_source") or 0)
    by_column = diff.get("by_column") or {}
    out = []
    for col in (ev.get("plan") or {}).get("columns") or []:
        target = col.get("target")
        causes = by_column.get(target) or {}
        differing = sum(int(c.get("rows") or 0) for c in causes.values())
        unexplained = sum(int(c.get("rows") or 0) for c in causes.values() if not c.get("explained"))
        status = (
            "excluded" if col.get("cls") == "excluded"
            else "unexplained" if unexplained
            else "explained" if differing
            else "match"
        )
        out.append({
            "column": target,
            "source_column": col.get("source"),
            "compared_by": {"engine": "in_engine", "stream": "streamed_fingerprint",
                            "excluded": "not_compared"}.get(col.get("cls"), col.get("cls")),
            "rows_compared": rows if col.get("cls") != "excluded" else 0,
            "rows_differing": differing,
            "differing_pct": round(100.0 * differing / rows, 4) if rows else 0.0,
            "status": status,
            "causes": [
                {"code": code, "label": c.get("label") or code, "rows": int(c.get("rows") or 0),
                 "explained": bool(c.get("explained"))}
                for code, c in sorted(causes.items())
            ],
            "protected": str(target or "").lower() in _protected(job),
        })
    return {"table": table, "rows_compared": rows, "columns": out,
            "complete": bool((ev.get("coverage") or {}).get("complete"))}


# ── Keys ────────────────────────────────────────────────────────────────────


def _texts(keys: list[Any]) -> list[str]:
    from services.parity_scale.report import _key_texts

    return _key_texts(keys)


def keys(job: dict[str, Any], table: str) -> dict[str, Any]:
    ev = _evidence(job, table)
    diff = ev.get("differences") or {}
    keyless = diff.get("key_attribution") == "cannot_attribute_to_key"
    protected_key = any(k.lower() in _protected(job) for k in _key_targets(job))

    def category(count_field: str, keys_field: str) -> dict[str, Any]:
        count = int(diff.get(count_field) or 0)
        shown = [] if keyless or protected_key else _texts(diff.get(keys_field) or [])
        return {"count": count, "keys": shown, "keys_shown": len(shown),
                "at_least": bool(diff.get("at_least"))}

    return {
        "table": table,
        "keyless": keyless,
        "key_columns": _key_targets(job),
        "key_protected": protected_key,
        "note": (
            "The table has no key: these are rows of content, not keys."
            if keyless else ""
        ),
        "missing_in_destination": category("missing_in_destination", "missing_keys"),
        "extra_in_destination": category("extra_in_destination", "extra_keys"),
        "duplicate_keys": category("duplicate_keys", "duplicate_key_values"),
        "changed": category("changed_rows", "changed_keys"),
    }


def _key_targets(job: dict[str, Any]) -> list[str]:
    from services.parity_evidence import key_columns_of

    return key_columns_of(job)


# ── Values (a live re-read of the named keys) ───────────────────────────────


def _sides(job: dict[str, Any]):
    """Plan and both sides, dialled under the job's workspace allowlist (as the run's dials were)."""
    from services.egress_guard import egress_workspace
    from services.parity_scale.verification import plan_for, side_opener, table_spec

    spec, why = table_spec(job)
    if spec is None:
        raise ExplorerUnavailable(why)
    with egress_workspace(str(job.get("workspace_id") or "")):
        plan = plan_for(spec)
        opener = side_opener(spec)
        src = opener("source", plan)
        try:
            dst = opener("destination", plan)
        except BaseException:
            src.close()
            raise
    return plan, src, dst


def _cell_text(value: Any) -> str | None:
    from services.parity_scale.judge import wire_text

    return wire_text(value)


def values(job: dict[str, Any], table: str, *, offset: int = 0, limit: int = 50) -> dict[str, Any]:
    """Changed keys, source and destination side by side (protected cells never shown)."""
    from services.parity_scale.judge import fingerprint
    from services.parity_scale.store import decode_key

    ev = _evidence(job, table)
    diff = ev.get("differences") or {}
    if diff.get("key_attribution") == "cannot_attribute_to_key":
        return {"table": table, "keyless": True, "rows": [], "total": 0,
                "note": "The table has no key, so values cannot be paired row by row."}
    encoded = diff.get("changed_keys") or []
    limit = max(1, min(int(limit), PAGE_MAX))
    page = encoded[offset: offset + limit]
    keys_ = [decode_key(k) for k in page]
    plan, src, dst = _sides(job)
    by_target, number_locale, date_locale = _write_context(job)

    def written(value: Any, col: Any) -> Any:
        return as_written(value, col, plan.dest_engine, mapping=by_target.get(col.target),
                          number_locale=number_locale, date_locale=date_locale)

    try:
        s_rows = src.fetch_rows(keys_)
        d_rows = dst.fetch_rows(keys_)
        protected = _protected(job)
        out = []
        for key in keys_:
            srow = (s_rows.get(src.key_hex(key)) or [None])[0]
            drow = (d_rows.get(dst.key_hex(key)) or [None])[0]
            cells = []
            for i, col in enumerate(plan.compared_columns):
                hidden = col.target.lower() in protected
                s_val = srow[i] if srow is not None else None
                d_val = drow[i] if drow is not None else None
                differs = (srow is None) != (drow is None) or (
                    srow is not None and drow is not None
                    and fingerprint(s_val, col, plan.dest_engine, source=True)
                    != fingerprint(d_val, col, plan.dest_engine, source=False)
                )
                try:
                    shown = written(s_val, col)
                except _Unwritable:
                    shown = s_val  # the run's pipeline refuses it: show what the source holds
                cells.append({
                    "column": col.target,
                    "source": PROTECTED if hidden else _cell_text(shown),
                    "destination": PROTECTED if hidden else _cell_text(d_val),
                    "differs": bool(differs),
                    "protected": hidden,
                })
            out.append({
                "key": [PROTECTED if k.lower() in protected else _cell_text(v)
                        for k, v in zip(_key_targets(job), key)],
                "in_source": srow is not None,
                "in_destination": drow is not None,
                "cells": cells,
            })
    finally:
        src.close()
        dst.close()
    return {"table": table, "keyless": False, "offset": offset, "limit": limit,
            "total": int(diff.get("changed_rows_by_engine_digest") or len(encoded)),
            "stored": len(encoded), "rows": out}


# ── Repair SQL (text only; never executed here) ─────────────────────────────


def _quote(engine: str, name: str) -> str:
    if engine == "mysql":
        return "`" + name.replace("`", "``") + "`"
    if engine == "sqlserver":
        return "[" + name.replace("]", "]]") + "]"
    return '"' + name.replace('"', '""') + '"'


def literal(engine: str, value: Any) -> str:
    """A SQL literal for one value in the destination's dialect."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return ("TRUE" if value else "FALSE") if engine == "postgresql" else ("1" if value else "0")
    if isinstance(value, (int, decimal.Decimal)):
        text = format(value, "f") if isinstance(value, decimal.Decimal) else str(value)
        return f"'{text}'" if text in {"NaN", "Infinity", "-Infinity"} else text
    if isinstance(value, float):
        return repr(value) if value == value and value not in (float("inf"), float("-inf")) else f"'{value}'"
    if isinstance(value, (bytes, bytearray, memoryview)):
        hexed = bytes(value).hex()
        return {"postgresql": f"'\\x{hexed}'::bytea", "mysql": f"X'{hexed}'"}.get(engine, f"0x{hexed}")
    if isinstance(value, (_dt.datetime, _dt.date, _dt.time)):
        return "'" + value.isoformat(sep=" ") + "'" if isinstance(value, _dt.datetime) else f"'{value.isoformat()}'"
    if isinstance(value, _uuid.UUID):
        value = str(value)
    text = str(value).replace("'", "''")
    if engine == "mysql":
        text = text.replace("\\", "\\\\")
    return ("N'" if engine == "sqlserver" else "'") + text + "'"


_IDENTITY = frozenset({"", "none", "identity", "passthrough"})


class _Unwritable(ValueError):
    """The run's transform could not be replayed on this value: no statement is written for it."""


def _comment(text: Any) -> str:
    """Text safe inside a ``--`` comment line: no newline or control character can end it."""
    import re

    return re.sub(r"[\x00-\x1f\x7f\u0085\u2028\u2029]+", " ", str(text))


def _rows(n: int, noun: str) -> str:
    return f"{n:,} {noun}{'' if n == 1 else 's'}"


def as_written(value: Any, col: Any, engine: str, *, mapping: dict[str, Any] | None = None,
               number_locale: str = "", date_locale: str = "") -> Any:
    """The source value as the run wrote it: the writer's own per-cell pipeline, replayed.

    Transform, code crosswalk and the run's locales go through
    :func:`services.write_replay.replay_write` (the writer's function, not a
    re-implementation); the decimal-fit projection through the scan's own.
    :class:`_Unwritable` when the pipeline refuses the value.
    """
    from services.parity_scale.judge import source_as_written
    from services.write_replay import replay_write, write_mapping

    steps = write_mapping(mapping or {})
    transform = str(steps.get("transform") or "").strip().lower()
    if transform not in _IDENTITY or steps.get("code_crosswalk") is not None or steps.get("rule_contracts"):
        value, err = replay_write(value, steps, target=col.target, number_locale=number_locale,
                                  date_locale=date_locale)
        if err:
            raise _Unwritable(f"{col.target}: {err}")
    return source_as_written(value, col, engine)


def _write_context(job: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], str, str]:
    """The run's mappings by target, and its number and date locales."""
    from services.parity_evidence import mappings_of, request_of

    by_target = {str(m.get("target") or m.get("source")): m for m in mappings_of(job)}
    req = request_of(job)
    return by_target, str(req.get("number_locale") or ""), str(req.get("date_locale") or "")


def _run_scope(job: dict[str, Any], key_cols: list[str]) -> tuple[bool, set[str]]:
    """(the destination held rows before this run, the keys the run quarantined)."""
    from services.parity_evidence import _OVERWRITE_MODES, quarantined_keys_of, sync_mode_of

    recon = job.get("reconciliation") if isinstance(job.get("reconciliation"), dict) else {}
    dest = job.get("destination_summary") if isinstance(job.get("destination_summary"), dict) else {}
    before = int(recon.get("target_rows_before") or dest.get("target_rows_before") or 0)
    return (sync_mode_of(job) not in _OVERWRITE_MODES and before > 0), quarantined_keys_of(job, key_cols)


def repair_sql(job: dict[str, Any], table: str) -> dict[str, Any]:
    """Statements that would make the destination match the source for the named keys.

    For a reviewer to read and run themselves: Datawrap never executes them.
    Missing rows become INSERTs of the source row as the run wrote it (its
    transforms replayed), extra rows DELETEs, changed rows UPDATEs of the
    differing columns only. Left out, and said so: protected columns; rows the
    run quarantined (they failed validation); extra rows when the run wrote
    into a table that already held rows (they may be outside this run); and
    any row whose transform cannot be replayed. A keyless table gets no
    repair SQL. Comment text never carries a newline from a name.
    """
    from services.parity_scale.judge import fingerprint, key_text
    from services.parity_scale.store import decode_key

    ev = _evidence(job, table)
    diff = ev.get("differences") or {}
    if diff.get("key_attribution") == "cannot_attribute_to_key":
        return {"table": table, "available": False, "sql": "",
                "reason": "The table has no key, so no statement can name the rows to repair."}
    plan, src, dst = _sides(job)
    engine = plan.dest_engine
    protected = _protected(job)
    key_cols = [t for _s, t in plan.key]
    if any(k.lower() in protected for k in key_cols):
        src.close()
        dst.close()
        return {"table": table, "available": False, "sql": "",
                "reason": "The key is a protected column, so repair statements cannot name rows."}
    outside_run, quarantined = _run_scope(job, key_cols)
    by_target, number_locale, date_locale = _write_context(job)

    def written(value: Any, col: Any) -> Any:
        return as_written(value, col, engine, mapping=by_target.get(col.target),
                          number_locale=number_locale, date_locale=date_locale)
    dest_ref = dst.table_ref
    cols = list(plan.compared_columns)
    names = [c.target for c in cols]
    lines = [
        f"-- Repair statements for {_comment(table)}, generated by Datawrap for review.",
        "-- Datawrap does not run these. Read them, test them, and run them yourself.",
        "-- They make the destination match the source for the keys the verification named.",
    ]
    skipped_protected = [n for n in names if n.lower() in protected]
    if skipped_protected:
        lines.append(f"-- Protected columns are left out: {_comment(', '.join(skipped_protected))}.")
    counts = {"insert": 0, "delete": 0, "update": 0}
    notes: list[str] = []
    try:
        def where(key: tuple[Any, ...]) -> str:
            return " AND ".join(f"{_quote(engine, k)} = {literal(engine, v)}" for k, v in zip(key_cols, key))

        missing = [decode_key(k) for k in (diff.get("missing_keys") or [])[:REPAIR_KEYS_MAX]]
        def quarantine_text(key: tuple[Any, ...]) -> str:
            # As services.parity_evidence.quarantined_keys_of spells a key.
            if len(key_cols) == 1:
                return key_text(plan, key)
            return ", ".join(f"{c}={key_text(plan, (v,))}" for c, v in zip(key_cols, key))

        held = [k for k in missing if quarantine_text(k) in quarantined]
        missing = [k for k in missing if quarantine_text(k) not in quarantined]
        if held:
            notes.append(f"-- {_rows(len(held), 'missing row')} the run quarantined left out: they failed "
                         "validation; fix them in the source or release them from quarantine.")
        unwritable: set[str] = set()
        if missing:
            rows = src.fetch_rows(missing)
            writable = [i for i, n in enumerate(names) if n.lower() not in protected]
            for key in missing:
                row = (rows.get(src.key_hex(key)) or [None])[0]
                if row is None:
                    continue
                try:
                    vals = ", ".join(literal(engine, written(row[i], cols[i])) for i in writable)
                except _Unwritable as exc:
                    unwritable.add(str(exc))
                    continue
                col_list = ", ".join(_quote(engine, names[i]) for i in writable)
                lines.append(f"INSERT INTO {dest_ref} ({col_list}) VALUES ({vals});")
                counts["insert"] += 1
        extras = [decode_key(k) for k in (diff.get("extra_keys") or [])[:REPAIR_KEYS_MAX]]
        if extras and outside_run:
            notes.append(f"-- {_rows(len(extras), 'extra row')} left alone: the run wrote into a table that "
                         "already held rows, so they may not be this run's.")
        else:
            for key in extras:
                lines.append(f"DELETE FROM {dest_ref} WHERE {where(key)};")
                counts["delete"] += 1
        changed = [decode_key(k) for k in (diff.get("changed_keys") or [])[:REPAIR_KEYS_MAX]]
        if changed:
            s_rows, d_rows = src.fetch_rows(changed), dst.fetch_rows(changed)
            for key in changed:
                srow = (s_rows.get(src.key_hex(key)) or [None])[0]
                drow = (d_rows.get(dst.key_hex(key)) or [None])[0]
                if srow is None or drow is None:
                    continue
                sets = []
                try:
                    for i, col in enumerate(cols):
                        if col.target.lower() in protected or col.target in key_cols:
                            continue
                        if fingerprint(srow[i], col, engine, source=True) != fingerprint(drow[i], col, engine,
                                                                                       source=False):
                            sets.append(f"{_quote(engine, col.target)} = "
                                        f"{literal(engine, written(srow[i], col))}")
                except _Unwritable as exc:
                    unwritable.add(str(exc))
                    continue
                if sets:
                    lines.append(f"UPDATE {dest_ref} SET {', '.join(sets)} WHERE {where(key)};")
                    counts["update"] += 1
        if unwritable:
            notes.append("-- Rows the run's write pipeline refuses are left out (it would not have written "
                         f"them either): {_comment('; '.join(sorted(unwritable)))}.")
    finally:
        src.close()
        dst.close()
    lines.extend(notes)
    total = sum(int(diff.get(f) or 0) for f in ("missing_in_destination", "extra_in_destination", "changed_rows"))
    covered = sum(counts.values())
    if covered < total:
        lines.append(f"-- {covered:,} of {total:,} differing rows are covered; re-verify after running these "
                     "to name the rest.")
    return {"table": table, "available": covered > 0, "engine": engine, "statements": counts,
            "rows_differing": total, "sql": "\n".join(lines) + "\n", "executed_by_datawrap": False}


def verification_summary(job: dict[str, Any]) -> dict[str, Any]:
    from services.parity_report import _verification_block

    return _verification_block(job) or {"state": None, "tables": []}


__all__ = ["ExplorerUnavailable", "block_of", "columns", "keys", "literal", "repair_sql", "values",
           "verification_summary"]
