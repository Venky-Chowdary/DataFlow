"""Reversibility gate: can the destination be turned back into the source?

One invertibility table for every transform and type mapping, applied per
column, deterministically:

* **invertible** - identity, rename, widening, a lossless cast (to text, integer
  to decimal, date to datetime, boolean to integer) or the engine's
  type-preserving numeric bind;
* **lossy** - hashing, masking, redaction, an omitted or dropped column, any
  narrowing the type system recognises (string, decimal, integer, float,
  temporal, binary, interval, bit width), truncation (datetime to date/time),
  rounding (to integer, decimal to float, float to decimal), concatenation,
  recipe steps that change values or rows, and a row cap or source filter (only
  part of the source lands);
* **conditional** - invertible only when the observed values round-trip
  exactly: a cast out of text, a normalising transform (trim, case, unicode,
  e-mail, ...), ``replace:`` / ``default:`` / ``assume_timezone:``, or a column
  whose type is not declared.

A conditional column is checked against observed rows (the plan's sample): a
value fits when the column's transform, applied to the value's exact text,
gives the identical text back. A sample that fits makes the column invertible
*for the data seen*, and the reason says so; a narrowing stays lossy whatever
the sample holds. A table is reversible only when every column is; a lossy key
column makes the table not reversible (its rows cannot be matched back).

Nothing here claims an undo: the rollback note reuses the
:mod:`services.migration_rollback` vocabulary (a documented runbook, the source
kept as the fallback), never a one-click reversal.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from services.reversibility_types import (
    CONDITIONAL,
    INVERTIBLE,
    LOSSY,
    TEXT,
    TYPE_CHECKED,
    check_type_values,
    logical,
    type_pair,
)

GATE_ID = "g24_reversibility"

REVERSIBLE, PARTLY, NOT_REVERSIBLE = "reversible", "partly_reversible", "not_reversible"

#: Map transforms that never change a value the engine binds.
_IDENTITY = frozenset({"", "none", "identity", "passthrough", "string", "varchar", "text"})
#: Map transforms whose output cannot give the input back.
_LOSSY_TRANSFORMS: dict[str, tuple[str, str]] = {
    "hash_pii": ("hash_pii", "is hashed (hash_pii); the original value cannot be recovered from the destination"),
    "mask_pii": ("mask_pii", "is masked (mask_pii); the masked characters cannot be recovered"),
    "redact": ("redact", "is redacted; the original value is not kept in the destination"),
    "vector": ("vector", "is replaced by an embedding vector; the original text cannot be recovered"),
}
#: Recipe steps (``services.shape_models``) that change values beyond recovery.
_LOSSY_STEPS = frozenset({
    "drop_column", "keep_columns", "null_if", "round_number", "truncate_number", "absolute", "clamp",
    "hash_identity", "hash_pii", "mask_pii", "strip_characters", "set_value", "concat_columns",
})
#: Recipe steps that change which rows land (a filter, a top-N, a merge, a reshape).
_ROW_STEPS = frozenset({
    "filter_rows", "divert_rows", "dedupe_rows", "aggregate", "union", "join", "router", "unnest_json",
    "nest_json", "lookup", "scd2", "rank_rows",
})
#: Recipe steps that add or rename without touching an existing value - as long as
#: the column they write (``options.to`` / ``keys``) is new: written onto a column
#: the row already holds, they overwrite it (``recipe_lossy``).
_INVERTIBLE_STEPS = frozenset({
    "rename_column", "sort_rows", "constant_column", "derive_column", "surrogate_key", "flatten_json",
})
#: Target logical type -> the engine cast that binds a text value into it.
_CAST_FOR: dict[str, str] = {
    "integer": "integer", "decimal": "decimal", "float": "decimal", "boolean": "boolean",
    "date": "date", "datetime": "datetime", "time": "time", "json": "json",
}


_REASON = {
    "identity": "is copied unchanged",
    "rename": "is renamed to {target}; the values are copied unchanged",
    "widen": "widens {stype} to {ttype}; every value fits",
    "timezone": "drops the time zone ({stype} to {ttype}); the original offset cannot be recovered",
    "json_normalize": ("is stored as {ttype}, which keeps one value per key in its own key order and spacing; "
                       "the original {stype} text cannot be rebuilt"),
    "charset": ("narrows the character set ({stype} to {ttype}); reversible only if every character survives "
                "the code page (checked as plain ASCII)"),
    "enum": "is cast {stype} to {ttype}; reversible only if every value is one of its labels",
    "blank_pad": "is padded to the fixed width of {ttype}; reversible only if no value ends in a space",
    "text_width": "is cast {stype} to {ttype}; reversible only if every value's text fits the width",
    "lossless_cast": "is cast {stype} to {ttype} without losing information",
    "type_preserving_bind": "is bound as its own type ({ttype})",
    "omit": "is not written to the destination (omitted)",
    "dropped": "is not mapped, so the destination does not hold it",
    "narrowing": "narrows {stype} to {ttype}; values that do not fit lose data",
    "truncation": "truncates {stype} to {ttype}; the dropped part cannot be recovered",
    "rounding": "rounds {stype} to {ttype}; the lost digits cannot be recovered",
    "concat": "is combined with other text (concat); the parts cannot be split back reliably",
    "recipe_lossy": "is changed by the recipe step {op}, which does not keep the original value",
    "row_change": "the recipe step {op} changes which rows land, so the source rows cannot be rebuilt",
    "recipe_conditional": "is changed by the recipe step {op}; whether every value survives was not checked",
    "recipe_invertible": "is touched only by the recipe step {op}, which keeps every value",
    "cast": "is cast {stype} to {ttype}; reversible only if every value round-trips exactly",
    "normalize": "is normalised ({transform}); reversible only if no value changes",
    "replace": "has text replaced ({transform}); reversible only if no value changes",
    "default": "has blanks filled ({transform}); reversible only if no value is blank",
    "assume_timezone": "is read with an assumed zone ({transform}); reversible only if no value changes",
    "type_unknown": "has no declared {side} type, so whether it fits cannot be decided from types",
}


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _transform_of(mapping: Mapping[str, Any]) -> str:
    from services.transform_engine import normalize_engine_transform

    raw = mapping.get("transform")
    if raw is None or _norm(raw) == "":
        raw = mapping.get("engine_transform") or ""
    canon = normalize_engine_transform(str(raw))
    return (canon if canon is not None else _norm(raw)).lower() if _norm(raw) else ""


def _is_omitted(mapping: Mapping[str, Any], transform: str) -> bool:
    return bool(
        mapping.get("omit") or mapping.get("omitted") or mapping.get("intentional_omit")
        or mapping.get("intentionalOmit") or transform == "omit"
    )


# --------------------------------------------------------------- data check


def strict_text(value: Any) -> str | None:
    """A value's exact text: the round-trip check compares these, never a normalised form."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return str(value)  # keeps the scale: 1.50 stays 1.50
    from services.value_serializer import cell_to_string

    return cell_to_string(value)


def round_trips(value: Any, transform: str) -> bool:
    """True when ``transform`` gives ``value``'s exact text back (it changed nothing)."""
    from services.transform_engine import apply_transform

    text = strict_text(value)
    if text is None:
        return True
    out, error = apply_transform(text, transform)
    return error is None and strict_text(out) == text


def check_values(values: Iterable[Any], transform: str) -> dict[str, Any]:
    """{status, rows_checked, rows_not_fitting} for one conditional column."""
    checked = failing = 0
    for value in values:
        if value is None:
            continue
        checked += 1
        if not round_trips(value, transform):
            failing += 1
    status = "not_checked" if not checked else ("does_not_fit" if failing else "fits")
    return {"status": status, "rows_checked": checked, "rows_not_fitting": failing}


_WIDTH = re.compile(r"\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")


def _overflow(values: Sequence[Any], ttype: str, tlogical: str) -> dict[str, Any] | None:
    """How many observed values a narrowed width/scale would cut (a disclosure, never a pass)."""
    match = _WIDTH.search(_norm(ttype))
    if not match:
        return None
    width, scale = int(match.group(1)), match.group(2)
    checked = failing = 0
    for value in values:
        text = strict_text(value)
        if text is None:
            continue
        checked += 1
        if tlogical in TEXT and len(text) > width:
            failing += 1
        elif tlogical == "decimal" and scale is not None:
            try:
                exp = Decimal(text).normalize().as_tuple().exponent
            except Exception:  # noqa: BLE001 - not a number: the cast refuses it anyway
                continue
            if isinstance(exp, int) and -exp > int(scale):
                failing += 1
    if not checked:
        return None
    return {"status": "does_not_fit" if failing else "fits", "rows_checked": checked, "rows_not_fitting": failing}


# ------------------------------------------------------------ classification


def _entry(source: str | None, target: str | None, stype: str | None, ttype: str | None,
           transforms: list[str], kind: str, code: str, check: dict[str, Any] | None = None,
           **fmt: Any) -> dict[str, Any]:
    name = source or target or "column"
    template = _REASON[code]
    body = template.format(
        target=target, stype=stype or "?", ttype=ttype or "?", **fmt
    )
    reason = body if code == "row_change" else f"{name} {body}"
    if source and target and source != target and code != "rename":
        reason += f" (renamed to {target})"
    if kind == CONDITIONAL and check:
        if check["status"] == "fits":
            reason += (f"; every one of the {check['rows_checked']} observed value(s) round-trips, "
                       "so it is invertible for the data seen (rows not sampled are not proven)")
        elif check["status"] == "does_not_fit":
            reason += f"; {check['rows_not_fitting']} of {check['rows_checked']} observed value(s) would change"
        else:
            reason += "; no observed value to check it against"
    elif kind == LOSSY and check and check.get("rows_checked"):
        reason += f"; {check['rows_not_fitting']} of {check['rows_checked']} observed row(s) would lose data"
    return {
        "source": source, "target": target, "source_type": stype or None, "target_type": ttype or None,
        "transforms": transforms, "invertibility": kind, "reason_code": code, "reason": reason + ".",
        "check": check,
    }


def classify_mapping(
    mapping: Mapping[str, Any],
    *,
    source_type: str = "",
    target_type: str = "",
    dest_db: str = "",
    source_db: str = "",
    observed: Sequence[Any] | None = None,
) -> dict[str, Any]:
    """The invertibility of one mapped column (see the module docstring)."""
    source = _norm(mapping.get("source")) or None
    target = _norm(mapping.get("target")) or source
    stype = _norm(mapping.get("source_type")) or _norm(source_type)
    ttype = _norm(mapping.get("target_type")) or _norm(target_type)
    transform = _transform_of(mapping)
    transforms = [] if transform in _IDENTITY else [transform]
    values = list(observed or [])
    if _is_omitted(mapping, transform):
        return _entry(source, None, stype, None, transforms, LOSSY, "omit")
    if transform in _LOSSY_TRANSFORMS:
        code, text = _LOSSY_TRANSFORMS[transform]
        out = _entry(source, target, stype, ttype, transforms, LOSSY, "omit")
        out.update(reason_code=code, reason=f"{source or target} {text}.")
        return out
    if transform.startswith("concat:"):
        return _entry(source, target, stype, ttype, transforms, LOSSY, "concat")
    kind, code = type_pair(stype, ttype, dest_db=dest_db, source_db=source_db)
    if kind == LOSSY:
        check = _overflow(values, ttype, logical(ttype)) if code == "narrowing" else None
        return _entry(source, target, stype, ttype, transforms, LOSSY, code, check)
    slog, tlog = logical(stype), logical(ttype)
    if transform and transform not in _IDENTITY:
        from services.mapping_quality import _is_passthrough_transform

        if kind == INVERTIBLE and _is_passthrough_transform(transform, slog, tlog):
            return _entry(source, target, stype, ttype, [], INVERTIBLE, "type_preserving_bind")
        tcode = next((c for c in ("replace", "default", "assume_timezone") if transform.startswith(c)),
                     "normalize")
        check = check_values(values, transform)
        return _entry(source, target, stype, ttype, transforms, CONDITIONAL, tcode, check, transform=transform)
    if kind == CONDITIONAL:
        side = "source" if not slog else "destination"
        cast = _CAST_FOR.get(tlog)
        check = check_values(values, cast) if (code == "cast" and cast) else None
        if code in TYPE_CHECKED:
            check = check_type_values(code, values, ttype, strict_text)
        if check is None:
            check = {"status": "not_checked", "rows_checked": 0, "rows_not_fitting": 0}
        return _entry(source, target, stype, ttype, transforms, CONDITIONAL, code, check, side=side)
    if code == "identity" and source and target and source != target:
        code = "rename"
    return _entry(source, target, stype, ttype, transforms, INVERTIBLE, code)


def _shape_step(op: str, column: str | None, options: Mapping[str, Any]) -> Any:
    from services.shape_models import ShapeStep

    return ShapeStep(op=op, column=column or "", options=dict(options))


def classify_recipe_step(
    step: Mapping[str, Any],
    columns: list[str] | None = None,
    *,
    known: Iterable[str] = (),
) -> dict[str, Any] | None:
    """A shape-recipe step's own row (``None`` for a disabled step).

    ``columns`` is the row's column set as it stands before this step (the
    source columns, then each earlier step's effect); it is advanced in place.
    A step that adds a column (``constant_column``, ``derive_column``, ...) is
    invertible only when the column it writes is new: written onto a column the
    row already holds it overwrites those values. With ``columns=None`` (the
    pre-recipe columns are not known) a written name among ``known`` is not
    assumed new: the step is conditional and unchecked.
    """
    if step.get("enabled") is False:
        return None
    op = _norm(step.get("op")).lower()
    options = step.get("options") if isinstance(step.get("options"), Mapping) else {}
    column = _norm(step.get("column") or options.get("column")) or None
    unchecked = {"status": "not_checked", "rows_checked": 0, "rows_not_fitting": 0}
    shaped = _shape_step(op, column, options)
    if op in _ROW_STEPS:
        entry = _entry(None, None, None, None, [op], LOSSY, "row_change", op=op)
    elif op in _LOSSY_STEPS:
        entry = _entry(column, column, None, None, [op], LOSSY, "recipe_lossy", op=op)
    elif op in _INVERTIBLE_STEPS:
        writes = [str(w) for w in shaped.writes if str(w or "").strip()]
        present = {c.lower() for c in (columns if columns is not None else known)}
        if op == "rename_column" and column:
            present.discard(column.lower())
        hit = next((w for w in writes if w.lower() in present), None)
        target = (writes[0] if writes else None) if op != "rename_column" else column
        if hit is None:
            entry = _entry(target, target, None, None, [op], INVERTIBLE, "recipe_invertible", op=op)
        elif columns is not None:
            entry = _entry(hit, hit, None, None, [op], LOSSY, "recipe_lossy", op=op)
            entry["reason"] = (f"{hit} is overwritten by the recipe step {op}, which writes onto a column the row "
                               "already holds; the original values are not kept.")
        else:
            entry = _entry(hit, hit, None, None, [op], CONDITIONAL, "recipe_conditional", unchecked, op=op)
            entry["reason"] = (f"{hit} is written by the recipe step {op}; whether it overwrites a source column "
                               "could not be decided (the columns before the recipe are not known).")
    else:
        entry = _entry(column, column, None, None, [op], CONDITIONAL, "recipe_conditional", unchecked, op=op)
    if columns is not None:
        from services.shape_models import _apply_column_effect

        _apply_column_effect(shaped, columns)
    return entry


def _effective(entry: dict[str, Any]) -> str:
    """A conditional column that fits the data seen counts as invertible."""
    if entry["invertibility"] == CONDITIONAL and (entry.get("check") or {}).get("status") == "fits":
        return INVERTIBLE
    return entry["invertibility"]


def table_verdict(columns: list[dict[str, Any]], key_columns: Iterable[str] = ()) -> tuple[str, str]:
    """(verdict, reason) for one table from its column rows."""
    keys = {k.lower() for k in key_columns if k}
    effective = [(_effective(c), c) for c in columns]
    lossy = [c for kind, c in effective if kind == LOSSY]
    open_ = [c for kind, c in effective if kind == CONDITIONAL]
    held = [c for c in columns if c.get("target") is not None]
    lossy_key = [c for c in lossy if str(c.get("source") or "").lower() in keys
                 or str(c.get("target") or "").lower() in keys]
    if not lossy and not open_:
        renamed = any(c["reason_code"] == "rename" for c in columns)
        return REVERSIBLE, ("Every mapping is lossless"
                            + (": renamed columns, widened or lossless types" if renamed else "")
                            + ", and no transform drops information.")
    names = "; ".join(c["reason"].rstrip(".") for c in (lossy + open_)[:6])
    more = len(lossy) + len(open_) - 6
    tail = f" (+{more} more)" if more > 0 else ""
    if lossy_key:
        return NOT_REVERSIBLE, (f"The key {lossy_key[0].get('source') or lossy_key[0].get('target')} is lossy, "
                                f"so destination rows cannot be matched back: {names}{tail}.")
    if held and all(_effective(c) == LOSSY for c in held):
        return NOT_REVERSIBLE, f"No column the destination holds can be turned back: {names}{tail}."
    return PARTLY, f"{names}{tail}."


def rollback_note(table: str, verdict: str) -> tuple[str, str]:
    """(rollback kind, note) in the migration_rollback vocabulary; never an undo claim."""
    from services.migration_rollback import DEFAULT_ROLLBACK_STRATEGY

    if verdict == REVERSIBLE:
        return "restore_from_destination", (
            f"{table} can be restored from the destination (every mapping is lossless). Rollback plan: "
            f"{DEFAULT_ROLLBACK_STRATEGY} runbook; Datawrap does not undo committed rows."
        )
    return "keep_source_as_fallback", f"{table} needs the source kept as the fallback until sign-off."


def population_entries(row_limit: int = 0, source_filter: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """A row cap or a source filter writes only part of the source: a lossy ``row_change``."""
    out = []
    if int(row_limit or 0) > 0:
        entry = _entry(None, None, None, None, ["limit"], LOSSY, "row_change", op="limit")
        entry["reason"] = (f"the row cap (limit {int(row_limit)}) writes at most {int(row_limit)} source row(s), "
                           "so the rows left out cannot be rebuilt from the destination.")
        out.append(entry)
    if source_filter:
        entry = _entry(None, None, None, None, ["source_filter"], LOSSY, "row_change", op="source_filter")
        entry["reason"] = ("the source filter writes only the rows it matches, so the rows it leaves out "
                           "cannot be rebuilt from the destination.")
        out.append(entry)
    return out


def analyze_table(
    *,
    table: str,
    mappings: Sequence[Mapping[str, Any]],
    source_columns: Sequence[str] = (),
    source_types: Mapping[str, str] | None = None,
    target_types: Mapping[str, str] | None = None,
    recipe_steps: Sequence[Mapping[str, Any]] = (),
    key_columns: Sequence[str] = (),
    sample_rows: Sequence[Mapping[str, Any]] = (),
    dest_db: str = "",
    source_db: str = "",
    row_limit: int = 0,
    source_filter: Mapping[str, Any] | None = None,
    recipe_input_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """One table's ReversibilityTable (contract 1).

    ``recipe_input_columns`` are the columns the shape recipe reads (before any
    step); ``None`` when they are not known.
    """
    stypes = {str(k): str(v) for k, v in (source_types or {}).items()}
    ttypes = {str(k): str(v) for k, v in (target_types or {}).items()}
    columns: list[dict[str, Any]] = []
    mapped: set[str] = set()
    for mapping in mappings:
        if not isinstance(mapping, Mapping):
            continue
        source = _norm(mapping.get("source"))
        target = _norm(mapping.get("target")) or source
        if source:
            mapped.add(source.lower())
        observed = [row.get(source) for row in sample_rows if isinstance(row, Mapping)] if source else []
        columns.append(classify_mapping(
            mapping, source_type=stypes.get(source, ""), target_type=ttypes.get(target, ""),
            dest_db=dest_db, source_db=source_db, observed=observed,
        ))
    for name in source_columns:
        if _norm(name) and _norm(name).lower() not in mapped:
            columns.append(_entry(_norm(name), None, stypes.get(_norm(name)), None, [], LOSSY, "dropped"))
    inputs = [str(c) for c in recipe_input_columns] if recipe_input_columns is not None else None
    known = [str(c) for c in source_columns] + [str(m.get("source") or "") for m in mappings
                                                 if isinstance(m, Mapping)]
    for step in recipe_steps:
        if isinstance(step, Mapping):
            row = classify_recipe_step(step, inputs, known=known)
            if row is not None:
                columns.append(row)
    columns.extend(population_entries(row_limit, source_filter))
    verdict, reason = table_verdict(columns, key_columns)
    kind, note = rollback_note(table, verdict)
    return {"name": table, "verdict": verdict, "reason": reason, "rollback": kind,
            "rollback_note": note, "columns": columns}


def build_report(
    tables: list[dict[str, Any]],
    *,
    scope: str,
    subject_id: str,
    require: bool,
    data_rows: int,
    data_source: str,
) -> dict[str, Any]:
    """The ReversibilityReport around analysed tables (contract 1)."""
    counts = {INVERTIBLE: 0, CONDITIONAL: 0, LOSSY: 0}
    lossy: list[dict[str, Any]] = []
    for table in tables:
        for col in table["columns"]:
            counts[_effective(col)] += 1
            if col["invertibility"] == LOSSY:
                lossy.append({"table": table["name"], "column": col.get("source") or col.get("target") or "(rows)",
                              "reason_code": col["reason_code"], "reason": col["reason"]})
    verdicts = {t["verdict"] for t in tables}
    if verdicts <= {REVERSIBLE}:
        verdict = REVERSIBLE
    elif verdicts == {NOT_REVERSIBLE}:
        verdict = NOT_REVERSIBLE
    else:
        verdict = PARTLY
    reason = (f"{len(tables)} table(s): {counts[INVERTIBLE]} column(s) invertible, {counts[CONDITIONAL]} "
              f"conditional and not shown to fit, {counts[LOSSY]} lossy.")
    report = {
        "scope": scope, "subject_id": subject_id,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "verdict": verdict, "verdict_reason": reason, "require_reversibility": bool(require),
        "data_checked": {"rows": int(data_rows), "source": data_source if data_rows else "none"},
        "counts": {"invertible": counts[INVERTIBLE], "conditional": counts[CONDITIONAL], "lossy": counts[LOSSY]},
        "lossy_columns": lossy, "tables": tables,
        "rollback_note": " ".join(t["rollback_note"] for t in tables),
    }
    report["gate"] = gate_for(report)
    return report


def gate_for(report: Mapping[str, Any]) -> dict[str, Any]:
    """``g24_reversibility``: skip unless required; block until every table is reversible."""
    if not report.get("require_reversibility"):
        return {"id": GATE_ID, "status": "skip",
                "message": "Reversibility is not required for this transfer; the analysis is shown for review."}
    if report.get("verdict") == REVERSIBLE:
        return {"id": GATE_ID, "status": "pass", "message": "Every table is reversible."}
    open_cols = [
        f"{t['name']}.{c.get('source') or c.get('target') or '(rows)'}"
        for t in report.get("tables") or [] for c in t["columns"] if _effective(c) != INVERTIBLE
    ]
    shown = ", ".join(open_cols[:12]) + (f" (+{len(open_cols) - 12} more)" if len(open_cols) > 12 else "")
    return {
        "id": GATE_ID, "status": "block",
        "message": ("This transfer must be reversible, and it is not: " + shown
                    + ". Remove the lossy transform, widen the destination, or turn off require reversibility."),
    }
