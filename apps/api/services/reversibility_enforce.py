"""Writer-side enforcement of conditional columns when a run must be reversible (review 3, M3).

The reversibility gate (:mod:`services.reversibility`) reads a sample: a
conditional column (a normalising transform, a cast out of text, a
value-dependent type fit) may fit every sampled row and still change a row the
sample never saw - one inserted after the gate ran, one past the first page, a
CDC event next week. So when ``require_reversibility`` is on, Execute stamps
each such mapping (:func:`stamp`) and the transform stage every writer shares
(``connectors.writer_common._convert_mapped_cell``) calls
:func:`refuse_changed_value` on each value it converts: the first value whose
exact text the conversion would change (or that the destination type would
not keep) raises :class:`ReversibilityViolation`. The batch that holds it is
not written and the run fails through the engine's own failure path (what
earlier batches wrote is kept or undone exactly as for any writer failure).

The error names the column, its rule and how many rows had been checked; never
the value. With the flag off no mapping carries a stamp and none of this runs
(one dictionary membership test per converted value is the whole footprint).
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Mapping, Sequence
from typing import Any

#: The mapping key Execute stamps on a conditional column it enforces while writing.
STAMP = "reversibility_enforce"
#: Destination summary key: which columns were enforced and how many rows were checked.
SUMMARY_KEY = "reversibility_enforcement"

_LOCK = threading.Lock()
#: (job id, table, column) -> rows whose batch was checked and accepted. Bounded: a run that never
#: reports (a crashed worker) is evicted by newer ones.
_ROWS: OrderedDict[tuple[str, str, str], int] = OrderedDict()
_MAX_TRACKED = 20_000


#: ``services.reversibility_types.TYPE_CHECKED`` (kept in step by a test): type conditions checked per value.
_TYPE_CODES = frozenset({"charset", "enum", "blank_pad", "text_width", "byte_length", "boolean_domain",
                         "timestamp_range"})


class ReversibilityViolation(ValueError):
    """A value would not round-trip on a run that must be reversible (the write stops)."""


def _job_id() -> str:
    from services.logging_config import current_job_id

    return current_job_id() or ""


def stamp(mapping: Any, *, table: str, column: str, code: str, transform: str = "", target_type: str = "",
          job_id: str | None = None, dest_db: str = "") -> bool:
    """Mark ``mapping`` (the dict the writer will read) as enforced; ``False`` when it cannot be marked."""
    if not isinstance(mapping, dict):
        return False
    job = _job_id() if job_id is None else job_id
    mapping[STAMP] = {"job_id": job, "table": table, "column": column, "code": code,
                      "transform": transform, "target_type": target_type, "dest_db": dest_db}
    with _LOCK:
        _ROWS.setdefault((job, table, column), 0)
        while len(_ROWS) > _MAX_TRACKED:
            _ROWS.popitem(last=False)
    return True


def enforced_columns(mappings: Sequence[Any] | None) -> list[str]:
    """Source columns of the mappings that carry the stamp (a server-side copy cannot check them)."""
    return [str(m[STAMP].get("column") or m.get("source") or m.get("target") or "")
            for m in mappings or [] if isinstance(m, dict) and isinstance(m.get(STAMP), dict)]


def _is_null(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and "_" in value:
        from services.transform_engine import _engine_cell_sentinel

        return _engine_cell_sentinel(value) == "null"
    return False


def changes(rule: Mapping[str, Any], raw: Any, converted: Any, error: str | None = None) -> bool:
    """Whether writing ``converted`` for the source value ``raw`` changes it (the gate's own round-trip rule)."""
    if error:
        return True  # a value the writer cannot convert does not land: that is a change
    if _is_null(raw):
        return not _is_null(converted)  # NULL filled in (``default:``)
    if isinstance(raw, str) and rule.get("code") in {"cast", "normalize"}:
        # A cast from text (or a transform that only casts into the target's type): what the destination
        # renders, not the Python conversion, decides; the gate's sample check asks the same rule (review 4,
        # M-2).
        from services.reversibility_render import cast_changes, renders_rule

        target = str(rule.get("target_type") or "")
        if renders_rule(str(rule.get("transform") or ""), target):
            verdict = cast_changes(raw, None if _is_null(converted) else converted, target,
                                   str(rule.get("dest_db") or ""))
            if verdict is not None:
                return verdict
    if type(raw) is str and type(converted) is str:
        if raw != converted:  # the common case, without the generic text rendering
            return True
    else:
        from services.reversibility import strict_text

        if _is_null(converted) or strict_text(converted) != strict_text(raw):
            return True
    code = rule.get("code")
    if code in _TYPE_CODES:
        from services.reversibility import strict_text
        from services.reversibility_types import check_type_values

        return check_type_values(str(code), [raw], str(rule.get("target_type") or ""),
                                 strict_text)["status"] != "fits"
    return False


def describe_rule(rule: Mapping[str, Any]) -> tuple[str, str]:
    """(``table.column``, how it changes) of a stamped rule, for a message that never repeats a value."""
    how = (f"its transform ({rule.get('transform')})" if rule.get("transform")
           else f"the cast into {rule.get('target_type') or 'the destination type'}")
    table, column = str(rule.get("table") or ""), str(rule.get("column") or "")
    return (f"{table}.{column}" if table else column), how


#: The phrase a violation carries; replaced when earlier batches were already committed (review 4, M-1).
NOTHING_KEPT = "reversibility blocks partial write"
ROWS_KEPT = "rows earlier batches committed were kept (this is a partial write)"


def refuse_changed_value(mapping: Mapping[str, Any], raw: Any, converted: Any, error: str | None = None) -> None:
    """Raise :class:`ReversibilityViolation` when the stamped ``mapping`` would change ``raw``."""
    rule = mapping.get(STAMP)
    if not isinstance(rule, Mapping) or not changes(rule, raw, converted, error):
        return
    key = (str(rule.get("job_id") or ""), str(rule.get("table") or ""), str(rule.get("column") or ""))
    with _LOCK:
        checked = _ROWS.get(key, 0)
    where, how = describe_rule(rule)
    violation = ReversibilityViolation(
        f"Reversibility is required and {where} would change under {how}: a value in the batch after the first "
        f"{checked:,} checked row(s) does not round-trip ({rule.get('code')}). That batch was not written; "
        f"{NOTHING_KEPT}. Fix the value at the source, remove the transform, or turn off require reversibility."
    )
    violation.rule = dict(rule)
    raise violation


def note_rows_checked(mappings: Sequence[Any] | None, first_row: int, rows: int) -> None:
    """A batch (``rows`` rows from row number ``first_row``) passed every enforced column's check.

    Called once per mapped batch. The same rows pass the transform stage more
    than once (the write, then the verification's re-read), so the tally is the
    highest row number that has passed, not a count of passes.
    """
    if rows <= 0:
        return
    reached = max(1, int(first_row or 1)) - 1 + int(rows)
    for mapping in mappings or []:
        rule = mapping.get(STAMP) if isinstance(mapping, dict) else None
        if isinstance(rule, Mapping):
            key = (str(rule.get("job_id") or ""), str(rule.get("table") or ""), str(rule.get("column") or ""))
            with _LOCK:
                _ROWS[key] = max(_ROWS.get(key, 0), reached)


def summary(job_id: str, *, forget: bool = True) -> dict[str, Any] | None:
    """{enforced, columns: [{table, column, rows_checked}]} for ``job_id``'s run, or ``None`` when none was."""
    with _LOCK:
        keys = [k for k in _ROWS if k[0] == job_id]
        rows = [{"table": k[1], "column": k[2], "rows_checked": _ROWS[k]} for k in keys]
        if forget:
            for k in keys:
                _ROWS.pop(k, None)
    if not rows:
        return None
    return {"enforced": True, "columns": sorted(rows, key=lambda r: (r["table"], r["column"]))}


def _shortfall_note(columns: Sequence[Mapping[str, Any]], rows_written: int) -> str | None:
    short = [c for c in columns if int(c.get("rows_checked") or 0) < rows_written]
    if not short:
        return None
    named = "; ".join(f"{c['table']}.{c['column']}" if c.get("table") else str(c["column"]) for c in short[:6])
    least = min(int(c.get("rows_checked") or 0) for c in short)
    return (f"Reversibility is required and {named} was checked on only {least:,} of {rows_written:,} written "
            "row(s): a bulk path wrote the rest without the per-value check, so the run is failed rather than "
            "reported as enforced. Run it again; if it repeats, report it.")


def record_on_success(job_id: str, fields: dict[str, Any]) -> str | None:
    """Keep the run's enforcement tally on its destination summary (read by the job's reversibility report).

    Returns why the run must fail instead when an enforced column was checked
    on fewer rows than it wrote: the tally is then recorded with
    ``enforced: false`` and that reason, never as enforced.
    """
    found = summary(job_id)
    if found is None:
        return None
    note = shortfall_of(found, fields.get("records_processed"))
    if note:
        found = {**found, "enforced": False, "reason": note}
    dest = fields.get("destination_summary")
    if isinstance(dest, dict):
        dest[SUMMARY_KEY] = found
    else:
        fields["destination_summary"] = {SUMMARY_KEY: found}
    return note


def shortfall_of(found: Mapping[str, Any], rows_written: Any) -> str | None:
    """Why a run's tally (:func:`summary`) cannot be called enforced: an enforced column checked on fewer
    rows than were written (the fail-closed backstop behind every bulk path's decline, review 4, H-1)."""
    try:
        written = int(rows_written or 0)
    except (TypeError, ValueError):
        return None
    return _shortfall_note(list(found.get("columns") or []), written) if written > 0 else None
