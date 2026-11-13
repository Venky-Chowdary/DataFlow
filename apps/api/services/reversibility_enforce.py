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

import functools
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, TypeVar

#: The mapping key Execute stamps on a conditional column it enforces while writing.
STAMP = "reversibility_enforce"
#: Destination summary key: which columns were enforced and how many rows were checked.
SUMMARY_KEY = "reversibility_enforcement"

_LOCK = threading.Lock()
#: (job id, table, column) -> rows whose batch was checked and accepted. Bounded: a run that never
#: reports (a crashed worker) is evicted by newer ones.
_ROWS: OrderedDict[tuple[str, str, str], int] = OrderedDict()
_MAX_TRACKED = 20_000
#: job id -> what the pre-write scan read (rows, seconds, streams), for the run's destination summary (review 5).
_PRESCANS: OrderedDict[str, dict[str, Any]] = OrderedDict()
#: job id -> the checkpoint a resumed run started from ({rows, stream}), noted where the resume is resolved
#: (``src.transfer.resume_state``); its rows were written by an earlier attempt and never checked by this one.
_RESUMES: OrderedDict[str, dict[str, Any]] = OrderedDict()
#: Destination summary key (the run's, or one stream's entry): rows the stream's tally was seeded with from the
#: checkpoint, i.e. written by an earlier attempt (review 6). The streaming writer stamps it under this name.
RESUMED_KEY = "resumed_checkpoint_rows"
#: Set while rows are mapped a second time for something that is not their write (a pre-write scan, Gate-8's
#: re-read): they are still checked, but not counted as written rows (review 5, H-1).
_UNCOUNTED: ContextVar[bool] = ContextVar("reversibility_uncounted", default=False)
_F = TypeVar("_F", bound=Callable[..., Any])


@contextmanager
def uncounted() -> Iterator[None]:
    """Rows mapped inside are checked as always but not added to the written-rows tally."""
    token = _UNCOUNTED.set(True)
    try:
        yield
    finally:
        _UNCOUNTED.reset(token)


def scan_pass(fn: _F) -> _F:
    """A SQL writer's pre-write scan: it maps every row the write then maps again, so it is not counted."""

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with uncounted():
            return fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


#: ``services.reversibility_types.TYPE_CHECKED`` (kept in step by a test): type conditions checked per value.
_TYPE_CODES = frozenset({"charset", "enum", "blank_pad", "text_width", "byte_length", "boolean_domain",
                         "timestamp_range"})


class ReversibilityViolation(ValueError):
    """A value would not round-trip on a run that must be reversible (the write stops)."""


def _job_id() -> str:
    from services.logging_config import current_job_id

    return current_job_id() or ""


def stamp(mapping: Any, *, table: str, column: str, code: str, transform: str = "", target_type: str = "",
          job_id: str | None = None, dest_db: str = "", transform_names: list[str] | None = None) -> bool:
    """Mark ``mapping`` (the dict the writer will read) as enforced; ``False`` when it cannot be marked.

    ``transform_names`` are the transforms by name only, for messages (never their arguments).
    """
    if not isinstance(mapping, dict):
        return False
    job = _job_id() if job_id is None else job_id
    mapping[STAMP] = {"job_id": job, "table": table, "column": column, "code": code,
                      "transform": transform, "target_type": target_type, "dest_db": dest_db,
                      "transform_names": list(transform_names) if transform_names is not None
                      else [str(transform or "").split(":", 1)[0]]}
    with _LOCK:
        _ROWS.setdefault((job, table, column), 0)
        while len(_ROWS) > _MAX_TRACKED:
            _ROWS.popitem(last=False)
    return True


def rekey(mappings: Sequence[Any] | None, job_id: str) -> None:
    """Move the stamps (and their tally) of ``mappings`` onto ``job_id``.

    A quarantine replay runs g24 before its child job exists, so its stamps
    carry no job id; once the child is created they are keyed to it, so the
    child's run reports its own ``rows_checked`` (review 4).
    """
    for mapping in mappings or []:
        rule = mapping.get(STAMP) if isinstance(mapping, dict) else None
        if not isinstance(rule, dict) or str(rule.get("job_id") or "") == job_id:
            continue
        old = (str(rule.get("job_id") or ""), str(rule.get("table") or ""), str(rule.get("column") or ""))
        with _LOCK:
            rows = _ROWS.pop(old, 0)
            new = (job_id, old[1], old[2])
            _ROWS[new] = _ROWS.get(new, 0) + rows
        rule["job_id"] = job_id


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
    # The transform by name only: its arguments (``default:<literal>``) are configuration that may carry a value.
    listed = rule.get("transform_names")
    if not isinstance(listed, (list, tuple)):
        listed = [str(rule.get("transform") or "").split(":", 1)[0]]
    names = ", ".join(dict.fromkeys(str(n).strip() for n in listed if str(n).strip()))
    how = (f"its transform ({names})" if names
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
    """A write batch of ``rows`` rows passed every enforced column's check.

    Called once per batch the writer maps for its write. The tally is a count
    of the rows that passed, per (table, column): most non-SQL writers never
    pass a running row number, so every batch starts at row 1 and the highest
    row number said nothing past the first batch (review 5, H-2). The
    pre-write scan of a SQL writer and the verification's re-read map the same
    rows again and are not a write: they run :func:`uncounted`. ``first_row``
    is kept for the callers' signature.
    """
    del first_row
    if rows <= 0 or _UNCOUNTED.get():
        return
    for mapping in mappings or []:
        rule = mapping.get(STAMP) if isinstance(mapping, dict) else None
        if isinstance(rule, Mapping):
            key = (str(rule.get("job_id") or ""), str(rule.get("table") or ""), str(rule.get("column") or ""))
            with _LOCK:
                _ROWS[key] = _ROWS.get(key, 0) + int(rows)


#: job id -> {(table, column): rows} an earlier pass of the job checked for a table this pass skipped
#: because that pass finished it (C5): the proof is carried, never counted as this pass's writing.
_CARRIED: OrderedDict[str, dict[tuple[str, str], int]] = OrderedDict()


def checked_for_table(job_id: str, table: str) -> dict[str, int]:
    """Rows checked so far in this job for each enforced column of ``table`` (case-insensitive)."""
    want = str(table or "").strip().lower()
    with _LOCK:
        return {k[2]: int(v) for k, v in _ROWS.items()
                if k[0] == job_id and str(k[1]).strip().lower() == want}


def carry_checked(job_id: str, table: str, checked: Mapping[str, Any] | None) -> None:
    """Carry an earlier pass's per-column check counts for a table this pass did not write."""
    if not job_id or not isinstance(checked, Mapping) or not checked:
        return
    with _LOCK:
        carried = _CARRIED.setdefault(job_id, {})
        for column, rows in checked.items():
            try:
                carried[(str(table), str(column))] = int(rows or 0)
            except (TypeError, ValueError):
                continue
        while len(_CARRIED) > _MAX_TRACKED:
            _CARRIED.popitem(last=False)


def note_prescan(job_id: str, rows: int, seconds: float) -> None:
    """The pre-write scan read ``rows`` source rows in ``seconds`` for ``job_id`` (summed over its streams)."""
    with _LOCK:
        found = _PRESCANS.setdefault(job_id, {"rows_scanned": 0, "seconds": 0.0, "scans": 0})
        found["rows_scanned"] += int(rows)
        found["seconds"] = round(found["seconds"] + float(seconds), 3)
        found["scans"] += 1
        while len(_PRESCANS) > _MAX_TRACKED:
            _PRESCANS.popitem(last=False)


def note_resumed(job_id: str, *, rows: Any, stream: Any = "") -> None:
    """``job_id`` resumed from a checkpoint of ``rows`` committed rows that names ``stream`` (``""``: none)."""
    try:
        count = max(0, int(rows or 0))
    except (TypeError, ValueError):
        count = 0
    with _LOCK:
        _RESUMES[job_id] = {"rows": count, "stream": str(stream or "").strip()}
        while len(_RESUMES) > _MAX_TRACKED:
            _RESUMES.popitem(last=False)


def note_resumed_reapplied(job_id: str) -> None:
    """``job_id``'s resume re-applies its whole population: no written count starts at the checkpoint."""
    with _LOCK:
        found = _RESUMES.get(job_id)
        if found is not None:
            found["reapplied"] = True


def summary(job_id: str, *, forget: bool = True) -> dict[str, Any] | None:
    """{enforced, columns: [{table, column, rows_checked}]} for ``job_id``'s run, or ``None`` when none was."""
    with _LOCK:
        keys = [k for k in _ROWS if k[0] == job_id]
        rows = [{"table": k[1], "column": k[2], "rows_checked": _ROWS[k]} for k in keys]
        scan = dict(_PRESCANS.get(job_id) or {})
        resumed = dict(_RESUMES.get(job_id) or {})
        carried = dict(_CARRIED.get(job_id) or {})
        if forget:
            for k in keys:
                _ROWS.pop(k, None)
            _PRESCANS.pop(job_id, None)
            _RESUMES.pop(job_id, None)
            _CARRIED.pop(job_id, None)
    for row in rows:
        folded = (str(row["table"]).strip().lower(), str(row["column"]))
        hit = next((n for (t, c), n in carried.items() if (t.strip().lower(), c) == folded), None)
        if hit is not None:
            # Checked by the earlier pass that wrote the table; this pass wrote none of it.
            row["carried_from_earlier_pass"] = hit
    if not rows:
        return None
    out = {"enforced": True, "columns": sorted(rows, key=lambda r: (r["table"], r["column"]))}
    if scan:
        out["prescan"] = scan  # rows the pre-write scan read and how long it took (review 5)
    if resumed:
        out["resumed"] = resumed  # the checkpoint this run resumed from (review 6)
    return out


def _named(column: Mapping[str, Any]) -> str:
    return f"{column['table']}.{column['column']}" if column.get("table") else str(column.get("column") or "")


def _written_by_table(tables: set[str], rows_written: Any, streams: Any) -> dict[str, int | None]:
    """Each enforced table's own written count (``None``: the run recorded none for it).

    A multi-stream run records each stream's ``records_processed``; a run of one
    table wrote ``rows_written``. The job total is never compared with one
    table of several (review 5, H-1).
    """
    entries = streams.values() if isinstance(streams, Mapping) else streams if isinstance(streams, list) else []
    per: dict[str, int] = {}
    for entry in entries:
        if isinstance(entry, Mapping) and str(entry.get("name") or "").strip():
            try:
                per[str(entry["name"]).strip().lower()] = int(entry.get("records_processed") or 0)
            except (TypeError, ValueError):
                continue
    try:
        total: int | None = int(rows_written or 0)
    except (TypeError, ValueError):
        total = None
    one_table = len(tables) == 1 and len(per) <= 1
    return {t: per.get(t.strip().lower(), total if one_table else None) if per else (total if len(tables) == 1
                                                                                       else None)
            for t in tables}


def _resumed_by_table(tables: set[str], dest: Any, streams: Any, resumed: Any) -> dict[str, int | None]:
    """Rows each table's written count holds from the checkpoint it resumed from (``None``: not recorded).

    Only tables a resume seeded are listed. The streaming writer stamps
    :data:`RESUMED_KEY` on its summary (a multi-stream run on each stream's
    entry); a run noted as resumed (:func:`note_resumed`) without that count
    for the table the checkpoint applies to is ``None`` (review 6).
    """
    entries = streams.values() if isinstance(streams, Mapping) else streams if isinstance(streams, list) else []
    per = {str(e["name"]).strip().lower(): e for e in entries
           if isinstance(e, Mapping) and str(e.get("name") or "").strip()}
    top = dest.get(RESUMED_KEY) if isinstance(dest, Mapping) else None
    one_table = len(tables) == 1 and len(per) <= 1
    resume = resumed if isinstance(resumed, Mapping) else {}
    owner = str(resume.get("stream") or "").strip().lower()
    out: dict[str, int | None] = {}
    for table in tables:
        entry = per.get(table.strip().lower()) or {}
        value = entry.get(RESUMED_KEY)
        if value is None and one_table:
            value = top
        if value is not None:
            try:
                out[table] = int(value)
            except (TypeError, ValueError):
                out[table] = None
            continue
        # A multi-stream run applies the checkpoint only to the stream it names; the others read from their start.
        # The buffered route re-applies every row it holds (``reapplied``): its count starts at 0.
        if (int(resume.get("rows") or 0) > 0 and not resume.get("reapplied")
                and (one_table or (owner and owner == table.strip().lower()))):
            out[table] = None
    return {t: n for t, n in out.items() if n is None or n > 0}


def assess(found: Mapping[str, Any], rows_written: Any, streams: Any = None,
           dest: Mapping[str, Any] | None = None) -> tuple[dict[str, Any], str | None]:
    """(the tally with each column's ``rows_written``, why the run cannot be called enforced or ``None``).

    An enforced column checked on fewer rows than its own table wrote fails the
    run (the fail-closed backstop behind every bulk path's decline, review 4,
    H-1). Where no per-table written count exists the comparison is not made:
    the tally says so under ``unverified`` instead of failing an honest run.
    A resumed run's written count holds the rows an earlier attempt committed:
    they are subtracted (``resumed_from``), and where that count is not known
    the column is ``unverified`` too (review 6).
    """
    columns = [dict(c) for c in found.get("columns") or [] if isinstance(c, Mapping)]
    tables = {str(c.get("table") or "") for c in columns}
    written = _written_by_table(tables, rows_written, streams)
    resumed = _resumed_by_table(tables, dest, streams, found.get("resumed"))
    for column in columns:
        table = str(column.get("table") or "")
        column["rows_written"] = written.get(table)
        if table in resumed:
            column["resumed_from"] = resumed[table]
            if resumed[table] is not None and column["rows_written"] is not None:
                column["rows_written"] = max(0, column["rows_written"] - resumed[table])
    out: dict[str, Any] = {**found, "columns": columns}
    unverified = [_named(c) for c in columns if c["rows_written"] is None]
    unknown = [_named(c) for c in columns if c["rows_written"] is not None and "resumed_from" in c
               and c["resumed_from"] is None]
    notes = []
    if unverified:
        notes.append(f"{', '.join(unverified[:6])}: no per-table written count was recorded for the run, "
                     "so the rows checked were not compared with the rows written.")
    if unknown:
        notes.append(f"{', '.join(unknown[:6])}: the run resumed from a checkpoint whose row count was not "
                     "recorded for the table, so the rows checked were not compared with the rows written.")
    if notes:
        out["unverified"] = " ".join(notes)
    short = [c for c in columns if c["rows_written"] is not None and c.get("resumed_from", 0) is not None
             and int(c.get("rows_checked") or 0) < c["rows_written"]]
    if not short:
        return out, None
    named = "; ".join(f"{_named(c)} was checked on only {int(c.get('rows_checked') or 0):,} of "
                      f"{c['rows_written']:,} written row(s)" for c in short[:6])
    note = (f"Reversibility is required and {named}: a bulk path wrote the rest without the per-value check, so "
            "the run is failed rather than reported as enforced. Run it again; if it repeats, report it.")
    out.update(enforced=False, reason=note)
    return out, note


def record_on_success(job_id: str, fields: dict[str, Any]) -> str | None:
    """Keep the run's enforcement tally on its destination summary (read by the job's reversibility report).

    Returns why the run must fail instead when an enforced column was checked
    on fewer rows than its table wrote (:func:`assess`): the tally is then
    recorded with ``enforced: false`` and that reason, never as enforced.
    """
    found = summary(job_id)
    if found is None:
        return None
    dest = fields.get("destination_summary")
    streams = dest.get("streams") if isinstance(dest, dict) else None
    found, note = assess(found, fields.get("records_processed"), streams, dest if isinstance(dest, dict) else None)
    if isinstance(dest, dict):
        dest[SUMMARY_KEY] = found
    else:
        fields["destination_summary"] = {SUMMARY_KEY: found}
    return note
