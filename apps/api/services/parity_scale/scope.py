"""What the run was responsible for, and whether an in-database scan can compare exactly that.

The scan compares the source table with the destination table. That is the
run's scope only when the run read the whole table and wrote what it read.
A run that read a window (incremental, CDC), a query or procedure, a row
limit, or rows its recipe or source filter dropped or diverted is responsible
for less than the table; the scan cannot apply the same selection in the
database yet, so it does not run (Gate-8's own record decides, with the
reason said plainly).

Inside an eligible run the rows the run held back by design are merged, not
replaced: quarantined rows (named by key), rows its duplicate policy skipped,
and a destination's rows from before an append are Gate-8's explained
observations, and they stay explained (:func:`merge_explained`). A table
Gate-8 explained can therefore never become "proven", nor "unexplained", by
the scan alone. Rows held out by a decimal-fit contract are excluded inside
the scan itself (its held-out predicate).
"""

from __future__ import annotations

from typing import Any

_QUERY_KEYS = ("source_query", "query", "procedure_call", "procedure", "sql")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _recipe_changes_rows(recipe: dict[str, Any]) -> bool:
    from services.shape_models import STEP_CATALOG

    for step in recipe.get("steps") or []:
        if not isinstance(step, dict) or step.get("enabled") is False:
            continue
        definition = STEP_CATALOG.get(str(step.get("op") or ""))
        if (definition is not None and definition.active) or str(step.get("on_error") or "") == "divert":
            return True
    return False


def scope_refusal(job: dict[str, Any]) -> str:
    """``""`` when the scan compares exactly what the run wrote, else why not (plain words)."""
    from services.parity_evidence import request_of, sync_mode_of
    from services.sync_cursor import INCREMENTAL_MODES

    req = request_of(job)
    source = _dict(req.get("source"))
    extra = _dict(source.get("extra"))
    dest = _dict(job.get("destination_summary"))
    if sync_mode_of(job) in INCREMENTAL_MODES or req.get("cursor_column") or extra.get("cursor_column"):
        return ("the run read a window of the source (incremental); comparing the whole table would not "
                "match what this run wrote")
    if _int(req.get("limit")) > 0 or _int(req.get("row_limit")) > 0:
        return "the run read a limited number of rows; comparing the whole table would not match it"
    if any(str(source.get(k) or extra.get(k) or "").strip() for k in _QUERY_KEYS) or str(
            extra.get("source_read_mode") or source.get("source_read_mode") or "").lower() in {"query", "procedure"}:
        return "the run read the source through a query or procedure, not the table"
    if _dict(req.get("source_filter")):
        return "the run filtered source rows; the comparison cannot apply the same filter in the database yet"
    shaped = _int(dest.get("rows_shape_filtered")) + _int(dest.get("rows_shape_diverted"))
    if shaped > 0 or _recipe_changes_rows(_dict(req.get("shape_recipe"))):
        return ("the run's recipe filtered or diverted rows; the comparison cannot apply the same recipe in "
                "the database yet")
    return ""


def _explained(obs: Any) -> bool:
    return bool(obs.cause) and obs.cause.get("explained") is True


#: Gate-8 observations of rows the run held back by design: never trimmed by a scan.
HELD_ORIGINS = frozenset({"quarantine", "writer_policy", "recipe_effect"})


def merge_explained(prior: list[Any], scan: list[Any], *, fit_excluded: int = 0, table: str = "") -> list[Any]:
    """The scan's observations with Gate-8's explained ones kept, counted exactly.

    * Rows the run held back (quarantined, skipped by policy, held by the
      write contract) are kept in full, whatever the scan found: they are why
      the run is at most "explained", never "proven". The scan does not see a
      row the write contract held out (it is excluded on the source side), so
      only the held rows it can see are taken off its missing count; the rest
      of its missing rows stay its own (unexplained unless it named a cause).
    * Extra rows: Gate-8's explained rows (a destination's rows from before an
      append) account for the scan's extra rows first, trimmed to what the
      scan counted.
    * Values: Gate-8's explained value observations stay for a column the
      scan reported no difference in.
    """
    from services.parity_classifier import QUARANTINED, cause
    from services.parity_evidence import Observation

    out: list[Any] = []
    # Missing: held rows first, in full.
    held = [o for o in prior if o.kind == "missing_in_destination" and _explained(o) and o.origin in HELD_ORIGINS]
    found = [o for o in scan if o.kind == "missing_in_destination"]
    named = [o for o in found if _explained(o)]
    held_rows = sum(int(o.rows) for o in held)
    if held:
        named_keys = [k for o in named for k in o.keys_sample]
        for o in held:
            if o.origin == "quarantine" and not o.keys_sample and named_keys:
                o.keys_sample = list(named_keys)
        out.extend(held)
    else:
        out.extend(named)  # quarantined keys the scan named, when Gate-8 recorded no count
        held_rows = sum(int(o.rows) for o in named)
    if fit_excluded > held_rows:
        out.append(Observation(
            table=table or (found[0].table if found else ""), kind="missing_in_destination",
            rows=fit_excluded - held_rows, origin="quarantine",
            cause=cause(QUARANTINED, "Rows the write contract held out (they do not fit the destination column).",
                        explained=True),
        ))
        held_rows = fit_excluded
    visible_held = max(0, held_rows - int(fit_excluded))
    left = max(0, sum(int(o.rows) for o in found) - visible_held)
    for o in found:
        if _explained(o) or left <= 0:
            continue
        o.rows = min(int(o.rows), left)
        left -= o.rows
        out.append(o)
    # Extra: Gate-8's explanation (rows from before an append) covers what the scan counted.
    found = [o for o in scan if o.kind == "extra_in_destination"]
    out.extend(o for o in found if _explained(o))
    budget = sum(int(o.rows) for o in found if not _explained(o))
    for o in prior:
        if budget <= 0:
            break
        if o.kind == "extra_in_destination" and _explained(o):
            o.rows = min(int(o.rows), budget)
            budget -= o.rows
            out.append(o)
    left = budget  # the scan's extra rows no explanation covers
    for o in found:
        if _explained(o) or left <= 0:
            continue
        o.rows = min(int(o.rows), left)
        left -= o.rows
        out.append(o)
    scanned_columns = {o.column for o in scan if o.kind == "value" and o.column}
    out.extend(o for o in scan if o.kind not in {"missing_in_destination", "extra_in_destination"})
    out.extend(o for o in prior if o.kind == "value" and _explained(o) and o.column not in scanned_columns)
    return out
