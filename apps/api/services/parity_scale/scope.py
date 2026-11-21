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


def merge_explained(prior: list[Any], scan: list[Any]) -> list[Any]:
    """The scan's observations with Gate-8's explained ones kept (never double counted).

    * Missing / extra rows: Gate-8's explained rows of that kind (quarantine,
      skipped by policy, rows from before an append) account for the scan's
      rows of that kind first; only the remainder stays the scan's own (and
      unexplained, unless the scan named a cause). Gate-8's explained rows
      beyond what the scan found are trimmed: the scan counted every row.
    * Values: Gate-8's explained value observations stay for a column the
      scan reported no difference in (the scan compares through the run's
      transforms, so what Gate-8 explained as a transform is not re-found).
    """
    out: list[Any] = []
    for kind in ("missing_in_destination", "extra_in_destination"):
        found = [o for o in scan if o.kind == kind]
        total = sum(int(o.rows) for o in found)
        named = [o for o in found if _explained(o)]
        explained_by_scan = sum(int(o.rows) for o in named)
        capacity = sum(int(o.rows) for o in prior if o.kind == kind and _explained(o))
        # The scan's own explained rows (quarantined keys) are part of Gate-8's quarantine count.
        quarantine_prior = sum(int(o.rows) for o in prior if o.kind == kind and _explained(o)
                               and o.origin == "quarantine")
        capacity = capacity - min(quarantine_prior, explained_by_scan)
        out.extend(named)
        remaining = total - explained_by_scan
        covered = max(0, min(capacity, remaining))
        budget = covered
        for o in prior:
            if budget <= 0:
                break
            if o.kind != kind or not _explained(o):
                continue
            if o.origin == "quarantine" and explained_by_scan:
                continue  # already named by key above
            take = min(int(o.rows), budget)
            o.rows = take
            out.append(o)
            budget -= take
        left = remaining - covered
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
