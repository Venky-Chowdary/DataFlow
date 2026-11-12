"""Periodic full key census for incremental routes (audit C1, roadmap R1b).

An incremental run proves the rows inside its cursor window. A row that sits
*behind* the watermark and never reached the destination — committed late on a
route with no commit-order bound, committed outside the re-read window, written
with a cursor value the source ordered below the watermark — is outside every
later window, so no later run can notice it.

The census is the check that can: every source key at or behind the watermark
is joined to the destination's keys, using the same cross-engine key census
Gate-8 uses to name culprit keys (``reconcile_step._generic_key_census``). A
source key with no destination row is reported as drift and the run is not
green.

It reads both tables, so it is off by default. A stream turns it on with
``key_census_every: N`` (every Nth run of the route; ``1`` = every run) — a
schedule's contract carries it like any other declaration, and a one-off job
with ``key_census_every: 1`` triggers it on demand. The plan recommends it
whenever the route cannot be called commit-safe.
"""

from __future__ import annotations

from typing import Any, Callable

CENSUS_KEY = "incremental_key_census"
RUNS_SINCE_KEY = "runs_since_key_census"
#: Source keys this route quarantined, kept with its watermark (review M6):
#: missing at the destination by design, so the census does not report them.
ROUTE_QUARANTINE_KEY = "quarantined_keys"
ROUTE_QUARANTINE_COMPLETE_KEY = "quarantined_keys_complete"
ROUTE_QUARANTINE_MAX = 10_000
_SAMPLE = 5


def _census_key_text(parts: Any) -> str:
    """One key as the census spells it: the value, or the parts joined by U+001F."""
    values = [str(p) for p in (parts if isinstance(parts, (list, tuple)) else [parts])]
    return values[0] if len(values) == 1 else "\x1f".join(values)


def route_quarantined_keys(
    previous: Any, dest_summary: Any, key_cols: list[str]
) -> tuple[list[list[str]], bool]:
    """``(keys, complete)``: the route's quarantined keys with this run's added.

    Bounded at :data:`ROUTE_QUARANTINE_MAX`; past it the record is marked
    incomplete and the census says keys beyond it may be reported.
    """
    from src.transfer.reconcile_step import _rejected_key

    keys: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    complete = True

    def add(parts: Any) -> None:
        nonlocal complete
        key = tuple(str(p) for p in (parts if isinstance(parts, (list, tuple)) else [parts]))
        if not key or key in seen:
            return
        if len(keys) >= ROUTE_QUARANTINE_MAX:
            complete = False
            return
        seen.add(key)
        keys.append(list(key))

    for parts in previous or []:
        add(parts)
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    for detail in summary.get("rejected_details") or []:
        found = _rejected_key(detail, key_cols) if isinstance(detail, dict) and key_cols else None
        if found:
            add(found)
    return keys, complete


def census_schedule(every: int, runs_since: Any) -> tuple[bool, int]:
    """``(due this run, counter to store)`` for a route censused every N runs."""
    try:
        every = int(every or 0)
    except (TypeError, ValueError):
        every = 0
    if every <= 0:
        return False, 0
    try:
        seen = int(runs_since or 0)
    except (TypeError, ValueError):
        seen = 0
    due = seen + 1 >= every
    return due, (0 if due else seen + 1)


def request_census(dest_summary: dict[str, Any], *, cursor_column: str, every: int) -> None:
    """Mark this run's summary: Gate-8 must run the key census."""
    if isinstance(dest_summary, dict) and cursor_column:
        dest_summary[CENSUS_KEY] = {
            "due": True,
            "every": int(every),
            "cursor_column": cursor_column,
        }


def _behind(watermark: str) -> Callable[[Any], bool]:
    from services.commit_safe_window import cursor_part
    from services.sync_cursor import compare_cursor_values
    from services.value_serializer import cell_to_string

    top = cursor_part(watermark)

    def keep(cell: Any) -> bool:
        if cell is None:
            return False
        text = cell_to_string(cell)
        if text is None or str(text) == "":
            return False
        return compare_cursor_values(str(text), top) <= 0

    return keep


def census_sentence(census: dict[str, Any]) -> str:
    n = int(census.get("missing_behind_watermark") or 0)
    if n <= 0:
        return ""
    keys = ", ".join(str(k) for k in census.get("missing_keys") or [])
    more = "at least " if census.get("capped") else ""
    return (
        f"Key census: {more}{n:,} source row(s) at or behind the watermark are missing "
        "from the destination"
        + (f" (e.g. key {keys})" if keys else "")
        + ". The incremental read selects rows past the watermark, so no later run "
        "delivers them. Re-read them: clear the route cursor and re-run on a keyed "
        "(upsert) route, or run a full refresh."
    )


def _with_window_evidence(recon: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    """Carry the run's commit-safe window onto the Gate-8 report.

    Rows the source reports behind the watermark that the route could not
    re-read (an append with no key) are a warning on the report, not a line in
    a log: the run did not deliver them and cannot say they were delivered.
    """
    window = summary.get("commit_safe_window")
    if not isinstance(window, dict) or not window:
        return recon
    out = dict(recon)
    out["commit_safe_window"] = dict(window)
    late = int(window.get("late_rows_detected") or 0)
    failed = str(window.get("probe_error") or "")
    if failed:
        # A probe the window depends on did not complete: the run read without
        # the bound it planned, and the report says so (never a log line only).
        limit = (
            "rows committed behind the watermark are not guaranteed this run"
            if window.get("guarantee") == "not_guaranteed"
            else "rows committed behind the watermark are covered only within the "
            "re-read window this run"
        )
        out["warnings"] = list(out.get("warnings") or []) + [
            f"Commit-safe window: a source probe failed ({failed}); {limit}. "
            + str(window.get("reason") or "")
        ]
    if late and window.get("guarantee") == "not_guaranteed":
        note = (
            f"Up to {late:,} source row(s) were committed behind the watermark since the "
            "last read and were not re-read: " + str(window.get("reason") or "")
        ).strip()
        out["warnings"] = list(out.get("warnings") or []) + [note]
    return out


def apply_incremental_census(
    recon: dict[str, Any],
    *,
    source_endpoint: Any,
    endpoint: Any,
    dest_summary: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None,
    key_census: Callable[..., tuple[list[Any], list[Any], list[Any], str | None]],
) -> dict[str, Any]:
    """Run the census when this run is due one; never green on a missing key."""
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    recon = _with_window_evidence(recon, summary)
    request = summary.get(CENSUS_KEY)
    if not isinstance(request, dict) or not request.get("due") or source_endpoint is None:
        return recon
    from services.commit_safe_window import cursor_part
    from services.incremental_drift import DRIFT_KEY
    from services.sync_cursor import get_watermark_record

    cursor_column = str(request.get("cursor_column") or "")
    cursor_key = str(summary.get("incremental_cursor_key") or summary.get("cursor_key") or "")
    watermark, metadata = get_watermark_record(cursor_key) if cursor_key else (None, {})
    metadata = dict(metadata or {})
    out = dict(recon)
    census: dict[str, Any] = {"ran": False, "every": request.get("every"), "watermark": watermark}
    if watermark in (None, ""):
        census["reason"] = "the route has no watermark yet — nothing is behind it"
        out[CENSUS_KEY] = census
        return out
    changed, missing, extra, reason = key_census(
        source_endpoint=source_endpoint,
        endpoint=endpoint,
        dest_summary=summary,
        mappings=mappings,
        source_cursor_bound=(cursor_column, _behind(str(watermark)), cursor_part(watermark)),
    )
    if reason is not None:
        # An unrun census is said, not passed off as a clean one.
        census["reason"] = "the key census could not run — " + reason
        out[CENSUS_KEY] = census
        warnings = list(out.get("warnings") or [])
        warnings.append("Key census requested for this run did not run: " + reason)
        out["warnings"] = warnings
        return out
    # Keys the route quarantined are missing at the destination by design.
    key_cols = [str(c) for c in summary.get("primary_key_columns") or summary.get("conflict_columns") or []]
    quarantined, complete = route_quarantined_keys(
        metadata.get(ROUTE_QUARANTINE_KEY), summary, key_cols
    )
    held_out = {_census_key_text(k) for k in quarantined}
    before = len(missing)
    missing = [k for k in missing if str(k) not in held_out]
    census["quarantined_excluded"] = before - len(missing)
    if not (complete and metadata.get(ROUTE_QUARANTINE_COMPLETE_KEY, True)):
        census["quarantine_record_complete"] = False
    census["source_bound"] = str(summary.get("incremental_key_census_bound") or "")
    census.update(
        ran=True,
        missing_behind_watermark=len(missing),
        missing_keys=[str(k) for k in missing[:_SAMPLE]],
        changed=len(changed),
        destination_only=len(extra),
        capped=len(missing) > _SAMPLE and len(missing) >= 20,
    )
    out[CENSUS_KEY] = census
    sentence = census_sentence(census)
    if not sentence:
        return out
    drift = dict(out.get(DRIFT_KEY) or {})
    drift["rows"] = int(drift.get("rows") or 0) + len(missing)
    drift["missing_in_destination"] = int(drift.get("missing_in_destination") or 0) + len(missing)
    drift["behind_watermark"] = len(missing)
    drift["sample_keys"] = (list(drift.get("sample_keys") or []) + census["missing_keys"])[:_SAMPLE]
    out[DRIFT_KEY] = drift
    out["passed"] = False
    out["verification_verdict"] = "failed"
    out["behind_watermark_drift_enforced"] = True
    base = str(out.get("message") or "").rstrip()
    out["message"] = f"{sentence} {base}".strip()
    out["warnings"] = list(out.get("warnings") or []) + [sentence]
    return out
