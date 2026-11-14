"""The pre-write population scan of enforced columns (review 4, M-1).

Under ``require_reversibility`` each enforced column is checked while writing
(:mod:`services.reversibility_enforce`): the first value that would change
stops the run. An overwrite is undone by the destination's own backup, but an
append or an upsert commits batch by batch, so a value past the first batches
left those batches written (20,000 of 30,000 rows in the reviewer's run).

So before anything is written, Execute runs every enforced column of the
source population through the writer's own mapping stage
(``connectors.writer_common.build_mapped_rows_with_details``, the stage that
enforces while writing), as the population-fit gate already does for strict
transform errors: the first value that would change blocks the run and
Execute commits nothing. The scan reads only the enforced columns.

It covers a run whose population it can read before the write: the rows
Execute already holds when they are the whole source (the in-memory path, a
quarantine replay), or a database table the preflight population walk can page
(the same walk the population-fit gate uses). Elsewhere (a stream, CDC, a
file larger than its sample) the write-time check still stops the run on the
first changed value, and the error then says how many rows were kept; it
never claims to have blocked a partial write.

The tally it keeps is its own (``prescan:<job>``), so the writer's
``rows_checked`` still counts only rows the writer checked.
"""

from __future__ import annotations

import copy
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

logger = logging.getLogger(__name__)

#: Rows handed to the writer's mapping stage per call.
_CHUNK = 5000


def _stamped(mappings: Sequence[Any] | None) -> list[dict[str, Any]]:
    from services.reversibility_enforce import STAMP

    return [m for m in mappings or [] if isinstance(m, dict) and isinstance(m.get(STAMP), dict)]


def needs_prescan(request: Any, mappings: Sequence[Any] | None) -> bool:
    """Whether a violation found while writing would leave rows behind (append, upsert, incremental)."""
    from services.sync_cursor import is_overwrite_sync, normalize_sync_mode

    if not _stamped(mappings):
        return False
    mode = normalize_sync_mode(str(getattr(request, "sync_mode", "") or ""), default="")
    return mode != "cdc" and not is_overwrite_sync(mode)


def _population(request: Any, mappings: Sequence[Any], columns: list[str], column_types: Mapping[str, str] | None,
                dest_types: Mapping[str, str] | None, held: Sequence[Mapping[str, Any]],
                population: int | None) -> Iterable[Mapping[str, Any]] | None:
    """The source rows to scan (only ``columns`` are read), or ``None`` when they cannot be read first."""
    if held and population is not None and len(held) >= population:
        return held  # Execute already holds every source row
    source = getattr(request, "source", None)
    if str(getattr(source, "kind", "") or "").lower() not in {"database", "cloud"}:
        return None
    from services.preflight_cursor_gate import read_scope_for_transfer_request
    from services.sync_cursor import incremental_read_narrows
    from src.transfer.source_peek import iter_bounded_table_population_rows

    scope = read_scope_for_transfer_request(request)
    dest = getattr(request, "destination", None)
    return iter_bounded_table_population_rows(
        source, list(mappings), column_types=dict(column_types or {}), dest_types=dict(dest_types or {}),
        dest_db=str(getattr(dest, "format", "") or ""), source_kind=str(getattr(source, "kind", "") or ""),
        source_format=str(getattr(source, "format", "") or ""),
        read_scope=scope if incremental_read_narrows(str(getattr(request, "sync_mode", "") or "")) else None,
        source_filter=getattr(request, "source_filter", None) or None, source_columns=columns,
    )


def _stream_request(request: Any, dicts: Sequence[Any] | None, target: Mapping[str, Any]) -> Any:
    """``request`` as the sequential writer runs one stream: its source, its contract, its sync mode."""
    if target.get("stream") is None:
        return request
    contract = dict(target.get("contract") or {})
    sub = copy.copy(request)
    sub.source = target.get("source") or request.source
    sub.stream_contracts = [{**contract, "name": target["stream"], "selected": True}]
    sub.sync_mode = str(contract.get("sync_mode") or getattr(request, "sync_mode", "") or "")
    sub.mappings = list(dicts or [])
    return sub


def prescan_streams(request: Any, targets: Sequence[tuple[Sequence[Any] | None, Mapping[str, Any] | None]],
                    ) -> tuple[str | None, list[str]]:
    """(why Execute must commit nothing, the streams whose population could not be read before writing).

    Every selected stream's stamped mappings are scanned against that stream's
    own source (review 5, M-1): a multi-stream append commits stream by stream,
    so a late value in any of them would otherwise leave earlier streams
    written. A stream analysed nowhere (``None``) is already blocked by g24.
    """
    unread: list[str] = []
    for dicts, target in targets:
        if target is None:
            continue
        sub = _stream_request(request, dicts, target)
        if not needs_prescan(sub, dicts):
            continue
        missed: list[bool] = []
        found = prescan(sub, dicts, target.get("column_types"), target.get("dest_types"),
                        held=list(target.get("sample") or []), population=target.get("total"), unread=missed)
        if found:
            return found, unread
        if missed:
            unread.append(str(target.get("stream") or _table_name(sub)))
    return None, unread


def _table_name(request: Any) -> str:
    source = getattr(request, "source", None)
    return str(getattr(source, "table", "") or getattr(source, "collection", "") or "the source")


def prescan(request: Any, mappings: Sequence[Any] | None, column_types: Mapping[str, str] | None = None,
            dest_types: Mapping[str, str] | None = None, *, held: Sequence[Mapping[str, Any]] | None = None,
            population: int | None = None, unread: list[bool] | None = None) -> str | None:
    """Why Execute must commit nothing (the first enforced value that would change), else ``None``.

    ``None`` too when the run needs no scan or its population cannot be read
    before the write (the write-time check then stops the run); the latter is
    noted on ``unread`` when given.
    """
    if not needs_prescan(request, mappings):
        return None
    from connectors.writer_common import build_mapped_rows_with_details
    from services.reversibility_enforce import STAMP, ReversibilityViolation, describe_rule, note_prescan, summary

    stamped = _stamped(mappings)
    columns = list(dict.fromkeys(str(m.get("source") or "") for m in stamped if m.get("source")))
    try:
        rows = _population(request, list(mappings or []), columns, column_types, dest_types,
                           list(held or []), population)
    except Exception:  # noqa: BLE001 - an unreadable population leaves the write-time check in charge
        logger.warning("reversibility pre-scan could not read the source population", exc_info=True)
        rows = None
    if rows is None:
        if unread is not None:
            unread.append(True)
        return None
    job = str(stamped[0][STAMP].get("job_id") or "")
    scan_key = f"prescan:{job}"
    probes = []
    for m in stamped:
        probe = copy.deepcopy(m)
        probe[STAMP]["job_id"] = scan_key  # its own tally: the writer's rows_checked stays the writer's
        probes.append(probe)
    targets = [str(m.get("target") or m.get("source") or "") for m in probes]
    scanned = 0
    chunk: list[list[Any]] = []

    def check(batch: list[list[Any]], first: int) -> str | None:
        try:
            build_mapped_rows_with_details(headers=columns, data_rows=batch, mappings=probes, target_cols=targets,
                                           column_types=dict(column_types or {}), dest_types=dict(dest_types or {}),
                                           error_policy="quarantine", row_number_start=first)
        except ReversibilityViolation as exc:
            rule = getattr(exc, "rule", None) or stamped[0][STAMP]
            where, how = describe_rule(rule)
            return (f"Reversibility is required and {where} would change under {how}: a source value in rows "
                    f"{first:,}-{first + len(batch) - 1:,} of the population scanned before writing does not "
                    f"round-trip ({rule.get('code')}). Nothing was written; Execute would commit nothing. Fix the "
                    "value at the source, remove the transform, or turn off require reversibility.")
        return None

    started = time.monotonic()

    def walk() -> str | None:
        nonlocal scanned, chunk
        for row in rows:
            chunk.append([row.get(c) for c in columns])
            if len(chunk) >= _CHUNK:
                found = check(chunk, scanned + 1)
                if found:
                    return found
                scanned += len(chunk)
                chunk = []
        return check(chunk, scanned + 1) if chunk else None

    try:
        return walk()
    except Exception:  # noqa: BLE001 - a walk that failed leaves the write-time check in charge, never a pass
        logger.warning("reversibility pre-scan stopped after %s row(s)", scanned, exc_info=True)
        if unread is not None:
            unread.append(True)
        return None
    finally:
        summary(scan_key)  # forget the scan's own tally
        read, seconds = scanned + len(chunk), time.monotonic() - started
        logger.info("reversibility pre-write scan read %s row(s) of %s in %.2f s", f"{read:,}", _table_name(request),
                    seconds)
        note_prescan(job, read, seconds)  # stated on the run's destination summary
