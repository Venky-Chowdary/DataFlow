"""Datawrap's internal NULL marker must never land as destination data.

On the transfer wire a SQL NULL is spelled ``__DF_SQL_NULL__``
(:data:`services.value_serializer.SQL_NULL_SENTINEL`), and a user text that
happens to spell a marker is escaped with ``__DF_ESC__``. A wire cell encoded a
second time turns the NULL marker into user text: the CDC drift repair re-read
source rows as wire cells and re-encoded them, so a NULL key landed in
PostgreSQL as the text ``__DF_SQL_NULL__`` and the run said nothing about it.

Two backstops behind the source fix (wire cells are marked where they are
read, so they are never encoded twice):

* :func:`refuse_unbound_marker` — a writer about to bind a *plain* ``str`` that
  spells a marker, or still carries the escape prefix, is binding a cell nobody
  decoded. A user literal reaches the binder as a
  :class:`~services.value_serializer.WireLiteral`; a plain one is a leak, so the
  write is refused before the row is sent.
* :func:`null_marker_verdict` — Gate-8 looks for marker text in each
  destination table the run wrote and fails the run when the destination holds
  more of it than the source does, the raw marker and its escaped spelling
  judged apart (a source that really stores the text keeps passing). The probe
  itself (scope, timeout, cap, exact match) is :mod:`services.null_marker_probe`.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable

from services.value_serializer import (
    DF_MISSING_SENTINEL,
    ESCAPED_WIRE_PREFIX,
    SQL_NULL_SENTINEL,
)

logger = logging.getLogger(__name__)

_MARKERS = frozenset(m.lower() for m in (SQL_NULL_SENTINEL, DF_MISSING_SENTINEL))

#: SQL destinations Gate-8 can probe with one bounded query.
_SQL_PROBE_TYPES = frozenset({
    "postgresql", "postgres", "mysql", "mariadb", "sqlite", "sqlserver", "mssql",
    "oracle", "duckdb", "generic_sql",
})


class NullMarkerLeak(ValueError):
    """A writer was about to store Datawrap's NULL marker as data."""


def _leaked(value: Any, *, copy_mode: bool = False) -> bool:
    """True for a cell nobody decoded: a marker (plain or a marked wire cell) or a live escape.

    A decoded user literal (``WireLiteral``) is never a leak. In ``copy_mode``
    a NULL marker is SQL NULL by design (COPY writes it as ``\\N``), so only a
    cell still carrying the escape prefix is refused there.
    """
    from services.value_serializer import WireLiteral

    if not isinstance(value, str) or isinstance(value, WireLiteral):
        return False
    if not value.startswith("__"):
        return False
    if value.startswith(ESCAPED_WIRE_PREFIX):
        return True
    return not copy_mode and value.strip().lower() in _MARKERS


def refuse_unbound_marker(
    rows: Iterable[Any], columns: list[str] | None = None, *, table: str = "", copy_mode: bool = False,
) -> None:
    """Raise :class:`NullMarkerLeak` when a row about to be written carries an undecoded marker."""
    for index, row in enumerate(rows):
        cells = row.values() if isinstance(row, dict) else row
        for position, value in enumerate(cells):
            if _leaked(value, copy_mode=copy_mode):
                column = (
                    list(row.keys())[position] if isinstance(row, dict)
                    else (columns[position] if columns and position < len(columns) else f"#{position + 1}")
                )
                where = f" in {table}" if table else ""
                raise NullMarkerLeak(
                    f"Refused to write row {index + 1}{where}: column {column!r} holds "
                    f"Datawrap's internal NULL marker {str(value)!r} as text. A NULL "
                    "was not decoded before the write; nothing in this batch was "
                    "written."
                )


def _engine_type(endpoint: Any) -> str:
    from src.transfer.connector_capabilities import resolve_driver_type

    return str(resolve_driver_type(str(getattr(endpoint, "format", "") or "")) or "").lower()


def _table_verdict(
    *,
    dest_type: str,
    dest_cfg: dict[str, Any],
    table: str,
    targets: list[str],
    scope: Any,
    source_endpoint: Any,
    source_table: str,
    sources: list[str],
    mappings: list[dict[str, Any]] | None,
    records: list[Any] | None,
    rows_written: int,
) -> dict[str, Any]:
    """One destination table's verdict; the raw marker and its escaped spelling judged apart (review F4)."""
    from services.null_marker_probe import (
        count_literal_marker_records,
        count_marker_rows,
        probe_timeout_seconds,
    )
    from src.transfer.adapters import resolve_connector_config

    if isinstance(scope, str):
        return {"status": "skipped", "probed": False, "scope": "none", "reason": scope,
                "leaked": False, "table": table}
    base = {"probed": True, "scope": scope.kind, "table": table, "timeout_seconds": probe_timeout_seconds()}
    dest = count_marker_rows(dest_cfg, table, targets, scope)
    base["timeout_enforced"] = dest.timeout_enforced
    if dest.not_applicable:
        # Datawrap's own lack of a route never fails or undoes a run.
        return {**base, "probed": False, "status": "not_applicable", "reason": dest.reason,
                "destination_rows": None, "leaked": False}
    if dest.rows is None:
        return {**base, "status": "not_measured", "reason": f"destination: {dest.reason}",
                "destination_rows": None, "leaked": False}
    base.update(destination_rows=dest.rows, destination_raw_rows=dest.raw, destination_escaped_rows=dest.escaped)
    if dest.rows == 0:
        return {**base, "status": "passed", "leaked": False}
    source = None
    source_scope = scope.on_source(mappings)
    if (source_endpoint is not None and source_scope is not None
            and _engine_type(source_endpoint) in _SQL_PROBE_TYPES):
        source = count_marker_rows(
            resolve_connector_config(source_endpoint), source_table, sources, source_scope,
        )
        if source.not_applicable:
            source = None  # no route to the source: count it on the records, or do not judge
        elif source.capped and dest.raw <= source.raw and dest.escaped <= source.escaped:
            # The source holds at least as many of each form as the (fully
            # counted) destination: nothing leaked.
            return {**base, "status": "passed", "leaked": False, "source_rows": None,
                    "source_raw_rows_at_least": source.raw, "source_escaped_rows_at_least": source.escaped}
    if (source is None or source.rows is None) and records and len(records) >= int(rows_written or 0):
        # Only a whole population can say how many literals the source holds.
        source = count_literal_marker_records(records) or source
    if source is not None and source.rows is None:
        # The source probe ran and stopped (timeout, cap, error): not measured.
        return {**base, "status": "not_measured", "reason": f"source: {source.reason}",
                "source_rows": None, "leaked": False}
    if source is None:
        # A source nobody can count is not judged: the marker text may be the
        # user's own literal (a file cell that really says it).
        return {**base, "status": "not_judged", "source_rows": None, "leaked": False}
    leaked = dest.raw > source.raw or dest.escaped > source.escaped
    return {**base, "status": "leaked" if leaked else "passed", "leaked": leaked,
            "source_rows": source.rows, "source_raw_rows": source.raw, "source_escaped_rows": source.escaped}


#: Worst first: the status a multi-table check reports.
_STATUS_ORDER = ("leaked", "not_measured", "not_judged", "not_applicable", "passed", "skipped")


def _not_applicable(table: str, reason: str) -> dict[str, Any]:
    return {"status": "not_applicable", "probed": False, "scope": "none", "reason": reason,
            "leaked": False, "table": table}


def _stream_verdicts(
    *, dest_type: str, endpoint: Any, source_endpoint: Any, dest_summary: dict[str, Any],
    mappings: list[dict[str, Any]] | None, records: list[Any] | None, rows_written: int,
    validation_mode: str | None,
) -> list[dict[str, Any]]:
    """A verdict for every table a multi-table run wrote (review F4: not only the last)."""
    import dataclasses

    from services.null_marker_probe import resolve_probe_scope
    from src.transfer.adapters import resolve_connector_config

    last_table = str(dest_summary.get("table") or "")
    out: list[dict[str, Any]] = []
    for entry in dest_summary.get("streams") or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "")
        table = str(entry.get("table") or name)
        written = int(entry.get("records_processed") or 0)
        if str(entry.get("status") or "completed") != "completed" or written <= 0:
            reason = ("an earlier pass of this job wrote it" if entry.get("rows_from_earlier_pass")
                      else "this run wrote no rows to it")
            out.append({"status": "skipped", "probed": False, "scope": "none", "table": table,
                        "reason": reason, "leaked": False})
            continue
        last = table == last_table
        maps = mappings if last else None
        scope = resolve_probe_scope(
            dest_summary=dest_summary if last else {"sync_mode": entry.get("sync_mode")},
            records=records if last else None, mappings=maps,
            rows_written=rows_written if last else written, validation_mode=validation_mode,
        )
        source = source_endpoint
        if source is not None and dataclasses.is_dataclass(source) and name:
            source = dataclasses.replace(source, table=name)
        out.append(_table_verdict(
            dest_type=dest_type, dest_cfg=resolve_connector_config(endpoint), table=table,
            targets=_names(maps, "target"), scope=scope, source_endpoint=source,
            source_table=str(getattr(source, "table", "") or ""), sources=_names(maps, "source"),
            mappings=maps, records=records if last else None, rows_written=rows_written if last else written,
        ))
    return out


def _names(mappings: list[dict[str, Any]] | None, side: str) -> list[str]:
    return [str(m.get(side) or "") for m in mappings or [] if isinstance(m, dict) and m.get(side)]


def null_marker_verdict(
    *,
    endpoint: Any,
    source_endpoint: Any = None,
    mappings: list[dict[str, Any]] | None = None,
    dest_summary: dict[str, Any] | None = None,
    records: list[Any] | None = None,
    rows_written: int = 0,
    validation_mode: str | None = "strict",
) -> dict[str, Any] | None:
    """Gate-8: the destination's marker text against the source's.

    ``status`` is ``passed``, ``leaked``, ``not_judged`` (marker text the
    source could not be counted for), ``not_measured`` (the check applies but
    could not complete: timeout, cap, query error, no connection back through
    the writer's engine), ``not_applicable`` (the probe has no route to this
    destination; never fails a run) or ``skipped`` (the run's rows cannot be
    named and the mode is not strict).
    The source is counted on its engine (SQL) or, for any other source, on the
    run's own source records, where a user literal is still escaped or marked.
    A multi-table run is judged table by table (``tables``).
    """
    from services.null_marker_probe import resolve_probe_scope
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table

    dest_type = _engine_type(endpoint)
    summary = dest_summary if isinstance(dest_summary, dict) else {}
    if dest_type not in _SQL_PROBE_TYPES:
        return _not_applicable(
            str(summary.get("table") or getattr(endpoint, "table", "") or ""),
            f"no SQL marker probe for a {getattr(endpoint, 'format', '') or dest_type or 'non-SQL'} destination",
        )
    kw = {"mappings": mappings, "records": records, "rows_written": int(rows_written or 0)}
    if summary.get("multi_stream") and summary.get("streams"):
        tables = _stream_verdicts(dest_type=dest_type, endpoint=endpoint, source_endpoint=source_endpoint,
                                  dest_summary=summary, validation_mode=validation_mode, **kw)
        if tables:
            status = min((t["status"] for t in tables), key=_STATUS_ORDER.index)
            return {
                "status": status, "scope": "per_table", "tables": tables,
                "probed": any(t.get("probed") for t in tables),
                "leaked": any(t.get("leaked") for t in tables),
                "table": ", ".join(t["table"] for t in tables if t["status"] == status),
            }
    table = str(
        summary.get("table")
        or resolve_dest_table(dest_type, endpoint, getattr(source_endpoint, "table", "") or "")
        or ""
    )
    scope = resolve_probe_scope(dest_summary=summary, validation_mode=validation_mode, **kw)
    dest_cfg = resolve_connector_config(endpoint)
    if str(summary.get("schema") or "").strip():
        dest_cfg["schema"] = str(summary["schema"])  # where the writer says it wrote
    return _table_verdict(
        dest_type=dest_type, dest_cfg=dest_cfg, table=table,
        targets=_names(mappings, "target"), scope=scope, source_endpoint=source_endpoint,
        source_table=str(getattr(source_endpoint, "table", "") or ""), sources=_names(mappings, "source"),
        **kw,
    )


def _leak_note(v: dict[str, Any]) -> str:
    source = v.get("source_rows")
    held = "the source holds none" if source == 0 else (
        "the source was not probed" if source is None else f"the source holds {source}"
    )
    escaped = int(v.get("destination_escaped_rows") or 0)
    form = (f" ({escaped} in its escaped spelling {ESCAPED_WIRE_PREFIX}{SQL_NULL_SENTINEL}, "
            "an escape that was never decoded)") if escaped else ""
    return (
        f"Destination {v.get('table')} holds {v.get('destination_rows')} row(s) "
        f"with Datawrap's internal NULL marker ({SQL_NULL_SENTINEL}) as text{form}, and "
        f"{held}: a NULL was written as the marker text."
    )


def apply_null_marker_gate(recon: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    """Fail the Gate-8 report when the destination holds a leaked marker (never raises)."""
    if not isinstance(recon, dict):
        return recon
    try:
        verdict = null_marker_verdict(**kwargs)
    except Exception:  # noqa: BLE001 — the gate's own result stands
        logger.warning("NULL-marker gate did not run", exc_info=True)
        return recon
    if not verdict:
        return recon
    recon["null_marker_check"] = verdict
    tables = verdict.get("tables") or [verdict]
    unmeasured = [t for t in tables if t.get("status") == "not_measured"]
    if unmeasured:
        # A timeout, a cap hit or an unreadable table proves nothing: a report
        # that would pass is unproven instead (review F3). A report that already
        # failed on a measured finding stays failed, not "held".
        from services.null_marker_probe import PROBE_TIMEOUT_ENV, STRICT_MODES
        from services.validation_mode_contract import normalize_validation_mode

        recon["migration_proven"] = False
        if recon.get("passed", True):
            recon["unproven"] = True
            # Business-rule convention: strict and maximum fail closed on an
            # unverified check; balanced completes, still unproven.
            if normalize_validation_mode(kwargs.get("validation_mode", "strict")) in STRICT_MODES:
                recon["passed"] = False
                recon["details"] = {**dict(recon.get("details") or {}),
                                    "reason": "null_marker_check_not_measured"}
        note = " ".join(
            f"Datawrap's NULL-marker check of {t.get('table')} could not be measured "
            f"({t.get('reason')}): the destination is not proven free of the marker text. "
            f"Raise DATAWRAP_{PROBE_TIMEOUT_ENV} (DATAFLOW_{PROBE_TIMEOUT_ENV}) or run in "
            "balanced validation mode, where an unmeasured check is reported, not failed."
            for t in unmeasured
        )
        message = str(recon.get("message") or "").strip()
        recon["message"] = f"{message} {note}".strip()
    leaked = [t for t in tables if t.get("leaked")]
    if leaked:
        note = " ".join(_leak_note(t) for t in leaked) + " The destination is not verified."
        recon["passed"] = False
        message = str(recon.get("message") or "").strip()
        recon["message"] = f"{note} {message}".strip()
    return recon
