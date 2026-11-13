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
* :func:`null_marker_verdict` — Gate-8 looks for marker text in the destination
  table and fails the run when the destination holds more of it than the source
  does (a source that really stores the text keeps passing).
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterable

from services.value_serializer import (
    DF_MISSING_SENTINEL,
    ESCAPED_WIRE_PREFIX,
    SQL_NULL_SENTINEL,
)

logger = logging.getLogger(__name__)

#: Text a leaked marker is stored as: the marker itself, or its escaped form
#: when the escape survived too.
MARKER_TEXTS = (
    SQL_NULL_SENTINEL,
    DF_MISSING_SENTINEL,
    ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL,
    ESCAPED_WIRE_PREFIX + DF_MISSING_SENTINEL,
)
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


def _columns(conn: Any, table: str, schema: str | None, wanted: list[str], key: str = "") -> tuple[list[str], Any]:
    """(text columns to probe, the key column's reflected type) of ``table``."""
    import sqlalchemy as sa

    folded = {c.lower() for c in wanted}
    out: list[str] = []
    key_type: Any = None
    for col in sa.inspect(conn).get_columns(table, schema=schema or None):
        name = str(col.get("name") or "")
        if key and name == key:
            key_type = col.get("type")
        if folded and name.lower() not in folded:
            continue
        if isinstance(col.get("type"), sa.String):
            out.append(name)
    return out, key_type


def count_marker_rows(
    db_type: str, cfg: dict[str, Any], table: str, columns: list[str], scope: Any = None,
) -> Any:
    """Rows of ``scope`` whose text columns hold a marker spelling (a :class:`MarkerCount`).

    Bounded three ways (review F3): the scope (this run's rows where the route
    names them), a statement timeout, and a cap. Any of them reached, or an
    unreadable table, is a count of ``None`` with its reason: not measured.
    """
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table
    from services.null_marker_probe import (
        MarkerCount,
        ProbeScope,
        bounded_marker_count,
        probe_timeout_seconds,
        statement_timeout,
        timed_out,
    )

    scope = scope or ProbeScope("whole_table")
    if db_type not in _SQL_PROBE_TYPES or not table:
        return MarkerCount(None, "no SQL probe for this destination")
    probe_cfg = {**dict(cfg or {}), "type": db_type}
    schema, bare = split_qualified_table(table, (probe_cfg.get("schema") or "").strip() or None)
    seconds = probe_timeout_seconds()
    enforced = False
    try:
        engine = _engine(probe_cfg)
        with engine.connect() as conn:
            text_cols, key_type = _columns(conn, bare, schema, columns, scope.key_column)
            if not text_cols:
                return MarkerCount(0)
            if scope.kind == "run_keys" and key_type is None:
                return MarkerCount(None, f"the key column {scope.key_column!r} is not in {table}")
            cols = [sa.column(c) for c in text_cols]
            if scope.kind == "run_keys" and scope.key_column not in text_cols:
                cols.append(sa.column(scope.key_column, key_type))
            tbl = sa.table(bare, *cols, schema=schema or None)
            hit = sa.or_(*[tbl.c[c].in_(MARKER_TEXTS) for c in text_cols])
            with statement_timeout(conn, db_type, seconds) as enforced:
                result = bounded_marker_count(conn, tbl, hit, scope, time.monotonic() + seconds)
            result.timeout_enforced = enforced
            return result
    except Exception as exc:  # noqa: BLE001 — an unreadable probe is "not measured", not a pass
        if timed_out(exc):
            return MarkerCount(None, f"timed out after {seconds:g}s", enforced)
        logger.warning("NULL-marker probe of %s failed", table, exc_info=True)
        return MarkerCount(None, f"the probe failed ({type(exc).__name__})", enforced)


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
    """Gate-8: the destination's marker text against the source's; ``None`` when not probed.

    ``status`` is ``passed``, ``leaked``, ``not_judged`` (marker text the
    source could not be counted for), ``not_measured`` (timeout, cap or error)
    or ``skipped`` (the run's rows cannot be named and the mode is not strict).
    The source is counted on its engine (SQL) or, for any other source, on the
    run's own source records, where a user literal is still escaped or marked.
    """
    from services.null_marker_probe import probe_timeout_seconds, resolve_probe_scope
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table

    dest_type = _engine_type(endpoint)
    if dest_type not in _SQL_PROBE_TYPES:
        return None
    table = str(
        (dest_summary or {}).get("table")
        or resolve_dest_table(dest_type, endpoint, getattr(source_endpoint, "table", "") or "")
        or ""
    )
    scope = resolve_probe_scope(
        dest_summary=dest_summary, records=records, mappings=mappings,
        rows_written=int(rows_written or 0), validation_mode=validation_mode,
    )
    if isinstance(scope, str):
        return {"status": "skipped", "probed": False, "scope": "none", "reason": scope,
                "leaked": False, "table": table}
    base = {"probed": True, "scope": scope.kind, "table": table,
            "timeout_seconds": probe_timeout_seconds()}
    targets = [str(m.get("target") or "") for m in mappings or [] if isinstance(m, dict) and m.get("target")]
    dest = count_marker_rows(dest_type, resolve_connector_config(endpoint), table, targets, scope)
    base["timeout_enforced"] = dest.timeout_enforced
    if dest.rows is None:
        return {**base, "status": "not_measured", "reason": f"destination: {dest.reason}",
                "destination_rows": None, "leaked": False}
    if dest.rows == 0:
        return {**base, "status": "passed", "destination_rows": 0, "leaked": False}
    source_hits: int | None = None
    source_reason = ""
    source_scope = scope.on_source(mappings)
    if (source_endpoint is not None and source_scope is not None
            and _engine_type(source_endpoint) in _SQL_PROBE_TYPES):
        sources = [str(m.get("source") or "") for m in mappings or [] if isinstance(m, dict) and m.get("source")]
        source = count_marker_rows(
            _engine_type(source_endpoint), resolve_connector_config(source_endpoint),
            str(getattr(source_endpoint, "table", "") or ""), sources, source_scope,
        )
        source_hits, source_reason = source.rows, source.reason
        if source.capped:
            # More source rows than the cap hold the text: more than the
            # (fully counted) destination holds, so nothing leaked.
            return {**base, "status": "passed", "destination_rows": dest.rows,
                    "source_rows": None, "source_rows_at_least": source.at_least, "leaked": False}
    if source_hits is None and records and len(records) >= int(rows_written or 0):
        # Only a whole population can say how many literals the source holds.
        source_hits = count_literal_marker_records(records)
    # A source nobody could count is not judged: the marker text may be the
    # user's own literal (a file cell that really says it).
    leaked = source_hits is not None and dest.rows > source_hits
    status = "leaked" if leaked else ("passed" if source_hits is not None else "not_judged")
    out = {**base, "status": status, "destination_rows": dest.rows, "source_rows": source_hits,
           "leaked": leaked}
    if source_hits is None and source_reason:
        out["reason"] = f"source: {source_reason}"
    return out


def count_literal_marker_records(records: Any) -> int | None:
    """Source rows (wire records) holding a *literal* marker spelling; ``None`` when unreadable."""
    from services.value_serializer import WireLiteral

    literals = {ESCAPED_WIRE_PREFIX + SQL_NULL_SENTINEL, ESCAPED_WIRE_PREFIX + DF_MISSING_SENTINEL}
    try:
        rows = list(records)
    except TypeError:
        return None
    count = 0
    for row in rows:
        cells = row.values() if isinstance(row, dict) else row
        try:
            if any(
                (isinstance(c, WireLiteral) and c.strip().lower() in _MARKERS)
                or (isinstance(c, str) and c in literals)
                for c in cells
            ):
                count += 1
        except TypeError:
            return None
    return count


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
    if verdict.get("status") == "not_measured":
        # A timeout, a cap hit or an unreadable table proves nothing: a report
        # that would pass is unproven instead (review F3). A report that already
        # failed on a measured finding stays failed, not "held".
        recon["migration_proven"] = False
        if recon.get("passed", True):
            recon["unproven"] = True
        note = (
            f"Datawrap's NULL-marker check of {verdict.get('table')} was not measured "
            f"({verdict.get('reason')}): the destination is not proven free of the "
            "marker text."
        )
        message = str(recon.get("message") or "").strip()
        recon["message"] = f"{message} {note}".strip()
    if verdict.get("leaked"):
        source = verdict.get("source_rows")
        held = "the source holds none" if source == 0 else (
            "the source was not probed" if source is None else f"the source holds {source}"
        )
        note = (
            f"Destination {verdict.get('table')} holds {verdict['destination_rows']} row(s) "
            f"with Datawrap's internal NULL marker ({SQL_NULL_SENTINEL}) as text, and "
            f"{held}: a NULL was written as the marker text. The destination is not "
            "verified."
        )
        recon["passed"] = False
        message = str(recon.get("message") or "").strip()
        recon["message"] = f"{note} {message}".strip()
    return recon

