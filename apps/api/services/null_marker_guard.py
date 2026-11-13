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
#: Most marker rows Gate-8 counts (the probe is bounded, not a census).
_PROBE_LIMIT = 1000


class NullMarkerLeak(ValueError):
    """A writer was about to store Datawrap's NULL marker as data."""


def _leaked(value: Any) -> bool:
    if type(value) is not str:  # WireLiteral (a decoded user literal) is a subclass
        return False
    if not value.startswith("__"):
        return False
    return value.strip().lower() in _MARKERS or value.startswith(ESCAPED_WIRE_PREFIX)


def refuse_unbound_marker(rows: Iterable[Any], columns: list[str] | None = None, *, table: str = "") -> None:
    """Raise :class:`NullMarkerLeak` when a row about to be bound carries a raw marker."""
    for index, row in enumerate(rows):
        cells = row.values() if isinstance(row, dict) else row
        for position, value in enumerate(cells):
            if _leaked(value):
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


def _text_columns(conn: Any, table: str, schema: str | None, wanted: list[str]) -> list[str]:
    import sqlalchemy as sa

    folded = {c.lower() for c in wanted}
    out: list[str] = []
    for col in sa.inspect(conn).get_columns(table, schema=schema or None):
        name = str(col.get("name") or "")
        if folded and name.lower() not in folded:
            continue
        if isinstance(col.get("type"), sa.String):
            out.append(name)
    return out


def count_marker_rows(db_type: str, cfg: dict[str, Any], table: str, columns: list[str]) -> int | None:
    """Rows (bounded) whose text columns hold a marker spelling; ``None`` when unreadable."""
    import sqlalchemy as sa

    from connectors.generic_sql import _engine
    from connectors.sql_identifiers import split_qualified_table

    if db_type not in _SQL_PROBE_TYPES or not table:
        return None
    probe_cfg = {**dict(cfg or {}), "type": db_type}
    schema, bare = split_qualified_table(table, (probe_cfg.get("schema") or "").strip() or None)
    try:
        engine = _engine(probe_cfg)
        with engine.connect() as conn:
            text_cols = _text_columns(conn, bare, schema, columns)
            if not text_cols:
                return 0
            tbl = sa.table(bare, *[sa.column(c) for c in text_cols], schema=schema or None)
            hit = sa.or_(*[tbl.c[c].in_(MARKER_TEXTS) for c in text_cols])
            inner = sa.select(sa.literal(1)).select_from(tbl).where(hit).limit(_PROBE_LIMIT).subquery()
            return int(conn.execute(sa.select(sa.func.count()).select_from(inner)).scalar() or 0)
    except Exception:  # noqa: BLE001 — an unreadable probe is "not measured", not a pass
        logger.warning("NULL-marker probe of %s failed", table, exc_info=True)
        return None


def null_marker_verdict(
    *,
    endpoint: Any,
    source_endpoint: Any = None,
    mappings: list[dict[str, Any]] | None = None,
    dest_summary: dict[str, Any] | None = None,
    records: list[Any] | None = None,
    rows_written: int = 0,
) -> dict[str, Any] | None:
    """Gate-8: the destination's marker text against the source's; ``None`` when not probed.

    The source is counted on its engine (SQL) or, for any other source, on the
    run's own source records, where a user literal is still escaped or marked.
    """
    from src.transfer.adapters import resolve_connector_config, resolve_dest_table

    dest_type = _engine_type(endpoint)
    if dest_type not in _SQL_PROBE_TYPES:
        return None
    table = str(
        (dest_summary or {}).get("table")
        or resolve_dest_table(dest_type, endpoint, getattr(source_endpoint, "table", "") or "")
        or ""
    )
    targets = [str(m.get("target") or "") for m in mappings or [] if isinstance(m, dict) and m.get("target")]
    dest_hits = count_marker_rows(dest_type, resolve_connector_config(endpoint), table, targets)
    if dest_hits is None:
        return None
    if dest_hits == 0:
        return {"probed": True, "destination_rows": 0, "leaked": False, "table": table}
    source_hits: int | None = None
    if source_endpoint is not None and _engine_type(source_endpoint) in _SQL_PROBE_TYPES:
        sources = [str(m.get("source") or "") for m in mappings or [] if isinstance(m, dict) and m.get("source")]
        source_hits = count_marker_rows(
            _engine_type(source_endpoint), resolve_connector_config(source_endpoint),
            str(getattr(source_endpoint, "table", "") or ""), sources,
        )
    if source_hits is None and records and len(records) >= int(rows_written or 0):
        # Only a whole population can say how many literals the source holds.
        source_hits = count_literal_marker_records(records)
    # A source nobody could count is not judged: the marker text may be the
    # user's own literal (a file cell that really says it).
    leaked = source_hits is not None and dest_hits > source_hits
    return {
        "probed": True,
        "destination_rows": dest_hits,
        "source_rows": source_hits,
        "leaked": leaked,
        "table": table,
    }


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

