"""The transform proof's source re-read, through the write's own reader (D331).

The proof pins each key to the hash of the raw row the write consumed, then
re-reads the source, re-applies the recipe and compares with the destination.
Both sides only agree when the re-read renders every cell the way the write's
reader did.

On Oracle the proof used a hand-built ``SELECT "COL" ...`` through the raw
driver. python-oracledb returns TIMESTAMP WITH [LOCAL] TIME ZONE as a *naive*
``datetime`` (the offset is dropped, the wall clock kept), while the write's
reader renders those columns server side with ``TO_CHAR`` and keeps the instant
(``generic_sql._tz_safe_projection``); Oracle JSON came back as a ``dict`` the
reader writes as canonical JSON text. Every row holding such a value lost its
pin ("no longer read back as this run read them") and, re-shaped from the
wrong instant, no longer equalled the destination: "N of N keys do not hold
the recipe output" over exact data, on every Oracle source (QA r61-r75).

The re-read now goes through the same reader function the stream used
(``batch_readers._read_batch_impl``): same reflection, same catalog spelling of
upper-case identifiers, same projection, same wire text. Cells the reader
renders as numbers or zoneless timestamps are turned back into the driver's
value (the wire's inverse is exact), so keys and numeric/temporal values
compare in the same typed domain the proof always used; every other cell keeps
the reader's text, which is what the write's recipe consumed.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Iterator

_PAGE_ROWS = 5_000

_NUMERIC_DDL = re.compile(
    r"^\s*(?:DECIMAL|NUMERIC|NUMBER|DEC|INT|INTEGER|BIGINT|SMALLINT|TINYINT|"
    r"INT[248]|FLOAT|DOUBLE|REAL|BINARY_FLOAT|BINARY_DOUBLE)\b",
    re.I,
)
_IEEE_DDL = re.compile(r"^\s*(?:BINARY_FLOAT|BINARY_DOUBLE|DOUBLE|REAL)\b", re.I)
_NAIVE_TEMPORAL_DDL = re.compile(
    r"^\s*(?:DATE|DATETIME|TIMESTAMP_NTZ|TIMESTAMP(?!_TZ|_LTZ|\s*\(\d+\)\s*WITH|\s+WITH))\b",
    re.I,
)


class WireRows(list):
    """A page of row dicts whose cells are the write reader's wire text."""


def rereads_through_writer(src_type: str) -> bool:
    """True when the proof must re-read this source through the write's reader."""
    from .engine_family import is_oracle_family

    return is_oracle_family(src_type)


def is_wire_page(batch: Any) -> bool:
    return isinstance(batch, WireRows)


def iter_writer_wire_batches(
    *,
    source_endpoint: Any,
    src_type: str,
    src_cfg: dict[str, Any],
    read_cols: list[str],
) -> Iterator[WireRows]:
    """Pages of ``{read_col: wire text}`` read by the stream's own reader."""
    from connectors.sql_snapshot_scan import close_table_scan
    from services.keyset_pagination import header_column_index

    from .batch_readers import _read_batch_impl

    table = str(getattr(source_endpoint, "table", "") or src_cfg.get("table") or "")
    state: dict[str, Any] = {}
    offset = 0
    try:
        while True:
            page = _read_batch_impl(
                src_type, src_cfg, table, list(read_cols), offset, _PAGE_ROWS,
                scan_state=state,
            )
            rows = list(getattr(page, "rows", None) or [])
            if not rows:
                return
            headers = list(getattr(page, "headers", None) or [])
            index = {c: header_column_index(headers, c) for c in read_cols}
            missing = [c for c, i in index.items() if i is None]
            if missing:
                # Never a silent NULL column: that is exactly the false
                # "changed in source" this module exists to prevent.
                raise ValueError(
                    "the source reader did not return column(s) "
                    + ", ".join(missing)
                )
            yield WireRows(
                {c: (row[i] if i < len(row) else None) for c, i in index.items()}
                for row in rows
            )
            offset += len(rows)
    finally:
        close_table_scan(state)


def decode_wire_rows(
    rows: list[dict[str, Any]], ddl_of: Callable[[str], str]
) -> list[dict[str, Any]]:
    """Wire text back to the value the reader serialized, where that is exact."""
    from services.value_serializer import SQL_NULL_SENTINEL, unescape_wire_text

    def _cell(value: Any, ddl: str) -> Any:
        if value is None or value == SQL_NULL_SENTINEL:
            return None
        text = unescape_wire_text(value)
        if not isinstance(text, str) or not ddl:
            return text
        if _NUMERIC_DDL.match(ddl):
            if _IEEE_DDL.match(ddl):
                try:
                    return float(text)
                except ValueError:
                    return text
            from connectors.oracle_numbers import oracle_number_from_driver

            return oracle_number_from_driver(text)
        if _NAIVE_TEMPORAL_DDL.match(ddl):
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return text
            return parsed if parsed.tzinfo is None else text
        return text

    types = {}
    out: list[dict[str, Any]] = []
    for row in rows:
        decoded = {}
        for col, value in row.items():
            if col not in types:
                types[col] = str(ddl_of(col) or "")
            decoded[col] = _cell(value, types[col])
        out.append(decoded)
    return out


def oracle_zone_safe_rows(conn: Any, query: str, columns: list[str]) -> tuple[str, Callable]:
    """``(query, row_fixer)`` reading zone-bearing Oracle columns as instants.

    The proof's destination read-back on Oracle selected TIMESTAMP WITH [LOCAL]
    TIME ZONE raw, so it compared a naive wall clock with the recipe's instant.
    ``tz_projected_query`` renders those columns with ``TO_CHAR`` (the Gate-8
    read-back rule, D155) and the fixer parses them back to aware instants.
    """
    import sqlalchemy as sa

    from connectors.oracle_tstz import parse_oracle_tstz_text, tz_projected_query

    def _probe(sql: str) -> Any:
        result = conn.execute(sa.text(sql))
        return getattr(getattr(result, "cursor", None), "description", None)

    wrapped, positions = tz_projected_query(_probe, query)
    names = {columns[i] for i in positions if i < len(columns)}

    def _fix(row: dict[str, Any]) -> dict[str, Any]:
        if not names:
            return row
        return {
            k: (parse_oracle_tstz_text(v) if k in names else v) for k, v in row.items()
        }

    return wrapped, _fix
