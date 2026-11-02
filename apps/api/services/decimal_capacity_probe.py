"""Measure what an unconstrained decimal column actually holds.

PostgreSQL ``numeric`` without a typmod is unbounded — up to 131072 digits
before the point and 16383 after. No other engine has a carrier that wide
(MySQL tops out at ``DECIMAL(65,30)``, Snowflake at ``NUMBER(38,37)``), so a
declared-type comparison can only conclude that the destination is narrower and
refuse the write. That verdict is correct about the *type* and almost always
wrong about the *data*: a column declared ``numeric`` usually holds ordinary
money or quantity values that fit anywhere.

Rather than guess in either direction — inventing a capacity the source never
declared, or blocking a route that would have moved every row intact — ask the
source. ``max`` over the column is exact and covers the whole population, not a
sample, so the answer is proof rather than an estimate. When the probe cannot
run (no privilege, timeout, non-SQL source) the caller keeps the declared bare
type and stays fail-closed.

The measurement describes the rows present now. It is not a constraint on the
column, and a later row may exceed it; that is why the result is consumed as
evidence for *this* transfer's preflight rather than written back as DDL.
"""

from __future__ import annotations

import logging
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

#: Ceiling on how long the probe may hold the source. A capacity scan is worth
#: paying for when the alternative is a blocked route, but never worth hanging
#: a preflight over.
PROBE_TIMEOUT_MS = 15_000


class DecimalCapacity(NamedTuple):
    """Observed digit capacity of one column, across every non-null row."""

    int_digits: int
    scale: int

    @property
    def precision(self) -> int:
        return self.int_digits + self.scale

    def as_type(self) -> str:
        """Render as a DECIMAL carrier that holds every value measured."""
        return f"DECIMAL({max(self.precision, 1)},{self.scale})"


def probe_postgresql_decimal_capacity(
    cur: Any,
    schema: str,
    table: str,
    columns: list[str],
) -> dict[str, DecimalCapacity]:
    """Measure integer digits and scale for unconstrained ``numeric`` columns.

    One aggregate pass covers every requested column. ``scale()`` is a
    PostgreSQL built-in for ``numeric`` and reports the value's own scale, which
    for an unconstrained column varies per row — hence ``max``.

    Returns only columns that had at least one non-null value; a column with
    nothing in it has proven no capacity, and inventing one from an empty table
    is the guess this module exists to avoid.
    """
    if not columns:
        return {}
    from connectors.sql_identifiers import quote_sql_identifier, quote_table_ref

    table_ref = quote_table_ref(table, schema or "public", dialect="postgresql")
    selects: list[str] = []
    for col in columns:
        quoted = quote_sql_identifier(col)
        # ``trim(leading '-')`` drops the sign; splitting on '.' isolates the
        # integer side. to_char would pad to a template width, so the value's
        # own text is the only faithful source of digit count.
        selects.append(
            f"max(length(ltrim(split_part(trim(leading '-' from {quoted}::text), "
            f"'.', 1), '0')))"
        )
        selects.append(f"max(scale({quoted}))")
        selects.append(f"count({quoted})")

    sql = f"SELECT {', '.join(selects)} FROM {table_ref}"  # nosec B608
    try:
        cur.execute(f"SET LOCAL statement_timeout = {int(PROBE_TIMEOUT_MS)}")
    except Exception as exc:
        # Not fatal: without a timeout the probe still returns, and the caller
        # already treats any failure as "unmeasured".
        logger.debug("decimal capacity probe timeout unset: %s", exc)
    try:
        cur.execute(sql)
        row = cur.fetchone()
    except Exception as exc:
        logger.info(
            "decimal capacity probe unavailable for %s.%s: %s", schema, table, exc
        )
        return {}
    if not row:
        return {}

    out: dict[str, DecimalCapacity] = {}
    for i, col in enumerate(columns):
        int_digits, scale, non_null = row[i * 3 : i * 3 + 3]
        if not non_null:
            continue
        # A value like 0.5 has no integer digits once leading zeros are stripped;
        # it still needs one digit of room for the 0.
        digits = max(int(int_digits or 0), 1)
        out[col] = DecimalCapacity(int_digits=digits, scale=int(scale or 0))
    return out


def unconstrained_decimal_columns(columns: list[dict[str, Any]]) -> list[str]:
    """Names of columns introspected as a decimal with no declared precision."""
    from services.type_system import (
        LOGICAL_DECIMAL,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    out: list[str] = []
    for col in columns or []:
        name = str(col.get("name") or "").strip()
        declared = str(col.get("inferred_type") or "")
        if not name or normalize_logical_type(declared) != LOGICAL_DECIMAL:
            continue
        precision, scale = parse_numeric_precision_scale(declared)
        if precision is None and scale is None:
            out.append(name)
    return out
