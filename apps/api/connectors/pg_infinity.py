"""Keep PostgreSQL ``infinity`` / ``-infinity`` dates and timestamps literal.

psycopg2's stock typecasters decode them as ``date.max`` / ``datetime.max`` /
``datetime.min``: every PostgreSQL read handed the pipeline a finite
9999-12-31 or 0001-01-01, the destination stored that calendar date, and a
read-back through the same decode "verified" it (D63). The casters below
return the literal instead; :func:`connectors.sql_temporal.coerce_sql_temporal`
carries it to the PostgreSQL family and refuses it by name everywhere else.

The same holds for the dates Python's ``datetime`` cannot hold at all: a BC
date (``0044-03-15 BC``) and a year past 9999 (``10000-01-01``). The stock
caster raised ``year -44 is out of range`` on the *fetch*, so the whole read
failed and Validate blamed the connection / table name / timeout for a value
the source simply holds (D65). They come through as PostgreSQL's own literal,
and ``coerce_sql_temporal`` decides per destination: kept on the PostgreSQL
family and on text carriers, refused by name on an AD-only carrier.
"""

from __future__ import annotations

import threading
from typing import Any

from connectors.sql_temporal import beyond_python_calendar

_INFINITY_LITERALS = frozenset({"infinity", "-infinity"})
_lock = threading.Lock()
_registered = False


def register_pg_temporal_infinity() -> None:
    """Install the infinity-preserving date/timestamp casters (process-wide, once)."""
    global _registered
    if _registered:
        return
    with _lock:
        if _registered:
            return
        try:
            import psycopg2.extensions as ext
        except ImportError:  # pragma: no cover - driver optional
            return
        for base in (ext.PYDATE, ext.PYDATETIME, ext.PYDATETIMETZ):
            ext.register_type(
                ext.new_type(base.values, f"DF_{base.name}_INFINITY", _keep_infinity(base))
            )
        _registered = True


def _keep_infinity(base: Any) -> Any:
    def cast(value: str | None, cur: Any) -> Any:
        if value in _INFINITY_LITERALS:
            return value
        if value is not None and beyond_python_calendar(value):
            return value
        return base(value, cur)

    return cast
