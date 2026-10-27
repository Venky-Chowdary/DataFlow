"""Keep PostgreSQL ``infinity`` / ``-infinity`` dates and timestamps literal.

psycopg2's stock typecasters decode them as ``date.max`` / ``datetime.max`` /
``datetime.min``: every PostgreSQL read handed the pipeline a finite
9999-12-31 or 0001-01-01, the destination stored that calendar date, and a
read-back through the same decode "verified" it (D63). The casters below
return the literal instead; :func:`connectors.sql_temporal.coerce_sql_temporal`
carries it to the PostgreSQL family and refuses it by name everywhere else.
"""

from __future__ import annotations

import threading
from typing import Any

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
        return base(value, cur)

    return cast
