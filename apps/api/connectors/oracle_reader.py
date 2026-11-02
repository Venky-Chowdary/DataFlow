"""Oracle batch reader — delegates to generic_sql with oracle dialect."""

from __future__ import annotations

from typing import Any

from connectors.base import ReadBatch
from connectors.generic_sql import read_table_batch as _read
from connectors.generic_sql import read_table_cursor_batch as _read_cursor
from connectors.generic_sql import read_table_scan_batch as _read_scan


def read_table_batch(**kwargs: Any) -> ReadBatch:
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    return _read(**kwargs)


def read_table_cursor_batch(**kwargs: Any) -> ReadBatch:
    """Phase F2 — keyset/seek pagination (composite PK via generic_sql)."""
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    return _read_cursor(**kwargs)


def read_table_scan_batch(**kwargs: Any) -> ReadBatch:
    """One SELECT + fetchmany — no OFFSET pages."""
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    return _read_scan(**kwargs)
