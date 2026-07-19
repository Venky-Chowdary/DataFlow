"""Oracle batch writer — delegates to generic_sql with oracle dialect."""

from __future__ import annotations

from typing import Any

from connectors.generic_sql import WriteResult, write_mapped_rows as _write


def write_mapped_rows(**kwargs: Any) -> WriteResult:
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    result = _write(**kwargs)
    if getattr(result, "driver", None) in (None, "", "sqlalchemy"):
        result.driver = "oracle"
    return result
