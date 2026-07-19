"""Oracle batch reader — delegates to generic_sql with oracle dialect."""

from __future__ import annotations

from typing import Any

from connectors.base import ReadBatch
from connectors.generic_sql import read_table_batch as _read


def read_table_batch(**kwargs: Any) -> ReadBatch:
    kwargs = dict(kwargs)
    kwargs.setdefault("type", "oracle")
    return _read(**kwargs)
