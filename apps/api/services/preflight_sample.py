"""Validate sample rows read by the engine's own source reader.

Gate-5/8/9, the locale and profile passes, and the destination collision probe
all judge "the rows about to be written". When a caller posts no sample — the
copilot's sampler failed, or a client never fetched one — every one of them was
judging nothing, and Gate-8 blocked with "load a source sample" on a table
holding rows, without saying why the sample was missing (DEF-B-004 on MySQL).

The reader here is the one Execute uses (``read_source_database``), so Validate
judges what Execute reads rather than a second query planner's idea of it. A
read that fails returns its reason; the caller surfaces it, never a silent
empty list.
"""

from __future__ import annotations

import logging
from collections.abc import Sized
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Mapping

logger = logging.getLogger(__name__)

_SAMPLED_KINDS = frozenset({"database", "cloud"})


@dataclass(frozen=True)
class EngineSample:
    rows: list[dict[str, Any]] = field(default_factory=list)
    #: Why no rows were read; empty when the read ran (an empty table is rows=[]).
    unavailable_reason: str = ""
    attempted: bool = False
    #: The read succeeded and the source returned no row at all (before any
    #: row filter) — a measured empty population, not a missing sample.
    measured_empty: bool = False


def peek_population_empty(population: Any) -> tuple[Any, bool]:
    """``(population, is_empty)`` without consuming a lazy population.

    A stored upload or Execute's in-memory batch is the whole population, so
    an empty one is a measured empty source — a header-only file — rather than
    a sample nobody fetched.
    """
    if population is None:
        return None, False
    if isinstance(population, Sized):
        return population, len(population) == 0
    it = iter(population)
    first = next(it, _END)
    if first is _END:
        return [], True
    return chain((first,), it), False


_END = object()


def engine_sample_rows(
    *,
    source_kind: str,
    source_format: str = "",
    source_connector_id: str = "",
    source_config: Mapping[str, Any] | None = None,
    source_table: str = "",
    limit: int,
    source_filter: Mapping[str, Any] | None = None,
) -> EngineSample:
    """Up to ``limit`` source rows through the Execute reader, or the reason not.

    Callable extracts are peeked by their own planner and file sources carry
    their stored rows, so neither is read here.
    """
    kind = (source_kind or "").strip().lower()
    table = str(source_table or "").strip()
    if kind not in _SAMPLED_KINDS or not table or limit <= 0:
        return EngineSample()
    if not (source_connector_id or source_config):
        return EngineSample()
    from services.procedure_source import is_callable_source

    if is_callable_source(source_config):
        return EngineSample()
    try:
        from src.transfer.adapters import read_source_database
        from src.transfer.models import EndpointConfig
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from transfer.adapters import read_source_database
        from transfer.models import EndpointConfig

    data = dict(source_config or {})
    if source_connector_id:
        data["connector_id"] = source_connector_id
    data.setdefault("table", table)
    data.setdefault("collection", table)
    if source_format:
        data.setdefault("format", source_format)
        data.setdefault("type", source_format)
    try:
        endpoint = EndpointConfig.from_dict(kind, data)
        records, _headers, _schema = read_source_database(
            endpoint, limit=limit, raise_on_truncate=False
        )
    except Exception as exc:  # noqa: BLE001 — the reason is the product output
        logger.warning("Validate source sample read failed for %s: %s", table, exc)
        return EngineSample(
            unavailable_reason=f"source sample read failed: {exc}"[:400],
            attempted=True,
        )
    rows = [dict(r) for r in (records or [])[:limit] if isinstance(r, Mapping)]
    if not rows:
        return EngineSample(attempted=True, measured_empty=True)
    if source_filter:
        from services.row_filter import apply_row_filter

        read = len(rows)
        rows = apply_row_filter(rows, dict(source_filter))
        if not rows:
            return EngineSample(
                unavailable_reason=(
                    f"none of the first {read} source row(s) match the row filter, "
                    "so there is no sample to validate"
                ),
                attempted=True,
            )
    return EngineSample(rows=rows, attempted=True)
