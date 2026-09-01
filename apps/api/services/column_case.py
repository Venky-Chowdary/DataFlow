"""Canonical case-tolerant column lookup for schema/type dictionaries.

Oracle, Snowflake and DB2 fold unquoted identifiers to upper case in their
catalogs while the row payloads and Studio mappings carry the case the operator
typed. A plain ``schema[name]`` then misses, and the caller falls back to a
default carrier — that is how an Oracle ``NUMBER(12,2)`` source column reached
type invent as ``TEXT`` and materialised a whole table of text columns.

Exact match always wins. A case-insensitive match is only used when it is
unambiguous: PostgreSQL and SQLite permit ``"id"`` and ``"ID"`` in the same
table, and guessing between them would be worse than reporting nothing.
"""

from __future__ import annotations

from typing import Mapping, Sequence, TypeVar

_V = TypeVar("_V")

__all__ = ["lookup_column", "lookup_row_value", "header_index", "column_type_or_none"]


def lookup_column(source: Mapping[str, _V] | None, name: str | None) -> _V | None:
    """Value for ``name`` in ``source``, tolerating identifier case folding."""
    if not source or not name:
        return None
    key = str(name)
    if key in source:
        return source[key]
    folded = key.casefold()
    hits = [v for k, v in source.items() if str(k).casefold() == folded]
    if len(hits) == 1:
        return hits[0]
    return None


def lookup_row_value(
    row: Mapping[str, _V] | None, name: str | None, default: _V | None = None
) -> _V | None:
    """Cell for ``name`` on a row dict, same unambiguous fold as ``lookup_column``.

    Oracle/Snowflake/DB2 readers denormalize keys to ``AMOUNT`` while Map and
    introspect keep operator spelling ``amount``. ``row.get(src)`` then looks
    like an empty cell (EMPTY_VALUE_NOT_NULLABLE) after a correct read.
    """
    if not row or not name:
        return default
    key = str(name)
    if key in row:
        return row[key]
    folded = key.casefold()
    hits = [k for k in row if str(k).casefold() == folded]
    if len(hits) == 1:
        return row[hits[0]]
    return default


def header_index(headers: Sequence[str] | None, name: str | None) -> int | None:
    """Index of ``name`` in ``headers``, same unambiguous fold as ``lookup_column``.

    Dry-run and Map look up ``amount`` against Oracle peek headers ``AMOUNT``.
    An exact miss here reported ``Source column missing`` after a correct read.
    """
    if not headers or not name:
        return None
    key = str(name)
    for i, header in enumerate(headers):
        if str(header) == key:
            return i
    folded = key.casefold()
    hits = [i for i, header in enumerate(headers) if str(header).casefold() == folded]
    if len(hits) == 1:
        return hits[0]
    return None


def column_type_or_none(
    source: Mapping[str, str] | None, name: str | None
) -> str | None:
    """Non-empty declared type for ``name``, case-tolerant, else ``None``."""
    value = lookup_column(source, name)
    text = str(value or "").strip()
    return text or None
