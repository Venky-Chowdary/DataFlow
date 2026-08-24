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

from typing import Mapping, TypeVar

_V = TypeVar("_V")

__all__ = ["lookup_column", "column_type_or_none"]


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


def column_type_or_none(
    source: Mapping[str, str] | None, name: str | None
) -> str | None:
    """Non-empty declared type for ``name``, case-tolerant, else ``None``."""
    value = lookup_column(source, name)
    text = str(value or "").strip()
    return text or None
