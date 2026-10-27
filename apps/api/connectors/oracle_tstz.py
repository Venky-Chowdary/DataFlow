"""Oracle TIMESTAMP WITH [LOCAL] TIME ZONE reads that keep the offset.

python-oracledb hands back a *naive* ``datetime`` for both zone-bearing
TIMESTAMP flavours: a ``08:31+05:30`` row arrives as bare ``08:31`` and the
instant is silently rewritten. Every read of such a column — the typed batch
readers, the raw ``SELECT *`` fallbacks, the introspection sample and the
Gate-8 read-back — therefore renders it server side with ``TO_CHAR`` and parses
that text back here (D155). Which columns need it is decided from the type
*name* the catalog or driver reports, so a column the SQLAlchemy reflection did
not flag ``timezone=True`` is still caught.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import Any, Final

#: ``TO_CHAR`` mask whose output :func:`parse_oracle_tstz_text` reads back.
ORACLE_TSTZ_READ_FORMAT: Final = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'

#: Spellings of the zone-bearing TIMESTAMP types: the catalog's
#: ``all_tab_columns.data_type`` and python-oracledb's ``DbType`` names.
_TZ_TYPE_TOKENS: Final = (
    "WITH TIME ZONE",
    "WITH LOCAL TIME ZONE",
    "TIMESTAMP_TZ",
    "TIMESTAMP_LTZ",
)


def is_oracle_tz_type_name(type_name: Any) -> bool:
    """True for an Oracle TIMESTAMP WITH [LOCAL] TIME ZONE type name."""
    text = " ".join(str(getattr(type_name, "name", type_name) or "").upper().split())
    return any(tok in text for tok in _TZ_TYPE_TOKENS)


def oracle_tz_to_char(quoted_column: str) -> str:
    """SQL for one zone-bearing column rendered with its offset."""
    return f"TO_CHAR({quoted_column}, '{ORACLE_TSTZ_READ_FORMAT}')"


def oracle_tz_positions(description: Sequence[Any] | None) -> frozenset[int]:
    """Indexes of the zone-bearing columns in a DB-API ``cursor.description``."""
    return frozenset(
        idx
        for idx, entry in enumerate(description or ())
        if len(entry) > 1 and is_oracle_tz_type_name(entry[1])
    )


def oracle_tz_select_list(description: Sequence[Any] | None) -> str:
    """``SELECT`` list for a raw read: ``*`` unless a column needs ``TO_CHAR``.

    ``description`` is the zero-row probe's cursor description; its names are
    the physical (stored) spellings, so quoting them addresses the same columns
    ``*`` would and the labels keep the result keys unchanged.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    tz = oracle_tz_positions(description)
    if not tz:
        return "*"
    parts: list[str] = []
    for idx, entry in enumerate(description or ()):
        quoted = quote_sql_identifier(str(entry[0]), '"')
        parts.append(f"{oracle_tz_to_char(quoted)} AS {quoted}" if idx in tz else quoted)
    return ", ".join(parts)


def parse_oracle_tstz_text(value: Any) -> Any:
    """Aware ``datetime`` from :data:`ORACLE_TSTZ_READ_FORMAT` text.

    Anything else (SQL NULL, a value that is already a ``datetime``) is
    returned unchanged — never guessed into a zone.
    """
    if not isinstance(value, str) or not value.strip():
        return value
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return value
    return parsed if parsed.tzinfo is not None else value


def parse_oracle_tz_row(row: Iterable[Any], positions: frozenset[int]) -> tuple[Any, ...]:
    """``row`` with every ``TO_CHAR``-rendered cell parsed back to an aware instant."""
    if not positions:
        return tuple(row)
    return tuple(
        parse_oracle_tstz_text(cell) if idx in positions else cell
        for idx, cell in enumerate(row)
    )
