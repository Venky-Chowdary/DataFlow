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

#: ``TO_CHAR`` mask for ``TIMESTAMP WITH TIME ZONE``: it carries a stored offset.
ORACLE_TSTZ_READ_FORMAT: Final = 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'
#: Mask for ``WITH LOCAL TIME ZONE`` after ``SYS_EXTRACT_UTC``. Oracle rejects a
#: TZH/TZM element on an LTZ value (ORA-01821, D167): it stores an instant and
#: no offset, so the instant is rendered in UTC with an explicit ``+00:00``.
ORACLE_LTZ_READ_FORMAT: Final = 'YYYY-MM-DD"T"HH24:MI:SS.FF6'

_TZ = "tz"
_LTZ = "ltz"


def oracle_tz_kind(type_name: Any) -> str | None:
    """``"tz"`` / ``"ltz"`` for an Oracle zone-bearing TIMESTAMP type name, else ``None``.

    Accepts the catalog's ``data_type`` (``TIMESTAMP(6) WITH [LOCAL] TIME
    ZONE``) and python-oracledb's ``DB_TYPE_TIMESTAMP_TZ`` / ``_LTZ``.
    """
    text = " ".join(str(getattr(type_name, "name", type_name) or "").upper().split())
    if "WITH LOCAL TIME ZONE" in text or "TIMESTAMP_LTZ" in text:
        return _LTZ
    if "WITH TIME ZONE" in text or "TIMESTAMP_TZ" in text:
        return _TZ
    return None


def is_oracle_tz_type_name(type_name: Any) -> bool:
    """True for an Oracle TIMESTAMP WITH [LOCAL] TIME ZONE type name."""
    return oracle_tz_kind(type_name) is not None


def oracle_tz_to_char(quoted_column: str, kind: str = _TZ) -> str:
    """SQL rendering one zone-bearing column as ISO text that keeps its instant."""
    if kind == _LTZ:
        return (
            f"TO_CHAR(SYS_EXTRACT_UTC({quoted_column}), '{ORACLE_LTZ_READ_FORMAT}')"
            " || '+00:00'"
        )
    return f"TO_CHAR({quoted_column}, '{ORACLE_TSTZ_READ_FORMAT}')"


def oracle_tz_kinds(description: Sequence[Any] | None) -> dict[int, str]:
    """``{index: kind}`` of the zone-bearing columns in a DB-API ``cursor.description``."""
    out: dict[int, str] = {}
    for idx, entry in enumerate(description or ()):
        kind = oracle_tz_kind(entry[1]) if len(entry) > 1 else None
        if kind:
            out[idx] = kind
    return out


def oracle_tz_positions(description: Sequence[Any] | None) -> frozenset[int]:
    """Indexes of the zone-bearing columns in a DB-API ``cursor.description``."""
    return frozenset(oracle_tz_kinds(description))


def oracle_tz_select_list(description: Sequence[Any] | None) -> str:
    """``SELECT`` list for a raw read: ``*`` unless a column needs ``TO_CHAR``.

    ``description`` is the zero-row probe's cursor description; its names are
    the physical (stored) spellings, so quoting them addresses the same columns
    ``*`` would and the labels keep the result keys unchanged.
    """
    from connectors.sql_identifiers import quote_sql_identifier

    kinds = oracle_tz_kinds(description)
    if not kinds:
        return "*"
    parts: list[str] = []
    for idx, entry in enumerate(description or ()):
        quoted = quote_sql_identifier(str(entry[0]), '"')
        parts.append(
            f"{oracle_tz_to_char(quoted, kinds[idx])} AS {quoted}"
            if idx in kinds
            else quoted
        )
    return ", ".join(parts)


def tz_projected_query(
    probe_description: Any, query: str
) -> tuple[str, frozenset[int]]:
    """Wrap an arbitrary Oracle ``SELECT`` so its zone-bearing columns keep their instant.

    ``probe_description(sql)`` returns the DB-API ``cursor.description`` of a
    zero-row run of ``sql``. Returns ``(query, ∅)`` unchanged when no column is
    zone-bearing or the result names are not unique (a projection by name would
    be ambiguous); otherwise the outer select list from
    :func:`oracle_tz_select_list` over the original as an inline view, plus the
    positions :func:`parse_oracle_tz_row` should parse back.
    """
    description = probe_description(f"SELECT * FROM ({query}) WHERE 1=0")
    kinds = oracle_tz_kinds(description)
    names = [str(entry[0]) for entry in description or ()]
    if not kinds or len(set(names)) != len(names):
        return query, frozenset()
    return (
        f"SELECT {oracle_tz_select_list(description)} FROM ({query})",
        frozenset(kinds),
    )


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
