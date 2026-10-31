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
#: A zoneless TIMESTAMP(7..9): python-oracledb hands back six digits, so the
#: seventh (a SQL Server DATETIME2(7) tick DataFlow carried there) is read as
#: text too (connectors.sqlserver_tick).
_TS7 = "ts7"
ORACLE_TS7_READ_FORMAT: Final = 'YYYY-MM-DD"T"HH24:MI:SS.FF7'
#: Session format a TIMESTAMP bind given as text is converted with: the
#: seventh-digit text ``sql_temporal`` binds for a TIMESTAMP(7..9) column.
ORACLE_SESSION_TIMESTAMP_FORMAT: Final = "YYYY-MM-DD HH24:MI:SS.FF9"


def oracle_physical_with_scale(physical: str, catalog: str | None) -> str:
    """The catalog's ``TIMESTAMP(n)`` where SQLAlchemy reflected bare ``TIMESTAMP``.

    SQLAlchemy's Oracle reflection drops the fractional precision, so a column
    DataFlow created as ``TIMESTAMP(7)`` bound six digits on run 2. The
    destination catalog (ALL_TAB_COLUMNS) still states it; only a zoneless
    timestamp of the same family takes its precision from there.
    """
    phys = str(physical or "")
    cat = str(catalog or "").strip()
    if "(" in phys or "timestamp" not in phys.lower() or "(" not in cat:
        return phys
    if oracle_tz_kind(phys) or oracle_tz_kind(cat) or "TIMESTAMP" not in cat.upper():
        return phys
    return cat


def pin_oracle_timestamp_format(dbapi_conn: Any) -> None:
    """``NLS_TIMESTAMP_FORMAT`` for a text TIMESTAMP bind (ISO, nine digits)."""
    cur = dbapi_conn.cursor()
    try:
        cur.execute(
            f"ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '{ORACLE_SESSION_TIMESTAMP_FORMAT}'"
        )
    finally:
        cur.close()


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
    if kind == _TS7:
        return f"TO_CHAR({quoted_column}, '{ORACLE_TS7_READ_FORMAT}')"
    if kind == _LTZ:
        # Oracle's ``||`` treats NULL as '', so an unguarded suffix turns a NULL
        # cell into the text '+00:00' (D186): keep NULL as NULL explicitly.
        return (
            f"CASE WHEN {quoted_column} IS NULL THEN NULL ELSE "
            f"TO_CHAR(SYS_EXTRACT_UTC({quoted_column}), '{ORACLE_LTZ_READ_FORMAT}')"
            " || '+00:00' END"
        )
    return f"TO_CHAR({quoted_column}, '{ORACLE_TSTZ_READ_FORMAT}')"


def oracle_tz_kinds(description: Sequence[Any] | None) -> dict[int, str]:
    """``{index: kind}`` of the zone-bearing columns in a DB-API ``cursor.description``."""
    out: dict[int, str] = {}
    for idx, entry in enumerate(description or ()):
        kind = oracle_tz_kind(entry[1]) if len(entry) > 1 else None
        if kind is None and len(entry) > 5 and _zoneless_beyond_micro(entry):
            kind = _TS7
        if kind:
            out[idx] = kind
    return out


def _zoneless_beyond_micro(entry: Sequence[Any]) -> bool:
    """A driver description of a TIMESTAMP(7..9) column (no zone)."""
    name = str(getattr(entry[1], "name", entry[1]) or "").upper()
    try:
        scale = int(entry[5] or 0)
    except (TypeError, ValueError):
        return False
    return name in {"DB_TYPE_TIMESTAMP", "TIMESTAMP"} and scale > 6


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
    if parsed.tzinfo is None:
        from connectors.sqlserver_tick import tick_from_text, with_tick

        # TIMESTAMP(7..9) read as ORACLE_TS7_READ_FORMAT text: zoneless.
        if len(value.strip()) == 27 and value.strip()[10] == "T":
            return with_tick(parsed, tick_from_text(value))
        return value
    return parsed


def parse_oracle_tz_row(row: Iterable[Any], positions: frozenset[int]) -> tuple[Any, ...]:
    """``row`` with every ``TO_CHAR``-rendered cell parsed back to an aware instant."""
    if not positions:
        return tuple(row)
    return tuple(
        parse_oracle_tstz_text(cell) if idx in positions else cell
        for idx, cell in enumerate(row)
    )
