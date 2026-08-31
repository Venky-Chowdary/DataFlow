"""One owner for how a destination key literal is compared to a typed column.

The destination key census asks "which of these source keys does the
destination already hold?". The source key arrives as whatever the *source*
carried — a JSONL string, a Mongo ``int``, a Decimal from a warehouse — while
the destination column has a declared type of its own. Binding the source
spelling straight into ``WHERE key IN (…)`` makes the engine judge the
comparison, and a strict engine refuses it outright:

    operator does not exist: text = integer          (PostgreSQL, text column)
    invalid input syntax for type integer: "abc"     (PostgreSQL, int column)
    ORA-01722: invalid number                        (Oracle)

Both refusals abort the census, and an aborted census leaves keyed
conservation *unproven* for a route that was otherwise fine — an upsert run
reports no independent split between updates and inserts.

The rule this module owns:

1. A key value is coerced into the destination column's own domain before it is
   bound, so the comparison happens in that domain and the column's index stays
   usable (comparing ``CAST(col AS TEXT)`` would work on every engine and scan
   every row of a 100K destination).
2. A value the column's type **cannot represent** is not an error and not an
   unknown: it is a proven *miss*. No ``integer`` column can hold ``'abc'``, so
   no row keyed ``'abc'`` exists there. Such keys are dropped from the probe and
   counted as dropped, which leaves the hit count exactly right.
3. A column whose type is unknown (introspection unavailable, exotic carrier)
   leaves its value untouched — the previous behaviour — rather than guessing.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

from services.value_serializer import present_cell_text

logger = logging.getLogger(__name__)

_TRUE_TOKENS = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "f", "no", "n", "off"})

INT_DOMAIN = "int"
NUMERIC_DOMAIN = "numeric"
BOOL_DOMAIN = "bool"
TEXT_DOMAIN = "text"
OPAQUE_DOMAIN = ""


def key_domain(physical_type: str) -> str:
    """Which comparison domain a declared column type belongs to.

    Deliberately conservative: temporal, binary, array, json and anything
    unrecognised return :data:`OPAQUE_DOMAIN`, meaning "bind what the source
    carried" — the historical behaviour, which engines handle for those types.
    """
    name = (physical_type or "").strip().lower()
    if not name:
        return OPAQUE_DOMAIN
    base = name.split("(")[0].strip()
    if not base:
        return OPAQUE_DOMAIN
    if "bool" in base or base in {"bit"}:
        return BOOL_DOMAIN
    if base.startswith(("timestamp", "datetime", "date", "time", "interval", "year")):
        return OPAQUE_DOMAIN
    if base.startswith(("bytea", "blob", "binary", "varbinary", "raw", "image")):
        return OPAQUE_DOMAIN
    if base.startswith(("json", "xml", "array", "geo", "vector")) or base.endswith("[]"):
        return OPAQUE_DOMAIN
    if base in {"serial", "bigserial", "smallserial", "rowid", "urowid"}:
        return INT_DOMAIN
    if "int" in base:
        # ``interval`` and ``point`` already returned above; what is left with
        # "int" in the name is an integer carrier on every engine we speak to.
        return INT_DOMAIN
    if base.startswith(("numeric", "decimal", "dec", "number", "money", "smallmoney")):
        return NUMERIC_DOMAIN
    if base.startswith(("float", "double", "real", "binary_float", "binary_double")):
        return NUMERIC_DOMAIN
    if base.startswith(
        ("char", "varchar", "nchar", "nvarchar", "text", "ntext", "clob", "nclob", "string", "enum", "uuid", "uniqueidentifier", "long")
    ):
        return TEXT_DOMAIN
    return OPAQUE_DOMAIN


def coerce_key_value(value: Any, domain: str) -> tuple[Any, bool]:
    """``(bound_value, representable)`` for one key component.

    ``representable`` False means the destination column cannot hold this
    value at all, so the key is a proven miss rather than a probe failure.
    """
    if domain == OPAQUE_DOMAIN:
        return value, True
    if domain == TEXT_DOMAIN:
        text = present_cell_text(value)
        return (text, True) if text is not None else (None, False)
    if domain == BOOL_DOMAIN:
        if isinstance(value, bool):
            return value, True
        if isinstance(value, int) and value in (0, 1):
            return bool(value), True
        token = (present_cell_text(value) or "").strip().lower()
        if token in _TRUE_TOKENS:
            return True, True
        if token in _FALSE_TOKENS:
            return False, True
        return None, False
    number = _as_decimal(value)
    if number is None:
        return None, False
    if domain == INT_DOMAIN:
        if number != number.to_integral_value():
            # 22.4 cannot be the key of an integer-keyed row; it is a miss.
            return None, False
        return int(number), True
    if isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
        return value, True
    return number, True


def _as_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return Decimal(int(value))
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    text = present_cell_text(value)
    if text is None:
        return None
    token = text.strip().replace("_", "")
    if not token:
        return None
    try:
        parsed = Decimal(token)
    except (InvalidOperation, ValueError):
        return None
    if not parsed.is_finite():
        return None
    return parsed


def coerce_key_tuples(
    keys: Sequence[Sequence[Any]],
    cols: Sequence[str],
    col_types: Mapping[str, str] | None,
) -> tuple[list[tuple[Any, ...]], int]:
    """``(comparable_keys, dropped)`` — dropped keys are proven destination misses."""
    domains = [
        key_domain(str((col_types or {}).get(col) or (col_types or {}).get(str(col).lower()) or ""))
        for col in cols
    ]
    if all(d == OPAQUE_DOMAIN for d in domains):
        return [tuple(k) for k in keys], 0
    comparable: list[tuple[Any, ...]] = []
    dropped = 0
    for raw in keys:
        row = tuple(raw)
        if len(row) != len(domains):
            comparable.append(row)
            continue
        bound: list[Any] = []
        keep = True
        for value, domain in zip(row, domains, strict=True):
            coerced, ok = coerce_key_value(value, domain)
            if not ok:
                keep = False
                break
            bound.append(coerced)
        if keep:
            comparable.append(tuple(bound))
        else:
            dropped += 1
    return comparable, dropped


_INFO_SCHEMA_SQL = (
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_name = {ph} AND table_schema = {ph}"
)


def key_column_types_dbapi(
    conn: Any,
    *,
    dialect: str,
    schema: str,
    table_name: str,
    placeholder: str = "%s",
) -> dict[str, str]:
    """Declared types of a destination table's columns, on the census connection.

    Best-effort: an empty mapping means "unknown", and the caller then binds
    what the source carried. Never raises — a census must not fail because the
    catalog could not be read.
    """
    table = (table_name or "").strip()
    if not table:
        return {}
    try:
        if dialect == "sqlite":
            cur = conn.cursor()
            try:
                cur.execute(f'PRAGMA table_info("{table}")')  # nosec B608 - identifier only
                return {
                    str(row[1]).lower(): str(row[2] or "")
                    for row in cur.fetchall()
                    if row and row[1]
                }
            finally:
                cur.close()
        if dialect in {"postgresql", "redshift"}:
            args = (table, (schema or "public").strip() or "public")
        elif dialect == "mysql":
            args = (table, (schema or "").strip() or _mysql_current_schema(conn))
        else:
            return {}
        sql = _INFO_SCHEMA_SQL.format(ph=placeholder)
        cur = conn.cursor()
        try:
            cur.execute(sql, args)
            return {
                str(row[0]).lower(): str(row[1] or "")
                for row in cur.fetchall()
                if row and row[0]
            }
        finally:
            cur.close()
    except Exception as exc:  # pragma: no cover - catalog read is advisory
        logger.debug("key column types unavailable for %s.%s: %s", schema, table, exc)
        return {}


def _mysql_current_schema(conn: Any) -> str:
    cur = conn.cursor()
    try:
        cur.execute("SELECT DATABASE()")
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else ""
    finally:
        cur.close()


def key_column_types_sqlalchemy(
    conn: Any,
    *,
    schema: str,
    table_name: str,
) -> dict[str, str]:
    """Declared column types via SQLAlchemy reflection; ``{}`` when unknown."""
    table = (table_name or "").strip()
    if not table:
        return {}
    try:
        import sqlalchemy as sa

        inspector = sa.inspect(conn)
        cols = inspector.get_columns(table, schema=(schema or None) or None)
        out: dict[str, str] = {}
        for col in cols:
            name = str(col.get("name") or "")
            if not name:
                continue
            try:
                out[name.lower()] = str(col.get("type") or "")
            except Exception:  # pragma: no cover - exotic type repr
                continue
        return out
    except Exception as exc:  # pragma: no cover - reflection is advisory
        logger.debug("SQLAlchemy key column types unavailable for %s: %s", table, exc)
        return {}
